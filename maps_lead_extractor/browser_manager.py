from __future__ import annotations

import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import types
from typing import Iterable

from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .config import ScraperConfig, USER_AGENTS

_UC_INIT_LOCK = threading.Lock()
_CHROME_VERSION_LOCK = threading.Lock()
_CACHED_CHROME_MAJOR_VERSION: int | None = None


def _install_distutils_shim() -> None:
    """Install a minimal distutils.version shim for Python 3.12+."""
    if "distutils.version" in sys.modules:
        return

    class _LooseVersion:
        def __init__(self, value: str) -> None:
            self.vstring = str(value)
            self.version = self._parse(self.vstring)

        @staticmethod
        def _parse(vstring: str) -> list[str | int]:
            parts = re.split(r"([0-9]+)", vstring)
            out: list[str | int] = []
            for item in parts:
                if not item or item == ".":
                    continue
                out.append(int(item) if item.isdigit() else item.lower())
            return out

        def _cmp(self, other: object) -> int:
            if isinstance(other, _LooseVersion):
                rhs = other.version
            else:
                rhs = self._parse(str(other))
            lhs = self.version
            if lhs == rhs:
                return 0
            return -1 if lhs < rhs else 1

        def __lt__(self, other: object) -> bool:
            return self._cmp(other) < 0

        def __le__(self, other: object) -> bool:
            return self._cmp(other) <= 0

        def __eq__(self, other: object) -> bool:
            return self._cmp(other) == 0

        def __gt__(self, other: object) -> bool:
            return self._cmp(other) > 0

        def __ge__(self, other: object) -> bool:
            return self._cmp(other) >= 0

        def __repr__(self) -> str:
            return f"LooseVersion('{self.vstring}')"

    distutils_module = sys.modules.get("distutils") or types.ModuleType("distutils")
    version_module = types.ModuleType("distutils.version")
    setattr(version_module, "LooseVersion", _LooseVersion)
    setattr(distutils_module, "version", version_module)
    sys.modules["distutils"] = distutils_module
    sys.modules["distutils.version"] = version_module


def _load_uc():
    try:
        import undetected_chromedriver as uc  # type: ignore
        return uc
    except ModuleNotFoundError as exc:
        if exc.name != "distutils":
            raise
        _install_distutils_shim()
        import undetected_chromedriver as uc  # type: ignore
        return uc


class BrowserManager:
    def __init__(self, config: ScraperConfig) -> None:
        self.config = config

    def create_driver(self) -> WebDriver:
        delay = 1.0
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                return self._init_driver()
            except Exception as exc:  # noqa: BLE001 - webdriver throws varied errors
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                time.sleep(delay)
                delay *= 2
        raise RuntimeError(f"Failed to launch browser after retries: {last_error}") from last_error

    def _init_driver(self) -> WebDriver:
        uc = _load_uc()
        ua = random.choice(USER_AGENTS)
        width = random.randint(1280, 1600)
        height = random.randint(768, 1000)

        options = uc.ChromeOptions()
        options.add_argument(f"--user-agent={ua}")
        options.add_argument(f"--window-size={width},{height}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--lang=en-IN")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-notifications")
        if self.config.headless:
            options.add_argument("--headless=new")

        launch_kwargs: dict[str, object] = {
            "options": options,
            "use_subprocess": True,
        }
        detected_major = self._get_chrome_major_version()
        if detected_major:
            launch_kwargs["version_main"] = detected_major

        # UC patching is not thread-safe on Windows; serialize driver bootstrap.
        with _UC_INIT_LOCK:
            driver = uc.Chrome(**launch_kwargs)
        driver.set_page_load_timeout(self.config.timeout_sec)
        return driver

    def safe_get(self, driver: WebDriver, url: str) -> None:
        delay = 1.0
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                driver.get(url)
                return
            except (WebDriverException, TimeoutException) as exc:
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                time.sleep(delay)
                delay *= 2
        raise RuntimeError(f"Failed to navigate to URL: {url}") from last_error

    def handle_cookie_consent(self, driver: WebDriver) -> None:
        consent_selectors = [
            (By.XPATH, "//button[.//span[contains(., 'Accept all')]]"),
            (By.XPATH, "//button[contains(., 'I agree')]"),
            (By.XPATH, "//button[contains(., 'Accept')]"),
        ]
        for locator in consent_selectors:
            self._try_click(driver, [locator], timeout=4)

    @staticmethod
    def _try_click(
        driver: WebDriver,
        locators: Iterable[tuple[By, str]],
        timeout: int = 6,
    ) -> bool:
        for by, value in locators:
            try:
                element = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((by, value))
                )
                element.click()
                return True
            except Exception:  # noqa: BLE001
                continue
        return False

    @staticmethod
    def _get_chrome_major_version() -> int | None:
        global _CACHED_CHROME_MAJOR_VERSION
        with _CHROME_VERSION_LOCK:
            if _CACHED_CHROME_MAJOR_VERSION is not None:
                return _CACHED_CHROME_MAJOR_VERSION

            reg_version = BrowserManager._extract_major_version_from_windows_registry()
            if reg_version:
                _CACHED_CHROME_MAJOR_VERSION = reg_version
                return reg_version

            for binary in BrowserManager._candidate_chrome_binaries():
                version = BrowserManager._extract_major_version(binary)
                if version:
                    _CACHED_CHROME_MAJOR_VERSION = version
                    return version
            return None

    @staticmethod
    def _candidate_chrome_binaries() -> list[str]:
        local = os.environ.get("LOCALAPPDATA", "")
        program_files = os.environ.get("PROGRAMFILES", "")
        program_files_x86 = os.environ.get("PROGRAMFILES(X86)", "")
        paths = [
            shutil.which("chrome"),
            os.path.join(local, "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(program_files, "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(program_files_x86, "Google", "Chrome", "Application", "chrome.exe"),
        ]
        return [p for p in paths if p]

    @staticmethod
    def _extract_major_version(binary_path: str) -> int | None:
        try:
            completed = subprocess.run(
                [binary_path, "--version"],
                capture_output=True,
                text=True,
                timeout=4,
                check=False,
            )
        except Exception:  # noqa: BLE001
            return None

        output = f"{completed.stdout} {completed.stderr}".strip()
        match = re.search(r"(\d+)\.\d+\.\d+\.\d+", output)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    @staticmethod
    def _extract_major_version_from_windows_registry() -> int | None:
        if os.name != "nt":
            return None
        try:
            import winreg
        except Exception:  # noqa: BLE001
            return None

        reg_paths = [
            (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"Software\Google\Chrome\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\Google\Chrome\BLBeacon"),
        ]
        for hive, key_path in reg_paths:
            try:
                with winreg.OpenKey(hive, key_path) as key:
                    version, _ = winreg.QueryValueEx(key, "version")
            except Exception:  # noqa: BLE001
                continue
            if not version:
                continue
            match = re.search(r"(\d+)\.\d+\.\d+\.\d+", str(version))
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        return None

