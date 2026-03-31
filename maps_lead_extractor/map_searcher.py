from __future__ import annotations

import random
import re
import time
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .browser_manager import BrowserManager
from .config import ScraperConfig


class MapSearcher:
    MAPS_URL = "https://www.google.com/maps"

    def __init__(self, driver: WebDriver, browser_manager: BrowserManager, config: ScraperConfig) -> None:
        self.driver = driver
        self.browser_manager = browser_manager
        self.config = config

    def collect_listing_urls(self, query: str) -> list[str]:
        search_url = f"{self.MAPS_URL}/search/{quote_plus(query)}"
        self.browser_manager.safe_get(self.driver, search_url)
        self.browser_manager.handle_cookie_consent(self.driver)
        self._search_with_input(query)
        if not self._wait_for_results_feed():
            # Some queries open a direct place page instead of a multi-result feed.
            current_url = self.driver.current_url
            if "/maps/place/" in current_url:
                return [current_url.split("&", 1)[0]]
            return []
        urls = self._scroll_results_until_end()
        if self.config.max_listings_per_query > 0:
            return urls[: self.config.max_listings_per_query]
        return urls

    def _search_with_input(self, query: str) -> None:
        try:
            search_box = WebDriverWait(self.driver, self.config.timeout_sec).until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.ENTER)
            time.sleep(random.uniform(self.config.min_sleep_sec, self.config.max_sleep_sec))
        except TimeoutException:
            # URL-based search already contains the query; continue gracefully.
            return

    def _wait_for_results_feed(self) -> bool:
        try:
            WebDriverWait(self.driver, self.config.timeout_sec).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='feed']"))
            )
            return True
        except TimeoutException:
            return False

    def _scroll_results_until_end(self) -> list[str]:
        feed = self.driver.find_element(By.XPATH, "//div[@role='feed']")
        discovered_urls: set[str] = set()
        stable_rounds = 0
        max_stable_rounds = 16

        while stable_rounds < max_stable_rounds:
            before_count = len(discovered_urls)
            discovered_urls.update(self._extract_listing_urls_from_feed(feed))
            discovered_urls.update(self._extract_listing_urls_from_page_source())
            after_count = len(discovered_urls)

            if after_count == before_count:
                stable_rounds += 1
            else:
                stable_rounds = 0

            self.driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight * 0.9",
                feed,
            )
            try:
                feed.send_keys(Keys.END)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(random.uniform(self.config.scroll_sleep_min_sec, self.config.scroll_sleep_max_sec))

            page_text = self.driver.page_source.lower()
            if "you've reached the end of the list" in page_text or "end of results" in page_text:
                break
            if self.config.max_listings_per_query > 0 and len(discovered_urls) >= self.config.max_listings_per_query:
                break

        return sorted(discovered_urls)

    def _extract_listing_urls_from_feed(self, feed) -> set[str]:
        urls: set[str] = set()
        try:
            anchors = feed.find_elements(By.XPATH, ".//a[contains(@href, '/maps/place')]")
        except NoSuchElementException:
            anchors = []

        for anchor in anchors:
            href = (anchor.get_attribute("href") or "").strip()
            normalized = self._normalize_maps_place_url(href)
            if normalized:
                urls.add(normalized)
        return urls

    def _extract_listing_urls_from_page_source(self) -> set[str]:
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        urls: set[str] = set()
        for anchor in soup.select("a[href*='/maps/place']"):
            href = anchor.get("href", "").strip()
            normalized = self._normalize_maps_place_url(href)
            if normalized:
                urls.add(normalized)
        return urls

    @staticmethod
    def _normalize_maps_place_url(href: str) -> str:
        if not href:
            return ""
        if href.startswith("/"):
            href = f"https://www.google.com{href}"
        if "/maps/place" not in href:
            return ""
        href = href.split("&", 1)[0]
        href = re.sub(r"(?<!:)/{2,}", "/", href)
        return href

