from __future__ import annotations

import re
from typing import Iterable

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .config import ScraperConfig
from .models import LeadRecord


class ListingParser:
    def __init__(self, driver: WebDriver, config: ScraperConfig) -> None:
        self.driver = driver
        self.config = config

    def parse_listing(self, listing_url: str, query: str) -> LeadRecord:
        record = LeadRecord.for_query(query=query, listing_url=listing_url)
        self.driver.get(listing_url)
        self._wait_for_listing_loaded()

        record.business_name = self._clean_text(
            self._text_from_xpaths(
            [
                "//h1[contains(@class, 'DUwDvf')]",
                "//h1",
            ]
            )
        )
        record.category = self._clean_text(
            self._text_from_xpaths(
            [
                "//button[contains(@jsaction, 'pane.rating.category')]",
                "//div[contains(@class,'fontBodyMedium')]//button[contains(@aria-label, 'Category')]",
                "//span[contains(@class, 'DkEaL')]",
            ]
            )
        )
        record.rating = self._extract_rating()
        record.review_count = self._extract_review_count()
        record.address = self._clean_address_or_plus_code(
            self._text_from_xpaths(
            [
                "//button[@data-item-id='address']",
                "//button[contains(@data-item-id, 'address')]",
            ]
            )
        )
        record.phone = self._clean_phone_text(
            self._text_from_xpaths(
            [
                "//button[contains(@data-item-id, 'phone')]",
                "//a[starts-with(@href, 'tel:')]",
            ]
            )
        )
        record.website = self._attribute_from_xpaths(
            [
                "//a[@data-item-id='authority']",
                "//a[@data-item-id='authority']/@href",
            ],
            "href",
        )
        record.website = self._clean_website(record.website)
        record.plus_code = self._clean_address_or_plus_code(
            self._text_from_xpaths(
            [
                "//button[@data-item-id='oloc']",
                "//button[contains(@aria-label, 'Plus code')]",
            ]
            )
        )
        record.hours = self._extract_hours()
        record.services = self._extract_services()
        record.category = self._normalize_category(record.category)
        record.locality, record.city = self._split_address(record.address)
        return record

    def _wait_for_listing_loaded(self) -> None:
        WebDriverWait(self.driver, self.config.timeout_sec).until(
            EC.presence_of_element_located(
                (By.XPATH, "//h1 | //div[@role='main']")
            )
        )

    def _extract_rating(self) -> str:
        aria = self._attribute_from_xpaths(
            [
                "//span[@role='img' and contains(@aria-label, 'stars')]",
            ],
            "aria-label",
        )
        match = re.search(r"(\d+(?:\.\d+)?)", aria)
        return match.group(1) if match else ""

    def _extract_review_count(self) -> str:
        text = self._text_from_xpaths(
            [
                "//button[contains(@aria-label, 'reviews')]",
                "//span[contains(text(), 'reviews')]",
            ]
        )
        match = re.search(r"([\d,]+)", text)
        return match.group(1).replace(",", "") if match else ""

    def _extract_hours(self) -> str:
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        hours_rows: list[str] = []

        for row in soup.select("table tr"):
            parts = [segment.get_text(" ", strip=True) for segment in row.select("td, th")]
            if len(parts) >= 2 and parts[0] and parts[1]:
                hours_rows.append(f"{self._clean_text(parts[0])}: {self._clean_text(parts[1])}")
        if hours_rows:
            return " | ".join(hours_rows)

        # Fallback for inline day/hour blocks.
        for block in soup.select("div[aria-label*='Hours'] div"):
            text = block.get_text(" ", strip=True)
            if any(day in text for day in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")):
                hours_rows.append(self._clean_text(text))
        return " | ".join(dict.fromkeys(hours_rows))

    def _extract_services(self) -> str:
        # Keep this conservative to avoid UI noise entering lead data.
        known = {
            "on-site services",
            "online appointments",
            "wheelchair-accessible entrance",
            "wheelchair-accessible parking lot",
        }
        collected: list[str] = []
        for text in self._texts_from_xpaths(
            [
                "//div[@role='main']//span",
            ]
        ):
            clean = self._clean_text(text).lower()
            if clean in known:
                collected.append(clean)
        return " | ".join(dict.fromkeys(collected))

    def _text_from_xpaths(self, xpaths: Iterable[str]) -> str:
        for xpath in xpaths:
            try:
                element = self.driver.find_element(By.XPATH, xpath)
                text = (element.text or "").strip()
                if text:
                    return text
            except Exception:  # noqa: BLE001
                continue
        return ""

    def _attribute_from_xpaths(self, xpaths: Iterable[str], attribute: str) -> str:
        for xpath in xpaths:
            try:
                element = self.driver.find_element(By.XPATH, xpath)
                value = (element.get_attribute(attribute) or "").strip()
                if value:
                    return value
            except Exception:  # noqa: BLE001
                continue
        return ""

    def _texts_from_xpaths(self, xpaths: Iterable[str]) -> list[str]:
        values: list[str] = []
        for xpath in xpaths:
            try:
                elements = self.driver.find_elements(By.XPATH, xpath)
            except Exception:  # noqa: BLE001
                continue
            for element in elements:
                text = (element.text or "").strip()
                if text:
                    values.append(text)
        return values

    @staticmethod
    def _clean_text(value: str) -> str:
        text = str(value or "").replace("\n", " ").replace("\r", " ").strip()
        text = re.sub(r"[\u200e\u200f\u202a-\u202e]", "", text)
        text = re.sub(r"[^\w\s\-\.,:/&\(\)\+\|@]", " ", text, flags=re.UNICODE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _clean_address_or_plus_code(self, value: str) -> str:
        cleaned = self._clean_text(value)
        cleaned = re.sub(r"^(Address|Plus code)\s*:?\s*", "", cleaned, flags=re.I)
        return cleaned

    @staticmethod
    def _clean_phone_text(value: str) -> str:
        text = str(value or "").strip()
        match = re.search(r"(\+?[\d\-\s\(\)]{7,})", text)
        return match.group(1).strip() if match else ""

    @staticmethod
    def _clean_website(url: str) -> str:
        if not url:
            return ""
        lowered = url.lower()
        blocked = (
            "google.com/contributionpolicy",
            "support.google.com",
            "google.com/maps",
        )
        if any(item in lowered for item in blocked):
            return ""
        return url

    @staticmethod
    def _normalize_category(category: str) -> str:
        if not category:
            return ""
        if category.lower().strip().startswith("add website"):
            return ""
        return category

    @staticmethod
    def _split_address(address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        pieces = [part.strip() for part in address.split(",") if part.strip()]
        if len(pieces) >= 2:
            return pieces[-2], pieces[-1]
        return pieces[0], pieces[0]

