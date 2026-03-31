from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_QUERIES = [
    "real estate agent Delhi",
    "property dealer South Delhi",
    "flat broker Noida",
    "residential property agent Gurgaon",
    "real estate consultant Dwarka",
    "luxury apartment broker Delhi NCR",
    "commercial property agent Connaught Place",
]


USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
]


@dataclass(slots=True)
class ScraperConfig:
    headless: bool = False
    timeout_sec: int = 20
    max_retries: int = 3
    min_sleep_sec: float = 0.75
    max_sleep_sec: float = 1.75
    scroll_sleep_min_sec: float = 1.1
    scroll_sleep_max_sec: float = 2.0
    max_workers: int = 3
    listing_retry_count: int = 2
    query_bootstrap_retries: int = 2
    rotate_driver_every: int = 35
    max_listings_per_query: int = 0
    fast_mode: bool = False
    output_dir: Path = Path("output")

