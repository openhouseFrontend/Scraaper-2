from __future__ import annotations

import argparse
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException

from maps_lead_extractor.browser_manager import BrowserManager
from maps_lead_extractor.config import DEFAULT_QUERIES, ScraperConfig
from maps_lead_extractor.data_pipeline import DataPipeline
from maps_lead_extractor.listing_parser import ListingParser
from maps_lead_extractor.map_searcher import MapSearcher
from maps_lead_extractor.models import LeadRecord


logger = logging.getLogger("gmaps_lead_extractor")
console = Console()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Production-grade Google Maps Lead Extractor (Pure Python + Selenium)."
    )
    parser.add_argument("--queries", nargs="+", help="One or more search queries.")
    parser.add_argument("--query-file", type=Path, help="Path to query text file (one query per line).")
    parser.add_argument("--max-workers", type=int, default=3, help="Parallel query workers.")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode.")
    parser.add_argument("--output-dir", type=Path, default=Path("output"), help="Output directory.")
    parser.add_argument(
        "--listing-retry-count",
        type=int,
        default=2,
        help="Retries for a failed listing before skipping.",
    )
    parser.add_argument(
        "--query-bootstrap-retries",
        type=int,
        default=2,
        help="Retries to recover query startup when navigation/session fails.",
    )
    parser.add_argument(
        "--rotate-driver-every",
        type=int,
        default=35,
        help="Recreate browser after N parsed listings to reduce stale sessions.",
    )
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
    )


def load_queries(args: argparse.Namespace) -> list[str]:
    if args.queries:
        return [q.strip() for q in args.queries if q.strip()]

    if args.query_file:
        if not args.query_file.exists():
            raise FileNotFoundError(f"Query file not found: {args.query_file}")
        lines = args.query_file.read_text(encoding="utf-8").splitlines()
        queries = [line.strip() for line in lines if line.strip()]
        if queries:
            return queries

    entered = input(
        "Enter queries separated by ';' (or press Enter to use default Delhi/NCR queries): "
    ).strip()
    if entered:
        queries = [item.strip() for item in entered.split(";") if item.strip()]
        if queries:
            return queries

    return DEFAULT_QUERIES.copy()


def _is_session_or_driver_error(exc: Exception) -> bool:
    if isinstance(exc, (InvalidSessionIdException, WebDriverException)):
        return True
    message = str(exc).lower()
    indicators = (
        "invalid session id",
        "session deleted",
        "disconnected",
        "chrome not reachable",
        "target window already closed",
        "no such window",
        "timed out receiving message from renderer",
    )
    return any(flag in message for flag in indicators)


def _build_components(
    browser_manager: BrowserManager,
    config: ScraperConfig,
):
    driver = browser_manager.create_driver()
    searcher = MapSearcher(driver=driver, browser_manager=browser_manager, config=config)
    parser = ListingParser(driver=driver, config=config)
    return driver, searcher, parser


def _safe_quit(driver) -> None:
    try:
        driver.quit()
    except Exception:  # noqa: BLE001
        pass


def scrape_single_query(query: str, config: ScraperConfig) -> list[LeadRecord]:
    logger.info("Starting query: %s", query)
    browser_manager = BrowserManager(config)
    records: list[LeadRecord] = []
    driver = None
    parser = None
    listing_urls: list[str] = []

    try:
        for attempt in range(config.query_bootstrap_retries + 1):
            try:
                if driver is not None:
                    _safe_quit(driver)
                driver, searcher, parser = _build_components(browser_manager, config)
                listing_urls = searcher.collect_listing_urls(query)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt >= config.query_bootstrap_retries:
                    raise
                logger.warning(
                    "Query bootstrap failed for '%s' (attempt %d/%d): %s",
                    query,
                    attempt + 1,
                    config.query_bootstrap_retries + 1,
                    exc,
                )
                if not _is_session_or_driver_error(exc):
                    raise
        logger.info("Query '%s' produced %d listing URLs", query, len(listing_urls))

        parsed_since_rotation = 0
        for index, url in enumerate(listing_urls, start=1):
            if (
                config.rotate_driver_every > 0
                and parsed_since_rotation >= config.rotate_driver_every
            ):
                _safe_quit(driver)
                driver, _, parser = _build_components(browser_manager, config)
                parsed_since_rotation = 0

            listing_completed = False
            for attempt in range(config.listing_retry_count + 1):
                try:
                    record = parser.parse_listing(url, query=query)
                    records.append(record)
                    parsed_since_rotation += 1
                    listing_completed = True
                    break
                except Exception as exc:  # noqa: BLE001
                    recoverable = _is_session_or_driver_error(exc)
                    if recoverable and attempt < config.listing_retry_count:
                        logger.warning(
                            "Recovering driver for query '%s' listing %d/%d (attempt %d): %s",
                            query,
                            index,
                            len(listing_urls),
                            attempt + 1,
                            exc,
                        )
                        _safe_quit(driver)
                        driver, _, parser = _build_components(browser_manager, config)
                        continue
                    logger.warning(
                        "Failed listing parse for query '%s' URL '%s': %s",
                        query,
                        url,
                        exc,
                    )
                    break

            if index % 25 == 0 or (not listing_completed and index % 10 == 0):
                logger.info("Query '%s': parsed %d/%d", query, index, len(listing_urls))
    finally:
        if driver is not None:
            _safe_quit(driver)

    logger.info("Completed query: %s, records=%d", query, len(records))
    return records


async def scrape_queries_parallel(queries: Iterable[str], config: ScraperConfig) -> list[LeadRecord]:
    query_list = list(queries)
    all_records: list[LeadRecord] = []
    failed_queries: list[str] = []

    async def run_query(query: str, pool: ThreadPoolExecutor) -> tuple[str, list[LeadRecord], Exception | None]:
        loop = asyncio.get_running_loop()
        try:
            records = await loop.run_in_executor(pool, scrape_single_query, query, config)
            return query, records, None
        except Exception as exc:  # noqa: BLE001
            return query, [], exc

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    )

    with progress:
        task_id = progress.add_task("Running queries", total=len(query_list))
        with ThreadPoolExecutor(max_workers=config.max_workers) as pool:
            tasks = [asyncio.create_task(run_query(query, pool)) for query in query_list]
            for task in asyncio.as_completed(tasks):
                query, records, error = await task
                if error is None:
                    all_records.extend(records)
                else:
                    failed_queries.append(query)
                    logger.error("Query failed: '%s' :: %s", query, error)
                progress.advance(task_id, 1)

    if failed_queries:
        logger.warning("Failed queries: %d/%d", len(failed_queries), len(query_list))
        for query in failed_queries:
            logger.warning(" - %s", query)

    if failed_queries and len(failed_queries) == len(query_list):
        raise RuntimeError("All queries failed. Check Chrome/driver compatibility and retry.")

    return all_records


def print_results_table(total_raw: int, total_clean: int, csv_path: Path, json_path: Path) -> None:
    table = Table(title="Google Maps Lead Extraction Complete")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Raw records", str(total_raw))
    table.add_row("Deduplicated records", str(total_clean))
    table.add_row("CSV", str(csv_path))
    table.add_row("JSON", str(json_path))
    console.print(table)


async def async_main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    queries = load_queries(args)
    if not queries:
        raise RuntimeError("No valid queries available.")

    config = ScraperConfig(
        headless=args.headless,
        max_workers=max(1, args.max_workers),
        listing_retry_count=max(0, args.listing_retry_count),
        query_bootstrap_retries=max(0, args.query_bootstrap_retries),
        rotate_driver_every=max(0, args.rotate_driver_every),
        output_dir=args.output_dir,
    )
    pipeline = DataPipeline(output_dir=config.output_dir)

    console.print(f"[bold]Queries loaded:[/bold] {len(queries)}")
    for q in queries:
        console.print(f" - {q}")

    records = await scrape_queries_parallel(queries=queries, config=config)
    dataframe = pipeline.to_dataframe(records)
    csv_path, json_path = pipeline.export(dataframe)
    print_results_table(len(records), len(dataframe), csv_path, json_path)
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        return 130
    except Exception as exc:  # noqa: BLE001
        logger.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

