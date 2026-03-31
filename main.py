from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import threading
from datetime import datetime, timezone
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


class CheckpointManager:
    def __init__(self, output_dir: Path, queries: list[str], fresh_run: bool = False) -> None:
        self._lock = threading.Lock()
        self.queries = queries
        joined = "\n".join(q.strip() for q in queries if q.strip())
        self.fingerprint = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]
        self.root = output_dir / "checkpoints" / self.fingerprint
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / "state.json"
        self.records_path = self.root / "records.jsonl"
        self.snapshot_csv = self.root / "snapshot.csv"
        self.snapshot_json = self.root / "snapshot.json"
        self.state = self._load_state(fresh_run=fresh_run)

    def _load_state(self, fresh_run: bool) -> dict:
        if fresh_run:
            if self.records_path.exists():
                self.records_path.unlink()
            state = self._new_state()
            self._write_state(state)
            return state

        if self.state_path.exists():
            try:
                raw = json.loads(self.state_path.read_text(encoding="utf-8"))
                if raw.get("fingerprint") == self.fingerprint:
                    raw.setdefault("completed_queries", [])
                    raw.setdefault("failed_queries", {})
                    return raw
            except Exception:  # noqa: BLE001
                pass

        state = self._new_state()
        self._write_state(state)
        return state

    def _new_state(self) -> dict:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return {
            "fingerprint": self.fingerprint,
            "created_at": now,
            "updated_at": now,
            "total_queries": len(self.queries),
            "completed_queries": [],
            "failed_queries": {},
        }

    def _write_state(self, state: dict) -> None:
        state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    def pending_queries(self) -> list[str]:
        done = set(self.state.get("completed_queries", []))
        return [q for q in self.queries if q not in done]

    def mark_query_completed(self, query: str) -> None:
        with self._lock:
            completed = set(self.state.get("completed_queries", []))
            if query not in completed:
                self.state["completed_queries"].append(query)
                self._write_state(self.state)

    def mark_query_failed(self, query: str, error: Exception) -> None:
        with self._lock:
            failed = self.state.setdefault("failed_queries", {})
            failed[query] = str(error)
            self._write_state(self.state)

    def append_record(self, record: LeadRecord) -> None:
        payload = record.to_dict()
        with self._lock:
            with self.records_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def load_records(self) -> list[LeadRecord]:
        if not self.records_path.exists():
            return []
        records: list[LeadRecord] = []
        with self.records_path.open("r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                    records.append(LeadRecord(**payload))
                except Exception:  # noqa: BLE001
                    continue
        return records

    def write_snapshot(self, pipeline: DataPipeline) -> tuple[int, int]:
        records = self.load_records()
        frame = pipeline.to_dataframe(records)
        frame.to_csv(self.snapshot_csv, index=False, encoding="utf-8-sig")
        self.snapshot_json.write_text(
            json.dumps(frame.to_dict(orient="records"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return len(records), len(frame)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Production-grade Google Maps Lead Extractor (Pure Python + Selenium)."
    )
    parser.add_argument("--queries", nargs="+", help="One or more search queries.")
    parser.add_argument("--query-file", type=Path, help="Path to query text file (one query per line).")
    parser.add_argument("--max-workers", type=int, default=3, help="Parallel query workers.")
    parser.add_argument("--timeout-sec", type=int, default=20, help="Selenium wait timeout in seconds.")
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
    parser.add_argument(
        "--max-listings-per-query",
        type=int,
        default=0,
        help="Hard cap listings per query (0 = no cap).",
    )
    parser.add_argument(
        "--snapshot-every",
        type=int,
        default=5,
        help="Write checkpoint snapshot every N completed queries.",
    )
    parser.add_argument(
        "--fast-mode",
        action="store_true",
        help="Faster scrape: shorter sleeps, optional field skips, and quicker rotations.",
    )
    parser.add_argument(
        "--fresh-run",
        action="store_true",
        help="Ignore previous checkpoint for the same query set and start fresh.",
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


def scrape_single_query(query: str, config: ScraperConfig, checkpoint: CheckpointManager | None = None) -> list[LeadRecord]:
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
                    if checkpoint is not None:
                        checkpoint.append_record(record)
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


async def scrape_queries_parallel(
    queries: Iterable[str],
    config: ScraperConfig,
    checkpoint: CheckpointManager,
    pipeline: DataPipeline,
    snapshot_every: int = 5,
) -> list[LeadRecord]:
    query_list = list(queries)
    all_records: list[LeadRecord] = []
    failed_queries: list[str] = []

    async def run_query(query: str, pool: ThreadPoolExecutor) -> tuple[str, list[LeadRecord], Exception | None]:
        loop = asyncio.get_running_loop()
        try:
            records = await loop.run_in_executor(pool, scrape_single_query, query, config, checkpoint)
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
        completed_count = 0
        with ThreadPoolExecutor(max_workers=config.max_workers) as pool:
            tasks = [asyncio.create_task(run_query(query, pool)) for query in query_list]
            for task in asyncio.as_completed(tasks):
                query, records, error = await task
                if error is None:
                    all_records.extend(records)
                    checkpoint.mark_query_completed(query)
                    completed_count += 1
                    if snapshot_every <= 1 or completed_count % snapshot_every == 0:
                        raw, clean = checkpoint.write_snapshot(pipeline)
                        logger.info(
                            "Checkpoint snapshot: completed=%d/%d raw=%d clean=%d",
                            len(checkpoint.state.get("completed_queries", [])),
                            len(query_list),
                            raw,
                            clean,
                        )
                else:
                    failed_queries.append(query)
                    checkpoint.mark_query_failed(query, error)
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

    min_sleep = 0.75
    max_sleep = 1.75
    scroll_min = 1.1
    scroll_max = 2.0
    rotate_every = max(0, args.rotate_driver_every)
    listing_retry_count = max(0, args.listing_retry_count)
    query_bootstrap_retries = max(0, args.query_bootstrap_retries)
    if args.fast_mode:
        min_sleep = 0.2
        max_sleep = 0.7
        scroll_min = 0.35
        scroll_max = 0.9
        if rotate_every <= 0:
            rotate_every = 25
        listing_retry_count = min(listing_retry_count, 1)
        query_bootstrap_retries = min(query_bootstrap_retries, 1)

    config = ScraperConfig(
        headless=args.headless,
        timeout_sec=max(5, args.timeout_sec),
        min_sleep_sec=min_sleep,
        max_sleep_sec=max_sleep,
        scroll_sleep_min_sec=scroll_min,
        scroll_sleep_max_sec=scroll_max,
        max_workers=max(1, args.max_workers),
        listing_retry_count=listing_retry_count,
        query_bootstrap_retries=query_bootstrap_retries,
        rotate_driver_every=rotate_every,
        max_listings_per_query=max(0, args.max_listings_per_query),
        fast_mode=args.fast_mode,
        output_dir=args.output_dir,
    )
    pipeline = DataPipeline(output_dir=config.output_dir)
    checkpoint = CheckpointManager(
        output_dir=config.output_dir,
        queries=queries,
        fresh_run=args.fresh_run,
    )

    pending_queries = checkpoint.pending_queries()

    console.print(f"[bold]Queries loaded:[/bold] {len(queries)}")
    console.print(f"[bold]Pending queries:[/bold] {len(pending_queries)}")
    console.print(f"[bold]Checkpoint:[/bold] {checkpoint.root}")
    for q in pending_queries:
        console.print(f" - {q}")

    if pending_queries:
        await scrape_queries_parallel(
            queries=pending_queries,
            config=config,
            checkpoint=checkpoint,
            pipeline=pipeline,
            snapshot_every=max(1, args.snapshot_every),
        )

    records = checkpoint.load_records()
    dataframe = pipeline.to_dataframe(records)
    csv_path, json_path = pipeline.export(dataframe)
    checkpoint.write_snapshot(pipeline)
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

