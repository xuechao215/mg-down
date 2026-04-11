#!/usr/bin/env python3
"""Download all FDALabel, DailyMed SPL, and DailyMed PDF files from the lung cancer drug CSV."""

import csv
import os
import sys
import time
import logging
from pathlib import Path
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ── Config ──────────────────────────────────────────────────────────────────
CSV_FILE = "肺癌-相关药品列表.csv"
OUTPUT_DIR = "downloads"
MAX_WORKERS = 8          # parallel downloads
REQUEST_TIMEOUT = 60     # seconds
RETRY_COUNT = 3
RETRY_DELAY = 5          # seconds between retries
DELAY_PER_REQUEST = 0.3  # polite delay (seconds)

# Column indices (0-based)
COL_TRADE_NAME = 5
COL_GENERIC_NAME = 6
COL_SET_ID = 20
COL_FDA_LABEL = 17
COL_DAILYMED_SPL = 18
COL_DAILYMED_PDF = 19

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("download.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Helpers ─────────────────────────────────────────────────────────────────

def safe_filename(name: str) -> str:
    """Make a string safe for use as a filename."""
    keepchars = (" ", "-", "_", ".")
    return "".join(c if c.isalnum() or c in keepchars else "_" for c in name).strip()


def derive_filepath(link_type: str, row: dict) -> str:
    """Build output path: downloads/<type>/<TradeName>_<GenericName>_<SetID>.ext"""
    trade = safe_filename(row["trade_name"][:50])
    generic = safe_filename(row["generic_name"][:50])
    set_id = safe_filename(row["set_id"])

    base = f"{trade}__{generic}__{set_id}"

    if link_type == "fdalabel":
        return os.path.join(OUTPUT_DIR, "fdalabel", f"{base}.xml")
    elif link_type == "dailymed_spl":
        return os.path.join(OUTPUT_DIR, "dailymed_spl", f"{base}.html")
    elif link_type == "dailymed_pdf":
        return os.path.join(OUTPUT_DIR, "dailymed_pdf", f"{base}.pdf")
    return None


def download_one(task: tuple) -> dict:
    """Download a single file with retries. Returns result dict."""
    link_type, url, filepath, trade_name = task

    if os.path.exists(filepath):
        return {"url": url, "status": "skipped", "reason": "exists"}

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "*/*",
            })
            resp.raise_for_status()

            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                f.write(resp.content)

            size_kb = len(resp.content) / 1024
            return {"url": url, "status": "ok", "size_kb": round(size_kb, 1)}

        except requests.RequestException as e:
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY)
            else:
                return {"url": url, "status": "error", "reason": str(e)[:200]}

    return {"url": url, "status": "error", "reason": "max retries exceeded"}


def parse_csv() -> list[dict]:
    """Parse CSV and return list of row dicts with relevant fields."""
    rows = []
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for r in reader:
            if len(r) <= max(COL_TRADE_NAME, COL_GENERIC_NAME, COL_SET_ID,
                             COL_FDA_LABEL, COL_DAILYMED_SPL, COL_DAILYMED_PDF):
                continue

            trade_name = r[COL_TRADE_NAME].strip()
            generic_name = r[COL_GENERIC_NAME].strip()
            set_id = r[COL_SET_ID].strip()

            fda_label = r[COL_FDA_LABEL].strip()
            dailymed_spl = r[COL_DAILYMED_SPL].strip()
            dailymed_pdf = r[COL_DAILYMED_PDF].strip()

            if not any([fda_label, dailymed_spl, dailymed_pdf]):
                continue

            rows.append({
                "trade_name": trade_name or "UNKNOWN",
                "generic_name": generic_name or "UNKNOWN",
                "set_id": set_id or "NO_SETID",
                "fdalabel": fda_label,
                "dailymed_spl": dailymed_spl,
                "dailymed_pdf": dailymed_pdf,
            })
    return rows


def build_tasks(rows: list[dict]) -> list[tuple]:
    """Build download task list, deduplicating by (type, set_id)."""
    seen = set()
    tasks = []

    for row in rows:
        for link_type in ("fdalabel", "dailymed_spl", "dailymed_pdf"):
            url = row[link_type]
            if not url.startswith("http"):
                continue

            # Deduplicate by URL (same URL = same file)
            if url in seen:
                continue
            seen.add(url)

            filepath = derive_filepath(link_type, row)
            if filepath:
                tasks.append((link_type, url, filepath, row["trade_name"]))

    return tasks


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    # Select which types to download
    types_to_download = {"fdalabel", "dailymed_spl", "dailymed_pdf"}
    if len(sys.argv) > 1:
        types_to_download = set(sys.argv[1:])

    log.info("Parsing CSV: %s", CSV_FILE)
    rows = parse_csv()
    log.info("Parsed %d rows", len(rows))

    all_tasks = build_tasks(rows)

    # Filter by requested types
    tasks = [t for t in all_tasks if t[0] in types_to_download]
    log.info("Total download tasks (deduplicated): %d", len(tasks))

    for t in types_to_download:
        count = sum(1 for task in tasks if task[0] == t)
        log.info("  %s: %d files", t, count)

    # Create output dirs
    for t in types_to_download:
        os.makedirs(os.path.join(OUTPUT_DIR, t), exist_ok=True)

    # Download with thread pool
    stats = {"ok": 0, "skipped": 0, "error": 0}
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(download_one, task): task for task in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            stats[result["status"]] = stats.get(result["status"], 0) + 1

            if result["status"] == "ok":
                log.info("[%d/%d] OK %.1fKB - %s", i, len(tasks),
                         result.get("size_kb", 0), os.path.basename(futures[future][2]))
            elif result["status"] == "skipped":
                pass  # quiet for skipped
            else:
                log.warning("[%d/%d] FAILED - %s | %s", i, len(tasks),
                            result.get("reason", ""), futures[future][1][:80])

            if i % 50 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                log.info("--- Progress: %d/%d (%.1f/s), ok=%d skip=%d err=%d ---",
                         i, len(tasks), rate, stats["ok"], stats["skipped"], stats["error"])

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("DONE in %.1f seconds", elapsed)
    log.info("  Downloaded: %d", stats.get("ok", 0))
    log.info("  Skipped:    %d", stats.get("skipped", 0))
    log.info("  Errors:     %d", stats.get("error", 0))
    log.info("  Total:      %d", len(tasks))
    log.info("Files saved to: %s/", os.path.abspath(OUTPUT_DIR))


if __name__ == "__main__":
    main()
