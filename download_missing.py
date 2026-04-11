#!/usr/bin/env python3
"""Use Playwright browser to visit DailyMed SPL pages and download PDFs for missing items."""

import csv
import os
import sys
import json
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CSV_FILE = "肺癌-相关药品列表.csv"
PDF_DIR = "downloads/dailymed_pdf"

def safe_filename(name: str) -> str:
    keepchars = (" ", "-", "_", ".")
    return "".join(c if c.isalnum() or c in keepchars else "_" for c in name).strip()

def get_missing_items():
    """Find items whose PDFs haven't been downloaded yet."""
    downloaded = set()
    if os.path.exists(PDF_DIR):
        for f in os.listdir(PDF_DIR):
            if f.endswith('.pdf'):
                parts = f.rsplit('__', 1)
                if len(parts) == 2:
                    downloaded.add(parts[1].replace('.pdf', ''))

    missing = []
    with open(CSV_FILE, encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        seen = set()
        for row in reader:
            if len(row) <= 20:
                continue
            set_id = row[20].strip()
            spl_link = row[18].strip()
            pdf_link = row[19].strip()
            trade_name = row[5].strip()
            generic_name = row[6].strip()
            if not set_id or not spl_link.startswith('http'):
                continue
            if set_id in seen:
                continue
            seen.add(set_id)
            if set_id not in downloaded:
                missing.append({
                    "trade_name": trade_name or "UNKNOWN",
                    "generic_name": generic_name or "UNKNOWN",
                    "set_id": set_id,
                    "spl_link": spl_link,
                    "pdf_link": pdf_link,
                })
    return missing


def main():
    missing = get_missing_items()
    log.info("Missing PDFs: %d", len(missing))

    if not missing:
        log.info("All PDFs already downloaded!")
        return

    # Output missing items as JSON for the Playwright MCP script to consume
    output = []
    for item in missing:
        filename = f"{safe_filename(item['trade_name'][:50])}__{safe_filename(item['generic_name'][:50])}__{item['set_id']}.pdf"
        output.append({
            "spl_url": item["spl_link"],
            "pdf_url": item["pdf_link"],
            "save_path": os.path.join(PDF_DIR, filename),
            "trade_name": item["trade_name"],
            "set_id": item["set_id"],
        })

    json_path = "missing_items.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log.info("Wrote %d missing items to %s", len(output), json_path)

    # Also print the direct PDF URLs for simple download
    for item in output:
        print(f"{item['pdf_url']}")


if __name__ == "__main__":
    main()
