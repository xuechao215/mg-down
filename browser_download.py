#!/usr/bin/env python3
"""Use Playwright to browse DailyMed SPL pages, find PDF links, and download them."""

import json
import os
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    from playwright.sync_api import sync_playwright

    with open("missing_items.json", encoding="utf-8") as f:
        items = json.load(f)

    log.info("Will browse %d SPL pages to download missing PDFs", len(items))
    os.makedirs("downloads/dailymed_pdf", exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            accept_downloads=True,
        )
        page = context.new_page()

        success = 0
        failed = 0

        for i, item in enumerate(items, 1):
            spl_url = item["spl_url"]
            save_path = item["save_path"]
            trade_name = item["trade_name"]
            set_id = item["set_id"]

            if os.path.exists(save_path):
                log.info("[%d/%d] SKIP (exists): %s", i, len(items), trade_name)
                success += 1
                continue

            log.info("[%d/%d] Browsing: %s | %s", i, len(items), trade_name, spl_url)

            try:
                # Navigate to the SPL page
                page.goto(spl_url, timeout=60000, wait_until="domcontentloaded")
                time.sleep(2)

                # Try to find PDF download link on the page
                # DailyMed pages typically have a link like "Download PDF" or a link to downloadpdffile.cfm
                pdf_link = None

                # Strategy 1: Look for download PDF link in the page
                links = page.query_selector_all("a")
                for link in links:
                    href = link.get_attribute("href") or ""
                    text = link.inner_text().strip().lower()
                    if "downloadpdffile" in href or "pdf" in text:
                        pdf_link = href
                        log.info("  Found PDF link on page: %s", href)
                        break

                # Strategy 2: If no link found on page, construct the direct PDF URL
                if not pdf_link:
                    pdf_link = f"https://dailymed.nlm.nih.gov/dailymed/downloadpdffile.cfm?setId={set_id}"
                    log.info("  Using direct PDF URL: %s", pdf_link)

                # Make URL absolute if relative
                if pdf_link.startswith("/"):
                    pdf_link = "https://dailymed.nlm.nih.gov" + pdf_link

                # Download the PDF
                # Use page.request to download (avoids navigation)
                resp = context.request.get(pdf_link)
                if resp.ok:
                    content_type = resp.headers.get("content-type", "")
                    body = resp.body()

                    # Verify it's actually a PDF
                    if body[:5] == b'%PDF-':
                        os.makedirs(os.path.dirname(save_path), exist_ok=True)
                        with open(save_path, "wb") as f:
                            f.write(body)
                        size_kb = len(body) / 1024
                        log.info("  OK: Saved %s (%.1fKB)", os.path.basename(save_path), size_kb)
                        success += 1
                    else:
                        log.warning("  NOT A PDF (content-type: %s, first bytes: %s)",
                                    content_type, body[:50])
                        # Save anyway for inspection
                        err_path = save_path.replace(".pdf", ".html")
                        with open(err_path, "wb") as f:
                            f.write(body)
                        failed += 1
                else:
                    log.warning("  HTTP %d for %s", resp.status, pdf_link)
                    failed += 1

            except Exception as e:
                log.error("  ERROR: %s", str(e)[:200])
                failed += 1

            time.sleep(1)  # polite delay

        browser.close()

    log.info("=" * 60)
    log.info("DONE: success=%d, failed=%d, total=%d", success, failed, len(items))


if __name__ == "__main__":
    main()
