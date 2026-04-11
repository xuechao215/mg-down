#!/usr/bin/env python3
"""
Convert FDALabel SPL XML files to PDF using Playwright browser.

Strategy:
1. Start a local HTTP server that serves the XML files
2. Also serve the FDA XSLT stylesheets (downloaded locally)
3. Open each XML in Playwright browser -> browser renders XML via XSLT into HTML
4. Save as PDF using Playwright's page.pdf()
"""

import os
import sys
import json
import time
import logging
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = os.path.abspath("downloads")
XML_DIR = os.path.join(BASE_DIR, "fdalabel")
PDF_OUT_DIR = os.path.join(BASE_DIR, "fdalabel_pdf")
STYLESHEET_DIR = os.path.join(BASE_DIR, "stylesheet")

# FDA stylesheet base URL
FDA_SPL_BASE = "https://www.accessdata.fda.gov/spl/stylesheet/"


def download_stylesheets():
    """Download FDA SPL XSLT stylesheets and CSS for local serving."""
    import requests

    os.makedirs(STYLESHEET_DIR, exist_ok=True)

    # Files needed by the XSLT
    files_to_download = [
        "spl.xsl",
        "spl-common.xsl",
        "spl.css",
    ]

    for fname in files_to_download:
        local_path = os.path.join(STYLESHEET_DIR, fname)
        if os.path.exists(local_path):
            log.info("Stylesheet already exists: %s", fname)
            continue

        url = FDA_SPL_BASE + fname
        log.info("Downloading stylesheet: %s", url)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            with open(local_path, "w", encoding="utf-8") as f:
                f.write(r.text)
            log.info("  Saved: %s (%d bytes)", fname, len(r.text))
        except Exception as e:
            log.error("  Failed to download %s: %s", fname, e)

    # Also check if spl-common.xsl imports more files
    common_path = os.path.join(STYLESHEET_DIR, "spl-common.xsl")
    if os.path.exists(common_path):
        with open(common_path, "r", encoding="utf-8") as f:
            content = f.read()
        # Look for import/include href
        import re
        imports = re.findall(r'href=["\']([^"\']+)["\']', content)
        for imp in imports:
            if imp.startswith("http") or imp.endswith(".xsl") or imp.endswith(".css"):
                fname = os.path.basename(imp)
                local_path = os.path.join(STYLESHEET_DIR, fname)
                if not os.path.exists(local_path) and not imp.startswith("http"):
                    url = FDA_SPL_BASE + imp
                    log.info("Downloading additional stylesheet: %s", url)
                    try:
                        r = requests.get(url, timeout=30)
                        r.raise_for_status()
                        with open(local_path, "w", encoding="utf-8") as f:
                            f.write(r.text)
                    except Exception as e:
                        log.warning("  Failed: %s", e)


def patch_xml_files():
    """
    Modify XML files so the XSLT reference points to our local stylesheet
    instead of the relative ../../stylesheet/spl.xsl path.

    The original: <?xml-stylesheet href="../../stylesheet/spl.xsl" type="text/xsl"?>
    New:          <?xml-stylesheet href="/stylesheet/spl.xsl" type="text/xsl"?>
    """
    xml_files = [f for f in os.listdir(XML_DIR) if f.endswith(".xml")]
    patched = 0
    for fname in xml_files:
        fpath = os.path.join(XML_DIR, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            content = f.read()

        if '../../stylesheet/spl.xsl' in content:
            content = content.replace(
                '../../stylesheet/spl.xsl',
                '/stylesheet/spl.xsl'
            )
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(content)
            patched += 1

    log.info("Patched %d XML files to use local stylesheet path", patched)


def start_http_server(port=8765):
    """Start a simple HTTP server in a background thread."""
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=BASE_DIR, **kwargs)

        def log_message(self, format, *args):
            pass  # suppress access logs

    server = HTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("HTTP server started on http://127.0.0.1:%d/", port)
    return server


def convert_xml_to_pdf(port=8765):
    """Use Playwright to open each XML and save as PDF."""
    from playwright.sync_api import sync_playwright

    os.makedirs(PDF_OUT_DIR, exist_ok=True)

    xml_files = sorted([f for f in os.listdir(XML_DIR) if f.endswith(".xml")])
    log.info("Found %d XML files to convert", len(xml_files))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        success = 0
        failed = 0

        for i, fname in enumerate(xml_files, 1):
            pdf_name = fname.replace(".xml", ".pdf")
            pdf_path = os.path.join(PDF_OUT_DIR, pdf_name)

            if os.path.exists(pdf_path):
                log.info("[%d/%d] SKIP (exists): %s", i, len(xml_files), pdf_name)
                success += 1
                continue

            url = f"http://127.0.0.1:{port}/fdalabel/{fname}"

            try:
                page.goto(url, timeout=60000, wait_until="networkidle")
                time.sleep(1)  # extra wait for XSLT rendering

                page.pdf(path=pdf_path, format="A4", print_background=True)
                size_kb = os.path.getsize(pdf_path) / 1024
                log.info("[%d/%d] OK: %s (%.0fKB)", i, len(xml_files), pdf_name, size_kb)
                success += 1

            except Exception as e:
                log.error("[%d/%d] FAILED: %s | %s", i, len(xml_files), pdf_name, str(e)[:150])
                failed += 1
                # Remove empty/broken PDF
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)

        browser.close()

    log.info("=" * 60)
    log.info("Conversion done: success=%d, failed=%d, total=%d", success, failed, len(xml_files))


def main():
    log.info("Step 1: Download FDA SPL stylesheets")
    download_stylesheets()

    log.info("Step 2: Patch XML files to use local stylesheet path")
    patch_xml_files()

    log.info("Step 3: Start local HTTP server")
    server = start_http_server(port=8765)

    log.info("Step 4: Convert XML -> PDF via Playwright browser")
    convert_xml_to_pdf(port=8765)

    server.shutdown()


if __name__ == "__main__":
    main()
