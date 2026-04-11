#!/usr/bin/env python3
"""
Convert FDALabel SPL XML -> HTML (via XSLT) -> PDF (via Playwright).

Step 1: Use lxml to apply FDA's spl.xsl stylesheet to each XML -> produce HTML
Step 2: Update HTML image paths to point to local downloaded images
Step 3: Use Playwright + local HTTP server to render each HTML and save as PDF
"""

import os
import re
import sys
import time
import logging
import threading
import lxml.etree as ET
from http.server import HTTPServer, SimpleHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = os.path.abspath("downloads")
XML_DIR = os.path.join(BASE_DIR, "fdalabel")
HTML_DIR = os.path.join(BASE_DIR, "fdalabel_html")
PDF_DIR = os.path.join(BASE_DIR, "fdalabel_pdf")
IMG_DIR = os.path.join(BASE_DIR, "fdalabel_images")
XSLT_PATH = os.path.join(BASE_DIR, "stylesheet", "spl.xsl")
CSS_PATH = os.path.join(BASE_DIR, "stylesheet", "spl.css")

HTTP_PORT = 8765


def xml_to_html():
    """Convert all XML files to HTML using XSLT."""
    os.makedirs(HTML_DIR, exist_ok=True)

    # Load XSLT
    xslt_doc = ET.parse(XSLT_PATH, base_url=f'file://{XSLT_PATH}')
    transform = ET.XSLT(xslt_doc)

    # Read CSS to inline it
    with open(CSS_PATH, 'r', encoding='utf-8') as f:
        css_content = f.read()

    xml_files = sorted([f for f in os.listdir(XML_DIR) if f.endswith(".xml")])
    log.info("Found %d XML files to convert to HTML", len(xml_files))

    success = 0
    failed = 0

    for i, fname in enumerate(xml_files, 1):
        html_name = fname.replace(".xml", ".html")
        html_path = os.path.join(HTML_DIR, html_name)

        if os.path.exists(html_path):
            success += 1
            continue

        xml_path = os.path.join(XML_DIR, fname)

        try:
            xml_doc = ET.parse(xml_path)
            html_doc = transform(xml_doc)
            result = ET.tostring(html_doc, pretty_print=True, encoding='unicode')

            # Replace external CSS link with inline CSS for offline viewing
            result = result.replace(
                '<link rel="stylesheet" type="text/css" href="http://www.accessdata.fda.gov/spl/stylesheet/spl.css"/>',
                f'<style type="text/css">\n{css_content}\n</style>'
            )
            # Also handle alternate link format
            result = result.replace(
                '<link rel="stylesheet" type="text/css" href="http://www.accessdata.fda.gov/spl/stylesheet/spl.css">',
                f'<style type="text/css">\n{css_content}\n</style>'
            )
            # Remove external JS reference (not needed for static content)
            result = result.replace(
                '<script src="http://www.accessdata.fda.gov/spl/stylesheet/spl.js" type="text/javascript" charset="utf-8">/* */</script>',
                ''
            )

            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(result)

            if i % 50 == 0 or i == len(xml_files):
                log.info("[%d/%d] Converted: %s (%.0fKB)", i, len(xml_files), html_name, len(result)/1024)
            success += 1

        except Exception as e:
            log.error("[%d/%d] FAILED: %s | %s", i, len(xml_files), fname, str(e)[:150])
            failed += 1

    log.info("XML->HTML done: success=%d, failed=%d", success, failed)
    return success


def update_image_paths():
    """Update HTML img src to point to local fdalabel_images directory."""
    updated = 0
    html_files = sorted([f for f in os.listdir(HTML_DIR) if f.endswith(".html")])

    for fname in html_files:
        parts = fname.rsplit('__', 1)
        if len(parts) != 2:
            continue
        set_id = parts[1].replace('.html', '')

        path = os.path.join(HTML_DIR, fname)
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()

        def replace_img(m):
            prefix = m.group(1)
            src = m.group(2)
            suffix = m.group(3)
            if src.startswith('http') or src.startswith('/') or src.startswith('data:'):
                return m.group(0)
            # Use path relative to HTML dir: ../fdalabel_images/<set_id>/<filename>
            new_src = f"../fdalabel_images/{set_id}/{src}"
            return f'{prefix}{new_src}{suffix}'

        new_content = re.sub(
            r'(<img[^>]+src=["\'])([^"\']+)(["\'])',
            replace_img,
            content
        )

        if new_content != content:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            updated += 1

    log.info("Updated image paths in %d HTML files", updated)


def start_http_server(port=HTTP_PORT):
    """Start a simple HTTP server serving from downloads/ directory."""
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


def html_to_pdf(port=HTTP_PORT):
    """Convert all HTML files to PDF using Playwright via HTTP server."""
    from playwright.sync_api import sync_playwright

    os.makedirs(PDF_DIR, exist_ok=True)

    html_files = sorted([f for f in os.listdir(HTML_DIR) if f.endswith(".html")])
    log.info("Found %d HTML files to convert to PDF", len(html_files))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        success = 0
        failed = 0

        for i, fname in enumerate(html_files, 1):
            pdf_name = fname.replace(".html", ".pdf")
            pdf_path = os.path.join(PDF_DIR, pdf_name)

            if os.path.exists(pdf_path):
                success += 1
                continue

            # Use HTTP URL so images can be loaded properly
            url = f"http://127.0.0.1:{port}/fdalabel_html/{fname}"

            try:
                page.goto(url, timeout=60000, wait_until="networkidle")
                # Wait for images to finish loading
                time.sleep(1)

                page.pdf(
                    path=pdf_path,
                    format="A4",
                    print_background=True,
                    margin={"top": "20mm", "bottom": "20mm", "left": "20mm", "right": "20mm"},
                )

                size_kb = os.path.getsize(pdf_path) / 1024
                if i % 50 == 0 or i == len(html_files):
                    log.info("[%d/%d] PDF: %s (%.0fKB)", i, len(html_files), pdf_name, size_kb)
                success += 1

            except Exception as e:
                log.error("[%d/%d] FAILED: %s | %s", i, len(html_files), pdf_name, str(e)[:150])
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                failed += 1

        browser.close()

    log.info("HTML->PDF done: success=%d, failed=%d", success, failed)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "pdf":
        log.info("Step 2: Updating image paths in HTML")
        update_image_paths()
        log.info("Step 3: Starting HTTP server")
        server = start_http_server()
        log.info("Step 4: Converting HTML -> PDF")
        html_to_pdf()
        server.shutdown()
    elif len(sys.argv) > 1 and sys.argv[1] == "html":
        log.info("Step 1: Converting XML -> HTML")
        xml_to_html()
        log.info("Step 2: Updating image paths in HTML")
        update_image_paths()
    else:
        log.info("Step 1: Converting XML -> HTML")
        xml_to_html()
        log.info("Step 2: Updating image paths in HTML")
        update_image_paths()
        log.info("Step 3: Starting HTTP server")
        server = start_http_server()
        log.info("Step 4: Converting HTML -> PDF")
        html_to_pdf()
        server.shutdown()


if __name__ == "__main__":
    main()
