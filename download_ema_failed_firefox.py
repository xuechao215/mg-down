#!/usr/bin/env python3
"""Retry failed EMA PDF downloads using Firefox page-context fetching."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse


BASE_DIR = Path(__file__).resolve().parent
VENDOR_DIR = BASE_DIR / ".vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: playwright. This script expects the vendored copy in .vendor."
    ) from exc


DEFAULT_CSV_PATH = BASE_DIR / "cancer-data.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "cancer-data_ema_product_information_pdfs"
DEFAULT_MANIFEST_PATH = DEFAULT_OUTPUT_DIR / "download_manifest.json"
FIREFOX_CACHE_DIR = Path.home() / "Library" / "Caches" / "ms-playwright"

PAGE_TIMEOUT_MS = 120_000
FETCH_TIMEOUT_MS = 120_000
PAGE_WAIT_MS = 3_000
ROW_RETRY_COUNT = 3
ROW_DELAY_SECONDS = 2.0
FETCH_JS = """
async (pdfUrl) => {
  const response = await fetch(pdfUrl, {
    credentials: 'include',
    headers: {
      'Accept': 'application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    },
  });
  await response.arrayBuffer();
  return true;
}
"""
BLOCK_MARKERS = (
    "temporarily unavailable",
    "server inaccessibility",
    "<title>sorry -",
)
NON_STANDARD_STATUSES = {"Application withdrawn", "Withdrawn", "Refused", "Opinion"}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry failed EMA PDF downloads using a Firefox page-context fetch.",
    )
    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV_PATH),
        help=f"CSV input path (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory to store downloaded PDFs (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST_PATH),
        help=f"Manifest JSON path (default: {DEFAULT_MANIFEST_PATH})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N failed rows after filtering.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="1-based CSV row index to start from (default: 1).",
    )
    parser.add_argument(
        "--end-index",
        type=int,
        default=None,
        help="1-based CSV row index to stop at, inclusive.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=ROW_DELAY_SECONDS,
        help=f"Delay in seconds between rows (default: {ROW_DELAY_SECONDS}).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload even if the target file already exists and looks valid.",
    )
    parser.add_argument(
        "--repair-missing-pdf-url",
        action="store_true",
        help=(
            "Also revisit successful manifest rows whose local PDF exists but whose "
            "manifest entry is still missing pdf_url."
        ),
    )
    return parser.parse_args(argv[1:])


def load_rows(csv_path: Path) -> dict[int, dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return {index: row for index, row in enumerate(csv.DictReader(handle), 1)}


def load_manifest(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_manifest(path: Path, items: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def is_pdf_bytes(content: bytes) -> bool:
    return content[:5] == b"%PDF-"


def valid_existing_pdf(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 1024:
        return False
    try:
        return is_pdf_bytes(path.read_bytes()[:5])
    except OSError:
        return False


def firefox_executable() -> Path:
    candidates: list[tuple[int, Path]] = []
    for path in FIREFOX_CACHE_DIR.glob("firefox-*"):
        if not path.is_dir():
            continue
        match = re.search(r"firefox-(\d+)$", path.name)
        if not match:
            continue
        exe = path / "firefox" / "Nightly.app" / "Contents" / "MacOS" / "firefox"
        if exe.exists():
            candidates.append((int(match.group(1)), exe))

    if not candidates:
        raise SystemExit("No usable Playwright Firefox executable found in ~/Library/Caches/ms-playwright.")

    candidates.sort(reverse=True)
    return candidates[0][1]


def candidate_score(medicine_status: str, href: str, context_text: str) -> int:
    href_lower = href.lower()
    text_lower = context_text.lower()
    score = 0

    if "/product-information/" in href_lower:
        score += 100
    if "product-information_en.pdf" in href_lower:
        score += 80
    if "/medicine-qa/" in href_lower:
        score += 70
    if "questions-and-answers" in href_lower:
        score += 30
    if "/overview/" in href_lower or "summary-public" in href_lower:
        score += 35

    if "product information" in text_lower:
        score += 60
    if "questions and answers" in text_lower:
        score += 50
    if "withdrawal" in text_lower:
        score += 30
    if "refusal" in text_lower:
        score += 30
    if "summary" in text_lower:
        score += 20

    if medicine_status in {"Authorised", "Lapsed"}:
        if "/product-information/" in href_lower:
            score += 120
        if "/overview/" in href_lower:
            score -= 20
        if "/medicine-qa/" in href_lower:
            score -= 10
    elif medicine_status in NON_STANDARD_STATUSES:
        if "/medicine-qa/" in href_lower:
            score += 80
        if "withdrawal" in href_lower or "questions-and-answers" in href_lower:
            score += 50

    return score


def collect_english_pdf_candidates(page, medicine_status: str) -> list[dict[str, Any]]:
    anchors = page.locator("a[href*='_en.pdf']")
    count = anchors.count()
    candidates: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for index in range(count):
        anchor = anchors.nth(index)
        href = anchor.get_attribute("href") or ""
        if not href:
            continue
        absolute = urljoin(page.url, href)
        if absolute in seen_urls:
            continue

        try:
            context_text = anchor.evaluate(
                """(node) => {
                    const card = node.closest('.bcl-file, .ema-document-type, article, section, li, div');
                    return (card ? card.innerText : node.innerText) || '';
                }"""
            )
        except PlaywrightError:
            context_text = ""

        candidate = {
            "href": absolute,
            "context_text": re.sub(r"\s+", " ", context_text or "").strip(),
        }
        candidate["score"] = candidate_score(medicine_status, absolute, candidate["context_text"])
        candidates.append(candidate)
        seen_urls.add(absolute)

    if not candidates:
        try:
            html = page.content()
        except PlaywrightError:
            html = ""

        for match in re.finditer(r'href="([^"]+_en\.pdf[^"]*)"', html):
            absolute = urljoin(page.url, match.group(1))
            if absolute in seen_urls:
                continue

            start = max(0, match.start() - 300)
            end = min(len(html), match.end() + 300)
            context_text = re.sub(r"\s+", " ", html[start:end]).strip()
            candidate = {
                "href": absolute,
                "context_text": context_text,
            }
            candidate["score"] = candidate_score(medicine_status, absolute, context_text)
            candidates.append(candidate)
            seen_urls.add(absolute)

    candidates.sort(key=lambda item: (-item["score"], item["href"]))
    return candidates


def fetch_pdf_via_page(page, pdf_url: str) -> tuple[bytes | None, str]:
    try:
        with page.expect_response(lambda response, url=pdf_url: response.url == url, timeout=FETCH_TIMEOUT_MS) as response_info:
            page.evaluate(FETCH_JS, pdf_url)
    except PlaywrightTimeoutError:
        return None, "fetch_timeout"
    except PlaywrightError as exc:
        return None, f"fetch_error:{type(exc).__name__}:{exc}"

    response = response_info.value
    try:
        body = response.body()
    except PlaywrightError as exc:
        return None, f"body_error:{type(exc).__name__}:{exc}"

    content_type = response.headers.get("content-type", "")
    if response.status == 200 and is_pdf_bytes(body):
        return body, "ok"
    snippet = body[:120].decode("utf-8", "ignore").strip().replace("\n", " ")
    return None, f"http_{response.status}:{content_type}:{snippet}"


def write_pdf(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".part")
    temp.write_bytes(content)
    temp.replace(path)


def page_is_blocked(page) -> bool:
    title = ""
    html = ""
    try:
        title = page.title().lower()
    except PlaywrightError:
        pass
    try:
        html = page.content().lower()
    except PlaywrightError:
        pass
    joined = f"{title}\n{html[:4000]}"
    return any(marker in joined for marker in BLOCK_MARKERS)


def output_path(item: dict[str, Any], output_dir: Path) -> Path:
    file_value = (item.get("file") or "").strip()
    if file_value:
        return Path(file_value)
    slug = urlparse(item.get("medicine_url", "")).path.rstrip("/").rsplit("/", 1)[-1]
    return output_dir / f"{slug or 'unknown'}.pdf"


def resolve_pdf_url(page, medicine_url: str, medicine_status: str) -> tuple[str, str]:
    try:
        response = page.goto(medicine_url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
        page.wait_for_timeout(PAGE_WAIT_MS)
        status_code = response.status if response is not None else "na"
        if page_is_blocked(page):
            return "", f"page_blocked:{status_code}"
    except PlaywrightTimeoutError:
        return "", "page_timeout"
    except PlaywrightError as exc:
        return "", f"page_error:{type(exc).__name__}:{exc}"

    candidates = collect_english_pdf_candidates(page, medicine_status)
    if not candidates:
        return "", "no_english_pdf_candidates"

    best = candidates[0]
    return best["href"], f"candidate_score={best['score']}"


def retry_failed_items(
    manifest_items: list[dict[str, Any]],
    rows_by_index: dict[int, dict[str, str]],
    output_dir: Path,
    *,
    start_index: int,
    end_index: int | None,
    limit: int | None,
    delay: float,
    overwrite: bool,
    repair_missing_pdf_url: bool,
) -> tuple[int, int]:
    firefox_path = firefox_executable()
    print(f"Using Firefox: {firefox_path}", flush=True)

    failed_items = []
    for item in manifest_items:
        index = int(item.get("index", 0) or 0)
        if index < max(1, start_index):
            continue
        if end_index is not None and index > end_index:
            continue

        status = (item.get("status") or "").strip()
        if status == "failed":
            failed_items.append(item)
            continue

        if (
            repair_missing_pdf_url
            and status == "downloaded"
            and not (item.get("pdf_url") or "").strip()
        ):
            failed_items.append(item)

    failed_items.sort(key=lambda item: int(item["index"]))
    if limit is not None:
        failed_items = failed_items[:limit]

    print(f"Processing {len(failed_items)} manifest items", flush=True)
    if not failed_items:
        return 0, 0

    recovered = 0
    still_failed = 0

    with sync_playwright() as playwright:
        browser = playwright.firefox.launch(headless=True, executable_path=str(firefox_path))
        context = browser.new_context(locale="en-US")
        try:
            for item in failed_items:
                index = int(item["index"])
                row = rows_by_index.get(index, {})
                medicine_url = item["medicine_url"]
                medicine_status = item.get("medicine_status") or row.get("Medicine status", "")
                name = item["name"]
                target_path = output_path(item, output_dir)
                needs_pdf_url = not (item.get("pdf_url") or "").strip()

                if target_path.exists() and valid_existing_pdf(target_path) and not overwrite:
                    item["status"] = "downloaded"
                    item["file"] = str(target_path)
                    detail = "existing_pdf_verified"
                    if needs_pdf_url:
                        page = context.new_page()
                        try:
                            pdf_url, resolve_detail = resolve_pdf_url(page, medicine_url, medicine_status)
                            if pdf_url:
                                item["pdf_url"] = pdf_url
                            detail = f"{detail}:{resolve_detail}"
                        finally:
                            try:
                                page.close()
                            except Exception:
                                pass
                    item["detail"] = detail
                    item["size_bytes"] = target_path.stat().st_size
                    recovered += 1
                    print(f"[{index}] OK   {name} existing file verified", flush=True)
                    continue

                details: list[str] = []
                success = False
                for attempt in range(1, ROW_RETRY_COUNT + 1):
                    page = context.new_page()
                    try:
                        response = page.goto(medicine_url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
                        page.wait_for_timeout(PAGE_WAIT_MS)
                        page_url = page.url
                        status_code = response.status if response is not None else "na"
                        if page_is_blocked(page):
                            details.append(f"attempt_{attempt}:page_blocked:{status_code}")
                            continue

                        candidates = collect_english_pdf_candidates(page, medicine_status)
                        if not candidates:
                            details.append(f"attempt_{attempt}:no_english_pdf_candidates")
                            continue

                        for candidate in candidates:
                            pdf_bytes, detail = fetch_pdf_via_page(page, candidate["href"])
                            details.append(
                                f"attempt_{attempt}:{candidate['href']}|score={candidate['score']} -> {detail}"
                            )
                            if pdf_bytes is None:
                                continue

                            write_pdf(target_path, pdf_bytes)
                            item["status"] = "downloaded"
                            item["pdf_url"] = candidate["href"]
                            item["file"] = str(target_path)
                            item["detail"] = f"firefox_page_fetch:{detail}"
                            item["attempts"] = int(item.get("attempts", 0) or 0) + attempt
                            item["size_bytes"] = len(pdf_bytes)
                            recovered += 1
                            success = True
                            print(
                                f"[{index}] OK   {name} size={len(pdf_bytes) // 1024} KB via={candidate['href']}",
                                flush=True,
                            )
                            break

                        if success:
                            break

                        if page_url != medicine_url:
                            details.append(f"attempt_{attempt}:final_page:{page_url}")
                    except PlaywrightTimeoutError:
                        details.append(f"attempt_{attempt}:page_timeout")
                    except PlaywrightError as exc:
                        details.append(f"attempt_{attempt}:page_error:{type(exc).__name__}:{exc}")
                    finally:
                        try:
                            page.close()
                        except Exception:
                            pass

                if not success:
                    item["status"] = "failed"
                    item["detail"] = " || ".join(details[-8:]) if details else "unknown_failure"
                    still_failed += 1
                    print(f"[{index}] FAIL {name}", flush=True)

                if delay:
                    time.sleep(delay)
        finally:
            browser.close()

    return recovered, still_failed


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    csv_path = Path(args.csv).resolve()
    output_dir = Path(args.output_dir).resolve()
    manifest_path = Path(args.manifest).resolve()

    rows_by_index = load_rows(csv_path)
    manifest_items = load_manifest(manifest_path)

    recovered, still_failed = retry_failed_items(
        manifest_items,
        rows_by_index,
        output_dir,
        start_index=args.start_index,
        end_index=args.end_index,
        limit=args.limit,
        delay=args.delay,
        overwrite=args.overwrite,
        repair_missing_pdf_url=args.repair_missing_pdf_url,
    )

    save_manifest(manifest_path, manifest_items)
    print(
        f"Summary: recovered={recovered} still_failed={still_failed} manifest={manifest_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
