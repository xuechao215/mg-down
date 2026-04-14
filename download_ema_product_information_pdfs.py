#!/usr/bin/env python3
"""Download EMA English product-information PDFs for rows in cancer-data.csv."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent
VENDOR_DIR = BASE_DIR / ".vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

try:
    from curl_cffi import requests
except Exception as exc:  # pragma: no cover - surfaced to the user immediately
    raise SystemExit(
        "Missing dependency: curl_cffi. This script expects the vendored copy in .vendor."
    ) from exc


DEFAULT_CSV_PATH = BASE_DIR / "cancer-data.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "cancer-data_ema_product_information_pdfs"
DEFAULT_MANIFEST_NAME = "download_manifest.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
PDF_ACCEPT = "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
REQUEST_TIMEOUT_SECONDS = 60
ITEM_DELAY_SECONDS = 1.2
MAX_ATTEMPTS_PER_URL = 2
IMPERSONATIONS = ("chrome124", "safari184")
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
PRODUCT_INFORMATION_SUFFIXES = ("", "_0", "_1", "_2", "_3")
BLOCK_PAGE_MARKERS = (
    "temporarily unavailable",
    "server inaccessibility",
    "we apologise for any inconvenience",
    "<title>sorry -",
)


@dataclass
class Result:
    index: int
    name: str
    ema_product_number: str
    medicine_status: str
    medicine_url: str
    slug: str
    file: str
    pdf_url: str
    status: str
    detail: str
    attempts: int
    size_bytes: int


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download EMA English product-information PDFs for cancer-data.csv rows.",
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
        "--limit",
        type=int,
        default=None,
        help="Only process the first N rows.",
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
        default=ITEM_DELAY_SECONDS,
        help=f"Delay in seconds between rows (default: {ITEM_DELAY_SECONDS}).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download PDFs even if the output file already exists.",
    )
    parser.add_argument(
        "--stop-after-consecutive-blocks",
        type=int,
        default=6,
        help="Stop the run after this many consecutive block-page/retryable failures (default: 6).",
    )
    parser.add_argument(
        "--manifest-status",
        action="append",
        default=[],
        help=(
            "Only process rows whose current manifest status matches this value. "
            "Repeat the flag to include multiple statuses."
        ),
    )
    return parser.parse_args(argv[1:])


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def safe_filename(value: str, *, fallback: str) -> str:
    keepchars = (" ", "-", "_", ".", "(", ")")
    cleaned = "".join(c if c.isalnum() or c in keepchars else "_" for c in (value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ._")
    return cleaned[:180].strip(" ._") or fallback


def slug_from_medicine_url(url: str) -> str:
    path = urlparse((url or "").strip()).path.rstrip("/")
    if not path:
        return ""
    return path.rsplit("/", 1)[-1]


def candidate_slugs(row: dict[str, str]) -> list[str]:
    slug = slug_from_medicine_url(row.get("Medicine URL", ""))
    candidates: list[str] = []
    stripped_slug = re.sub(r"-\d+$", "", slug)
    ordered_candidates = (stripped_slug, slug) if stripped_slug != slug else (slug,)
    for candidate in ordered_candidates:
        candidate = candidate.strip("-")
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def candidate_pdf_urls(row: dict[str, str]) -> list[str]:
    urls: list[str] = []
    for slug in candidate_slugs(row):
        for suffix in PRODUCT_INFORMATION_SUFFIXES:
            url = (
                "https://www.ema.europa.eu/en/documents/product-information/"
                f"{slug}-epar-product-information_en{suffix}.pdf"
            )
            if url not in urls:
                urls.append(url)
    return urls


def output_path_for(row: dict[str, str], output_dir: Path) -> Path:
    slug = slug_from_medicine_url(row.get("Medicine URL", "")) or "unknown"
    name = row.get("Name of medicine", "").strip() or slug
    ema_number = (row.get("EMA product number") or "").strip()
    label = f"{name} ({ema_number or slug})"
    return output_dir / f"{safe_filename(label, fallback=slug)}.pdf"


def is_pdf_bytes(content: bytes) -> bool:
    return content[:5] == b"%PDF-"


def looks_like_block_page(content: bytes) -> bool:
    sample = content[:10000].decode("utf-8", "ignore").lower()
    if not sample:
        return False
    return any(marker in sample for marker in BLOCK_PAGE_MARKERS)


def valid_existing_pdf(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 1024:
        return False
    try:
        return is_pdf_bytes(path.read_bytes()[:5])
    except OSError:
        return False


def manifest_path(output_dir: Path) -> Path:
    return output_dir / DEFAULT_MANIFEST_NAME


def result_key(index: int, medicine_url: str) -> str:
    return medicine_url or f"index:{index}"


def load_manifest(path: Path) -> dict[str, Result]:
    if not path.exists():
        return {}

    try:
        raw_items = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    results: dict[str, Result] = {}
    for raw_item in raw_items:
        try:
            result = Result(**raw_item)
        except TypeError:
            continue
        results[result_key(result.index, result.medicine_url)] = result
    return results


def ordered_results(results: dict[str, Result]) -> list[Result]:
    return sorted(results.values(), key=lambda result: result.index)


def write_manifest(path: Path, results: dict[str, Result]) -> None:
    path.write_text(
        json.dumps(
            [asdict(result) for result in ordered_results(results)],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def request_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": PDF_ACCEPT,
        "Accept-Language": "en-US,en;q=0.9",
    }


def parse_retry_after(value: str) -> float:
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        return 0.0


def fetch_candidate(url: str) -> tuple[bytes | None, str, int, str]:
    attempts = 0
    last_detail = "not_attempted"

    for round_idx in range(2):
        for impersonation in IMPERSONATIONS:
            attempts += 1
            if attempts > MAX_ATTEMPTS_PER_URL:
                return None, "", attempts - 1, last_detail

            try:
                response = requests.get(
                    url,
                    headers=request_headers(),
                    impersonate=impersonation,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    allow_redirects=True,
                )
            except Exception as exc:
                last_detail = f"{impersonation}:{type(exc).__name__}"
                time.sleep(0.8 + round_idx)
                continue

            final_url = str(response.url or url)
            body = response.content or b""
            content_type = response.headers.get("content-type", "")

            if response.status_code == 200 and is_pdf_bytes(body):
                return body, final_url, attempts, f"{impersonation}:ok"

            if response.status_code == 404:
                return None, final_url, attempts, f"{impersonation}:http_404"

            if looks_like_block_page(body):
                retry_after = parse_retry_after(response.headers.get("retry-after", ""))
                last_detail = (
                    f"{impersonation}:block_page:http_{response.status_code}:{content_type or 'unknown'}"
                )
                if attempts < MAX_ATTEMPTS_PER_URL:
                    time.sleep(retry_after or (1.5 + round_idx))
                    continue
                return None, final_url, attempts, last_detail

            if response.status_code in RETRYABLE_STATUS_CODES:
                retry_after = parse_retry_after(response.headers.get("retry-after", ""))
                last_detail = (
                    f"{impersonation}:retryable:http_{response.status_code}:{content_type or 'unknown'}"
                )
                if attempts < MAX_ATTEMPTS_PER_URL:
                    time.sleep(retry_after or (1.5 + round_idx))
                    continue
                return None, final_url, attempts, last_detail

            snippet = body[:80].decode("utf-8", "ignore").strip().replace("\n", " ")
            last_detail = (
                f"{impersonation}:non_pdf:http_{response.status_code}:{content_type or 'unknown'}:{snippet}"
            )
            break

    return None, "", attempts, last_detail


def write_pdf(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".part")
    temp_path.write_bytes(content)
    temp_path.replace(path)


def classify_missing(row: dict[str, str], details: list[str]) -> str:
    status = (row.get("Medicine status") or "").strip()
    joined = " | ".join(details).lower()
    if "block_page" in joined or "retryable" in joined:
        return "failed"
    if "http_404" in joined:
        return "unavailable"
    if status in {"Opinion", "Application withdrawn", "Withdrawn", "Refused", "Lapsed"}:
        return "unavailable"
    return "failed"


def summarize(results: list[Result]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    return summary


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    csv_path = Path(args.csv).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_path(output_dir)

    all_rows = load_rows(csv_path)
    indexed_rows = list(enumerate(all_rows, 1))
    indexed_rows = [item for item in indexed_rows if item[0] >= max(1, args.start_index)]
    if args.end_index is not None:
        indexed_rows = [item for item in indexed_rows if item[0] <= args.end_index]

    results = load_manifest(manifest_file)
    if args.manifest_status:
        allowed_statuses = {status.strip() for status in args.manifest_status if status.strip()}
        filtered_rows: list[tuple[int, dict[str, str]]] = []
        for index, row in indexed_rows:
            existing = results.get(result_key(index, row.get("Medicine URL", "")))
            if existing and existing.status in allowed_statuses:
                filtered_rows.append((index, row))
        indexed_rows = filtered_rows

    if args.limit is not None:
        indexed_rows = indexed_rows[: args.limit]

    consecutive_block_failures = 0
    print(f"Processing {len(indexed_rows)} rows from {csv_path}")
    print(f"Output directory: {output_dir}")

    for index, row in indexed_rows:
        output_path = output_path_for(row, output_dir)
        slug = slug_from_medicine_url(row.get("Medicine URL", ""))
        name = row.get("Name of medicine", "").strip() or slug or f"row-{index}"
        ema_product_number = (row.get("EMA product number") or "").strip()
        medicine_status = (row.get("Medicine status") or "").strip()

        if output_path.exists() and valid_existing_pdf(output_path) and not args.overwrite:
            result = Result(
                index=index,
                name=name,
                ema_product_number=ema_product_number,
                medicine_status=medicine_status,
                medicine_url=row.get("Medicine URL", ""),
                slug=slug,
                file=str(output_path),
                pdf_url="",
                status="skipped_existing",
                detail="already_exists",
                attempts=0,
                size_bytes=output_path.stat().st_size,
            )
            results[result_key(index, row.get("Medicine URL", ""))] = result
            print(f"[{index}/{len(all_rows)}] SKIP {name} ({result.size_bytes // 1024} KB)")
            write_manifest(manifest_file, results)
            continue

        details: list[str] = []
        total_attempts = 0
        downloaded = False
        downloaded_url = ""
        size_bytes = 0

        for candidate_url in candidate_pdf_urls(row):
            pdf_bytes, final_url, attempts, detail = fetch_candidate(candidate_url)
            total_attempts += attempts
            details.append(f"{candidate_url} -> {detail}")
            if pdf_bytes is None:
                if "block_page" in detail or "retryable" in detail:
                    break
                continue

            write_pdf(output_path, pdf_bytes)
            downloaded = True
            downloaded_url = final_url or candidate_url
            size_bytes = len(pdf_bytes)
            break

        if downloaded:
            result = Result(
                index=index,
                name=name,
                ema_product_number=ema_product_number,
                medicine_status=medicine_status,
                medicine_url=row.get("Medicine URL", ""),
                slug=slug,
                file=str(output_path),
                pdf_url=downloaded_url,
                status="downloaded",
                detail="ok",
                attempts=total_attempts,
                size_bytes=size_bytes,
            )
            print(
                f"[{index}/{len(all_rows)}] OK   {name} attempts={total_attempts} size={size_bytes // 1024} KB"
            )
        else:
            status = classify_missing(row, details)
            result = Result(
                index=index,
                name=name,
                ema_product_number=ema_product_number,
                medicine_status=medicine_status,
                medicine_url=row.get("Medicine URL", ""),
                slug=slug,
                file=str(output_path),
                pdf_url="",
                status=status,
                detail=" | ".join(details) if details else "no_candidate_urls",
                attempts=total_attempts,
                size_bytes=0,
            )
            print(f"[{index}/{len(all_rows)}] {status.upper():4} {name} attempts={total_attempts}")

        results[result_key(index, row.get("Medicine URL", ""))] = result
        write_manifest(manifest_file, results)

        if result.status == "failed" and (
            "block_page" in result.detail or "retryable" in result.detail
        ):
            consecutive_block_failures += 1
        else:
            consecutive_block_failures = 0

        if (
            args.stop_after_consecutive_blocks > 0
            and consecutive_block_failures >= args.stop_after_consecutive_blocks
        ):
            print(
                "Stopping early after "
                f"{consecutive_block_failures} consecutive block/retryable failures."
            )
            break

        if args.delay:
            time.sleep(args.delay)

    summary = summarize(ordered_results(results))
    summary_text = " ".join(f"{key}={value}" for key, value in sorted(summary.items()))
    print(f"\nSummary: {summary_text}")
    print(f"Manifest: {manifest_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
