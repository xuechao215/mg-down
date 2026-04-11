#!/usr/bin/env python3
"""Download real ESMO Open PDFs for ELCC entries using the local Chrome session."""

from __future__ import annotations

import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import browser_cookie3
from curl_cffi import requests


BASE_DIR = Path(__file__).resolve().parent
MAP_FILE = BASE_DIR / "elcc_pii_map.json"
OUTPUT_DIR = BASE_DIR / "ELCC"
MANIFEST_FILE = OUTPUT_DIR / "real_pdf_manifest.json"

MAX_ATTEMPTS = 4
DELAY_SECONDS = 0.5
IMPERSONATIONS = ("chrome124", "safari184")
TIMEOUT_SECONDS = 90


@dataclass
class Result:
    pii: str
    doi: str
    title: str
    status: str
    attempts: int
    file: str
    detail: str


def load_items() -> list[dict]:
    with MAP_FILE.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_pii(raw_pii: str) -> str:
    return f"{raw_pii[:5]}-{raw_pii[5:9]}({raw_pii[9:11]}){raw_pii[11:16]}-{raw_pii[16:]}"


def load_cookies():
    return browser_cookie3.chrome(domain_name="esmoopen.com")


def download_pdf(raw_pii: str, cookie_jar) -> tuple[bytes | None, str, int]:
    formatted = normalize_pii(raw_pii)
    url = f"https://www.esmoopen.com/action/showPdf?pii={formatted}"
    referer = f"https://www.esmoopen.com/article/{formatted}/fulltext"

    attempts = 0
    last_detail = "unknown_error"

    for round_idx in range(2):
        if round_idx > 0:
            cookie_jar = load_cookies()

        for impersonation in IMPERSONATIONS:
            attempts += 1
            if attempts > MAX_ATTEMPTS:
                break
            try:
                response = requests.get(
                    url,
                    headers={
                        "Accept": "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Referer": referer,
                    },
                    cookies=cookie_jar,
                    impersonate=impersonation,
                    timeout=TIMEOUT_SECONDS,
                    allow_redirects=True,
                )
            except Exception as exc:
                last_detail = f"{impersonation}:{type(exc).__name__}"
                time.sleep(DELAY_SECONDS)
                continue

            content_type = response.headers.get("content-type", "")
            body = response.content
            if response.status_code == 200 and body[:5] == b"%PDF-":
                return body, impersonation, attempts

            last_detail = f"{impersonation}:http_{response.status_code}:{content_type}"
            time.sleep(DELAY_SECONDS)

    return None, last_detail, attempts


def main(argv: list[str]) -> int:
    limit = None
    if len(argv) > 1:
        try:
            limit = int(argv[1])
        except ValueError:
            print("Usage: download_elcc_real_pdf.py [limit]", file=sys.stderr)
            return 2

    OUTPUT_DIR.mkdir(exist_ok=True)
    items = load_items()
    if limit is not None:
        items = items[:limit]

    results: list[Result] = []
    cookies = load_cookies()

    print(f"Downloading {len(items)} real PDFs into {OUTPUT_DIR}")

    for idx, item in enumerate(items, 1):
        raw_pii = item["pii"]
        out_path = OUTPUT_DIR / f"PII{raw_pii}.pdf"
        pdf_bytes, detail, attempts = download_pdf(raw_pii, cookies)

        if pdf_bytes is not None:
            out_path.write_bytes(pdf_bytes)
            result = Result(
                pii=raw_pii,
                doi=item["doi"],
                title=item["title"],
                status="ok",
                attempts=attempts,
                file=str(out_path),
                detail=detail,
            )
            print(f"[{idx}/{len(items)}] OK   {raw_pii} attempts={attempts} via={detail} size={len(pdf_bytes)}")
        else:
            result = Result(
                pii=raw_pii,
                doi=item["doi"],
                title=item["title"],
                status="failed",
                attempts=attempts,
                file=str(out_path),
                detail=detail,
            )
            print(f"[{idx}/{len(items)}] FAIL {raw_pii} attempts={attempts} detail={detail}")

        results.append(result)
        if idx % 25 == 0:
            MANIFEST_FILE.write_text(
                json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        time.sleep(DELAY_SECONDS)

    MANIFEST_FILE.write_text(
        json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    ok_count = sum(1 for r in results if r.status == "ok")
    fail_count = len(results) - ok_count
    print(f"\nSummary ok={ok_count} failed={fail_count} manifest={MANIFEST_FILE}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
