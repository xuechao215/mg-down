#!/usr/bin/env python3
"""Generate ELCC PDFs from mirrored article pages with metadata fallback."""

from __future__ import annotations

import html
import json
import re
import subprocess
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, Preformatted, SimpleDocTemplate, Spacer


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "ELCC"
MAP_FILE = BASE_DIR / "elcc_pii_map.json"
MANIFEST_FILE = OUTPUT_DIR / "download_manifest.json"
FONT_PATH = Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf")
FONT_NAME = "ArialUnicode"

MIRROR_URL = "https://r.jina.ai/http://doi.org/{doi}"
CROSSREF_URL = "https://api.crossref.org/works/{doi}"

REQUEST_TIMEOUT = 90
MAX_ATTEMPTS = 8
MAX_WORKERS = 3

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

SECURITY_MARKERS = (
    "Performing security verification",
    "Enable JavaScript and cookies to continue",
    "Title: Just a moment...",
    "Warning: Target URL returned error 403",
)

STOP_HEADERS = (
    "## Related articles",
    "## Related Articles",
    "## Recommended articles",
    "## Recommended Articles",
    "## References",
    "## Cited by",
)


@dataclass
class DownloadResult:
    pii: str
    doi: str
    title: str
    status: str
    source: str
    path: str
    attempts: int
    detail: str = ""


PREVIOUS_RESULTS: dict[str, dict[str, Any]] = {}


def register_fonts() -> None:
    if FONT_NAME not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont(FONT_NAME, str(FONT_PATH)))


def styles() -> dict[str, ParagraphStyle]:
    register_fonts()
    sample = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "ELCCTitle",
            parent=sample["Title"],
            fontName=FONT_NAME,
            fontSize=17,
            leading=22,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#10253f"),
            spaceAfter=14,
        ),
        "meta": ParagraphStyle(
            "ELCCMeta",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#4d6278"),
            spaceAfter=10,
        ),
        "note": ParagraphStyle(
            "ELCCNote",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=10,
            leading=14,
            backColor=colors.HexColor("#eef4fb"),
            borderPadding=8,
            borderRadius=None,
            borderWidth=0,
            textColor=colors.HexColor("#24415f"),
            spaceAfter=12,
        ),
        "h1": ParagraphStyle(
            "ELCCH1",
            parent=sample["Heading1"],
            fontName=FONT_NAME,
            fontSize=15,
            leading=18,
            textColor=colors.HexColor("#143a59"),
            spaceBefore=10,
            spaceAfter=8,
        ),
        "h2": ParagraphStyle(
            "ELCCH2",
            parent=sample["Heading2"],
            fontName=FONT_NAME,
            fontSize=12,
            leading=15,
            textColor=colors.HexColor("#204f73"),
            spaceBefore=8,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "ELCCBody",
            parent=sample["BodyText"],
            fontName=FONT_NAME,
            fontSize=10,
            leading=14,
            textColor=colors.black,
            spaceAfter=6,
        ),
        "table": ParagraphStyle(
            "ELCCTable",
            parent=sample["Code"],
            fontName=FONT_NAME,
            fontSize=8.2,
            leading=10,
            textColor=colors.black,
            leftIndent=6,
            spaceAfter=8,
        ),
    }


def fetch_text(url: str, timeout: int = REQUEST_TIMEOUT, accept: str | None = None) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept or "text/plain,text/html;q=0.9,*/*;q=0.8",
    }
    request = Request(url, headers=headers)
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset("utf-8")
        return response.read().decode(charset, "replace")


def doi_suffix(value: str) -> str:
    cleaned = (value or "").strip()
    prefixes = (
        "https://doi.org/",
        "http://doi.org/",
        "doi:",
    )
    for prefix in prefixes:
        if cleaned.lower().startswith(prefix):
            return cleaned[len(prefix):]
    return cleaned


def normalize(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value or "")).strip().lower()


def looks_like_security_page(text: str) -> bool:
    return any(marker in text for marker in SECURITY_MARKERS)


def looks_like_article(text: str, expected_title: str) -> bool:
    if looks_like_security_page(text):
        return False
    body = normalize(text)
    if normalize(expected_title) in body:
        return True
    return any(
        marker in text
        for marker in (
            "## Background",
            "## Methods",
            "## Results",
            "## Conclusions",
            "## Patients and methods",
            "## Introduction",
            "## Materials and methods",
        )
    )


def fetch_mirror_page(doi: str, title: str, max_attempts: int = MAX_ATTEMPTS) -> tuple[str | None, int]:
    url = MIRROR_URL.format(doi=doi_suffix(doi))
    last_text = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", "60", url],
                capture_output=True,
                text=True,
                check=True,
            )
            text = result.stdout
            last_text = text
            if looks_like_article(text, title):
                return text, attempt
        except (subprocess.SubprocessError, UnicodeDecodeError):
            pass
        time.sleep(min(attempt, 3))
    return last_text, max_attempts


def fetch_crossref_metadata(doi: str) -> dict[str, Any]:
    url = CROSSREF_URL.format(doi=quote(doi_suffix(doi), safe=""))
    try:
        raw = fetch_text(url, timeout=30, accept="application/json")
        payload = json.loads(raw)
        return payload.get("message", {})
    except Exception:
        return {}


def strip_markdown(line: str) -> str:
    line = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", line)
    line = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line)
    line = re.sub(r"<[^>]+>", "", line)
    line = html.unescape(line)
    line = line.replace("\xa0", " ")
    line = re.sub(r"\s+", " ", line)
    return line.strip()


def extract_markdown_content(raw_text: str) -> str:
    if "Markdown Content:" not in raw_text:
        return raw_text.strip()
    return raw_text.split("Markdown Content:", 1)[1].strip()


def find_start(lines: list[str], title: str) -> int:
    for idx, line in enumerate(lines):
        if line.startswith("Abstract[") or line.startswith("Abstract "):
            return idx
    exact_title = f"# {title}"
    for idx, line in enumerate(lines):
        if strip_markdown(line) == title:
            return idx
        if line.strip() == exact_title:
            return idx
    return 0


def find_stop(lines: list[str], start: int) -> int:
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip() in STOP_HEADERS:
            return idx
    return len(lines)


def clean_content_block(markdown: str, title: str) -> str:
    lines = [line.rstrip() for line in markdown.splitlines()]
    start = find_start(lines, title)
    stop = find_stop(lines, start)

    cleaned: list[str] = []
    for raw in lines[start:stop]:
        line = strip_markdown(raw)
        if not line:
            if cleaned and cleaned[-1] != "":
                cleaned.append("")
            continue

        if line in {"Share on", "Show Outline Hide Outline", "Access provided by"}:
            continue
        if line.startswith(("Open GPT Console", "Open Oracle Keywords", "Refresh Values")):
            continue
        if line.startswith(("Please enter a term before submitting your search", "Main menu")):
            continue

        cleaned.append(line)

    while cleaned and not cleaned[-1]:
        cleaned.pop()
    return "\n".join(cleaned).strip()


def build_metadata_fallback(item: dict[str, Any], crossref: dict[str, Any], attempts: int) -> str:
    authors = [
        " ".join(part for part in (author.get("given"), author.get("family")) if part)
        for author in crossref.get("author", [])
    ]
    lines = [
        f"# {item['title']}",
        "",
        "## Metadata Fallback",
        (
            f"The native publisher PDF and mirrored full text could not be retrieved automatically "
            f"after {attempts} attempts from this environment. This PDF preserves the verified metadata "
            "that could be recovered programmatically."
        ),
        "",
        f"DOI: {item['doi']}",
        f"PII: {item['pii']}",
    ]

    journal = first_value(crossref.get("container-title"))
    if journal:
        lines.append(f"Journal: {journal}")

    issued = crossref.get("issued", {}).get("date-parts", [[]])
    if issued and issued[0]:
        lines.append("Published: " + "-".join(str(part) for part in issued[0]))

    if crossref.get("volume"):
        lines.append(f"Volume: {crossref['volume']}")
    if crossref.get("page"):
        lines.append(f"Page / Article number: {crossref['page']}")
    elif crossref.get("article-number"):
        lines.append(f"Article number: {crossref['article-number']}")

    if authors:
        lines.extend(["", "## Authors", "; ".join(authors)])

    license_urls = [entry.get("URL") for entry in crossref.get("license", []) if entry.get("URL")]
    if license_urls:
        lines.extend(["", "## License", license_urls[0]])

    return "\n".join(lines).strip()


def first_value(value: Any) -> str:
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def split_blocks(text: str) -> list[tuple[str, str]]:
    lines = text.splitlines()
    blocks: list[tuple[str, str]] = []
    paragraph: list[str] = []
    table: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            blocks.append(("paragraph", " ".join(paragraph).strip()))
            paragraph = []

    def flush_table() -> None:
        nonlocal table
        if table:
            blocks.append(("table", "\n".join(table).strip()))
            table = []

    for line in lines:
        if not line.strip():
            flush_paragraph()
            flush_table()
            continue

        if line.startswith("# "):
            flush_paragraph()
            flush_table()
            blocks.append(("h1", line[2:].strip()))
            continue

        if line.startswith("## "):
            flush_paragraph()
            flush_table()
            blocks.append(("h2", line[3:].strip()))
            continue

        if "|" in line and line.count("|") >= 2:
            flush_paragraph()
            table.append(line)
            continue

        if table:
            flush_table()

        if line.startswith(("* ", "- ")):
            flush_paragraph()
            blocks.append(("paragraph", "\u2022 " + line[2:].strip()))
            continue

        paragraph.append(line.strip())

    flush_paragraph()
    flush_table()
    return blocks


def write_pdf(path: Path, item: dict[str, Any], body_text: str, source: str, attempts: int) -> None:
    sheet = styles()
    doc = SimpleDocTemplate(
        str(path),
        pagesize=A4,
        leftMargin=42,
        rightMargin=42,
        topMargin=44,
        bottomMargin=44,
        title=item["title"],
        author="Codex ELCC downloader",
    )

    source_label = {
        "mirror": "Generated from mirrored article text",
        "metadata_only": "Generated from metadata fallback",
        "existing_native": "Native publisher PDF",
    }.get(source, source)

    story = [
        Paragraph(html.escape(item["title"]), sheet["title"]),
        Paragraph(
            "<br/>".join(
                html.escape(line)
                for line in (
                    f"DOI: {item['doi']}",
                    f"PII: {item['pii']}",
                    f"Source: {source_label}",
                    f"Fetch attempts: {attempts}",
                )
            ),
            sheet["meta"],
        ),
    ]

    if source == "metadata_only":
        story.append(
            Paragraph(
                html.escape(
                    "Publisher access checks kept hitting automated protection from this environment, "
                    "so this file preserves the best verified metadata we could recover automatically."
                ),
                sheet["note"],
            )
        )

    for block_type, block in split_blocks(body_text):
        if block_type == "h1":
            story.append(Paragraph(html.escape(block), sheet["h1"]))
        elif block_type == "h2":
            story.append(Paragraph(html.escape(block), sheet["h2"]))
        elif block_type == "table":
            story.append(Preformatted(block, sheet["table"]))
        else:
            story.append(Paragraph(html.escape(block), sheet["body"]))

    doc.build(story)


def process_item(item: dict[str, Any]) -> DownloadResult:
    path = OUTPUT_DIR / f"PII{item['pii']}.pdf"
    previous = PREVIOUS_RESULTS.get(item["pii"], {})
    should_retry_existing = previous.get("source") == "metadata_only"
    should_reprocess_untracked = bool(PREVIOUS_RESULTS) and not previous

    if (
        path.exists()
        and path.stat().st_size > 1024
        and not should_retry_existing
        and not should_reprocess_untracked
    ):
        return DownloadResult(
            pii=item["pii"],
            doi=item["doi"],
            title=item["title"],
            status="skipped",
            source=previous.get("source", "existing_native"),
            path=str(path),
            attempts=0,
            detail=previous.get("detail", "already_exists"),
        )

    raw_text, attempts = fetch_mirror_page(item["doi"], item["title"])
    if raw_text and looks_like_article(raw_text, item["title"]):
        cleaned = clean_content_block(extract_markdown_content(raw_text), item["title"])
        if cleaned and len(cleaned) >= 250:
            write_pdf(path, item, cleaned, "mirror", attempts)
            return DownloadResult(
                pii=item["pii"],
                doi=item["doi"],
                title=item["title"],
                status="generated",
                source="mirror",
                path=str(path),
                attempts=attempts,
                detail="fulltext",
            )

    crossref = fetch_crossref_metadata(item["doi"])
    fallback = build_metadata_fallback(item, crossref, attempts)
    write_pdf(path, item, fallback, "metadata_only", attempts)
    return DownloadResult(
        pii=item["pii"],
        doi=item["doi"],
        title=item["title"],
        status="generated",
        source="metadata_only",
        path=str(path),
        attempts=attempts,
        detail="fallback",
    )


def load_items() -> list[dict[str, Any]]:
    with MAP_FILE.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_manifest(results: list[DownloadResult]) -> None:
    payload = [
        {
            "pii": result.pii,
            "doi": result.doi,
            "title": result.title,
            "status": result.status,
            "source": result.source,
            "path": result.path,
            "attempts": result.attempts,
            "detail": result.detail,
        }
        for result in results
    ]
    MANIFEST_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_previous_manifest() -> dict[str, dict[str, Any]]:
    if not MANIFEST_FILE.exists():
        return {}
    try:
        payload = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {entry["pii"]: entry for entry in payload if entry.get("pii")}


def main(argv: list[str]) -> int:
    global PREVIOUS_RESULTS
    OUTPUT_DIR.mkdir(exist_ok=True)
    PREVIOUS_RESULTS = load_previous_manifest()

    limit = None
    if len(argv) > 1:
        try:
            limit = int(argv[1])
        except ValueError:
            print("Usage: download_elcc_mirror.py [limit]", file=sys.stderr)
            return 2

    items = load_items()
    if limit is not None:
        items = items[:limit]

    results: list[DownloadResult] = []
    print(f"Processing {len(items)} ELCC records into {OUTPUT_DIR}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_item, item): item for item in items}
        for idx, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            print(
                f"[{idx}/{len(items)}] {result.status.upper():9s} "
                f"{result.source:14s} {result.pii} attempts={result.attempts}"
            )
            if idx % 25 == 0:
                save_manifest(results)

    results.sort(key=lambda entry: entry.pii)
    save_manifest(results)

    source_counts: dict[str, int] = {}
    for result in results:
        source_counts[result.source] = source_counts.get(result.source, 0) + 1

    print("\nSummary")
    for source, count in sorted(source_counts.items()):
        print(f"  {source:14s} {count}")
    print(f"Manifest: {MANIFEST_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
