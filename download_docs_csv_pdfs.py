#!/usr/bin/env python3
"""Download real PDFs for docs-csv rows and update CSV download statuses."""

from __future__ import annotations

import csv
import html
import io
import json
import math
import re
import sys
import time
import warnings
import zlib
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import quote, unquote, urljoin, urlparse

warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
)

BASE_DIR = Path(__file__).resolve().parent
VENDOR_DIR = BASE_DIR / ".vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

import requests as std_requests

try:
    import browser_cookie3
except Exception:
    browser_cookie3 = None

try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


DOCS_DIR = BASE_DIR / "docs-csv"
REQUEST_TIMEOUT = 25
RETRY_COUNT = 2
DELAY_SECONDS = 0.2
MAX_FILENAME_LEN = 180
HTML_FETCH_TIMEOUT = 20

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
PDF_ACCEPT = "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

ADVANCED_IMPERSONATIONS = ("chrome124", "safari184")

PUBMED_HOSTS = {
    "pubmed.ncbi.nlm.nih.gov",
    "www.ncbi.nlm.nih.gov",
}

BLOCK_PAGE_MARKERS = (
    "cloudpmc-viewer-pow",
    "Preparing to download ...",
    "enable javascript",
    "just a moment",
    "checking if the site connection is secure",
    "captcha",
)

CROSSREF_API = "https://api.crossref.org/works/{doi}"
OPENALEX_API = "https://api.openalex.org/works/https://doi.org/{doi}"

PDF_PATTERNS = (
    ".pdf",
    "/pdf",
    "/pdf/",
    "downloadpdf",
    "showpdf",
    "pdfft",
    "articlepdf",
    "/epdf/",
    "/doi/pdf/",
    "pdfdirect",
)

RAW_URL_PATTERNS = (
    r'"citation_pdf_url"\s*:\s*"([^"]+)"',
    r'"(?:pdfUrl|pdfURL|articlePdfUrl|downloadPdfUrl|pdfPath|pdfLink|downloadUrl)"\s*:\s*"([^"]+)"',
    r"(https?:\\/\\/[^\"'<>]+?\\.pdf(?:\\?[^\"'<>]+)?)",
    r"(https?://[^\"'<>]+?\\.pdf(?:\\?[^\"'<>]+)?)",
)

PDF_SUPPLEMENT_URL_MARKERS = (
    "supplement",
    "supplementary",
    "supporting-information",
    "supporting_information",
    "mediaobjects",
    "moesm",
    "_esm",
    "/esm",
    "/suppl/",
    "suppl_",
    "_suppl",
    "downloadasset/suppl",
    "appendix",
)

PDF_RESOURCE_URL_MARKERS = (
    "patient-resources",
    "patient-education",
    "/brochure",
    "/brochures/",
)

PDF_SUPPLEMENT_TEXT_MARKERS = (
    "supplementary material",
    "supplementary appendix",
    "supplementary methods",
    "supplementary table",
    "supplementary figure",
    "supporting information",
    "prisma checklist",
)

PDF_RESOURCE_TEXT_MARKERS = (
    "patient education",
    "patient information series",
    "not intended as a substitute for professional medical advice",
    "american brain tumor association",
)

TITLE_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "were",
    "have",
    "has",
    "had",
    "into",
    "onto",
    "their",
    "there",
    "than",
    "then",
    "also",
    "using",
    "used",
    "use",
    "among",
    "between",
    "after",
    "before",
    "during",
    "through",
    "within",
    "without",
    "systematic",
    "review",
    "meta",
    "analysis",
    "cancer",
    "lung",
    "patients",
    "study",
    "studies",
    "phase",
    "trial",
    "trials",
    "advanced",
    "non",
    "small",
    "cell",
    "disease",
    "risk",
    "based",
    "predicting",
    "predictive",
    "efficacy",
    "safety",
    "treatment",
    "results",
    "brief",
    "report",
    "early",
    "stage",
    "insights",
    "approaches",
}

DOI_REGEX = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", flags=re.IGNORECASE)
ASCII_FRAGMENT_REGEX = re.compile(rb"[A-Za-z0-9][A-Za-z0-9\-:/._(),% ]{20,}")
FLATE_STREAM_REGEX = re.compile(rb"<<.*?/Filter\s*/FlateDecode.*?>>\s*stream\r?\n", flags=re.DOTALL)


@dataclass
class DownloadResult:
    success: bool
    pdf_url: str = ""
    detail: str = ""


COOKIE_CACHE: dict[str, object | None] = {}
CROSSREF_CACHE: dict[str, dict] = {}
OPENALEX_CACHE: dict[str, dict] = {}


class HtmlLinkParser(HTMLParser):
    """Collect relevant attributes from HTML without external parsers."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.meta: dict[str, str] = {}
        self.attr_values: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs if value}

        href = attr_map.get("href")
        if href:
            self.links.append(href)

        content = attr_map.get("content")
        if content:
            key = (attr_map.get("name") or attr_map.get("property") or "").lower()
            if key:
                self.meta[key] = content

        for value in attr_map.values():
            if isinstance(value, str):
                self.attr_values.append(value)


def safe_filename(name: str) -> str:
    keepchars = (" ", "-", "_", ".", "(", ")")
    cleaned = "".join(c if c.isalnum() or c in keepchars else "_" for c in (name or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ._")
    cleaned = cleaned[:MAX_FILENAME_LEN].strip(" ._")
    return cleaned or "untitled"


def load_rows(csv_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    return rows, fieldnames


def save_rows(csv_path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def is_pdf_bytes(content: bytes, content_type: str = "") -> bool:
    prefix = content[:5]
    if prefix == b"%PDF-":
        return True
    return "application/pdf" in (content_type or "").lower() and content[:1] == b"%"


def normalized_text(value: str) -> str:
    return html.unescape((value or "").replace("\\/", "/")).strip()


def pick_source_url(row: dict[str, str]) -> str:
    second_link = (row.get("second_link") or "").strip()
    if second_link:
        return second_link

    doi = (row.get("DOI") or "").strip()
    if not doi:
        return ""
    if doi.startswith("http://") or doi.startswith("https://"):
        return doi
    return f"https://doi.org/{doi}"


def output_dir_for(csv_path: Path) -> Path:
    return csv_path.with_suffix("").with_name(f"{csv_path.stem}_pdfs")


def output_path_for(csv_path: Path, row: dict[str, str]) -> Path:
    return output_dir_for(csv_path) / f"{safe_filename(row_title(row))}.pdf"


def valid_existing_pdf(path: Path, row: dict[str, str], source_url: str) -> bool:
    verdict, _ = validate_existing_pdf(path, row, source_url)
    return verdict == "match"


def response_to_text(response) -> str:
    response.encoding = getattr(response, "encoding", None) or getattr(response, "apparent_encoding", None) or "utf-8"
    return response.text


def looks_like_block_page(text: str) -> bool:
    lowered = (text or "").lower()
    return any(marker.lower() in lowered for marker in BLOCK_PAGE_MARKERS)


def normalized_match_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def normalized_alnum(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def normalized_doi(value: str) -> str:
    return re.sub(r"\s+", "", doi_suffix(value).lower().strip(" .;,)("))


def row_title(row: dict[str, str]) -> str:
    return (row.get("Title") or row.get("title") or "").strip()


def title_significant_terms(title: str) -> list[str]:
    terms: list[str] = []
    for token in normalized_match_text(title).split():
        if len(token) < 4 or token.isdigit() or token in TITLE_STOPWORDS:
            continue
        if token not in terms:
            terms.append(token)
    return terms[:12]


def expected_pmid(row: dict[str, str]) -> str:
    for key in ("PMID", "pmid"):
        value = (row.get(key) or "").strip()
        if value.isdigit():
            return value

    match = re.search(r"/([0-9]+)/?$", (row.get("pubmed_url") or "").strip())
    return match.group(1) if match else ""


def expected_pmcid(row: dict[str, str]) -> str:
    value = (row.get("PMCID") or row.get("pmcid") or "").strip()
    if not value:
        match = re.search(r"(PMC\d+)", row.get("pubmed_url") or "", flags=re.IGNORECASE)
        value = match.group(1) if match else ""
    if value and not value.upper().startswith("PMC"):
        value = f"PMC{value}"
    return value.upper()


def root_domains(host: str) -> list[str]:
    host = (host or "").lower()
    if not host:
        return []

    parts = host.split(".")
    domains = [host]
    if len(parts) >= 2:
        domains.append(".".join(parts[-2:]))
    if len(parts) >= 3:
        domains.append(".".join(parts[-3:]))

    if host.endswith("sciencedirect.com") or host.endswith("elsevier.com") or host.endswith("thelancet.com"):
        domains.extend(["sciencedirect.com", "elsevier.com", "thelancet.com"])
    if host.endswith("pmc.ncbi.nlm.nih.gov") or host.endswith("pubmed.ncbi.nlm.nih.gov"):
        domains.extend(["pmc.ncbi.nlm.nih.gov", "pubmed.ncbi.nlm.nih.gov", "ncbi.nlm.nih.gov"])

    return unique_urls(domains)


def load_cookie_jar(url: str):
    if browser_cookie3 is None:
        return None

    host = urlparse(url).netloc.lower()
    for domain in root_domains(host):
        if domain not in COOKIE_CACHE:
            try:
                COOKIE_CACHE[domain] = browser_cookie3.chrome(domain_name=domain)
            except Exception:
                COOKIE_CACHE[domain] = None
        jar = COOKIE_CACHE[domain]
        if jar is not None:
            return jar
    return None


def request_with_retries(
    session: std_requests.Session,
    url: str,
    *,
    accept: str,
    referer: str = "",
    timeout: int = REQUEST_TIMEOUT,
):
    last_error: Exception | None = None
    headers = {"Accept": accept}
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            response = session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            )
            return response
        except std_requests.RequestException as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                time.sleep(min(attempt, 2))

    assert last_error is not None
    raise last_error


def advanced_request(url: str, *, accept: str, referer: str = "", timeout: int = REQUEST_TIMEOUT):
    if curl_requests is None:
        return None

    headers = {
        "Accept": accept,
        "User-Agent": USER_AGENT,
    }
    if referer:
        headers["Referer"] = referer

    cookie_jar = load_cookie_jar(url)

    for impersonation in ADVANCED_IMPERSONATIONS:
        try:
            return curl_requests.get(
                url,
                headers=headers,
                cookies=cookie_jar,
                impersonate=impersonation,
                allow_redirects=True,
                timeout=timeout,
            )
        except Exception:
            continue
    return None


def fetch_html_response(
    session: std_requests.Session,
    url: str,
    *,
    referer: str = "",
):
    response = None
    try:
        response = request_with_retries(
            session,
            url,
            accept=HTML_ACCEPT,
            referer=referer,
            timeout=HTML_FETCH_TIMEOUT,
        )
    except std_requests.RequestException:
        response = None

    if response is not None and response.status_code < 400:
        if is_pdf_bytes(response.content, response.headers.get("content-type", "")):
            return response
        text = response_to_text(response)
        if not looks_like_block_page(text):
            return response

    advanced = advanced_request(url, accept=HTML_ACCEPT, referer=referer, timeout=HTML_FETCH_TIMEOUT)
    return advanced or response


def fetch_metadata_json(url: str) -> dict:
    response = advanced_request(url, accept="application/json") or std_requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    if not response or response.status_code >= 400:
        return {}
    try:
        return json.loads(response_to_text(response))
    except Exception:
        return {}


def unique_urls(urls: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw_url in urls:
        url = normalized_text(raw_url)
        if not url or url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def extract_pii(value: str) -> str:
    match = re.search(r"/pii/([A-Z0-9\-()]+)", value or "", re.IGNORECASE)
    if match:
        return re.sub(r"[^A-Za-z0-9]", "", match.group(1))

    match = re.search(r"(S\d{4,}[A-Z0-9]*)", value or "", re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return ""


def expected_pii(row: dict[str, str]) -> str:
    for candidate in (
        row.get("second_link") or "",
        row.get("DOI") or "",
        row.get("pdf_url") or "",
    ):
        pii = extract_pii(candidate)
        if pii:
            return pii
    return ""


def url_contains_row_identifiers(url: str, row: dict[str, str]) -> bool:
    raw = normalized_text(unquote(url or "")).lower()
    alnum = normalized_alnum(raw)

    doi = normalized_doi(row.get("DOI") or "")
    if doi and (
        doi in raw
        or doi.replace("/", "%2f") in raw
        or doi.replace("/", "") in alnum
    ):
        return True

    pii = expected_pii(row)
    if pii and pii.lower() in alnum:
        return True

    pmid = expected_pmid(row)
    if pmid and pmid in raw:
        return True

    pmcid = expected_pmcid(row)
    if pmcid and pmcid.lower() in raw:
        return True

    return False


def url_looks_supplementary(url: str) -> bool:
    lowered = normalized_text(unquote(url or "")).lower()
    return any(marker in lowered for marker in PDF_SUPPLEMENT_URL_MARKERS)


def url_looks_resource_pdf(url: str) -> bool:
    lowered = normalized_text(unquote(url or "")).lower()
    return any(marker in lowered for marker in PDF_RESOURCE_URL_MARKERS)


def extract_ascii_fragments(blob: bytes, *, limit: int) -> list[str]:
    fragments: list[str] = []
    for match in ASCII_FRAGMENT_REGEX.finditer(blob):
        fragments.append(match.group().decode("latin1", "ignore"))
        if len(fragments) >= limit:
            break
    return fragments


def raw_pdf_text_fallback(content: bytes) -> str:
    parts = extract_ascii_fragments(content, limit=1500)

    for match in FLATE_STREAM_REGEX.finditer(content):
        start = match.end()
        end = content.find(b"endstream", start)
        if end == -1:
            continue
        stream = content[start:end].strip(b"\r\n")
        if not stream:
            continue
        try:
            decoded = zlib.decompress(stream)
        except Exception:
            continue
        parts.extend(extract_ascii_fragments(decoded, limit=500))
        if len(parts) >= 3000:
            break

    return "\n".join(parts)


def extract_pdf_text(content: bytes) -> tuple[str, list[str]]:
    page_texts: list[str] = []
    pieces: list[str] = []

    if PdfReader is not None:
        try:
            reader = PdfReader(io.BytesIO(content))
            metadata = reader.metadata or {}
            for key in ("/Title", "/Subject", "/Author", "/Keywords"):
                value = metadata.get(key)
                if value:
                    pieces.append(str(value))

            for page in reader.pages[:3]:
                try:
                    text = page.extract_text() or ""
                except Exception:
                    text = ""
                page_texts.append(text)
                if text:
                    pieces.append(text)
        except Exception:
            page_texts = []

    combined = "\n".join(pieces)
    if len(normalized_match_text(combined)) < 80:
        fallback = raw_pdf_text_fallback(content)
        combined = f"{combined}\n{fallback}".strip()

    return combined, page_texts


def validate_pdf_for_row(
    content: bytes,
    row: dict[str, str],
    source_url: str,
) -> tuple[str, str]:
    if url_looks_supplementary(source_url):
        return "mismatch", "supplementary_url"
    if url_looks_resource_pdf(source_url):
        return "mismatch", "resource_url"

    combined_text, page_texts = extract_pdf_text(content)
    first_page_text = page_texts[0] if page_texts else combined_text
    normalized_first_page = normalized_match_text(first_page_text[:2000])

    if any(marker in normalized_first_page for marker in PDF_SUPPLEMENT_TEXT_MARKERS):
        return "mismatch", "supplementary_text"
    if any(marker in normalized_first_page for marker in PDF_RESOURCE_TEXT_MARKERS):
        return "mismatch", "resource_text"

    expected_doi = normalized_doi(row.get("DOI") or "")
    detected_dois = {normalized_doi(match) for match in DOI_REGEX.findall(combined_text)}
    if expected_doi and detected_dois:
        if expected_doi in detected_dois:
            return "match", "doi_match"
        return "mismatch", "doi_mismatch"

    if url_contains_row_identifiers(source_url, row):
        return "match", "url_id_match"

    normalized_full_text = normalized_match_text(combined_text)
    title_terms = title_significant_terms(row_title(row))
    matches = [term for term in title_terms if term in normalized_full_text]
    if title_terms:
        required_matches = max(3, math.ceil(min(len(title_terms), 6) * 0.5))
        if len(matches) >= required_matches:
            return "match", f"title_match_{len(matches)}"

    if "microsoft powerpoint" in normalized_first_page or "layer 1" in normalized_first_page:
        return "mismatch", "wrong_document_text"

    return "unknown", "article_match_missing"


def validate_existing_pdf(
    path: Path,
    row: dict[str, str],
    source_url: str,
) -> tuple[str, str]:
    if not path.exists() or path.stat().st_size < 1024:
        return "mismatch", "missing_or_small"

    try:
        content = path.read_bytes()
    except OSError:
        return "mismatch", "read_failed"

    if not is_pdf_bytes(content):
        return "mismatch", "not_pdf_bytes"

    return validate_pdf_for_row(content, row, source_url)


def quarantine_invalid_pdf(csv_path: Path, path: Path) -> Path:
    quarantine_dir = output_dir_for(csv_path) / ".invalid"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    target = quarantine_dir / path.name
    counter = 1
    while target.exists():
        target = quarantine_dir / f"{path.stem}.{counter}{path.suffix}"
        counter += 1

    path.rename(target)
    return target


def audit_existing_success_rows(
    csv_path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
) -> int:
    reset_count = 0

    for row in rows:
        status = (row.get("download_status") or "").strip().lower()
        if status != "success":
            continue

        output_path = output_path_for(csv_path, row)
        verdict, reason = validate_existing_pdf(output_path, row, row.get("pdf_url") or "")
        if verdict == "match":
            continue

        if output_path.exists():
            quarantine_invalid_pdf(csv_path, output_path)

        row["download_status"] = "failed"
        row["download_error"] = f"invalid_existing_pdf:{reason}"
        row["pdf_url"] = ""
        reset_count += 1

    if reset_count:
        save_rows(csv_path, rows, fieldnames)

    return reset_count


def doi_suffix(value: str) -> str:
    cleaned = (value or "").strip()
    prefixes = (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "doi:",
    )
    lowered = cleaned.lower()
    for prefix in prefixes:
        if lowered.startswith(prefix):
            return cleaned[len(prefix):]
    return cleaned


def doi_url_from_row(row: dict[str, str]) -> str:
    doi = doi_suffix(row.get("DOI") or "")
    return f"https://doi.org/{doi}" if doi else ""


def fetch_crossref_metadata(row: dict[str, str]) -> dict:
    doi = doi_suffix(row.get("DOI") or "")
    if not doi:
        return {}
    if doi not in CROSSREF_CACHE:
        payload = fetch_metadata_json(CROSSREF_API.format(doi=quote(doi, safe="")))
        CROSSREF_CACHE[doi] = payload.get("message", {}) if payload else {}
    return CROSSREF_CACHE[doi]


def fetch_openalex_metadata(row: dict[str, str]) -> dict:
    doi = doi_suffix(row.get("DOI") or "")
    if not doi:
        return {}
    if doi not in OPENALEX_CACHE:
        OPENALEX_CACHE[doi] = fetch_metadata_json(OPENALEX_API.format(doi=quote(doi, safe="")))
    return OPENALEX_CACHE[doi] or {}


def build_source_urls(row: dict[str, str]) -> list[str]:
    sources: list[str] = []

    second_link = (row.get("second_link") or "").strip()
    pubmed_url = (row.get("pubmed_url") or "").strip()
    pdf_url = (row.get("pdf_url") or "").strip()

    if second_link:
        sources.append(second_link)

    doi_url = doi_url_from_row(row)
    if doi_url:
        sources.append(doi_url)

    if pubmed_url:
        sources.append(pubmed_url)

    if pdf_url and not url_looks_supplementary(pdf_url):
        sources.append(pdf_url)

    crossref = fetch_crossref_metadata(row)
    if crossref.get("URL"):
        sources.append(crossref["URL"])
    for link in crossref.get("link", []) or []:
        url = link.get("URL")
        if url and not url_looks_supplementary(url):
            sources.append(url)

    openalex = fetch_openalex_metadata(row)
    best_oa = openalex.get("best_oa_location") or {}
    open_access = openalex.get("open_access") or {}
    for candidate in (
        best_oa.get("pdf_url"),
        best_oa.get("landing_page_url"),
        open_access.get("oa_url"),
    ):
        if candidate and not url_looks_supplementary(candidate):
            sources.append(candidate)

    return unique_urls(sources)


def collect_pubmed_source_urls(
    row: dict[str, str],
    start_url: str,
    final_url: str,
    page_text: str,
) -> list[str]:
    host = urlparse(final_url or start_url).netloc.lower()
    if host not in PUBMED_HOSTS:
        return []

    parser = HtmlLinkParser()
    try:
        parser.feed(page_text)
    except Exception:
        return []

    sources: list[str] = []
    for href in parser.links:
        absolute = urljoin(final_url or start_url, href)
        parsed = urlparse(absolute)
        if not parsed.scheme.startswith("http"):
            continue
        target_host = parsed.netloc.lower()
        if target_host in PUBMED_HOSTS:
            continue
        if not target_host:
            continue
        if url_looks_supplementary(absolute) or url_looks_resource_pdf(absolute):
            continue
        if url_looks_pdfish(absolute) and not url_contains_row_identifiers(absolute, row):
            continue
        sources.append(absolute)

    return unique_urls(sources)


def collect_candidate_urls(
    row: dict[str, str],
    start_url: str,
    final_url: str,
    page_text: str,
) -> list[str]:
    parser = HtmlLinkParser()
    try:
        parser.feed(page_text)
    except Exception:
        pass

    candidates: list[str] = []

    for key in ("citation_pdf_url", "wkhealth_pdf_url", "dc.identifier.pdf", "og:pdf"):
        if parser.meta.get(key):
            candidates.append(parser.meta[key])

    for pattern in RAW_URL_PATTERNS:
        for match in re.findall(pattern, page_text, flags=re.IGNORECASE):
            candidates.append(match)

    fallback_pdf_url = (row.get("pdf_url") or "").strip()
    if fallback_pdf_url:
        candidates.append(fallback_pdf_url)

    for value in parser.links + parser.attr_values:
        lowered = normalized_text(value).lower()
        if any(token in lowered for token in PDF_PATTERNS):
            candidates.append(value)

    doi = doi_suffix(row.get("DOI") or "")
    host = urlparse(final_url or start_url).netloc.lower()
    pii = extract_pii(final_url) or extract_pii(start_url)
    if not pii:
        pii_match = re.search(r'"pii"\s*:\s*"([^"]+)"', page_text, flags=re.IGNORECASE)
        if pii_match:
            pii = re.sub(r"[^A-Za-z0-9]", "", pii_match.group(1)).upper()
    if not pii:
        pii_match = re.search(r"/science/article/pii/([A-Z0-9\-()]+)", page_text, flags=re.IGNORECASE)
        if pii_match:
            pii = re.sub(r"[^A-Za-z0-9]", "", pii_match.group(1)).upper()
    pmcid = (row.get("PMCID") or "").strip()
    if pmcid and not pmcid.upper().startswith("PMC"):
        pmcid = f"PMC{pmcid}"

    if pmcid and not fallback_pdf_url:
        candidates.append(f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/")

    if pii:
        candidates.append(
            f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft?isDTMRedir=true&download=true"
        )

    if doi and "tandfonline.com" in host:
        candidates.append(f"https://www.tandfonline.com/doi/pdf/{doi}?download=true")
        candidates.append(f"https://www.tandfonline.com/doi/epdf/{doi}?needAccess=true&role=button")

    if doi and "nature.com" in host:
        article_match = re.search(r"/articles/([^/?#]+)", final_url)
        if article_match:
            candidates.append(f"https://www.nature.com/articles/{article_match.group(1)}.pdf")

    if "pmc.ncbi.nlm.nih.gov" in host and final_url.rstrip("/").endswith("/pdf"):
        candidates.append(final_url)

    resolved = [urljoin(final_url or start_url, normalized_text(url)) for url in candidates]
    filtered = [
        url
        for url in resolved
        if not url_looks_supplementary(url) and not url_looks_resource_pdf(url)
    ]
    return unique_urls(filtered)


def url_looks_pdfish(url: str) -> bool:
    lowered = (url or "").lower()
    return any(token in lowered for token in PDF_PATTERNS)


def try_download_pdf(
    session: std_requests.Session,
    row: dict[str, str],
    url: str,
    output_path: Path,
    *,
    referer: str = "",
) -> tuple[bool, str]:
    errors: list[str] = []

    advanced = advanced_request(url, accept=PDF_ACCEPT, referer=referer)
    if advanced is not None:
        if advanced.status_code < 400 and is_pdf_bytes(advanced.content, advanced.headers.get("content-type", "")):
            verdict, reason = validate_pdf_for_row(advanced.content, row, advanced.url or url)
            if verdict == "match":
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(advanced.content)
                return True, advanced.url or url
            errors.append(f"pdf_{reason}")
        snippet = normalized_text(advanced.content[:120].decode("utf-8", "replace"))
        errors.append(f"{advanced.status_code}:{advanced.headers.get('content-type', '')}:{snippet[:80]}")

    try:
        response = request_with_retries(
            session,
            url,
            accept=PDF_ACCEPT,
            referer=referer,
        )
    except std_requests.RequestException as exc:
        response = None
        errors.append(type(exc).__name__)

    if response is not None:
        content = response.content
        content_type = response.headers.get("content-type", "")

        if response.status_code < 400 and is_pdf_bytes(content, content_type):
            verdict, reason = validate_pdf_for_row(content, row, response.url or url)
            if verdict == "match":
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(content)
                return True, response.url or url
            errors.append(f"pdf_{reason}")

        if response.status_code >= 400:
            errors.append(f"HTTP {response.status_code}")
        else:
            snippet = normalized_text(content[:120].decode("utf-8", "replace"))
            errors.append(f"not_pdf:{content_type}:{snippet[:80]}")

    return False, " | ".join(unique_urls(errors)) or "download_failed"


def download_row(
    session: std_requests.Session,
    csv_path: Path,
    row: dict[str, str],
) -> DownloadResult:
    output_path = output_path_for(csv_path, row)
    if valid_existing_pdf(output_path, row, row.get("pdf_url") or ""):
        return DownloadResult(True, row.get("pdf_url", ""), "already_exists")

    source_urls = build_source_urls(row)
    if not source_urls:
        return DownloadResult(False, "", "missing_second_link_and_doi")

    last_detail = "no_pdf_candidate"
    tried_pdf_urls: set[str] = set()
    queued_sources = list(source_urls)
    seen_sources: set[str] = set()

    while queued_sources:
        start_url = queued_sources.pop(0)
        if not start_url or start_url in seen_sources:
            continue
        seen_sources.add(start_url)

        if url_looks_pdfish(start_url):
            ok, detail = try_download_pdf(session, row, start_url, output_path, referer=doi_url_from_row(row))
            if ok:
                return DownloadResult(True, detail, "downloaded")
            last_detail = f"{start_url} -> {detail}"
            tried_pdf_urls.add(start_url)
            continue

        response = fetch_html_response(session, start_url)
        if response is None:
            last_detail = f"{start_url} -> no_response"
            continue

        if response.status_code >= 400:
            last_detail = f"{start_url} -> HTTP {response.status_code}"
            continue

        if is_pdf_bytes(response.content, response.headers.get("content-type", "")):
            verdict, reason = validate_pdf_for_row(response.content, row, response.url or start_url)
            if verdict == "match":
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(response.content)
                return DownloadResult(True, response.url or start_url, "direct_pdf")
            last_detail = f"{response.url or start_url} -> pdf_{reason}"
            continue

        final_url = response.url or start_url
        page_text = response_to_text(response)

        for extra_source in collect_pubmed_source_urls(row, start_url, final_url, page_text):
            if extra_source not in seen_sources:
                queued_sources.append(extra_source)

        candidates = collect_candidate_urls(row, start_url, final_url, page_text)
        for candidate in candidates:
            if candidate in tried_pdf_urls:
                continue
            tried_pdf_urls.add(candidate)
            ok, detail = try_download_pdf(session, row, candidate, output_path, referer=final_url)
            if ok:
                return DownloadResult(True, detail, "downloaded")
            last_detail = f"{candidate} -> {detail}"

    return DownloadResult(False, "", last_detail)


def process_csv(csv_path: Path) -> tuple[int, int]:
    rows, fieldnames = load_rows(csv_path)
    out_dir = output_dir_for(csv_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    session = std_requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    success_count = 0
    attempted_count = 0

    for index, row in enumerate(rows, 1):
        status = (row.get("download_status") or "").strip().lower()
        if status == "success":
            continue

        attempted_count += 1
        result = download_row(session, csv_path, row)

        if result.success:
            row["download_status"] = "success"
            row["download_error"] = ""
            if result.pdf_url:
                row["pdf_url"] = result.pdf_url
            success_count += 1
            print(
                f"[{csv_path.name} {index}/{len(rows)}] SUCCESS {safe_filename(row_title(row))}.pdf",
                flush=True,
            )
        else:
            print(
                f"[{csv_path.name} {index}/{len(rows)}] KEEP {row.get('download_status', '') or 'blank'} "
                f"- {result.detail}",
                flush=True,
            )

        save_rows(csv_path, rows, fieldnames)
        time.sleep(DELAY_SECONDS)

    return attempted_count, success_count


def main(argv: list[str]) -> int:
    if len(argv) > 1:
        csv_paths = [Path(arg).resolve() for arg in argv[1:]]
    else:
        csv_paths = sorted(DOCS_DIR.glob("*.csv"))

    if not csv_paths:
        print("No docs-csv files found.", file=sys.stderr)
        return 1

    total_attempted = 0
    total_success = 0

    for csv_path in csv_paths:
        print(f"\nProcessing {csv_path}")
        attempted, success = process_csv(csv_path)
        total_attempted += attempted
        total_success += success
        print(f"Summary for {csv_path.name}: attempted={attempted} newly_success={success}", flush=True)

    print(f"\nAll done: attempted={total_attempted} newly_success={total_success}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
