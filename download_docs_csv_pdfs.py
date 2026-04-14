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
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
)

BASE_DIR = Path(__file__).resolve().parent
VENDOR_DIR = BASE_DIR / ".vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

try:
    import requests as std_requests
    RequestException = std_requests.RequestException
except Exception:
    from curl_cffi import requests as std_requests

    RequestException = std_requests.exceptions.RequestException

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

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None


DOCS_DIR = BASE_DIR / "docs-csv"
REQUEST_TIMEOUT = 20
RETRY_COUNT = 2
DELAY_SECONDS = 0.05
MAX_FILENAME_LEN = 180
ROW_TIMEOUT_SECONDS = 150
HTML_FETCH_TIMEOUT = 15
BROWSER_NAV_TIMEOUT_MS = 20000
BROWSER_RENDER_WAIT_MS = 1800
BROWSER_BLOCK_WAIT_MS = 2500
BROWSER_CLICK_TIMEOUT_MS = 3000
BROWSER_DOWNLOAD_TIMEOUT_MS = 8000
BROWSER_FALLBACK_MAX_SOURCES = 8
BROWSER_FALLBACK_MAX_CANDIDATES = 6
ALLOW_PAGE_PRINT_PDF = False
BROWSER_TARGETS = (
    (
        "chrome",
        "chromium",
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    ),
    (
        "firefox",
        "firefox",
        Path("/Applications/Firefox.app/Contents/MacOS/firefox"),
    ),
)
PLAYWRIGHT_LAUNCH_ARGS = (
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--no-default-browser-check",
)
BROWSER_EXPAND_SELECTORS = (
    'button[aria-label*="open resources" i]',
    'button[aria-label*="resource" i]',
    'button[aria-label*="more" i]',
    'button[aria-label*="menu" i]',
    'button[title*="resource" i]',
)
BROWSER_PDF_SELECTORS = (
    'a:has-text("PDF")',
    'button:has-text("PDF")',
    '[aria-label*="PDF" i]',
    '[title*="PDF" i]',
    'a[href*="/pdf"]',
    'a[href$=".pdf"]',
)
BROWSER_COOKIE_ACCEPT_SELECTORS = (
    'button:has-text("Accept all")',
    'button:has-text("Accept")',
    'button:has-text("I agree")',
    'button:has-text("Agree")',
    'button[aria-label*="accept" i]',
    'button[title*="accept" i]',
)
BROWSER_DOI_PREFIXES = (
    "10.1001/",
    "10.1007/",
    "10.1038/",
    "10.1136/",
    "10.1159/",
    "10.6004/",
)
BROWSER_HELPFUL_HOST_MARKERS = (
    "pmc.ncbi.nlm.nih.gov",
    "jamanetwork.com",
    "jnccn.org",
    "karger.com",
    "bmj.com",
    "link.springer.com",
    "springer.com",
    "nature.com",
    "perspinsurg.com",
    "pieronline.jp",
)
BROWSER_CHALLENGE_HOST_MARKERS = (
    "wiley.com",
    "onlinelibrary.wiley.com",
    "sciencedirect.com",
    "elsevier.com",
    "thelancet.com",
    "nejm.org",
    "evidence.nejm.org",
    "tandfonline.com",
    "ascopubs.org",
    "aacrjournals.org",
    "journals.lww.com",
    "jto.org",
    "lungcancerjournal.info",
    "ejcancer.com",
    "clinical-lung-cancer.com",
    "thegreenjournal.com",
    "clinicalradiologyonline.net",
    "americanjournalofsurgery.com",
    "jvir.org",
    "academic.oup.com",
    "sagepub.com",
    "journals.sagepub.com",
    "eurekaselect.com",
)
BROWSER_UNLIKELY_HOST_MARKERS = (
    "wiley.com",
    "onlinelibrary.wiley.com",
    "sciencedirect.com",
    "elsevier.com",
    "thelancet.com",
    "nejm.org",
    "tandfonline.com",
    "springer.com",
    "nature.com",
)
PRINTABLE_HTML_HOST_MARKERS = (
    "pmc.ncbi.nlm.nih.gov",
    "link.springer.com",
    "springer.com",
    "nature.com",
    "perspinsurg.com",
    "pieronline.jp",
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
PDF_ACCEPT = "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

ADVANCED_IMPERSONATIONS = ("chrome124", "safari184")
ADVANCED_COOKIE_ROUNDS = 2
ADVANCED_DELAY_SECONDS = 0.35
BLOCKED_HOST_THRESHOLD = 2

ADVANCED_FIRST_HOST_MARKERS = (
    "sciencedirect.com",
    "elsevier.com",
    "thelancet.com",
    "jto.org",
    "lungcancerjournal.info",
    "ejcancer.com",
    "aacrjournals.org",
    "nejm.org",
    "wiley.com",
    "onlinelibrary.wiley.com",
    "sigmapubs.onlinelibrary.wiley.com",
    "tandfonline.com",
    "springer.com",
    "nature.com",
    "ascopubs.org",
    "journals.lww.com",
    "academic.oup.com",
    "journals.sagepub.com",
    "sagepub.com",
    "eurekaselect.com",
    "bmj.com",
)

PUBMED_HOSTS = {
    "pubmed.ncbi.nlm.nih.gov",
    "www.ncbi.nlm.nih.gov",
}

BLOCK_PAGE_MARKERS = (
    "cloudpmc-viewer-pow",
    "Preparing to download ...",
    "enable javascript and cookies",
    "just a moment",
    "checking if the site connection is secure",
    "security verification",
    "verify you are human",
    "please enable javascript and cookies",
    "g-recaptcha",
    "hcaptcha",
    "captcha challenge",
    "请稍候",
)

IGNORED_BLOCK_HOSTS = {
    "doi.org",
    "dx.doi.org",
    "www.doi.org",
}

CROSSREF_API = "https://api.crossref.org/works/{doi}"
OPENALEX_API = "https://api.openalex.org/works/https://doi.org/{doi}"
PUBMED_ELINK_API = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    "?dbfrom=pubmed&id={pmid}&cmd=llinks&retmode=json"
)
PUBMED_PRLINKS_API = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    "?dbfrom=pubmed&id={pmid}&cmd=prlinks&retmode=ref"
)

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

REPOSITORY_PDF_PATTERNS = (
    "/bitstreams/",
    "/bitstream/",
    "/server/api/core/bitstreams/",
)

STATIC_ASSET_HOST_MARKERS = (
    "gstatic.com",
    "googleusercontent.com",
    "googleapis.com",
    "doubleclick.net",
    "googletagmanager.com",
    "google-analytics.com",
    "cloudflareinsights.com",
)

NON_ARTICLE_HOST_MARKERS = (
    "facebook.com",
    "google.com",
    "x.com",
    "twitter.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "api.elsevier.com",
    ".local",
    "localhost",
)

STATIC_ASSET_EXTENSIONS = (
    ".js",
    ".css",
    ".svg",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".map",
    ".json",
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
    "supp1",
    "supp2",
    "supp3",
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
    "data sharing statement",
    "supporting documents",
)

PDF_RESOURCE_TEXT_MARKERS = (
    "patient education",
    "patient information series",
    "not intended as a substitute for professional medical advice",
    "american brain tumor association",
)

ARTICLE_PAGE_TEXT_MARKERS = (
    "abstract",
    "introduction",
    "background",
    "methods",
    "materials and methods",
    "patients and methods",
    "results",
    "conclusions",
    "discussion",
)

INVALID_PAGE_TEXT_MARKERS = {
    "clinicalkey_access_page": (
        "to access this content please choose one of the options below",
        "request a trial id",
        "clinicalkey",
    ),
    "pubmed_abstract_page": (
        "an official website of the united states government",
        "pubmed disclaimer",
    ),
}
OVID_WEBPAGE_MARKERS = (
    "check access",
    "current issue",
    "previous issues",
    "latest articles",
    "share",
    "cite",
)
ELSEVIER_WEBPAGE_MARKERS = (
    "download full issue",
    "get access outline share more",
    "affiliations notes article info",
    "get full text access",
    "log in subscribe or purchase for full access",
    "search for",
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
URL_IN_TEXT_REGEX = re.compile(r"https?://([^\s|<>'\"]+)", flags=re.IGNORECASE)
BLOCKED_HOST_REGEX = re.compile(r"host_blocked_cached:([^\s|]+)", flags=re.IGNORECASE)
META_REFRESH_URL_REGEX = re.compile(r'content="\d+\s*;\s*url=\'?([^"\'>]+)', flags=re.IGNORECASE)
PMC_EMBARGO_REGEXES = (
    re.compile(
        r"This article has a delayed release \(embargo\) and will be available in PMC on "
        r"([A-Za-z]+ \d{1,2}, \d{4})",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"This article will be available in PMC on ([A-Za-z]+ \d{1,2}, \d{4})",
        flags=re.IGNORECASE,
    ),
)


@dataclass
class DownloadResult:
    success: bool
    pdf_url: str = ""
    detail: str = ""


COOKIE_CACHE: dict[str, object | None] = {}
CROSSREF_CACHE: dict[str, dict] = {}
OPENALEX_CACHE: dict[str, dict] = {}
PUBMED_LINKOUT_CACHE: dict[str, list[str]] = {}
PUBMED_PRLINKS_CACHE: dict[str, str] = {}
HOST_FAILURE_COUNTS: dict[str, int] = {}
BLOCKED_HOSTS: dict[str, str] = {}


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


def build_session() -> std_requests.Session:
    session = std_requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def row_deadline() -> float | None:
    if ROW_TIMEOUT_SECONDS <= 0:
        return None
    return time.monotonic() + ROW_TIMEOUT_SECONDS


def deadline_expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def remaining_timeout(deadline: float | None, default_timeout: int | float, *, minimum: int | float = 1) -> int | float:
    if deadline is None:
        return default_timeout
    remaining = deadline - time.monotonic()
    if remaining <= minimum:
        return minimum
    return min(default_timeout, remaining)


def is_pdf_bytes(content: bytes, content_type: str = "") -> bool:
    prefix = content[:5]
    if prefix == b"%PDF-":
        return True
    return "application/pdf" in (content_type or "").lower() and content[:1] == b"%"


def normalized_text(value: str) -> str:
    return html.unescape((value or "").replace("\\/", "/")).strip()


def format_exception(exc: Exception, prefix: str = "", *, limit: int = 240) -> str:
    detail = re.sub(r"\s+", " ", normalized_text(str(exc)))
    head = type(exc).__name__
    if prefix:
        head = f"{prefix}:{head}"
    if detail:
        return f"{head}:{detail[:limit]}"
    return head


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
    encoding = getattr(response, "encoding", None) or getattr(response, "apparent_encoding", None) or "utf-8"
    try:
        if not getattr(response, "encoding", None):
            response.encoding = encoding
        return response.text
    except Exception:
        content = getattr(response, "content", b"") or b""
        try:
            return content.decode(encoding, "replace")
        except Exception:
            return content.decode("utf-8", "replace")


def looks_like_block_page(text: str) -> bool:
    lowered = (text or "").lower()
    return any(marker.lower() in lowered for marker in BLOCK_PAGE_MARKERS)


def extract_pmc_embargo_detail(text: str, url: str = "") -> str:
    host = host_for(url)
    if host and "pmc.ncbi.nlm.nih.gov" not in host and "available in pmc on" not in (text or "").lower():
        return ""

    for pattern in PMC_EMBARGO_REGEXES:
        match = pattern.search(text or "")
        if match:
            return f"pmc_embargo_until:{match.group(1)}"

    return ""


def normalized_match_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def invalid_page_text_reason(text: str) -> str:
    normalized = normalized_match_text(text)
    if "ovid" in normalized:
        hits = sum(marker in normalized for marker in OVID_WEBPAGE_MARKERS)
        if hits >= 4:
            return "ovid_webpage_print"
    if sum(marker in normalized for marker in ELSEVIER_WEBPAGE_MARKERS) >= 4:
        return "elsevier_webpage_print"
    for reason, markers in INVALID_PAGE_TEXT_MARKERS.items():
        if all(marker in normalized for marker in markers):
            return reason
    return ""


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


def title_significant_phrases(title: str) -> list[str]:
    terms = title_significant_terms(title)
    phrases: list[str] = []

    for size in (2, 3):
        for index in range(len(terms) - size + 1):
            phrase = " ".join(terms[index : index + size])
            if phrase not in phrases:
                phrases.append(phrase)

    return phrases[:12]


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


def extract_pmcid(value: str) -> str:
    match = re.search(r"(PMC\d+)", value or "", flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""


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


def cookie_jar_has_entries(jar) -> bool:
    if jar is None:
        return False

    try:
        iterator = iter(jar)
    except Exception:
        return False

    try:
        next(iterator)
    except StopIteration:
        return False
    except Exception:
        return False

    return True


def available_cookie_loaders() -> list:
    if browser_cookie3 is None:
        return []

    loaders = []
    # Prefer Firefox first when available, then fall back to Chrome.
    for name in ("firefox", "chrome"):
        loader = getattr(browser_cookie3, name, None)
        if loader is not None:
            loaders.append(loader)
    return loaders


def load_cookie_jar(url: str, *, refresh: bool = False):
    if browser_cookie3 is None:
        return None

    host = urlparse(url).netloc.lower()
    loaders = available_cookie_loaders()
    if not loaders:
        return None

    for domain in root_domains(host):
        if refresh or domain not in COOKIE_CACHE:
            COOKIE_CACHE[domain] = None
            for loader in loaders:
                try:
                    jar = loader(domain_name=domain)
                except Exception:
                    continue

                if cookie_jar_has_entries(jar):
                    COOKIE_CACHE[domain] = jar
                    break
        jar = COOKIE_CACHE[domain]
        if jar is not None:
            return jar
    return None


def should_try_advanced_first(url: str) -> bool:
    host = urlparse(url or "").netloc.lower()
    return any(marker in host for marker in ADVANCED_FIRST_HOST_MARKERS)


def should_try_browser_fallback(url: str) -> bool:
    host = urlparse(url or "").netloc.lower()
    if not host:
        return False

    if host in IGNORED_BLOCK_HOSTS:
        return True

    if host in PUBMED_HOSTS or host.endswith("pmc.ncbi.nlm.nih.gov"):
        return True

    if any(marker in host for marker in ("jamanetwork.com", "jnccn.org", "karger.com")):
        return True

    return should_try_advanced_first(url)


def browser_source_allowed(row: dict[str, str], url: str) -> bool:
    host = host_for(url)
    if not host:
        return False

    if url_looks_static_asset(url):
        return False
    if url_looks_supplementary(url) or url_looks_resource_pdf(url):
        return False
    if url_looks_non_article(url):
        return False

    if host.endswith("pmc.ncbi.nlm.nih.gov"):
        return True

    if any(marker in host for marker in BROWSER_CHALLENGE_HOST_MARKERS):
        return True

    if any(marker in host for marker in BROWSER_HELPFUL_HOST_MARKERS):
        return True

    doi = doi_suffix(row.get("DOI") or "").lower()
    if host in IGNORED_BLOCK_HOSTS:
        return bool(doi) or bool(expected_pmcid(row))

    if not url_looks_pdfish(url) and url_contains_row_identifiers(url, row):
        return True

    return False


def browser_source_priority(row: dict[str, str], url: str) -> tuple[int, int]:
    host = host_for(url)
    lowered = normalized_text(url).lower()

    if host in IGNORED_BLOCK_HOSTS and not url_looks_pdfish(url):
        return (8, 0)
    if host.endswith("pmc.ncbi.nlm.nih.gov") and not url_looks_pdfish(url):
        return (1, 0)
    if host.endswith("pmc.ncbi.nlm.nih.gov") and url_looks_pdfish(url):
        return (2, 0)
    if any(marker in host for marker in BROWSER_CHALLENGE_HOST_MARKERS) and not url_looks_pdfish(url):
        return (3, 0)
    if any(marker in host for marker in BROWSER_CHALLENGE_HOST_MARKERS) and url_looks_pdfish(url):
        return (4, 0)
    if any(marker in host for marker in ("karger.com", "jnccn.org", "bmj.com")) and not url_looks_pdfish(url):
        return (5, 0)
    if any(marker in host for marker in ("karger.com", "jnccn.org", "bmj.com")) and url_looks_pdfish(url):
        return (6, 0)
    if any(marker in host for marker in ("link.springer.com", "springer.com", "nature.com")) and not url_looks_pdfish(url):
        return (7, 0)
    if any(marker in host for marker in ("karger.com", "jnccn.org", "bmj.com")):
        return (8, 0)
    if any(marker in host for marker in ("link.springer.com", "springer.com", "nature.com")) and url_looks_pdfish(url):
        return (9, 0)
    if "jamanetwork.com" in host and url_looks_pdfish(url):
        return (10, 0)
    if "jamanetwork.com" in host:
        return (11, 0)
    if not url_looks_pdfish(url) and url_contains_row_identifiers(url, row):
        return (12, 0)
    if url_looks_pdfish(url):
        return (13, 0)
    return (14, len(lowered))


def host_for(url: str) -> str:
    return urlparse(url or "").netloc.lower()


def browser_source_family(url: str) -> str:
    host = host_for(url)
    if not host:
        return ""

    if host == "linkinghub.elsevier.com" or any(
        marker in host
        for marker in (
            "sciencedirect.com",
            "elsevier.com",
            "jto.org",
            "lungcancerjournal.info",
            "ejcancer.com",
            "clinical-lung-cancer.com",
            "thegreenjournal.com",
            "clinicalradiologyonline.net",
            "americanjournalofsurgery.com",
            "jvir.org",
        )
    ):
        return "elsevier"

    if any(marker in host for marker in ("onlinelibrary.wiley.com", "wiley.com")):
        return "wiley"
    if any(marker in host for marker in ("journals.lww.com", "ovid.com")):
        return "lww"
    if "tandfonline.com" in host:
        return "tandf"
    if "aacrjournals.org" in host:
        return "aacr"
    return host


def condense_browser_sources(source_urls: list[str]) -> list[str]:
    if not source_urls:
        return []

    has_elsevier_article = any(
        browser_source_family(url) == "elsevier"
        and host_for(url) != "linkinghub.elsevier.com"
        and not url_looks_pdfish(url)
        for url in source_urls
    )

    family_total_counts: dict[str, int] = {}
    family_html_counts: dict[str, int] = {}
    family_pdf_counts: dict[str, int] = {}
    condensed: list[str] = []

    for url in source_urls:
        family = browser_source_family(url)
        host = host_for(url)
        is_pdf = url_looks_pdfish(url)

        if family == "elsevier" and has_elsevier_article and host == "linkinghub.elsevier.com":
            continue

        total_limit = 3 if family == "elsevier" else 4
        html_limit = 2 if family == "elsevier" else 3
        pdf_limit = 1 if family in {"elsevier", "wiley", "lww", "tandf"} else 2

        if family_total_counts.get(family, 0) >= total_limit:
            continue

        if is_pdf:
            if family_pdf_counts.get(family, 0) >= pdf_limit:
                continue
            family_pdf_counts[family] = family_pdf_counts.get(family, 0) + 1
        else:
            if family_html_counts.get(family, 0) >= html_limit:
                continue
            family_html_counts[family] = family_html_counts.get(family, 0) + 1

        family_total_counts[family] = family_total_counts.get(family, 0) + 1
        condensed.append(url)

    return condensed


def blocked_host_detail(url: str) -> str:
    host = host_for(url)
    if host in IGNORED_BLOCK_HOSTS:
        return ""
    for domain in root_domains(host):
        detail = BLOCKED_HOSTS.get(domain)
        if detail:
            return detail
    return ""


def detail_indicates_host_block(detail: str) -> bool:
    lowered = (detail or "").lower()
    return any(
        marker in lowered
        for marker in (
            "http 403",
            "http_403",
            "just a moment",
            "checking if the site connection is secure",
            "security verification",
            "verify you are human",
            "enable javascript and cookies",
            "g-recaptcha",
            "hcaptcha",
            "captcha challenge",
            "cloudflare",
            "blocked by client",
        )
    )


def note_host_failure(url: str, detail: str) -> None:
    host = host_for(url)
    if not host or host in IGNORED_BLOCK_HOSTS or not should_try_advanced_first(url):
        return

    if detail_indicates_host_block(detail):
        HOST_FAILURE_COUNTS[host] = HOST_FAILURE_COUNTS.get(host, 0) + 1
        if HOST_FAILURE_COUNTS[host] >= BLOCKED_HOST_THRESHOLD:
            for domain in root_domains(host):
                BLOCKED_HOSTS[domain] = detail
        return

    HOST_FAILURE_COUNTS.pop(host, None)


def request_with_retries(
    session: std_requests.Session,
    url: str,
    *,
    accept: str,
    referer: str = "",
    timeout: int = REQUEST_TIMEOUT,
    deadline: float | None = None,
):
    last_error: Exception | None = None
    headers = {"Accept": accept}
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, RETRY_COUNT + 1):
        if deadline_expired(deadline):
            raise TimeoutError(f"row_timeout>{ROW_TIMEOUT_SECONDS}s")
        try:
            response = session.get(
                url,
                headers=headers,
                timeout=remaining_timeout(deadline, timeout),
                allow_redirects=True,
            )
            return response
        except RequestException as exc:
            last_error = exc
            if attempt < RETRY_COUNT:
                time.sleep(min(attempt, 2))

    assert last_error is not None
    raise last_error


def response_matches_accept(response, accept: str) -> bool:
    if response is None or getattr(response, "status_code", 500) >= 400:
        return False

    content_type = (response.headers.get("content-type", "") or "").lower()
    if accept == PDF_ACCEPT:
        return is_pdf_bytes(response.content, content_type)

    if "json" in accept.lower():
        text = response_to_text(response).strip()
        return "json" in content_type or text.startswith("{") or text.startswith("[")

    if is_pdf_bytes(response.content, content_type):
        return True

    return not looks_like_block_page(response_to_text(response))


def advanced_request(
    url: str,
    *,
    accept: str,
    referer: str = "",
    timeout: int = REQUEST_TIMEOUT,
    return_last: bool = False,
    deadline: float | None = None,
):
    if curl_requests is None:
        return None

    headers = {
        "Accept": accept,
        "User-Agent": USER_AGENT,
    }
    if referer:
        headers["Referer"] = referer

    last_response = None

    for round_idx in range(ADVANCED_COOKIE_ROUNDS):
        if deadline_expired(deadline):
            return last_response if return_last else None
        cookie_jar = load_cookie_jar(url, refresh=round_idx > 0)
        for impersonation in ADVANCED_IMPERSONATIONS:
            if deadline_expired(deadline):
                return last_response if return_last else None
            try:
                response = curl_requests.get(
                    url,
                    headers=headers,
                    cookies=cookie_jar,
                    impersonate=impersonation,
                    allow_redirects=True,
                    timeout=remaining_timeout(deadline, timeout),
                )
            except Exception:
                time.sleep(ADVANCED_DELAY_SECONDS)
                continue

            last_response = response
            if response_matches_accept(response, accept):
                return response
            time.sleep(ADVANCED_DELAY_SECONDS)

    return last_response if return_last else None


def fetch_html_response(
    session: std_requests.Session,
    url: str,
    *,
    referer: str = "",
    deadline: float | None = None,
):
    advanced_first = should_try_advanced_first(url)
    response = None

    if not advanced_first:
        try:
            response = request_with_retries(
                session,
                url,
                accept=HTML_ACCEPT,
                referer=referer,
                timeout=HTML_FETCH_TIMEOUT,
                deadline=deadline,
            )
        except RequestException:
            response = None

        if response_matches_accept(response, HTML_ACCEPT):
            return response

    advanced = advanced_request(
        url,
        accept=HTML_ACCEPT,
        referer=referer,
        timeout=HTML_FETCH_TIMEOUT,
        return_last=True,
        deadline=deadline,
    )
    if response_matches_accept(advanced, HTML_ACCEPT):
        return advanced

    if advanced_first and response is None:
        try:
            response = request_with_retries(
                session,
                url,
                accept=HTML_ACCEPT,
                referer=referer,
                timeout=HTML_FETCH_TIMEOUT,
                deadline=deadline,
            )
        except RequestException:
            response = None
        if response_matches_accept(response, HTML_ACCEPT):
            return response

    return advanced or response


def fetch_metadata_json(url: str, *, deadline: float | None = None) -> dict:
    if deadline_expired(deadline):
        return {}

    response = advanced_request(url, accept="application/json", deadline=deadline) or std_requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=remaining_timeout(deadline, REQUEST_TIMEOUT),
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


def extract_blocked_hosts(detail: str) -> set[str]:
    hosts: set[str] = set()

    for match in URL_IN_TEXT_REGEX.findall(detail or ""):
        cleaned = match.rstrip(".,);")
        parsed_host = urlparse(f"https://{cleaned}").netloc.lower()
        for domain in root_domains(parsed_host):
            if domain not in IGNORED_BLOCK_HOSTS:
                hosts.add(domain)

    for match in BLOCKED_HOST_REGEX.findall(detail or ""):
        cleaned = match.rstrip(".,);").lower()
        for domain in root_domains(cleaned):
            if domain not in IGNORED_BLOCK_HOSTS:
                hosts.add(domain)

    return hosts


def urls_from_text(value: str) -> list[str]:
    matches = re.findall(r"https?://[^\s|<>'\"]+", value or "", flags=re.IGNORECASE)
    matches = [match.rstrip(".,);") for match in matches]
    return unique_urls(matches)


def preload_historical_blocked_hosts(rows: list[dict[str, str]]) -> None:
    for row in rows:
        detail = (row.get("download_error") or "").strip()
        if not detail or not detail_indicates_host_block(detail):
            continue

        for host in extract_blocked_hosts(detail):
            BLOCKED_HOSTS.setdefault(host, detail)


def extract_pii(value: str) -> str:
    raw_value = value or ""

    match = re.search(r"/pii/([A-Z0-9\-()]+)", raw_value, re.IGNORECASE)
    if match:
        return re.sub(r"[^A-Za-z0-9]", "", match.group(1))

    match = re.search(r"\b1-s2\.0-(S[A-Z0-9]{10,})(?:[-_.]|$)", raw_value, re.IGNORECASE)
    if match:
        return re.sub(r"[^A-Za-z0-9]", "", match.group(1))

    # Require a much longer token so Springer-like DOIs such as s10552-... are
    # not mistaken for Elsevier PIIs.
    match = re.search(r"\b(S\d{12,}[A-Z0-9]*)(?:[-_.]|$)", raw_value, re.IGNORECASE)
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
    if any(marker in lowered for marker in PDF_SUPPLEMENT_URL_MARKERS):
        return True

    # JAMA/PMC supplementary files often use names such as `...-s001.pdf`.
    if re.search(r"[-_/]s\d{3}(?:[._-]|\.pdf|$)", lowered):
        return True

    return False


def url_looks_resource_pdf(url: str) -> bool:
    lowered = normalized_text(unquote(url or "")).lower()
    return any(marker in lowered for marker in PDF_RESOURCE_URL_MARKERS)


def url_looks_static_asset(url: str) -> bool:
    parsed = urlparse(normalized_text(unquote(url or "")))
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()

    if any(marker in host for marker in STATIC_ASSET_HOST_MARKERS):
        return True
    return any(path.endswith(ext) for ext in STATIC_ASSET_EXTENSIONS)


def url_looks_non_article(url: str) -> bool:
    parsed = urlparse(normalized_text(unquote(url or "")))
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if any(marker in host for marker in NON_ARTICLE_HOST_MARKERS):
        return True
    return (
        path.startswith("/content/article/pii")
        or "/doi/full-xml/" in path
        or "httpaccept=text/plain" in (parsed.query or "").lower()
    )


def url_looks_article_pdf(url: str) -> bool:
    lowered = normalized_text(unquote(url or "")).lower()
    article_markers = (
        "/article-pdf/",
        "/doi/pdf/",
        "/doi/epdf/",
        "downloadpdffile.cfm",
        "/pdfft",
        "articlepdf",
        "/pdf/",
    )
    return any(marker in lowered for marker in article_markers)


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
    *,
    request_url: str = "",
) -> tuple[str, str]:
    candidate_urls = [url for url in (source_url, request_url) if url]
    identity_urls = [url for url in candidate_urls if url_looks_pdfish(url)]

    if any(url_looks_supplementary(url) for url in candidate_urls):
        return "mismatch", "supplementary_url"
    if any(url_looks_resource_pdf(url) for url in candidate_urls):
        return "mismatch", "resource_url"

    combined_text, page_texts = extract_pdf_text(content)
    first_page_text = page_texts[0] if page_texts else combined_text
    normalized_first_page = normalized_match_text(first_page_text[:2000])
    invalid_reason = invalid_page_text_reason(f"{first_page_text}\n{combined_text[:4000]}")

    if invalid_reason:
        return "mismatch", invalid_reason

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

    if any(url_contains_row_identifiers(url, row) for url in identity_urls):
        return "match", "url_id_match"

    normalized_full_text = normalized_match_text(combined_text)
    title_terms = title_significant_terms(row_title(row))
    title_phrases = title_significant_phrases(row_title(row))
    matches = [term for term in title_terms if term in normalized_full_text]
    phrase_matches = [phrase for phrase in title_phrases if phrase in normalized_full_text]
    if title_terms:
        required_matches = min(
            len(title_terms),
            max(2, math.ceil(min(len(title_terms), 7) * 0.6)),
        )
        require_phrase = len(title_terms) >= 5

        if len(matches) >= required_matches and (phrase_matches or not require_phrase):
            return "match", f"title_match_{len(matches)}"
        if len(matches) >= 3 and phrase_matches and any(url_looks_article_pdf(url) for url in candidate_urls):
            return "match", f"title_soft_match_{len(matches)}"

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

    return validate_pdf_for_row(content, row, source_url, request_url=source_url)


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


def promote_existing_pending_rows(
    csv_path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
) -> int:
    promoted_count = 0

    for row in rows:
        status = (row.get("download_status") or "").strip().lower()
        if status == "success":
            continue

        output_path = output_path_for(csv_path, row)
        source_url = (row.get("pdf_url") or "").strip() or pick_source_url(row)
        verdict, _ = validate_existing_pdf(output_path, row, source_url)
        if verdict != "match":
            continue

        row["download_status"] = "success"
        row["download_error"] = ""
        if not (row.get("pdf_url") or "").strip():
            row["pdf_url"] = source_url
        promoted_count += 1

    if promoted_count:
        save_rows(csv_path, rows, fieldnames)

    return promoted_count


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


def formatted_doi_for_path(doi: str, *, uppercase_suffix: bool = False) -> str:
    cleaned = doi_suffix(doi)
    if not cleaned or not uppercase_suffix or "/" not in cleaned:
        return cleaned

    prefix, suffix = cleaned.split("/", 1)
    return f"{prefix}/{suffix.upper()}"


def derive_article_urls_from_url(url: str) -> list[str]:
    raw_url = normalized_text(url)
    if not raw_url:
        return []

    parsed = urlparse(raw_url)
    query = parse_qs(parsed.query or "")
    host = parsed.netloc.lower()
    path = parsed.path or ""
    lowered = normalized_text(unquote(raw_url)).lower()
    candidates: list[str] = []

    def _replace_path(fragment: str, replacement: str) -> str:
        return parsed._replace(
            path=path.replace(fragment, replacement),
            query="",
            fragment="",
        ).geturl()

    if "/doi/pdfdirect/" in lowered:
        candidates.append(_replace_path("/doi/pdfdirect/", "/doi/full/"))
        candidates.append(_replace_path("/doi/pdfdirect/", "/doi/abs/"))
    if "/doi/pdf/" in lowered:
        candidates.append(_replace_path("/doi/pdf/", "/doi/full/"))
        candidates.append(_replace_path("/doi/pdf/", "/doi/abs/"))
        candidates.append(_replace_path("/doi/pdf/", "/doi/"))
    if "/doi/epdf/" in lowered:
        candidates.append(_replace_path("/doi/epdf/", "/doi/full/"))
        candidates.append(_replace_path("/doi/epdf/", "/doi/abs/"))

    if "/article-pdf/" in lowered:
        candidates.append(raw_url.replace("/article-pdf/", "/article/").split("?", 1)[0])

    for redirect_key in ("Redirect", "redirect", "target", "url"):
        for value in query.get(redirect_key, []):
            redirected = normalized_text(unquote(value))
            if redirected.startswith("http://") or redirected.startswith("https://"):
                candidates.append(redirected)

    if host in {
        "www.jto.org",
        "www.lungcancerjournal.info",
        "www.ejcancer.com",
        "clinical-lung-cancer.com",
        "www.clinical-lung-cancer.com",
        "thegreenjournal.com",
        "www.thegreenjournal.com",
        "www.clinicalradiologyonline.net",
        "www.americanjournalofsurgery.com",
        "www.jvir.org",
    }:
        article_url = raw_url.split("?", 1)[0].rstrip("/")
        if article_url.endswith("/pdf"):
            article_url = article_url[: -len("/pdf")]
            candidates.append(article_url)
            candidates.append(f"{article_url}/fulltext")
        retrieve_match = re.search(r"/retrieve/pii/([A-Z0-9()\-]+)", article_url, flags=re.IGNORECASE)
        if retrieve_match:
            pii = re.sub(r"[^A-Za-z0-9]", "", retrieve_match.group(1)).upper()
            candidates.append(f"https://{host}/retrieve/pii/{pii}")
            candidates.append(f"https://{host}/article/{pii}/fulltext")
            candidates.append(f"https://{host}/article/{pii}/abstract")

    if "sciencedirect.com/science/article/pii/" in lowered and "pdfft" in lowered:
        pii = extract_pii(raw_url)
        if pii:
            candidates.append(f"https://www.sciencedirect.com/science/article/pii/{pii}")

    if "link.springer.com" in host and "/content/pdf/" in lowered:
        doi_match = DOI_REGEX.search(unquote(raw_url))
        if doi_match:
            candidates.append(f"https://link.springer.com/article/{doi_match.group(0)}")

    if path.endswith(".pdf") and "/doi/" in path:
        candidates.append(raw_url[: -len(".pdf")])

    return unique_urls(candidate for candidate in candidates if candidate != raw_url)


def build_direct_pdf_urls(
    row: dict[str, str],
    *,
    reference_url: str = "",
) -> list[str]:
    doi = doi_suffix(row.get("DOI") or "")
    doi_lower = doi.lower()
    ref_host = host_for(reference_url or pick_source_url(row))
    candidates: list[str] = []

    if doi_lower.startswith(("10.1002/", "10.1111/")):
        candidates.append(f"https://onlinelibrary.wiley.com/doi/pdf/{doi}")
        candidates.append(f"https://onlinelibrary.wiley.com/doi/{doi}")
        candidates.append(f"https://onlinelibrary.wiley.com/doi/full/{doi}")

    if doi_lower.startswith("10.1080/"):
        candidates.append(f"https://www.tandfonline.com/doi/pdf/{doi}?download=true")
        candidates.append(f"https://www.tandfonline.com/doi/epdf/{doi}?needAccess=true&role=button")
        candidates.append(f"https://www.tandfonline.com/doi/full/{doi}")
        candidates.append(f"https://www.tandfonline.com/doi/abs/{doi}")

    if doi_lower.startswith("10.1200/"):
        candidates.append(f"https://ascopubs.org/doi/pdfdirect/{formatted_doi_for_path(doi, uppercase_suffix=True)}")
        candidates.append(f"https://ascopubs.org/doi/full/{formatted_doi_for_path(doi, uppercase_suffix=True)}")
        candidates.append(f"https://ascopubs.org/doi/abs/{formatted_doi_for_path(doi, uppercase_suffix=True)}")

    if doi_lower.startswith("10.1056/"):
        candidates.append(f"https://www.nejm.org/doi/pdf/{formatted_doi_for_path(doi, uppercase_suffix=True)}")
        candidates.append(f"https://www.nejm.org/doi/full/{formatted_doi_for_path(doi, uppercase_suffix=True)}")
        candidates.append(f"https://www.nejm.org/doi/{formatted_doi_for_path(doi, uppercase_suffix=True)}")

    if doi_lower.startswith("10.1007/") or "springer.com" in ref_host or "link.springer.com" in ref_host:
        candidates.append(f"https://link.springer.com/content/pdf/{doi}.pdf")
        candidates.append(f"https://link.springer.com/article/{doi}")

    pii = expected_pii(row) or extract_pii(reference_url)
    if pii:
        candidates.append(f"https://www.sciencedirect.com/science/article/pii/{pii}")

    pmcid = expected_pmcid(row)
    if pmcid:
        candidates.append(f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/")
        candidates.append(f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/")

    return unique_urls(candidates)


def fetch_crossref_metadata(row: dict[str, str], *, deadline: float | None = None) -> dict:
    doi = doi_suffix(row.get("DOI") or "")
    if not doi:
        return {}
    if doi not in CROSSREF_CACHE:
        payload = fetch_metadata_json(CROSSREF_API.format(doi=quote(doi, safe="")), deadline=deadline)
        CROSSREF_CACHE[doi] = payload.get("message", {}) if payload else {}
    return CROSSREF_CACHE[doi]


def fetch_openalex_metadata(row: dict[str, str], *, deadline: float | None = None) -> dict:
    doi = doi_suffix(row.get("DOI") or "")
    if not doi:
        return {}
    if doi not in OPENALEX_CACHE:
        OPENALEX_CACHE[doi] = fetch_metadata_json(OPENALEX_API.format(doi=quote(doi, safe="")), deadline=deadline)
    return OPENALEX_CACHE[doi] or {}


def fetch_pubmed_linkouts(row: dict[str, str], *, deadline: float | None = None) -> list[str]:
    pmid = expected_pmid(row)
    if not pmid:
        return []

    if pmid not in PUBMED_LINKOUT_CACHE:
        payload = fetch_metadata_json(PUBMED_ELINK_API.format(pmid=quote(pmid, safe="")), deadline=deadline)
        linksets = payload.get("linksets") or []
        objurls = ((linksets[0].get("idurllist") or [{}])[0].get("objurls") or []) if linksets else []

        candidates: list[tuple[int, str]] = []
        for obj in objurls:
            categories = obj.get("categories") or []
            if "Full Text Sources" not in categories:
                continue

            raw_url = ((obj.get("url") or {}).get("value") or "").strip()
            if not raw_url:
                continue

            attributes = [normalized_text(value).lower() for value in (obj.get("attributes") or [])]
            free_rank = 0 if "free resource" in attributes else 1
            candidates.append((free_rank, raw_url))

        candidates.sort(key=lambda item: (item[0], len(item[1])))
        PUBMED_LINKOUT_CACHE[pmid] = unique_urls(url for _, url in candidates)

    return PUBMED_LINKOUT_CACHE.get(pmid, [])


def fetch_pubmed_prlinks_target(row: dict[str, str], *, deadline: float | None = None) -> str:
    pmid = expected_pmid(row)
    if not pmid:
        return ""

    if pmid not in PUBMED_PRLINKS_CACHE:
        target = ""
        response = advanced_request(
            PUBMED_PRLINKS_API.format(pmid=quote(pmid, safe="")),
            accept="text/html,*/*",
            timeout=HTML_FETCH_TIMEOUT,
            return_last=True,
            deadline=deadline,
        )
        if response is not None:
            text = response_to_text(response)
            match = META_REFRESH_URL_REGEX.search(text)
            if match:
                target = urljoin(response.url or PUBMED_PRLINKS_API.format(pmid=pmid), html.unescape(match.group(1)))
            elif response.url:
                target = response.url
        PUBMED_PRLINKS_CACHE[pmid] = normalized_text(target)

    return PUBMED_PRLINKS_CACHE.get(pmid, "")


def build_source_urls(
    row: dict[str, str],
    *,
    include_metadata: bool = True,
    deadline: float | None = None,
) -> list[str]:
    sources: list[str] = []

    second_link = (row.get("second_link") or "").strip()
    pubmed_url = (row.get("pubmed_url") or "").strip()
    pdf_url = (row.get("pdf_url") or "").strip()
    doi_url = doi_url_from_row(row)
    download_error = (row.get("download_error") or "").strip()

    if second_link:
        sources.append(second_link)

    if pdf_url and not url_looks_supplementary(pdf_url) and not url_looks_non_article(pdf_url):
        sources.append(pdf_url)

    sources.extend(build_direct_pdf_urls(row, reference_url=second_link or pdf_url or doi_url))

    if doi_url:
        sources.append(doi_url)

    if pubmed_url:
        sources.append(pubmed_url)

    for candidate in urls_from_text(download_error):
        if not url_looks_supplementary(candidate) and not url_looks_non_article(candidate):
            sources.append(candidate)

    if include_metadata:
        prlinks_target = fetch_pubmed_prlinks_target(row, deadline=deadline)
        if prlinks_target:
            sources.append(prlinks_target)

        for candidate in fetch_pubmed_linkouts(row, deadline=deadline):
            if candidate and not url_looks_supplementary(candidate) and not url_looks_non_article(candidate):
                sources.append(candidate)

        crossref = fetch_crossref_metadata(row, deadline=deadline)
        for link in crossref.get("link", []) or []:
            url = link.get("URL")
            if url and not url_looks_supplementary(url) and not url_looks_non_article(url):
                sources.append(url)
        if crossref.get("URL"):
            sources.append(crossref["URL"])

        openalex = fetch_openalex_metadata(row, deadline=deadline)
        best_oa = openalex.get("best_oa_location") or {}
        open_access = openalex.get("open_access") or {}
        for candidate in (
            best_oa.get("pdf_url"),
            best_oa.get("landing_page_url"),
            open_access.get("oa_url"),
        ):
            if candidate and not url_looks_supplementary(candidate) and not url_looks_non_article(candidate):
                sources.append(candidate)

    derived_sources: list[str] = []
    for source in list(sources):
        derived_sources.extend(derive_article_urls_from_url(source))

    return unique_urls([*derived_sources, *sources])


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
        if url_looks_static_asset(absolute):
            continue
        if url_looks_non_article(absolute):
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
        if any(token in lowered for token in PDF_PATTERNS) or any(
            token in lowered for token in REPOSITORY_PDF_PATTERNS
        ):
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
    pmcid = expected_pmcid(row)
    if not pmcid:
        pmcid = extract_pmcid(final_url) or extract_pmcid(start_url)
    if not pmcid:
        pmcid = extract_pmcid(page_text)

    if pmcid and not fallback_pdf_url:
        candidates.append(f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/")

    if pii:
        candidates.append(
            f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft?isDTMRedir=true&download=true"
        )

    pdf_meta_match = re.search(
        r'"pdfDownload"\s*:\s*\{.*?"queryParams"\s*:\s*\{.*?"md5"\s*:\s*"([0-9a-f]+)".*?"pid"\s*:\s*"([^"]+)"'
        r'.*?\}.*?"pii"\s*:\s*"([^"]+)"',
        page_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if pdf_meta_match:
        md5_value, pid_value, pii_value = pdf_meta_match.groups()
        candidates.append(
            "https://www.sciencedirect.com/science/article/pii/"
            f"{pii_value}/pdfft?md5={md5_value}&pid={quote(pid_value, safe='')}"
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

    candidates.extend(build_direct_pdf_urls(row, reference_url=final_url or start_url))

    resolved = [urljoin(final_url or start_url, normalized_text(url)) for url in candidates]
    filtered = [
        url
        for url in resolved
        if not url_looks_supplementary(url)
        and not url_looks_resource_pdf(url)
        and not url_looks_static_asset(url)
        and not url_looks_non_article(url)
    ]

    start_domains = set(root_domains(host_for(start_url)))
    final_domains = set(root_domains(host_for(final_url)))

    strict_filtered: list[str] = []
    for url in filtered:
        if not url_looks_pdfish(url):
            strict_filtered.append(url)
            continue

        candidate_domains = set(root_domains(host_for(url)))
        same_host_family = bool(candidate_domains & (start_domains | final_domains))
        if same_host_family or url_contains_row_identifiers(url, row):
            strict_filtered.append(url)

    return unique_urls(strict_filtered)


def url_looks_pdfish(url: str) -> bool:
    lowered = (url or "").lower()
    return any(token in lowered for token in PDF_PATTERNS)


class BrowserPDFDownloader:
    """Fallback downloader that uses a real browser session for blocked pages."""

    def __init__(self) -> None:
        self._manager = None
        self._playwright = None
        self._browsers: dict[str, object] = {}
        self._contexts: dict[str, object] = {}
        self._launch_errors: dict[str, str] = {}
        self._loaded_cookie_keys: set[tuple[str, str, str, str]] = set()
        self._playwright_error: str = ""

    def available(self) -> bool:
        return sync_playwright is not None and any(path.exists() for _, _, path in BROWSER_TARGETS)

    def close(self) -> None:
        for context in self._contexts.values():
            try:
                context.close()
            except Exception:
                pass
        self._contexts.clear()

        for browser in self._browsers.values():
            try:
                browser.close()
            except Exception:
                pass
        self._browsers.clear()

        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:
                pass
        elif self._manager is not None and hasattr(self._manager, "stop"):
            try:
                self._manager.stop()
            except Exception:
                pass

        self._manager = None
        self._playwright = None
        self._playwright_error = ""
        self._launch_errors.clear()
        self._loaded_cookie_keys.clear()

    def _ensure_playwright(self) -> bool:
        if sync_playwright is None:
            self._playwright_error = "playwright_unavailable"
            return False

        if self._playwright is None:
            try:
                self._manager = sync_playwright()
                self._playwright = self._manager.start()
            except Exception as exc:
                try:
                    if self._manager is not None:
                        if hasattr(self._manager, "stop"):
                            self._manager.stop()
                except Exception:
                    pass
                self._manager = None
                self._playwright = None
                self._playwright_error = format_exception(exc, "start_failed", limit=180)
                return False

        return True

    def _context_for(self, browser_name: str, engine_name: str, executable_path: Path):
        if browser_name in self._contexts:
            return self._contexts[browser_name]

        cached_error = self._launch_errors.get(browser_name)
        if cached_error and cached_error.startswith(("missing_browser", "playwright_unavailable")):
            return None
        if cached_error:
            self._launch_errors.pop(browser_name, None)

        if not executable_path.exists():
            self._launch_errors[browser_name] = "missing_browser"
            return None

        if not self._ensure_playwright():
            self._launch_errors[browser_name] = self._playwright_error or "playwright_unavailable"
            return None

        launcher = getattr(self._playwright, engine_name)
        launch_kwargs = {
            "executable_path": str(executable_path),
            "headless": True,
        }
        if engine_name == "chromium":
            launch_kwargs["args"] = list(PLAYWRIGHT_LAUNCH_ARGS)

        browser = None
        try:
            browser = launcher.launch(**launch_kwargs)
            context = browser.new_context(
                accept_downloads=True,
                ignore_https_errors=True,
                user_agent=USER_AGENT,
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
        except Exception as exc:
            try:
                if browser is not None:
                    browser.close()
            except Exception:
                pass
            self._launch_errors[browser_name] = format_exception(exc, "launch_failed", limit=220)
            return None

        self._browsers[browser_name] = browser
        self._contexts[browser_name] = context
        return context

    def _seed_context_cookies(self, context, url: str) -> None:
        jar = load_cookie_jar(url)
        if jar is None:
            return

        cookies = []
        new_keys: list[tuple[str, str, str, str]] = []
        for cookie in jar:
            name = getattr(cookie, "name", "") or ""
            value = getattr(cookie, "value", "") or ""
            domain = getattr(cookie, "domain", "") or ""
            path = getattr(cookie, "path", "") or "/"
            if not name or not value or not domain:
                continue

            key = (domain, path, name, value)
            if key in self._loaded_cookie_keys:
                continue

            payload = {
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "secure": bool(getattr(cookie, "secure", False)),
            }

            expires = int(getattr(cookie, "expires", 0) or 0)
            if expires > 0:
                payload["expires"] = expires

            rest = getattr(cookie, "_rest", {}) or {}
            if any(str(rest_key).lower() == "httponly" for rest_key in rest):
                payload["httpOnly"] = True

            cookies.append(payload)
            new_keys.append(key)

        if not cookies:
            return

        try:
            context.add_cookies(cookies)
        except Exception:
            return

        self._loaded_cookie_keys.update(new_keys)

    def _save_pdf_content(
        self,
        content: bytes,
        row: dict[str, str],
        output_path: Path,
        *,
        source_url: str,
        request_url: str,
    ) -> tuple[bool, str]:
        if not is_pdf_bytes(content):
            return False, "not_pdf"

        verdict, reason = validate_pdf_for_row(content, row, source_url, request_url=request_url)
        if verdict != "match":
            return False, f"pdf_{reason}"

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)
        return True, source_url or request_url

    def _save_download_artifact(
        self,
        download,
        row: dict[str, str],
        output_path: Path,
        *,
        request_url: str,
    ) -> tuple[bool, str]:
        temp_path = output_path.with_suffix(".playwright.part")

        try:
            download.save_as(str(temp_path))
            content = temp_path.read_bytes()
        except Exception as exc:
            return False, format_exception(exc, "download_save_failed", limit=180)
        finally:
            try:
                temp_path.unlink()
            except OSError:
                pass

        source_url = ""
        try:
            source_url = download.url or request_url
        except Exception:
            source_url = request_url

        return self._save_pdf_content(
            content,
            row,
            output_path,
            source_url=source_url,
            request_url=request_url,
        )

    def _response_detail(self, response, body: bytes = b"") -> str:
        content_type = ""
        try:
            content_type = response.headers.get("content-type", "")
        except Exception:
            content_type = ""

        status = getattr(response, "status", None) or "unknown"
        snippet = normalized_text(body[:120].decode("utf-8", "replace")) if body else ""
        return f"http_{status}:{content_type}:{snippet[:80]}".rstrip(":")

    def _try_playwright_response(
        self,
        response,
        row: dict[str, str],
        output_path: Path,
        *,
        request_url: str,
    ) -> tuple[bool, str]:
        if response is None:
            return False, "no_response"

        try:
            body = response.body()
        except Exception as exc:
            return False, format_exception(exc, limit=180)

        content_type = ""
        try:
            content_type = response.headers.get("content-type", "")
        except Exception:
            content_type = ""

        source_url = getattr(response, "url", "") or request_url
        if is_pdf_bytes(body, content_type):
            return self._save_pdf_content(
                body,
                row,
                output_path,
                source_url=source_url,
                request_url=request_url,
            )

        return False, self._response_detail(response, body)

    def _try_request_pdf(
        self,
        context,
        row: dict[str, str],
        url: str,
        output_path: Path,
        *,
        referer: str = "",
        deadline: float | None = None,
    ) -> tuple[bool, str]:
        if deadline_expired(deadline):
            return False, f"row_timeout>{ROW_TIMEOUT_SECONDS}s"
        headers = {"Accept": PDF_ACCEPT}
        if referer:
            headers["Referer"] = referer

        self._seed_context_cookies(context, url)

        try:
            response = context.request.get(
                url,
                headers=headers,
                timeout=int(remaining_timeout(deadline, BROWSER_NAV_TIMEOUT_MS / 1000, minimum=1) * 1000),
                fail_on_status_code=False,
            )
        except Exception as exc:
            return False, format_exception(exc, limit=180)

        return self._try_playwright_response(response, row, output_path, request_url=url)

    def _try_page_pdf_navigation(
        self,
        page,
        row: dict[str, str],
        url: str,
        output_path: Path,
        *,
        referer: str = "",
        deadline: float | None = None,
    ) -> tuple[bool, str]:
        if deadline_expired(deadline):
            return False, f"row_timeout>{ROW_TIMEOUT_SECONDS}s"
        try:
            if referer:
                page.set_extra_http_headers({"Referer": referer})
            nav_timeout = int(remaining_timeout(deadline, BROWSER_NAV_TIMEOUT_MS / 1000, minimum=1) * 1000)
            response = page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
            page.wait_for_timeout(1200)
        except Exception as exc:
            return False, format_exception(exc, "navigate_pdf", limit=180)
        finally:
            try:
                page.set_extra_http_headers({})
            except Exception:
                pass

        return self._try_playwright_response(response, row, output_path, request_url=url)

    def _collect_page_candidates(
        self,
        page,
        row: dict[str, str],
        start_url: str,
    ) -> list[str]:
        try:
            page_text = page.content()
        except Exception:
            page_text = ""

        final_url = page.url or start_url
        candidates = collect_candidate_urls(row, start_url, final_url, page_text)

        try:
            dom_values = page.eval_on_selector_all(
                "a[href],iframe[src],embed[src],object[data],[data-href],[data-url],[data-pdf-url],[data-download-url]",
                """
                elements => elements.flatMap(element => {
                    const values = [];
                    for (const name of ['href', 'src', 'data', 'data-href', 'data-url', 'data-pdf-url', 'data-download-url']) {
                        const value = element.getAttribute(name);
                        if (value) values.push(value);
                    }
                    return values;
                })
                """,
            )
        except Exception:
            dom_values = []

        for value in dom_values:
            candidates.append(urljoin(final_url, normalized_text(value)))

        filtered = [
            url
            for url in unique_urls(candidates)
            if url
            and not url_looks_static_asset(url)
            and not url_looks_non_article(url)
            and not url_looks_supplementary(url)
            and not url_looks_resource_pdf(url)
        ]
        return filtered[:BROWSER_FALLBACK_MAX_CANDIDATES]

    def _maybe_save_captured_responses(
        self,
        responses: list,
        row: dict[str, str],
        output_path: Path,
        *,
        request_url: str,
    ) -> tuple[bool, str]:
        seen_urls: set[str] = set()

        for response in reversed(responses[-20:]):
            response_url = getattr(response, "url", "") or ""
            if response_url in seen_urls:
                continue
            seen_urls.add(response_url)

            ok, detail = self._try_playwright_response(
                response,
                row,
                output_path,
                request_url=request_url or response_url,
            )
            if ok:
                return True, detail

        return False, "no_browser_pdf_response"

    def _accept_cookie_banners(self, page) -> None:
        for selector in BROWSER_COOKIE_ACCEPT_SELECTORS:
            try:
                locator = page.locator(selector)
                count = min(locator.count(), 2)
            except Exception:
                continue

            for index in range(count):
                target = locator.nth(index)
                try:
                    if not target.is_visible():
                        continue
                    target.click(timeout=BROWSER_CLICK_TIMEOUT_MS)
                    page.wait_for_timeout(900)
                    return
                except Exception:
                    continue

    def _expand_page_menus(self, page) -> None:
        for selector in BROWSER_EXPAND_SELECTORS:
            try:
                locator = page.locator(selector)
                count = min(locator.count(), 2)
            except Exception:
                continue

            for index in range(count):
                try:
                    target = locator.nth(index)
                    if not target.is_visible():
                        continue
                    target.click(timeout=BROWSER_CLICK_TIMEOUT_MS)
                    page.wait_for_timeout(700)
                    break
                except Exception:
                    continue

    def _click_pdf_controls(
        self,
        page,
        row: dict[str, str],
        output_path: Path,
        *,
        request_url: str,
    ) -> tuple[bool, str]:
        last_detail = "no_pdf_control"

        for selector in BROWSER_PDF_SELECTORS:
            try:
                locator = page.locator(selector)
                count = min(locator.count(), 3)
            except Exception:
                continue

            for index in range(count):
                target = locator.nth(index)
                try:
                    if not target.is_visible():
                        continue
                except Exception:
                    continue

                try:
                    with page.expect_download(timeout=BROWSER_DOWNLOAD_TIMEOUT_MS) as download_info:
                        target.click(timeout=BROWSER_CLICK_TIMEOUT_MS)
                    download = download_info.value
                except Exception:
                    try:
                        target.click(timeout=BROWSER_CLICK_TIMEOUT_MS)
                        page.wait_for_timeout(1200)
                    except Exception as exc:
                        last_detail = format_exception(exc, "click_failed", limit=180)
                    continue

                ok, detail = self._save_download_artifact(
                    download,
                    row,
                    output_path,
                    request_url=request_url,
                )
                if ok:
                    return True, detail
                last_detail = detail

        return False, last_detail

    def _try_candidate_urls(
        self,
        context,
        page,
        row: dict[str, str],
        candidate_urls: list[str],
        output_path: Path,
        *,
        referer: str = "",
        deadline: float | None = None,
    ) -> tuple[bool, str]:
        last_detail = "no_browser_candidate"

        for candidate_url in candidate_urls[:BROWSER_FALLBACK_MAX_CANDIDATES]:
            if deadline_expired(deadline):
                return False, f"row_timeout>{ROW_TIMEOUT_SECONDS}s"
            ok, detail = self._try_request_pdf(
                context,
                row,
                candidate_url,
                output_path,
                referer=referer,
                deadline=deadline,
            )
            if ok:
                return True, detail
            last_detail = f"{candidate_url} -> {detail}"

            if not url_looks_pdfish(candidate_url):
                continue

            ok, detail = self._try_page_pdf_navigation(
                page,
                row,
                candidate_url,
                output_path,
                referer=referer,
                deadline=deadline,
            )
            if ok:
                return True, detail
            last_detail = f"{candidate_url} -> {detail}"

        return False, last_detail

    def _page_body_text(self, page) -> str:
        try:
            return page.inner_text("body")
        except Exception:
            try:
                return page.content()
            except Exception:
                return ""

    def _page_is_printable_article(self, page, row: dict[str, str], *, request_url: str) -> bool:
        body_text = self._page_body_text(page)
        try:
            title = page.title()
        except Exception:
            title = ""

        if looks_like_block_page(f"{title}\n{body_text}"):
            return False
        if invalid_page_text_reason(f"{title}\n{body_text}"):
            return False

        normalized_body = normalized_match_text(body_text)
        if len(normalized_body) < 1600:
            return False

        pmcid = expected_pmcid(row)
        if pmcid and pmcid.lower() in normalized_body:
            return True

        doi = normalized_doi(row.get("DOI") or "")
        if doi and doi in normalized_body.replace(" ", ""):
            return True

        title_terms = title_significant_terms(row_title(row))
        title_hits = sum(1 for term in title_terms[:6] if term in normalized_body)
        if title_terms and title_hits >= min(3, len(title_terms)):
            return True

        if title_terms and title_hits >= 2 and any(marker in normalized_body for marker in ARTICLE_PAGE_TEXT_MARKERS):
            return True

        return False

    def _try_print_page_pdf(
        self,
        page,
        row: dict[str, str],
        output_path: Path,
        *,
        request_url: str,
    ) -> tuple[bool, str]:
        if not ALLOW_PAGE_PRINT_PDF:
            return False, "print_skipped"
        if not self._page_is_printable_article(page, row, request_url=request_url):
            return False, "print_skipped"

        temp_path = output_path.with_suffix(".playwright-print.pdf")
        try:
            page.pdf(
                path=str(temp_path),
                format="A4",
                print_background=True,
                margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"},
            )
            content = temp_path.read_bytes()
        except Exception as exc:
            return False, format_exception(exc, "print_failed", limit=180)
        finally:
            try:
                temp_path.unlink()
            except OSError:
                pass

        return self._save_pdf_content(
            content,
            row,
            output_path,
            source_url=page.url or request_url,
            request_url=request_url,
        )

    def _download_with_context(
        self,
        context,
        row: dict[str, str],
        start_url: str,
        output_path: Path,
        *,
        deadline: float | None = None,
    ) -> tuple[bool, str]:
        if deadline_expired(deadline):
            return False, f"row_timeout>{ROW_TIMEOUT_SECONDS}s"
        page = context.new_page()
        responses: list = []
        last_detail = "browser_no_pdf"

        page.set_default_timeout(BROWSER_CLICK_TIMEOUT_MS)
        page.set_default_navigation_timeout(BROWSER_NAV_TIMEOUT_MS)
        self._seed_context_cookies(context, start_url)

        def remember_response(response) -> None:
            try:
                response_url = response.url or ""
                content_type = response.headers.get("content-type", "")
            except Exception:
                return

            lowered_type = (content_type or "").lower()
            if url_looks_static_asset(response_url):
                return
            if url_looks_supplementary(response_url) or url_looks_resource_pdf(response_url):
                return
            if url_looks_non_article(response_url):
                return

            if "pdf" in lowered_type or url_looks_pdfish(response_url):
                responses.append(response)

        page.on("response", remember_response)

        try:
            if url_looks_pdfish(start_url):
                ok, detail = self._try_request_pdf(context, row, start_url, output_path, deadline=deadline)
                if ok:
                    return True, detail
                last_detail = f"{start_url} -> {detail}"

            try:
                nav_timeout = int(remaining_timeout(deadline, BROWSER_NAV_TIMEOUT_MS / 1000, minimum=1) * 1000)
                response = page.goto(start_url, wait_until="domcontentloaded", timeout=nav_timeout)
            except Exception as exc:
                try:
                    nav_timeout = int(
                        remaining_timeout(deadline, max(5000, BROWSER_NAV_TIMEOUT_MS // 2) / 1000, minimum=1) * 1000
                    )
                    response = page.goto(
                        start_url,
                        wait_until="commit",
                        timeout=nav_timeout,
                    )
                except Exception:
                    return False, f"{start_url} -> {format_exception(exc, 'navigate', limit=180)}"

            page.wait_for_timeout(BROWSER_RENDER_WAIT_MS)

            try:
                title = page.title()
            except Exception:
                title = ""
            try:
                page_text = page.content()
            except Exception:
                page_text = ""

            if looks_like_block_page(f"{title}\n{page_text}"):
                page.wait_for_timeout(BROWSER_BLOCK_WAIT_MS)
                try:
                    title = page.title()
                except Exception:
                    title = ""
                try:
                    page_text = page.content()
                except Exception:
                    page_text = ""

            self._accept_cookie_banners(page)
            try:
                page_text = page.content()
            except Exception:
                page_text = page_text or ""

            if looks_like_block_page(f"{title}\n{page_text}"):
                return False, f"{start_url} -> block_page"

            embargo_detail = extract_pmc_embargo_detail(f"{title}\n{page_text}", page.url or start_url)
            if embargo_detail:
                return False, embargo_detail

            ok, detail = self._try_playwright_response(response, row, output_path, request_url=start_url)
            if ok:
                return True, detail
            last_detail = f"{start_url} -> {detail}"

            ok, detail = self._maybe_save_captured_responses(
                responses,
                row,
                output_path,
                request_url=start_url,
            )
            if ok:
                return True, detail
            if detail != "no_browser_pdf_response" or last_detail == "browser_no_pdf":
                last_detail = detail

            ok, detail = self._try_print_page_pdf(
                page,
                row,
                output_path,
                request_url=start_url,
            )
            if ok:
                return True, detail
            if detail not in {"print_skipped"}:
                last_detail = detail

            candidate_urls = self._collect_page_candidates(page, row, start_url)
            ok, detail = self._try_candidate_urls(
                context,
                page,
                row,
                candidate_urls,
                output_path,
                referer=page.url or start_url,
                deadline=deadline,
            )
            if ok:
                return True, detail
            last_detail = detail

            ok, detail = self._try_print_page_pdf(
                page,
                row,
                output_path,
                request_url=start_url,
            )
            if ok:
                return True, detail
            if detail not in {"print_skipped"}:
                last_detail = detail

            self._expand_page_menus(page)
            candidate_urls = self._collect_page_candidates(page, row, start_url)
            ok, detail = self._try_candidate_urls(
                context,
                page,
                row,
                candidate_urls,
                output_path,
                referer=page.url or start_url,
                deadline=deadline,
            )
            if ok:
                return True, detail
            last_detail = detail

            ok, detail = self._try_print_page_pdf(
                page,
                row,
                output_path,
                request_url=start_url,
            )
            if ok:
                return True, detail
            if detail not in {"print_skipped"}:
                last_detail = detail

            ok, detail = self._click_pdf_controls(
                page,
                row,
                output_path,
                request_url=start_url,
            )
            if ok:
                return True, detail
            last_detail = detail

            ok, detail = self._maybe_save_captured_responses(
                responses,
                row,
                output_path,
                request_url=start_url,
            )
            if ok:
                return True, detail
            if detail != "no_browser_pdf_response" or last_detail == "browser_no_pdf":
                last_detail = detail

            return False, last_detail
        finally:
            try:
                page.close()
            except Exception:
                pass

    def download_row(
        self,
        row: dict[str, str],
        source_urls: list[str],
        output_path: Path,
        *,
        deadline: float | None = None,
    ) -> DownloadResult:
        if deadline_expired(deadline):
            return DownloadResult(False, "", f"row_timeout>{ROW_TIMEOUT_SECONDS}s")
        if not self.available():
            return DownloadResult(False, "", "browser_unavailable")

        prioritized_sources: list[str] = []
        for source_url in source_urls:
            if not source_url or not browser_source_allowed(row, source_url):
                continue
            if url_looks_static_asset(source_url):
                continue
            if url_looks_supplementary(source_url) or url_looks_resource_pdf(source_url):
                continue
            if url_looks_non_article(source_url):
                continue
            prioritized_sources.append(source_url)

        prioritized_sources = unique_urls(prioritized_sources)
        prioritized_sources.sort(key=lambda source_url: browser_source_priority(row, source_url))
        prioritized_sources = condense_browser_sources(prioritized_sources)
        prioritized_sources = prioritized_sources[:BROWSER_FALLBACK_MAX_SOURCES]
        if not prioritized_sources:
            return DownloadResult(False, "", "browser_skipped")

        errors: list[str] = []
        for browser_name, engine_name, executable_path in BROWSER_TARGETS:
            if deadline_expired(deadline):
                break
            context = self._context_for(browser_name, engine_name, executable_path)
            if context is None:
                errors.append(f"{browser_name}:{self._launch_errors.get(browser_name, 'launch_failed')}")
                continue

            family_block_counts: dict[str, int] = {}
            for start_url in prioritized_sources:
                if deadline_expired(deadline):
                    errors.append(f"{browser_name}:row_timeout>{ROW_TIMEOUT_SECONDS}s")
                    break
                family = browser_source_family(start_url)
                if family and family_block_counts.get(family, 0) >= 2:
                    errors.append(f"{browser_name}:{family}:block_page_budget")
                    continue
                ok, detail = self._download_with_context(context, row, start_url, output_path, deadline=deadline)
                if ok:
                    return DownloadResult(True, detail, f"browser_{browser_name}")
                errors.append(f"{browser_name}:{detail}")
                if "block_page" in detail and family:
                    family_block_counts[family] = family_block_counts.get(family, 0) + 1

        summary = " | ".join(unique_urls(errors)[:6]) or "browser_failed"
        return DownloadResult(False, "", summary)


def try_download_pdf(
    session: std_requests.Session,
    row: dict[str, str],
    url: str,
    output_path: Path,
    *,
    referer: str = "",
    deadline: float | None = None,
) -> tuple[bool, str]:
    errors: list[str] = []
    order = ("advanced", "standard") if should_try_advanced_first(url) else ("standard", "advanced")

    for mode in order:
        if deadline_expired(deadline):
            return False, f"row_timeout>{ROW_TIMEOUT_SECONDS}s"
        if mode == "advanced":
            advanced = advanced_request(
                url,
                accept=PDF_ACCEPT,
                referer=referer,
                timeout=REQUEST_TIMEOUT,
                return_last=True,
                deadline=deadline,
            )
            if advanced is None:
                continue

            if response_matches_accept(advanced, PDF_ACCEPT):
                verdict, reason = validate_pdf_for_row(advanced.content, row, advanced.url or url, request_url=url)
                if verdict == "match":
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(advanced.content)
                    return True, advanced.url or url
                errors.append(f"pdf_{reason}")

            snippet = normalized_text(advanced.content[:120].decode("utf-8", "replace"))
            errors.append(f"{advanced.status_code}:{advanced.headers.get('content-type', '')}:{snippet[:80]}")
            continue

        try:
            response = request_with_retries(
                session,
                url,
                accept=PDF_ACCEPT,
                referer=referer,
                deadline=deadline,
            )
        except TimeoutError:
            return False, f"row_timeout>{ROW_TIMEOUT_SECONDS}s"
        except RequestException as exc:
            response = None
            errors.append(format_exception(exc, limit=180))

        if response is None:
            continue

        content = response.content
        content_type = response.headers.get("content-type", "")

        if response_matches_accept(response, PDF_ACCEPT):
            verdict, reason = validate_pdf_for_row(content, row, response.url or url, request_url=url)
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

    final_detail = " | ".join(unique_urls(errors)) or "download_failed"
    note_host_failure(url, final_detail)
    return False, final_detail


def download_row(
    session: std_requests.Session,
    csv_path: Path,
    row: dict[str, str],
    browser_downloader: BrowserPDFDownloader | None = None,
    *,
    deadline: float | None = None,
) -> DownloadResult:
    output_path = output_path_for(csv_path, row)
    if valid_existing_pdf(output_path, row, row.get("pdf_url") or ""):
        return DownloadResult(True, row.get("pdf_url", ""), "already_exists")

    source_urls = build_source_urls(row, include_metadata=False, deadline=deadline)
    if not source_urls:
        source_urls = build_source_urls(row, include_metadata=True, deadline=deadline)
        if not source_urls:
            return DownloadResult(False, "", "missing_second_link_and_doi")

    browser_sources = list(source_urls)
    last_detail = "no_pdf_candidate"
    tried_pdf_urls: set[str] = set()
    queued_sources = list(source_urls)
    seen_sources: set[str] = set()
    metadata_loaded = False

    while queued_sources or not metadata_loaded:
        if deadline_expired(deadline):
            return DownloadResult(False, "", f"row_timeout>{ROW_TIMEOUT_SECONDS}s")
        if not queued_sources and not metadata_loaded:
            metadata_loaded = True
            for extra_source in build_source_urls(row, include_metadata=True, deadline=deadline):
                if extra_source not in seen_sources and extra_source not in queued_sources:
                    queued_sources.append(extra_source)
            continue

        start_url = queued_sources.pop(0)
        if not start_url or start_url in seen_sources:
            continue
        seen_sources.add(start_url)

        if url_looks_pdfish(start_url):
            ok, detail = try_download_pdf(
                session,
                row,
                start_url,
                output_path,
                referer=doi_url_from_row(row),
                deadline=deadline,
            )
            if ok:
                return DownloadResult(True, detail, "downloaded")
            last_detail = f"{start_url} -> {detail}"
            tried_pdf_urls.add(start_url)
            continue

        try:
            response = fetch_html_response(session, start_url, deadline=deadline)
        except TimeoutError:
            return DownloadResult(False, "", f"row_timeout>{ROW_TIMEOUT_SECONDS}s")
        if response is None:
            last_detail = f"{start_url} -> no_response"
            continue

        if response.status_code >= 400:
            last_detail = f"{start_url} -> HTTP {response.status_code}"
            continue

        if is_pdf_bytes(response.content, response.headers.get("content-type", "")):
            verdict, reason = validate_pdf_for_row(
                response.content,
                row,
                response.url or start_url,
                request_url=start_url,
            )
            if verdict == "match":
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(response.content)
                return DownloadResult(True, response.url or start_url, "direct_pdf")
            last_detail = f"{response.url or start_url} -> pdf_{reason}"
            continue

        final_url = response.url or start_url
        page_text = response_to_text(response)
        if final_url and final_url not in browser_sources:
            browser_sources.append(final_url)
        embargo_detail = extract_pmc_embargo_detail(page_text, final_url or start_url)
        if embargo_detail:
            last_detail = embargo_detail
            continue

        for extra_source in collect_pubmed_source_urls(row, start_url, final_url, page_text):
            if extra_source not in seen_sources:
                queued_sources.append(extra_source)

        candidates = collect_candidate_urls(row, start_url, final_url, page_text)
        for candidate in candidates:
            if candidate in tried_pdf_urls:
                continue
            tried_pdf_urls.add(candidate)
            ok, detail = try_download_pdf(
                session,
                row,
                candidate,
                output_path,
                referer=final_url,
                deadline=deadline,
            )
            if ok:
                return DownloadResult(True, detail, "downloaded")
            last_detail = f"{candidate} -> {detail}"

    if browser_downloader is not None:
        browser_result = browser_downloader.download_row(row, browser_sources, output_path, deadline=deadline)
        if browser_result.success:
            return browser_result
        if browser_result.detail not in {"browser_skipped", "browser_unavailable"}:
            last_detail = f"{last_detail} | {browser_result.detail}" if last_detail else browser_result.detail

    return DownloadResult(False, "", last_detail)


def process_csv(csv_path: Path) -> tuple[int, int]:
    rows, fieldnames = load_rows(csv_path)
    out_dir = output_dir_for(csv_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    promoted_count = promote_existing_pending_rows(csv_path, rows, fieldnames)
    if promoted_count:
        print(f"Promoted existing PDFs for {csv_path.name}: {promoted_count}", flush=True)
        rows, fieldnames = load_rows(csv_path)

    session = build_session()
    browser_downloader = BrowserPDFDownloader()

    success_count = 0
    attempted_count = 0

    try:
        for index, row in enumerate(rows, 1):
            status = (row.get("download_status") or "").strip().lower()
            if status == "success":
                continue

            attempted_count += 1
            try:
                result = download_row(
                    session,
                    csv_path,
                    row,
                    browser_downloader,
                    deadline=row_deadline(),
                )
            except TimeoutError:
                result = DownloadResult(False, "", f"row_timeout>{ROW_TIMEOUT_SECONDS}s")
                try:
                    browser_downloader.close()
                except Exception:
                    pass
                browser_downloader = BrowserPDFDownloader()
                try:
                    session.close()
                except Exception:
                    pass
                session = build_session()
            except Exception as exc:
                result = DownloadResult(False, "", format_exception(exc, "row_exception", limit=240))
                try:
                    browser_downloader.close()
                except Exception:
                    pass
                browser_downloader = BrowserPDFDownloader()
                try:
                    session.close()
                except Exception:
                    pass
                session = build_session()

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
                row["download_error"] = result.detail
                print(
                    f"[{csv_path.name} {index}/{len(rows)}] KEEP {row.get('download_status', '') or 'blank'} "
                    f"- {result.detail}",
                    flush=True,
                )

            save_rows(csv_path, rows, fieldnames)
            time.sleep(DELAY_SECONDS)
    finally:
        try:
            session.close()
        except Exception:
            pass
        browser_downloader.close()

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
