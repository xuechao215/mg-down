#!/usr/bin/env python3
"""Download docs-csv PDFs by reusing the user's live Google Chrome session."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlsplit, urlunsplit

import download_docs_csv_pdfs as base


LIVE_RENDER_WAIT_SECONDS = 5.0
LIVE_AFTER_CLICK_WAIT_SECONDS = 4.0
LIVE_BETWEEN_ROWS_SECONDS = 1.0
LIVE_MAX_SOURCE_URLS = 8
LIVE_MAX_DISCOVERY_HOPS = 4
LIVE_DISCOVERY_POLL_ROUNDS = 4
LIVE_DISCOVERY_POLL_SECONDS = 1.5
LIVE_DOWNLOAD_WAIT_SECONDS = 45
LIVE_ROW_TIMEOUT_SECONDS = 120
LIVE_OSASCRIPT_TIMEOUT_SECONDS = 15.0


def log(*parts: object) -> None:
    print(*parts, flush=True)


def run_osascript(lines: Iterable[str]) -> str:
    line_list = [str(line) for line in lines]
    cmd = ["osascript"]
    for line in line_list:
        cmd.extend(["-e", line])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=LIVE_OSASCRIPT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"osascript_timeout>{LIVE_OSASCRIPT_TIMEOUT_SECONDS:.0f}s") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(detail or f"osascript_failed:{result.returncode}")
    return (result.stdout or "").strip()


def chrome_activate() -> None:
    run_osascript(
        [
            'tell application "Google Chrome"',
            "activate",
            "end tell",
        ]
    )


def chrome_set_front_url(url: str) -> None:
    chrome_activate()
    run_osascript(
        [
            'tell application "Google Chrome"',
            "set URL of active tab of front window to " + applescript_string(url),
            "end tell",
        ]
    )


def chrome_front_url() -> str:
    chrome_activate()
    return run_osascript(
        [
            'tell application "Google Chrome"',
            "return URL of active tab of front window",
            "end tell",
        ]
    )


def chrome_exec_js(js_code: str) -> str:
    chrome_activate()
    return run_osascript(
        [
            'tell application "Google Chrome"',
            "return execute active tab of front window javascript " + applescript_string(js_code),
            "end tell",
        ]
    )


def applescript_string(value: str) -> str:
    escaped = (value or "").replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def wait_for_front_tab(seconds: float) -> None:
    deadline = time.time() + max(seconds, 0.1)
    last_ready = ""
    while time.time() < deadline:
        try:
            last_ready = chrome_exec_js("document.readyState")
            if last_ready in {"complete", "interactive"}:
                time.sleep(0.8)
                return
        except Exception:
            pass
        time.sleep(0.4)
    time.sleep(0.8)


def canonicalize_url(url: str) -> str:
    value = base.normalized_text(url)
    if not value:
        return ""
    try:
        parts = urlsplit(value)
    except Exception:
        return value.split("#", 1)[0]
    host = (parts.netloc or "").lower()
    if host.endswith("linkinghub.elsevier.com"):
        query = parse_qs(parts.query, keep_blank_values=False)
        for key in ("Redirect", "redirect"):
            redirected = base.normalized_text((query.get(key) or [""])[0])
            if redirected:
                return canonicalize_url(redirected)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


def ensure_chrome_at_url(url: str, *, wait_seconds: float) -> str:
    target = canonicalize_url(url)
    if not target:
        return canonicalize_url(chrome_front_url())
    current = canonicalize_url(chrome_front_url())
    if current != target:
        chrome_set_front_url(target)
        wait_for_front_tab(wait_seconds)
    return canonicalize_url(chrome_front_url())


def live_source_allowed(url: str) -> bool:
    normalized = canonicalize_url(url)
    if not normalized:
        return False
    host = base.host_for(normalized)
    path = urlsplit(normalized).path.lower()
    if not host:
        return False
    if host.endswith("account.ncbi.nlm.nih.gov"):
        return False
    if host.endswith("pubmed.ncbi.nlm.nih.gov"):
        return not (
            base.url_looks_static_asset(normalized)
            or base.url_looks_non_article(normalized)
            or base.url_looks_supplementary(normalized)
            or base.url_looks_resource_pdf(normalized)
        )
    if host.endswith("pmc.ncbi.nlm.nih.gov"):
        return not (
            base.url_looks_static_asset(normalized)
            or base.url_looks_non_article(normalized)
            or base.url_looks_supplementary(normalized)
            or base.url_looks_resource_pdf(normalized)
        )
    if host in {"nih.gov", "www.nih.gov"} or host.endswith(".nih.gov"):
        return False
    if host.endswith("elsevier.support"):
        return False
    if "articleselect" in path:
        return False
    if base.url_looks_static_asset(normalized):
        return False
    if base.url_looks_non_article(normalized):
        return False
    if base.url_looks_supplementary(normalized) or base.url_looks_resource_pdf(normalized):
        return False
    return True


def downloads_dir() -> Path:
    return Path.home() / "Downloads"


def snapshot_download_files() -> dict[str, tuple[float, int]]:
    files: dict[str, tuple[float, int]] = {}
    for path in downloads_dir().glob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        files[path.name] = (stat.st_mtime, stat.st_size)
    return files


def js_discovery_payload() -> str:
    return r"""
(() => {
  const toAbs = (value) => {
    if (!value) return "";
    try {
      return new URL(String(value), location.href).href;
    } catch (error) {
      return "";
    }
  };

  const wanted = (value) => {
    const lowered = String(value || "").toLowerCase();
    if (!lowered) return false;
    return (
      lowered.includes("pdf.sciencedirectassets.com") ||
      lowered.includes("main.pdf") ||
      lowered.includes("/pdfft") ||
      lowered.includes("/doi/pdf") ||
      lowered.includes("/doi/epdf") ||
      lowered.includes("/article-pdf/") ||
      lowered.includes("showpdf") ||
      lowered.includes("downloadpdf") ||
      lowered.includes("pdfdirect") ||
      lowered.endsWith(".pdf") ||
      lowered.includes("/science/article/pii/")
    );
  };

  const seen = new Set();
  const candidates = [];
  const push = (value) => {
    const absolute = toAbs(value);
    if (!absolute || !wanted(absolute) || seen.has(absolute)) return;
    seen.add(absolute);
    candidates.push(absolute);
  };

  push(location.href);

  try {
    for (const entry of performance.getEntriesByType("resource")) {
      push(entry.name);
    }
  } catch (error) {}

  const attrs = ["href", "src", "data", "data-href", "data-url", "data-pdf-url", "data-download-url", "content"];
  try {
    for (const element of document.querySelectorAll("a[href],iframe[src],embed[src],object[data],[data-href],[data-url],[data-pdf-url],[data-download-url],meta[content]")) {
      for (const name of attrs) {
        try {
          const value = element.getAttribute(name);
          if (value) push(value);
        } catch (error) {}
      }
    }
  } catch (error) {}

  const controls = [];
  const pubmedLinks = [];
  const pubmedSeen = new Set();
  const addControl = (element, text, href) => {
    const compact = String(text || "").replace(/\s+/g, " ").trim();
    const lowered = (compact + " " + String(href || "")).toLowerCase();
    if (!lowered) return;
    if (!(
      lowered.includes("pdf") ||
      lowered.includes("download") ||
      lowered.includes("view") ||
      lowered.includes("full text")
    )) return;
    controls.push({
      text: compact,
      href: toAbs(href || ""),
      tag: element.tagName || "",
      aria: element.getAttribute("aria-label") || "",
      title: element.getAttribute("title") || "",
    });
  };

  const addPubmedLink = (element) => {
    const href = toAbs(element.getAttribute("href") || "");
    if (!href || pubmedSeen.has(href)) return;
    pubmedSeen.add(href);
    pubmedLinks.push({
      href,
      text: String(element.innerText || element.textContent || "").replace(/\s+/g, " ").trim(),
      title: element.getAttribute("title") || "",
      ref: element.getAttribute("ref") || "",
      aria: element.getAttribute("aria-label") || "",
    });
  };

  try {
    for (const element of document.querySelectorAll("a,button,[role=button]")) {
      const text = element.innerText || element.textContent || "";
      const href = element.getAttribute("href") || "";
      addControl(element, text, href);
    }
  } catch (error) {}

  if (location.hostname.includes("pubmed.ncbi.nlm.nih.gov")) {
    try {
      for (const element of document.querySelectorAll(
        ".full-text-links-list a[href], #full-text-links-dialog a[href], .linkout-category-links a[href], a.link-item[href][ref*='fulltext'], a.link-item[href][title*='full text' i]"
      )) {
        addPubmedLink(element);
      }
    } catch (error) {}
  }

  return JSON.stringify({
    url: location.href,
    title: document.title || "",
    body: ((document.body && document.body.innerText) || "").slice(0, 2000),
    candidates,
    controls: controls.slice(0, 40),
    pubmedLinks: pubmedLinks.slice(0, 20),
  });
})()
"""


def js_click_best_pdf_control() -> str:
    return r"""
(() => {
  const visible = (element) => !!(
    element &&
    (element.offsetWidth || element.offsetHeight || element.getClientRects().length)
  );

  const scoreFor = (text, href) => {
    const lowered = (String(text || "") + " " + String(href || "")).toLowerCase();
    let score = 0;
    if (lowered.includes("view pdf")) score += 40;
    if (lowered.includes("download pdf")) score += 38;
    if (lowered.includes("open pdf")) score += 36;
    if (lowered.includes(" pdf ")) score += 20;
    if (lowered.includes("/pdfft")) score += 45;
    if (lowered.includes("/article-pdf/")) score += 40;
    if (lowered.includes("/doi/pdf")) score += 30;
    if (lowered.includes("/doi/epdf")) score += 28;
    if (lowered.includes(".pdf")) score += 24;
    if (lowered.includes("full text")) score += 8;
    return score;
  };

  let best = null;
  for (const element of document.querySelectorAll("a,button,[role=button]")) {
    if (!visible(element)) continue;
    const text = String(element.innerText || element.textContent || "").replace(/\s+/g, " ").trim();
    const href = element.getAttribute("href") || "";
    const score = scoreFor(text, href);
    if (!score) continue;
    if (!best || score > best.score) {
      best = { element, text, href, score };
    }
  }

  if (!best) {
    return JSON.stringify({ clicked: false, reason: "no_control" });
  }

  if (best.href) {
    location.href = new URL(best.href, location.href).href;
    return JSON.stringify({ clicked: true, via: "href", href: best.href, text: best.text, score: best.score });
  }

  best.element.click();
  return JSON.stringify({ clicked: true, via: "click", text: best.text, score: best.score });
})()
"""


def js_trigger_native_download(filename: str) -> str:
    safe_name = json.dumps(filename)
    return rf"""
(() => {{
  const anchor = document.createElement('a');
  anchor.href = location.href;
  anchor.download = {safe_name};
  document.body.appendChild(anchor);
  anchor.click();
  return JSON.stringify({{ok:true, href: location.href, filename: {safe_name}}});
}})()
"""


def parse_snapshot(raw: str) -> dict:
    cleaned = (raw or "").strip()
    if not cleaned:
        return {"url": "", "title": "", "body": "", "candidates": [], "controls": [], "pubmedLinks": []}
    try:
        return json.loads(cleaned)
    except Exception:
        return {"url": "", "title": "", "body": cleaned, "candidates": [], "controls": [], "pubmedLinks": []}


def live_snapshot() -> dict:
    return parse_snapshot(chrome_exec_js(js_discovery_payload()))


def prioritized_pubmed_links(entries: Iterable[dict], row: dict[str, str]) -> list[str]:
    prioritized: list[str] = []
    for entry in entries or []:
        href = canonicalize_url((entry or {}).get("href") or "")
        if not href or not live_source_allowed(href):
            continue
        if not (
            base.url_contains_row_identifiers(href, row)
            or base.url_looks_pdfish(href)
            or base.url_looks_article_pdf(href)
            or "pmc.ncbi.nlm.nih.gov/articles/" in href.lower()
        ):
            continue
        prioritized.append(href)

    prioritized = base.unique_urls(prioritized)

    def pubmed_priority(url: str) -> tuple[int, int]:
        lowered = url.lower()
        score = 100
        if "pmc.ncbi.nlm.nih.gov/articles/" in lowered and "/pdf/" in lowered:
            score -= 90
        elif "pmc.ncbi.nlm.nih.gov/articles/" in lowered:
            score -= 70
        elif base.url_looks_article_pdf(url) or base.url_looks_pdfish(url):
            score -= 60
        elif base.host_for(url).endswith("pubmed.ncbi.nlm.nih.gov"):
            score -= 40
        return score, len(url)

    prioritized.sort(key=pubmed_priority)
    return prioritized


def discover_live_pdf_urls(start_url: str, row: dict[str, str]) -> tuple[list[str], str]:
    queue = [canonicalize_url(start_url)]
    visited: set[str] = set()
    best_referer = canonicalize_url(start_url)

    for _ in range(LIVE_MAX_DISCOVERY_HOPS):
        if not queue:
            break
        current = canonicalize_url(queue.pop(0))
        if not current or current in visited:
            continue
        visited.add(current)

        chrome_set_front_url(current)
        wait_for_front_tab(LIVE_RENDER_WAIT_SECONDS)
        snapshot: dict = {}
        candidates: list[str] = []
        pubmed_links: list[str] = []

        for round_index in range(LIVE_DISCOVERY_POLL_ROUNDS):
            snapshot = live_snapshot()
            current_url = canonicalize_url(snapshot.get("url") or current)
            best_referer = current_url or current

            body_text = base.normalized_text(snapshot.get("body") or "")
            if body_text and base.looks_like_block_page(body_text):
                return [], f"block_page:{body_text[:160]}"

            candidates = normalize_candidates([current_url, *(snapshot.get("candidates") or [])], row)
            direct = direct_pdf_candidates(candidates, row)
            if direct:
                return direct, best_referer

            pubmed_links = prioritized_pubmed_links(snapshot.get("pubmedLinks") or [], row)
            if pubmed_links:
                for href in reversed(pubmed_links):
                    if href not in visited and href not in queue:
                        queue.insert(0, href)
                break

            if round_index + 1 < LIVE_DISCOVERY_POLL_ROUNDS:
                time.sleep(LIVE_DISCOVERY_POLL_SECONDS)

        if pubmed_links:
            continue

        clicked = parse_snapshot(chrome_exec_js(js_click_best_pdf_control()))
        if clicked.get("clicked"):
            wait_for_front_tab(LIVE_AFTER_CLICK_WAIT_SECONDS)
            for round_index in range(LIVE_DISCOVERY_POLL_ROUNDS):
                snapshot = live_snapshot()
                current_url = canonicalize_url(snapshot.get("url") or current)
                best_referer = current_url or best_referer
                candidates = normalize_candidates([current_url, *(snapshot.get("candidates") or [])], row)
                direct = direct_pdf_candidates(candidates, row)
                if direct:
                    return direct, best_referer
                pubmed_links = prioritized_pubmed_links(snapshot.get("pubmedLinks") or [], row)
                if pubmed_links:
                    for href in reversed(pubmed_links):
                        if href not in visited and href not in queue:
                            queue.insert(0, href)
                    break
                if round_index + 1 < LIVE_DISCOVERY_POLL_ROUNDS:
                    time.sleep(LIVE_DISCOVERY_POLL_SECONDS)

        for candidate in candidates:
            candidate = canonicalize_url(candidate)
            if not live_source_allowed(candidate):
                continue
            if candidate not in visited and candidate not in queue:
                queue.append(candidate)

        for control in snapshot.get("controls") or []:
            href = canonicalize_url((control or {}).get("href") or "")
            if not href:
                continue
            if not live_source_allowed(href):
                continue
            if href not in visited and href not in queue:
                queue.append(href)

    return [], best_referer


def normalize_candidates(candidates: Iterable[str], row: dict[str, str]) -> list[str]:
    normalized: list[str] = []
    for candidate in candidates:
        url = canonicalize_url(candidate)
        if not url:
            continue
        if not live_source_allowed(url):
            continue
        if (
            not base.url_contains_row_identifiers(url, row)
            and not base.url_looks_pdfish(url)
            and not base.url_looks_article_pdf(url)
        ):
            continue
        normalized.append(url)
    return base.unique_urls(normalized)


def direct_pdf_candidates(candidates: Iterable[str], row: dict[str, str]) -> list[str]:
    direct: list[str] = []
    for candidate in candidates:
        url = canonicalize_url(candidate)
        if not url:
            continue
        lowered = url.lower()
        if "pdf.sciencedirectassets.com" in lowered or "main.pdf" in lowered:
            direct.append(url)
            continue
        if base.url_looks_pdfish(url):
            direct.append(url)
            continue
        if base.url_looks_article_pdf(url):
            direct.append(url)
            continue
    unique = base.unique_urls(direct)
    unique.sort(key=direct_pdf_priority)
    return unique


def direct_pdf_priority(url: str) -> tuple[int, int]:
    lowered = url.lower()
    score = 100
    if "pdfdirect" in lowered:
        score -= 70
    if "pdf.sciencedirectassets.com" in lowered and "main.pdf" in lowered:
        score -= 60
    if "main.pdf" in lowered:
        score -= 20
    if lowered.endswith(".pdf"):
        score -= 10
    if "pdfft" in lowered:
        score += 10
    return score, len(url)


def wait_for_download_completion(before: dict[str, tuple[float, int]], *, timeout_seconds: float) -> Path | None:
    deadline = time.time() + max(timeout_seconds, 1.0)
    newest_final: Path | None = None
    newest_score = (-1.0, -1)

    while time.time() < deadline:
        crdownload_seen = False
        for path in downloads_dir().glob("*"):
            if not path.is_file():
                continue

            name = path.name
            try:
                stat = path.stat()
            except OSError:
                continue

            previous = before.get(name)
            changed = previous is None or stat.st_mtime > previous[0] or stat.st_size != previous[1]
            if not changed:
                continue

            if name.endswith(".crdownload"):
                crdownload_seen = True
                continue

            score = (stat.st_mtime, stat.st_size)
            if score > newest_score:
                newest_score = score
                newest_final = path

        if newest_final is not None and not crdownload_seen:
            return newest_final
        time.sleep(1.0)

    return newest_final


def trigger_browser_download(row: dict[str, str], output_path: Path, *, source_url: str = "") -> tuple[bool, str]:
    if source_url:
        try:
            ensure_chrome_at_url(source_url, wait_seconds=LIVE_AFTER_CLICK_WAIT_SECONDS)
        except Exception as exc:
            return False, base.format_exception(exc, "browser_nav_failed", limit=180)

    before = snapshot_download_files()
    chrome_exec_js(js_trigger_native_download(output_path.name))
    downloaded = wait_for_download_completion(before, timeout_seconds=LIVE_DOWNLOAD_WAIT_SECONDS)
    if downloaded is None:
        return False, "download_timeout"

    try:
        content = downloaded.read_bytes()
    except OSError as exc:
        return False, base.format_exception(exc, "download_read_failed", limit=180)

    final_url = canonicalize_url(chrome_front_url())
    verdict, reason = base.validate_pdf_for_row(content, row, final_url, request_url=final_url or source_url)
    if verdict != "match":
        try:
            downloaded.unlink()
        except OSError:
            pass
        return False, f"pdf_{reason}"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(content)

    try:
        downloaded.unlink()
    except OSError:
        pass

    return True, final_url or source_url


def should_trigger_browser_download(source_url: str, response_url: str = "", *, content_type: str = "", status_code: int = 0) -> bool:
    candidate = canonicalize_url(response_url or source_url)
    lowered = candidate.lower()
    family = base.browser_source_family(candidate)
    normalized_content_type = (content_type or "").lower()

    if "pdf.sciencedirectassets.com" in lowered:
        return True
    if family == "elsevier":
        return True
    if status_code == 403 and family in {"aacr", "lww"}:
        return True
    if status_code == 403 and "application/pdf" in normalized_content_type and family in {"wiley", "tandf"}:
        return True
    return False


def try_http_download(session, row: dict[str, str], source_url: str, output_path: Path, *, referer: str = "") -> tuple[bool, str]:
    errors: list[str] = []
    headers = {
        "Accept": base.PDF_ACCEPT,
        "User-Agent": base.USER_AGENT,
    }
    if referer:
        headers["Referer"] = referer

    browser_fallback_candidate = any(
        (
            base.url_looks_pdfish(source_url),
            base.url_looks_article_pdf(source_url),
        )
    )

    jar = base.load_cookie_jar(source_url, refresh=True, preferred_browsers=("chrome",))

    if base.curl_requests is not None:
        for impersonation in base.ADVANCED_IMPERSONATIONS:
            try:
                response = base.curl_requests.get(
                    source_url,
                    headers=headers,
                    cookies=jar,
                    impersonate=impersonation,
                    timeout=base.REQUEST_TIMEOUT,
                    allow_redirects=True,
                )
            except Exception as exc:
                errors.append(base.format_exception(exc, f"curl_{impersonation}", limit=180))
                continue

            content_type = response.headers.get("content-type", "")
            if base.is_pdf_bytes(response.content, content_type):
                verdict, reason = base.validate_pdf_for_row(
                    response.content,
                    row,
                    response.url or source_url,
                    request_url=source_url,
                )
                if verdict == "match":
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(response.content)
                    return True, response.url or source_url
                errors.append(f"pdf_{reason}")
            elif browser_fallback_candidate and should_trigger_browser_download(
                source_url,
                response.url or source_url,
                content_type=content_type,
                status_code=response.status_code,
            ):
                fallback_ok, fallback_detail = trigger_browser_download(
                    row,
                    output_path,
                    source_url=response.url or source_url,
                )
                if fallback_ok:
                    return True, fallback_detail
                errors.append(fallback_detail)
            elif browser_fallback_candidate and "html" in content_type.lower() and should_trigger_browser_download(
                source_url,
                response.url or source_url,
                content_type=content_type,
                status_code=response.status_code,
            ):
                fallback_ok, fallback_detail = trigger_browser_download(
                    row,
                    output_path,
                    source_url=response.url or source_url,
                )
                if fallback_ok:
                    return True, fallback_detail
                errors.append(fallback_detail)
            else:
                errors.append(f"http_{response.status_code}:{content_type}")

    try:
        response = session.get(
            source_url,
            headers=headers,
            cookies=jar,
            timeout=base.REQUEST_TIMEOUT,
            allow_redirects=True,
        )
    except Exception as exc:
        errors.append(base.format_exception(exc, "requests", limit=180))
        return False, " | ".join(base.unique_urls(errors)) or "download_failed"

    content_type = response.headers.get("content-type", "")
    if base.is_pdf_bytes(response.content, content_type):
        verdict, reason = base.validate_pdf_for_row(
            response.content,
            row,
            response.url or source_url,
            request_url=source_url,
        )
        if verdict == "match":
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(response.content)
            return True, response.url or source_url
        errors.append(f"pdf_{reason}")
    elif browser_fallback_candidate and should_trigger_browser_download(
        source_url,
        response.url or source_url,
        content_type=content_type,
        status_code=response.status_code,
    ):
        fallback_ok, fallback_detail = trigger_browser_download(
            row,
            output_path,
            source_url=response.url or source_url,
        )
        if fallback_ok:
            return True, fallback_detail
        errors.append(fallback_detail)
    elif browser_fallback_candidate and "html" in content_type.lower() and should_trigger_browser_download(
        source_url,
        response.url or source_url,
        content_type=content_type,
        status_code=response.status_code,
    ):
        fallback_ok, fallback_detail = trigger_browser_download(
            row,
            output_path,
            source_url=response.url or source_url,
        )
        if fallback_ok:
            return True, fallback_detail
        errors.append(fallback_detail)
    else:
        errors.append(f"http_{response.status_code}:{content_type}")

    return False, " | ".join(base.unique_urls(errors)) or "download_failed"


def source_candidates_for_row(row: dict[str, str]) -> list[str]:
    source_urls: list[str] = []
    preferred_pdf_url = canonicalize_url(row.get("pdf_url") or "")
    pubmed_url = canonicalize_url(
        row.get("pubmed_url") or f"https://pubmed.ncbi.nlm.nih.gov/{base.expected_pmid(row)}/"
    )

    for candidate in (
        pubmed_url,
        row.get("pdf_url") or "",
        row.get("second_link") or "",
        base.choose_best_second_link(row),
    ):
        candidate = canonicalize_url(candidate)
        if candidate:
            source_urls.append(candidate)

    for candidate in base.build_source_urls(row, include_metadata=True):
        normalized = canonicalize_url(candidate)
        if normalized:
            source_urls.append(normalized)
    prioritized = base.unique_urls(
        canonicalize_url(url)
        for url in source_urls
        if live_source_allowed(url)
    )
    def source_priority(url: str) -> tuple[int, int]:
        lowered = url.lower()
        host = base.host_for(url)
        path = urlsplit(url).path.lower()
        family = base.browser_source_family(url)
        score = 100
        if preferred_pdf_url == canonicalize_url(url):
            score -= 100
        if host.endswith("pubmed.ncbi.nlm.nih.gov"):
            score += 120
        if "pmc.ncbi.nlm.nih.gov/articles/" in lowered and "/pdf/" in lowered:
            score -= 90
        elif "pmc.ncbi.nlm.nih.gov/articles/" in lowered:
            score -= 70
        if family == "wiley" and "/doi/" in path and not base.url_looks_pdfish(url):
            score -= 95
        if family == "tandf" and "/doi/" in path and not base.url_looks_pdfish(url):
            score -= 90
        if host.endswith("ascopubs.org") and "/doi/" in path and "pdfdirect" not in path:
            score -= 90
        if family == "elsevier" and "/science/article/pii/" in path and not base.url_looks_pdfish(url):
            score -= 85
        if "pdf.sciencedirectassets.com" in lowered and "main.pdf" in lowered:
            score -= 80
        if "pdfft" in lowered:
            score -= 60
        if "/doi/pdf/" in lowered or "/doi/epdf/" in lowered:
            score -= 15
        if base.url_looks_article_pdf(url) or base.url_looks_pdfish(url):
            score -= 15
        if family in {"tandf", "wiley", "lww"} and (base.url_looks_article_pdf(url) or base.url_looks_pdfish(url)):
            score += 10
        return score, len(url)

    prioritized.sort(key=source_priority)
    return prioritized[:LIVE_MAX_SOURCE_URLS]


def process_csv(
    csv_path: Path,
    *,
    pmids: set[str],
    statuses: set[str],
    only_doi_prefixes: set[str],
    skip_doi_prefixes: set[str],
    limit: int,
    delay: float,
) -> tuple[int, int]:
    rows, fieldnames = base.load_rows(csv_path)
    session = base.build_session()
    chrome_activate()

    attempted = 0
    success = 0

    try:
        for index, row in enumerate(rows, 1):
            pmid = base.normalized_text(row.get("PMID") or "")
            if pmids and pmid not in pmids:
                continue
            doi_prefix = base.normalized_text((row.get("DOI") or "").strip().lower().split("/", 1)[0])
            if only_doi_prefixes and doi_prefix not in only_doi_prefixes:
                continue
            if skip_doi_prefixes and doi_prefix in skip_doi_prefixes:
                continue
            status = base.normalized_text(row.get("download_status") or "").lower()
            if status == "success":
                continue
            if statuses and status not in statuses:
                continue
            if limit and attempted >= limit:
                break

            output_path = base.output_path_for(csv_path, row)
            source_url = (row.get("pdf_url") or "").strip() or base.pick_source_url(row)
            if base.valid_existing_pdf(output_path, row, source_url):
                row["download_status"] = "success"
                row["download_error"] = ""
                if source_url and not (row.get("pdf_url") or "").strip():
                    row["pdf_url"] = source_url
                success += 1
                base.save_rows(csv_path, rows, fieldnames)
                log(f"[{csv_path.name} {index}/{len(rows)}] SUCCESS {output_path.name} (already exists)")
                continue

            attempted += 1
            row_deadline = time.monotonic() + LIVE_ROW_TIMEOUT_SECONDS
            row_sources = source_candidates_for_row(row)
            log(f"[{csv_path.name} {index}/{len(rows)}] TRY {pmid or row.get('Title','')[:80]} | sources={len(row_sources)}")

            row_ok = False
            row_detail = "no_live_candidate"
            for start_url in row_sources:
                if time.monotonic() >= row_deadline:
                    row_detail = f"row_timeout>{LIVE_ROW_TIMEOUT_SECONDS}s"
                    break
                try:
                    pdf_candidates, referer = discover_live_pdf_urls(start_url, row)
                except Exception as exc:
                    row_detail = base.format_exception(exc, "live_discovery", limit=220)
                    continue

                if not pdf_candidates:
                    row_detail = f"{start_url} -> no_live_pdf"
                    continue

                for pdf_candidate in pdf_candidates:
                    if time.monotonic() >= row_deadline:
                        row_detail = f"row_timeout>{LIVE_ROW_TIMEOUT_SECONDS}s"
                        break
                    ok, detail = try_http_download(
                        session,
                        row,
                        pdf_candidate,
                        output_path,
                        referer=referer or start_url,
                    )
                    if ok:
                        row["download_status"] = "success"
                        row["download_error"] = ""
                        row["pdf_url"] = detail
                        row_ok = True
                        success += 1
                        log(f"[{csv_path.name} {index}/{len(rows)}] SUCCESS {output_path.name}")
                        break
                    row_detail = f"{pdf_candidate} -> {detail}"

                if row_ok:
                    break

            if not row_ok:
                row["download_status"] = "failed"
                row["download_error"] = row_detail
                log(f"[{csv_path.name} {index}/{len(rows)}] FAILED - {row_detail}")

            base.save_rows(csv_path, rows, fieldnames)
            time.sleep(delay)
    finally:
        try:
            session.close()
        except Exception:
            pass

    return attempted, success


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(base.DOCS_DIR / "csv-LungNeopla-set-Meta.csv"),
        help="docs-csv file to process",
    )
    parser.add_argument(
        "--pmid",
        action="append",
        default=[],
        help="Only process the specified PMID. Repeat to target multiple rows.",
    )
    parser.add_argument(
        "--status",
        action="append",
        default=[],
        help="Only process rows whose download_status matches this value. Repeat to target multiple statuses.",
    )
    parser.add_argument(
        "--only-doi-prefix",
        action="append",
        default=[],
        help="Only process rows whose DOI starts with this prefix. Repeat as needed.",
    )
    parser.add_argument(
        "--skip-doi-prefix",
        action="append",
        default=[],
        help="Skip rows whose DOI starts with this prefix. Repeat as needed.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of non-success rows to attempt.")
    parser.add_argument("--delay", type=float, default=LIVE_BETWEEN_ROWS_SECONDS, help="Delay between rows in seconds.")
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    csv_path = Path(args.csv_path).resolve()
    pmids = {base.normalized_text(value) for value in args.pmid if base.normalized_text(value)}
    statuses = {base.normalized_text(value).lower() for value in args.status if base.normalized_text(value)}
    only_doi_prefixes = {
        base.normalized_text(value).lower()
        for value in args.only_doi_prefix
        if base.normalized_text(value)
    }
    skip_doi_prefixes = {
        base.normalized_text(value).lower()
        for value in args.skip_doi_prefix
        if base.normalized_text(value)
    }
    attempted, success = process_csv(
        csv_path,
        pmids=pmids,
        statuses=statuses,
        only_doi_prefixes=only_doi_prefixes,
        skip_doi_prefixes=skip_doi_prefixes,
        limit=max(args.limit, 0),
        delay=max(args.delay, 0.0),
    )
    log(f"Summary for {csv_path.name}: attempted={attempted} newly_success={success}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
