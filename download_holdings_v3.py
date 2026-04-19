#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量下载 ETF / 基金产品页中的 Holdings 文件。

本版修复：
1. 不再把 Invesco 角色选择页 / 首页这类 HTML 误保存成 .bin。
2. 动态抓取时增加对 “Export data / Export / Download / CSV” 的点击与原生下载捕获。
3. 对 Invesco 直连 holdings 接口，如果发生 HTML 跳转，会判定为失败并继续找真实下载入口。

输入文件格式（每行一条）：
    URL<空格>可选名称

建议：
1. 对 Invesco / 景顺 ETF，第二列最好直接填 ticker（如 PBP、DBA、QQQM）。
2. 如第二列是 ticker，脚本会优先尝试直连其 holdings 下载接口；失败后再回退到 Playwright。

使用方法：
    pip install requests beautifulsoup4 lxml playwright
    playwright install chromium
    python download_holdings_v3.py -i 景顺每日ETF下载地址.txt -o downloads
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DOWNLOAD_EXTS = (".pdf", ".csv", ".xlsx", ".xls")
BINARY_CT_HINTS = (
    "application/pdf",
    "text/csv",
    "application/csv",
    "spreadsheet",
    "ms-excel",
    "vnd.openxmlformats-officedocument",
    "octet-stream",
)
CSV_HEADER_HINTS = (
    "Fund Ticker,",
    "Security Identifier,",
    "Holding Ticker,",
    "Shares/Par Value,",
    "MarketValue,",
    "Weight,",
)
HOLDINGS_HINTS = (
    "holding",
    "holdings",
    "fund holdings",
    "portfolio holdings",
    "complete holdings",
    "view all holdings",
)
NEGATIVE_HINTS = (
    "fact sheet",
    "factsheet",
    "prospectus",
    "summary prospectus",
    "annual report",
    "semi-annual",
    "marketing",
    "brochure",
    "overview",
    "performance",
    "commentary",
    "quarterly commentary",
    "press release",
    "news",
    "proxy",
)
HOLDINGS_CLICK_TEXTS = [
    "Portfolio Holdings",
    "View all holdings",
    "View all Holdings",
    "View all",
    "Fund holdings",
    "Complete holdings",
    "Portfolio holdings",
    "Holdings",
]
EXPORT_CLICK_TEXTS = [
    "Export data",
    "Export",
    "CSV",
    "Download CSV",
    "Download",
]
COOKIE_CLICK_TEXTS = [
    "Accept",
    "Accept all",
    "I agree",
    "Agree",
    "Got it",
]
ROLE_CLICK_TEXTS = [
    "Individual Investor",
    "Financial Professional",
    "Institutional",
    "Confirm",
    "Continue",
    "Visit site",
]
TICKER_STOPWORDS = {
    "ETF",
    "ETP",
    "NAV",
    "CUSIP",
    "NYSE",
    "NASDAQ",
    "ARCA",
    "USD",
    "US",
    "QQQ",
}


@dataclass
class Job:
    url: str
    name: str


@dataclass
class Candidate:
    url: str
    source: str
    score: int
    text: str = ""
    content_type: str = ""


@dataclass
class DownloadResult:
    ok: bool
    page_url: str
    page_name: str
    saved_path: str = ""
    file_url: str = ""
    via: str = ""
    note: str = ""


def safe_name(text: str, max_len: int = 120) -> str:
    text = re.sub(r'[<>:"/|?*\x00-\x1f]+', '_', text)
    text = re.sub(r"\s+", " ", text).strip().rstrip(". ")
    return (text[:max_len].strip() or "holdings")


def normalize_url(url: str) -> str:
    return url.split("#", 1)[0].strip()


def is_binary_content_type(content_type: str) -> bool:
    ct = (content_type or "").lower()
    return any(hint in ct for hint in BINARY_CT_HINTS)


def looks_like_csv_text(text: str) -> bool:
    if not text:
        return False
    head = text[:4000].replace("\ufeff", "")
    if any(hint in head for hint in CSV_HEADER_HINTS):
        return True
    first_line = head.splitlines()[0] if head.splitlines() else head
    if first_line.count(",") >= 5 and ("Ticker" in first_line or "Weight" in first_line):
        return True
    return False


def looks_like_html_text(text: str) -> bool:
    if not text:
        return False
    head = text[:3000].lower()
    html_markers = (
        "<!doctype html",
        "<html",
        "<head",
        "<body",
        "select your role",
        "visit site",
        "skip to main content",
    )
    return any(marker in head for marker in html_markers)


def is_probable_ticker(text: str) -> bool:
    t = (text or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{1,6}", t)) and t not in TICKER_STOPWORDS


def extract_probable_ticker(*texts: str) -> Optional[str]:
    patterns = [
        r"\bFund ticker\s*[:：]?\s*([A-Z]{1,6})\b",
        r"\bTicker\s*[:：]?\s*([A-Z]{1,6})\b",
        r'"ticker"\s*[:=]\s*"([A-Z]{1,6})"',
        r'"symbol"\s*[:=]\s*"([A-Z]{1,6})"',
        r"\b([A-Z]{1,6})\b\s+Fund description\b",
    ]
    for raw in texts:
        text = (raw or "").strip()
        if not text:
            continue
        if is_probable_ticker(text):
            return text.upper()
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                ticker = m.group(1).upper()
                if is_probable_ticker(ticker):
                    return ticker
    return None


def is_invesco_etf_product_page(url: str) -> bool:
    p = urlparse(url)
    host = p.netloc.lower()
    path = p.path.lower()
    return (
        "invesco.com" in host
        and "/financial-products/etfs/" in path
        and path.endswith(".html")
    )


def is_invesco_holdings_download_url(url: str) -> bool:
    p = urlparse(url)
    host = p.netloc.lower()
    path = p.path.lower()
    q = parse_qs(p.query)
    if "invesco.com" not in host:
        return False
    if "/financial-products/etfs/holdings/main/holdings/" in path and q.get("action", [""])[0].lower() == "download":
        return True
    return False


def looks_like_file_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in DOWNLOAD_EXTS):
        return True
    if is_invesco_holdings_download_url(url):
        return True
    return False


def build_invesco_holdings_candidates(page_url: str, page_name: str, text_pool: str = "") -> list[Candidate]:
    if not is_invesco_etf_product_page(page_url):
        return []

    ticker = extract_probable_ticker(page_name, text_pool)
    if not ticker:
        return []

    parsed = urlparse(page_url)
    base = f"{parsed.scheme}://{parsed.netloc}/us/financial-products/etfs/holdings/main/holdings/0"
    queries = [
        {"action": "download", "audienceType": "Institutional", "ticker": ticker},
        {"action": "download", "audienceType": "Advisor", "ticker": ticker},
        {"action": "download", "audienceType": "investors", "ticker": ticker},
        {"action": "download", "audienceType": "individualInvestor", "ticker": ticker},
        {"action": "download", "ticker": ticker},
    ]

    out: list[Candidate] = []
    for idx, query in enumerate(queries, start=1):
        url = f"{base}?{urlencode(query)}"
        out.append(
            Candidate(
                url=url,
                source=f"invesco-direct-{idx}",
                score=260 - idx,
                text=f"Invesco direct holdings download for {ticker}",
                content_type="text/csv",
            )
        )
    return out


def score_candidate(url: str, text: str = "", content_type: str = "") -> int:
    hay = " ".join([url or "", text or "", content_type or ""]).lower()
    score = 0

    if is_invesco_holdings_download_url(url):
        score += 260
    if "action=download" in hay:
        score += 90
    if "/holdings/" in hay:
        score += 80
    if "fund holdings" in hay:
        score += 150
    if "view all holdings" in hay:
        score += 140
    if re.search(r"\bholdings?\b", hay):
        score += 120
    if "portfolio holdings" in hay:
        score += 80
    if "complete holdings" in hay:
        score += 70
    if looks_like_file_url(url):
        score += 45
    if "contentdetail?contentid=" in hay:
        score += 15
    if is_binary_content_type(content_type):
        score += 35
    if "portfolio" in hay:
        score += 10
    if "ticker=" in hay:
        score += 12
    if "export data" in hay:
        score += 150
    if "download csv" in hay or " csv" in hay:
        score += 60

    for bad in NEGATIVE_HINTS:
        if bad in hay:
            score -= 90

    return score


def dedupe_candidates(candidates: Iterable[Candidate]) -> list[Candidate]:
    best: dict[str, Candidate] = {}
    for cand in candidates:
        key = normalize_url(cand.url)
        if key not in best or cand.score > best[key].score:
            best[key] = cand
    return sorted(best.values(), key=lambda c: c.score, reverse=True)


def parse_jobs(input_file: Path) -> list[Job]:
    jobs: list[Job] = []
    for lineno, raw in enumerate(input_file.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(https?://\S+)(?:\s+(.+))?$", line)
        if not m:
            raise ValueError(f"第 {lineno} 行格式不正确：{raw!r}")
        url = m.group(1).strip()
        name = safe_name(m.group(2).strip()) if m.group(2) else safe_name(urlparse(url).path.rsplit("/", 1)[-1] or "holdings")
        jobs.append(Job(url=url, name=name))
    return jobs


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
        }
    )
    return s


def extract_dom_candidates(base_url: str, html_text: str) -> list[Candidate]:
    candidates: list[Candidate] = []
    soup = BeautifulSoup(html_text, "lxml")

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = normalize_url(urljoin(base_url, href))
        text = " ".join(a.stripped_strings)
        score = score_candidate(full_url, text=text)
        if score >= 60:
            candidates.append(Candidate(url=full_url, source="static-dom", score=score, text=text))

    regexes = [
        r'https?://[^"\'\s<>]+(?:\.pdf|\.csv|\.xlsx?|contentdetail\?contentId=[^"\'\s<>]+)',
        r'/[^"\'\s<>]+(?:\.pdf|\.csv|\.xlsx?|contentdetail\?contentId=[^"\'\s<>]+)',
        r'https?://[^"\'\s<>]+/financial-products/etfs/holdings/main/holdings/0\?[^"\'\s<>]+',
        r'/[^"\'\s<>]+/financial-products/etfs/holdings/main/holdings/0\?[^"\'\s<>]+',
    ]
    seen = set()
    for pattern in regexes:
        for m in re.finditer(pattern, html_text, flags=re.IGNORECASE):
            raw = m.group(0)
            full_url = normalize_url(urljoin(base_url, raw))
            if full_url in seen:
                continue
            seen.add(full_url)
            score = score_candidate(full_url)
            if score >= 60:
                candidates.append(Candidate(url=full_url, source="static-regex", score=score))

    return dedupe_candidates(candidates)


def filename_from_response(response: requests.Response, base_name: str, preview_text: str = "") -> str:
    cd = response.headers.get("content-disposition", "")
    if cd:
        m = re.search(r"filename\*=UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
        if m:
            filename = unquote(m.group(1).strip())
            ext = Path(filename).suffix or ".bin"
            return f"{safe_name(base_name)}{ext.lower()}"
        m = re.search(r'filename="?([^";]+)"?', cd, flags=re.IGNORECASE)
        if m:
            filename = m.group(1).strip()
            ext = Path(filename).suffix or ".bin"
            return f"{safe_name(base_name)}{ext.lower()}"

    path = unquote(urlparse(response.url).path)
    ext = Path(path).suffix.lower()
    ct = (response.headers.get("content-type") or "").lower()

    if ext not in DOWNLOAD_EXTS:
        if is_invesco_holdings_download_url(response.url) or looks_like_csv_text(preview_text):
            ext = ".csv"
        elif "pdf" in ct:
            ext = ".pdf"
        elif "csv" in ct:
            ext = ".csv"
        elif "spreadsheet" in ct or "excel" in ct:
            ext = ".xlsx"
        else:
            ext = ".bin"
    return f"{safe_name(base_name)}{ext}"


def classify_download_response(
    candidate_url: str,
    final_url: str,
    content_type: str,
    preview_text: str,
) -> tuple[bool, str]:
    ct = (content_type or "").lower()
    final_score = score_candidate(final_url, content_type=ct)
    html = looks_like_html_text(preview_text)
    csv_text = looks_like_csv_text(preview_text)

    if html and not csv_text:
        return False, "HTML page"

    if csv_text:
        return True, "csv-preview"

    if looks_like_file_url(final_url) or is_binary_content_type(ct):
        return True, "binary-or-file-url"

    if is_invesco_holdings_download_url(candidate_url) and is_invesco_holdings_download_url(final_url):
        return True, "invesco-direct-kept"

    if final_score >= 120 and not html:
        return True, "high-score"

    return False, "not-download"


def try_download_candidate(
    session: requests.Session,
    candidate: Candidate,
    output_dir: Path,
    base_name: str,
    overwrite: bool,
) -> Optional[DownloadResult]:
    try:
        resp = session.get(candidate.url, timeout=45, allow_redirects=True)
    except requests.RequestException as e:
        return DownloadResult(
            ok=False,
            page_url="",
            page_name=base_name,
            via=candidate.source,
            note=f"请求失败: {e}",
        )

    ct = resp.headers.get("content-type", "")
    final_url = normalize_url(resp.url)

    preview_text = ""
    try:
        if "text" in ct.lower() or is_invesco_holdings_download_url(candidate.url) or is_invesco_holdings_download_url(final_url):
            preview_text = resp.text[:4000]
    except Exception:
        preview_text = ""

    is_download, reason = classify_download_response(candidate.url, final_url, ct, preview_text)
    if not is_download:
        return None

    filename = filename_from_response(resp, base_name, preview_text=preview_text)
    target = output_dir / filename
    if target.exists() and not overwrite:
        return DownloadResult(
            ok=True,
            page_url="",
            page_name=base_name,
            saved_path=str(target),
            file_url=final_url,
            via=f"{candidate.source}-cached",
            note="文件已存在，已跳过重新下载",
        )

    target.write_bytes(resp.content)
    return DownloadResult(
        ok=True,
        page_url="",
        page_name=base_name,
        saved_path=str(target),
        file_url=final_url,
        via=candidate.source,
        note=f"{ct} | {reason}",
    )


def static_download(
    session: requests.Session,
    page_url: str,
    page_name: str,
    output_dir: Path,
    overwrite: bool,
) -> Optional[DownloadResult]:
    try:
        resp = session.get(page_url, timeout=45)
        resp.raise_for_status()
    except requests.RequestException:
        return None

    extra_candidates = build_invesco_holdings_candidates(resp.url, page_name, resp.text)
    candidates = dedupe_candidates([*extra_candidates, *extract_dom_candidates(resp.url, resp.text)])
    for candidate in candidates[:20]:
        result = try_download_candidate(session, candidate, output_dir, page_name, overwrite)
        if result and result.ok:
            result.page_url = page_url
            return result
    return None


async def _best_locator(page, label: str):
    import re as _re

    pattern = _re.compile(label, _re.I)
    locators = [
        page.get_by_role("button", name=pattern),
        page.get_by_role("link", name=pattern),
        page.get_by_text(pattern),
    ]
    for locator in locators:
        try:
            if await locator.first.count() > 0:
                return locator.first
        except Exception:
            pass
    return None


async def _safe_click(locator) -> bool:
    if locator is None:
        return False
    try:
        await locator.scroll_into_view_if_needed(timeout=2000)
    except Exception:
        pass
    try:
        await locator.click(timeout=3000)
        return True
    except Exception:
        try:
            await locator.click(timeout=3000, force=True)
            return True
        except Exception:
            return False


async def _save_playwright_download(download, output_dir: Path, page_name: str, overwrite: bool) -> Optional[str]:
    suggested = (download.suggested_filename or "").strip()
    ext = Path(suggested).suffix.lower()
    if ext not in DOWNLOAD_EXTS:
        if suggested.lower().endswith(".csv"):
            ext = ".csv"
        elif suggested.lower().endswith(".pdf"):
            ext = ".pdf"
        elif suggested.lower().endswith(".xlsx"):
            ext = ".xlsx"
        else:
            ext = ".bin"
    target = output_dir / f"{safe_name(page_name)}{ext}"
    if target.exists() and not overwrite:
        return str(target)
    await download.save_as(str(target))
    return str(target)


async def _try_click_and_capture_download(page, label: str, output_dir: Path, page_name: str, overwrite: bool):
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    except Exception:
        return None

    locator = await _best_locator(page, label)
    if locator is None:
        return None

    try:
        async with page.expect_download(timeout=5000) as dl_info:
            clicked = await _safe_click(locator)
            if not clicked:
                return None
        download = await dl_info.value
        saved_path = await _save_playwright_download(download, output_dir, page_name, overwrite)
        return DownloadResult(
            ok=True,
            page_url=page.url,
            page_name=page_name,
            saved_path=saved_path or "",
            file_url=download.url,
            via=f"playwright-download:{label}",
            note=download.suggested_filename or "",
        )
    except PlaywrightTimeoutError:
        return None
    except Exception:
        return None


async def dynamic_download(
    page_url: str,
    page_name: str,
    output_dir: Path,
    overwrite: bool,
    timeout_ms: int = 60000,
) -> Optional[DownloadResult]:
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except Exception:
        return None

    seen_responses: list[Candidate] = []
    original_path = urlparse(page_url).path.lower()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            accept_downloads=True,
        )
        page = await context.new_page()

        async def record_response(response):
            try:
                headers = await response.all_headers()
            except Exception:
                headers = {}
            url = normalize_url(response.url)
            ct = headers.get("content-type", "")
            score = score_candidate(url, content_type=ct)
            if score >= 45:
                seen_responses.append(
                    Candidate(url=url, source="dynamic-network", score=score, content_type=ct)
                )

        page.on("response", lambda r: asyncio.create_task(record_response(r)))

        async def goto_original():
            try:
                await page.goto(page_url, wait_until="domcontentloaded", timeout=timeout_ms)
            except PlaywrightTimeoutError:
                pass
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

        await goto_original()

        for label in COOKIE_CLICK_TEXTS:
            locator = await _best_locator(page, label)
            if await _safe_click(locator):
                try:
                    await page.wait_for_timeout(800)
                except Exception:
                    pass

        for label in ROLE_CLICK_TEXTS:
            locator = await _best_locator(page, label)
            clicked = await _safe_click(locator)
            if not clicked:
                continue
            try:
                await page.wait_for_timeout(1200)
            except Exception:
                pass
            curr_path = urlparse(page.url).path.lower()
            if curr_path != original_path:
                await goto_original()

        try:
            html_text = await page.content()
        except Exception:
            html_text = ""
        try:
            body_text = await page.text_content("body") or ""
        except Exception:
            body_text = ""

        dom_links = await page.eval_on_selector_all(
            "a[href]",
            """
            els => els.map(a => ({
                href: a.href || a.getAttribute('href') || '',
                text: (a.innerText || a.textContent || '').trim()
            }))
            """,
        )
        dom_candidates = [
            Candidate(
                url=normalize_url(item["href"]),
                source="dynamic-dom",
                score=score_candidate(item["href"], text=item.get("text", "")),
                text=item.get("text", ""),
            )
            for item in dom_links
            if item.get("href") and score_candidate(item["href"], text=item.get("text", "")) >= 60
        ]

        extra_candidates = build_invesco_holdings_candidates(page.url, page_name, html_text + "\n" + body_text)
        dom_candidates = [*extra_candidates, *dom_candidates]

        for label in HOLDINGS_CLICK_TEXTS:
            result = await _try_click_and_capture_download(page, label, output_dir, page_name, overwrite)
            if result:
                await browser.close()
                return result

            locator = await _best_locator(page, label)
            if locator is None:
                continue
            before = len(seen_responses)
            clicked = await _safe_click(locator)
            if not clicked:
                continue

            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            await page.wait_for_timeout(1500)

            for export_label in EXPORT_CLICK_TEXTS:
                result = await _try_click_and_capture_download(page, export_label, output_dir, page_name, overwrite)
                if result:
                    await browser.close()
                    return result

            try:
                dom_links_after = await page.eval_on_selector_all(
                    "a[href]",
                    """
                    els => els.map(a => ({
                        href: a.href || a.getAttribute('href') || '',
                        text: (a.innerText || a.textContent || '').trim()
                    }))
                    """,
                )
                for item in dom_links_after:
                    href = item.get("href") or ""
                    text = item.get("text") or ""
                    score = score_candidate(href, text=text)
                    if score >= 60:
                        dom_candidates.append(
                            Candidate(
                                url=normalize_url(href),
                                source=f"dynamic-dom-after-click:{label}",
                                score=score + 40,
                                text=text,
                            )
                        )
            except Exception:
                pass

            for cand in seen_responses[before:]:
                cand.score += 80
                cand.source = f"{cand.source}-after-click:{label}"

        for export_label in EXPORT_CLICK_TEXTS:
            result = await _try_click_and_capture_download(page, export_label, output_dir, page_name, overwrite)
            if result:
                await browser.close()
                return result

        all_candidates = dedupe_candidates([*dom_candidates, *seen_responses])

        for candidate in all_candidates[:24]:
            try:
                resp = await context.request.get(candidate.url, timeout=45000)
            except Exception:
                continue

            try:
                headers = dict(resp.headers)
            except Exception:
                headers = {}
            ct = headers.get("content-type", "")
            final_url = normalize_url(resp.url)
            preview_text = ""
            if (
                "text" in ct.lower()
                or is_invesco_holdings_download_url(final_url)
                or is_invesco_holdings_download_url(candidate.url)
            ):
                try:
                    preview_text = (await resp.text())[:4000]
                except Exception:
                    preview_text = ""

            is_download, reason = classify_download_response(candidate.url, final_url, ct, preview_text)
            if not is_download:
                continue

            cd = headers.get("content-disposition", "")
            filename = None
            if cd:
                m = re.search(r"filename\*=UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
                if m:
                    ext = Path(unquote(m.group(1))).suffix or ".bin"
                    filename = f"{safe_name(page_name)}{ext.lower()}"
                else:
                    m = re.search(r'filename="?([^";]+)"?', cd, flags=re.IGNORECASE)
                    if m:
                        ext = Path(m.group(1)).suffix or ".bin"
                        filename = f"{safe_name(page_name)}{ext.lower()}"
            if not filename:
                path = unquote(urlparse(final_url).path)
                ext = Path(path).suffix.lower()
                if ext not in DOWNLOAD_EXTS:
                    if is_invesco_holdings_download_url(final_url) or looks_like_csv_text(preview_text):
                        ext = ".csv"
                    elif "pdf" in ct.lower():
                        ext = ".pdf"
                    elif "csv" in ct.lower():
                        ext = ".csv"
                    elif "spreadsheet" in ct.lower() or "excel" in ct.lower():
                        ext = ".xlsx"
                    else:
                        ext = ".bin"
                filename = f"{safe_name(page_name)}{ext}"

            target = output_dir / filename
            if target.exists() and not overwrite:
                await browser.close()
                return DownloadResult(
                    ok=True,
                    page_url=page_url,
                    page_name=page_name,
                    saved_path=str(target),
                    file_url=final_url,
                    via=f"{candidate.source}-cached",
                    note="文件已存在，已跳过重新下载",
                )

            body = await resp.body()
            target.write_bytes(body)
            await browser.close()
            return DownloadResult(
                ok=True,
                page_url=page_url,
                page_name=page_name,
                saved_path=str(target),
                file_url=final_url,
                via=candidate.source,
                note=f"{ct} | {reason}",
            )

        await browser.close()
        return None


def write_log(log_path: Path, rows: list[DownloadResult]) -> None:
    with log_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["page_name", "page_url", "ok", "saved_path", "file_url", "via", "note"])
        for row in rows:
            writer.writerow([
                row.page_name,
                row.page_url,
                "Y" if row.ok else "N",
                row.saved_path,
                row.file_url,
                row.via,
                row.note,
            ])


async def main_async(args: argparse.Namespace) -> int:
    input_file = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    jobs = parse_jobs(input_file)
    if not jobs:
        print("输入文件为空，没有可处理的 URL。", file=sys.stderr)
        return 1

    session = build_session()
    results: list[DownloadResult] = []

    for idx, job in enumerate(jobs, start=1):
        print(f"[{idx}/{len(jobs)}] 处理: {job.name} -> {job.url}")
        result = static_download(session, job.url, job.name, output_dir, args.overwrite)
        if result is None and not args.no_dynamic:
            result = await dynamic_download(job.url, job.name, output_dir, args.overwrite)

        if result is None:
            result = DownloadResult(
                ok=False,
                page_url=job.url,
                page_name=job.name,
                via="none",
                note="未找到可下载的 Holdings 文件",
            )
            print("    未找到可下载的 Holdings 文件")
        else:
            if not result.page_url:
                result.page_url = job.url
            status = "已下载" if result.ok else "失败"
            print(f"    {status}: {result.saved_path or result.note}")
        results.append(result)

    log_path = output_dir / "download_log.csv"
    write_log(log_path, results)
    ok_count = sum(1 for r in results if r.ok)
    print(f"\n完成：{ok_count}/{len(results)} 成功。日志已写入: {log_path}")
    return 0 if ok_count == len(results) else 2


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="批量下载基金/ETF 页面中的 Holdings 文件")
    parser.add_argument("-i", "--input", required=True, help="输入 txt 文件路径，每行格式：URL [可选名称]")
    parser.add_argument("-o", "--output", default="downloads", help="下载目录，默认 downloads")
    parser.add_argument("--overwrite", action="store_true", help="如果文件已存在则覆盖")
    parser.add_argument("--no-dynamic", action="store_true", help="禁用 Playwright 动态回退，仅做静态解析")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("用户中断。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
