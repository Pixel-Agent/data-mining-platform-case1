# backend/scraper.py
# Case 1 ONLY: Manufacturing industries near me
# MODE: IndiaMART directory scraping (D1)

from __future__ import annotations

import os
import re
import json
import time
import random
from typing import Dict, List, Tuple
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
}

RAW_DIR = os.path.join("data", "raw")

DEFAULT_MAX_RESULTS = 40
DEFAULT_PAGES = 2


def _sleep_polite(a: float = 0.8, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))


def _ensure_raw_dir() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        x = (x or "").strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def fetch_html(url: str, debug: bool = False) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r.text
    except Exception as e:
        if debug:
            print("[FETCH] failed:", url, "err:", e)
        return ""


def _extract_phones(text: str) -> List[str]:
    candidates = re.findall(r"(?:\+?\d[\d\s\-().]{7,}\d)", text or "")
    cleaned: List[str] = []
    for c in candidates:
        p = re.sub(r"[^\d+]", "", c)
        digits = re.sub(r"\D", "", p)
        if 8 <= len(digits) <= 15:
            cleaned.append(p)
    return _dedupe_keep_order(cleaned)[:3]


def _extract_emails(text: str) -> List[str]:
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    emails = [e.lower() for e in emails]
    return _dedupe_keep_order(emails)[:3]


def _guess_raw_category(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["manufacturer", "manufacturing", "factory", "plant"]):
        return "Manufacturing"
    if any(k in t for k in ["supplier", "trader", "wholesaler", "distributor"]):
        return "Industrial Supplier"
    if any(k in t for k in ["packaging", "corrugated", "carton"]):
        return "Packaging"
    if any(k in t for k in ["engineering", "fabrication", "machinery"]):
        return "Engineering"
    if any(k in t for k in ["chemical", "chemicals"]):
        return "Chemical"
    if any(k in t for k in ["textile", "garment", "fabric"]):
        return "Textile"
    if any(k in t for k in ["electrical", "electronics"]):
        return "Electrical"
    return ""


def _normalize_indiamart_href(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""

    # Remove tracking garbage after first '|'
    if "|" in href:
        href = href.split("|", 1)[0].strip()

    # Normalize scheme/relative
    if href.startswith("//"):
        href = "https:" + href
    elif href.startswith("/"):
        href = "https://www.indiamart.com" + href

    return href.strip()


def indiamart_search_urls(
    query: str,
    location: str,
    pages: int = DEFAULT_PAGES,
    debug: bool = False,
) -> List[str]:
    """
    Collect candidate company/product/listing URLs from IndiaMART directory search pages.
    Uses only dir.indiamart.com/search.mp (working endpoint).
    """
    q = (query or "").strip()
    loc = (location or "").strip()
    if not q:
        return []

    # Use only the working pattern (avoid www.indiamart.com/search.mp -> 404)
    base = "https://dir.indiamart.com/search.mp?ss={q}&prdsrc=1&page={page}"

    search_term = f"{q} {loc}".strip()
    qenc = quote_plus(search_term)

    urls: List[str] = []

    for page in range(1, pages + 1):
        search_url = base.format(q=qenc, page=page)
        html = fetch_html(search_url, debug=debug)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.find_all("a", href=True)

        if debug:
            print("[IM] Search page:", search_url, "anchors:", len(anchors))

        for a in anchors:
            href = _normalize_indiamart_href(a.get("href") or "")
            if not href:
                continue

            if "indiamart.com" not in href:
                continue

            # Skip useless pages
            if href.rstrip("/") in [
                "https://www.indiamart.com",
                "https://www.indiamart.com/",
                "https://www.indiamart.com/search.html",
            ]:
                continue

            # Keep only meaningful pages
            if not any(k in href for k in ["/proddetail/", ".html", "/company/"]):
                continue

            urls.append(href)

        _sleep_polite(0.6, 1.2)

    urls = _dedupe_keep_order(urls)

    if debug:
        print("[IM] Candidate URLs:", len(urls))
        print("[IM] Sample:", urls[:5])

    return urls


def parse_indiamart_page(url: str, html: str) -> Dict[str, str]:
    """
    Extract Name/Phone/Email/Address/Category heuristically from an IndiaMART page.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    title = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"\s+\|\s+.*$", "", title).strip()
    title = re.sub(r"\s+-\s+.*$", "", title).strip()

    if not title:
        title = urlparse(url).netloc.replace("www.", "")

    phones = _extract_phones(text)
    emails = _extract_emails(text)
    raw_cat = _guess_raw_category(text)

    addr = ""
    m = re.search(r"(Address|Office|Location)\s*[:\-]\s*(.{10,200})", text, re.IGNORECASE)
    if m:
        addr = m.group(2).strip()
    addr = re.sub(r"\s+", " ", addr).strip()
    if len(addr) > 220:
        addr = addr[:220].rsplit(" ", 1)[0] + "…"

    return {
        "name": title,
        "raw_category": raw_cat,
        "address": addr,
        "phone": phones[0] if phones else "",
        "email": emails[0] if emails else "",
        "website": url,  # IM page itself
        "source": url,
    }


def scrape_case1_to_raw(
    query: str,
    location: str,
    run_id: str,
    max_results: int = DEFAULT_MAX_RESULTS,
    pages: int = DEFAULT_PAGES,
    debug: bool = True,
) -> Tuple[List[Dict], str]:
    """
    Case 1 ONLY. IndiaMART-based scraping.
    Returns (raw_records, saved_json_path).
    """
    _ensure_raw_dir()

    search_q = f"{query} manufacturer supplier".strip()

    cand_urls = indiamart_search_urls(
        query=search_q,
        location=location,
        pages=pages,
        debug=debug,
    )

    # fetch a bit extra, but not too many
    cand_urls = cand_urls[: max_results * 2]

    records: List[Dict] = []

    if debug:
        print("[SCRAPER] Fetch candidates:", len(cand_urls))

    for u in cand_urls:
        _sleep_polite()
        html = fetch_html(u, debug=debug)
        if not html:
            continue

        rec = parse_indiamart_page(u, html)

        # relaxed gate: at least name
        if rec.get("name"):
            records.append(rec)

        if len(records) >= max_results:
            break

    out_path = os.path.join(RAW_DIR, f"case1_raw_{run_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    if debug:
        print("[SCRAPER] DONE records:", len(records))
        print("[SCRAPER] Saved raw:", out_path)

    return records, out_path
