# backend/scraper.py
# Case 1 ONLY: Manufacturing industries near me
# Real scraping strategy:
# 1) DuckDuckGo HTML search -> candidate URLs
# 2) Fetch each URL -> extract contact info with heuristics
# 3) Save raw JSON to data/raw/

from __future__ import annotations

import os
import re
import json
import time
import random
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
RAW_DIR = os.path.join("data", "raw")


def _sleep_polite(a: float = 0.8, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))


def _ensure_raw_dir() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)


def _normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if not u.startswith("http"):
        u = "https://" + u
    return u


def ddg_search(query: str, max_results: int = 20) -> List[str]:
    """
    DuckDuckGo HTML endpoint (lightweight).
    Returns list of result URLs (deduped).
    """
    q = (query or "").strip()
    if not q:
        return []

    url = "https://duckduckgo.com/html/"
    params = {"q": q}

    r = requests.get(url, headers=HEADERS, params=params, timeout=25)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links: List[str] = []

    # DDG HTML results often have a.result__a
    for a in soup.select("a.result__a"):
        href = a.get("href") or ""
        href = href.strip()
        if href.startswith("http"):
            links.append(href)

    # fallback: any anchor with http
    if not links:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("http"):
                links.append(href)

    # Deduplicate (keep order)
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= max_results:
            break
    return out


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text


def _extract_title(soup: BeautifulSoup) -> str:
    t = soup.title.get_text(strip=True) if soup.title else ""
    # cleanup
    t = re.sub(r"\s+\|\s+.*$", "", t)  # remove "| Something"
    t = re.sub(r"\s+-\s+.*$", "", t)   # remove "- Something"
    return t.strip()


def _extract_phones(text: str) -> List[str]:
    # India-ish + global-ish phone heuristics
    candidates = re.findall(r"(?:\+?\d[\d\s\-().]{7,}\d)", text or "")
    cleaned = []
    for c in candidates:
        p = re.sub(r"[^\d+]", "", c)
        # basic length check
        digits = re.sub(r"\D", "", p)
        if 8 <= len(digits) <= 15:
            cleaned.append(p)
    # dedupe
    out = []
    seen = set()
    for p in cleaned:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out[:3]


def _extract_emails(text: str) -> List[str]:
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    out, seen = [], set()
    for e in emails:
        e = e.lower()
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out[:3]


def _extract_address(soup: BeautifulSoup) -> str:
    # Try common address hints (very heuristic)
    # 1) elements with itemprop/address
    addr = ""
    addr_el = soup.select_one("[itemprop='address']")
    if addr_el:
        addr = addr_el.get_text(" ", strip=True)

    # 2) look for labels like "Address" nearby
    if not addr:
        text = soup.get_text("\n", strip=True)
        m = re.search(r"(Address|Registered Office|Office)\s*[:\-]\s*(.{20,200})", text, re.IGNORECASE)
        if m:
            addr = m.group(2).strip()

    # keep it not too long
    addr = re.sub(r"\s+", " ", addr).strip()
    if len(addr) > 220:
        addr = addr[:220].rsplit(" ", 1)[0] + "…"
    return addr


def _guess_raw_category(text: str) -> str:
    t = (text or "").lower()
    # simple keyword mapping (Case 1 only)
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


def parse_listing_page(url: str, html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_title(soup)

    # try canonical
    canonical = soup.select_one("link[rel='canonical']")
    website = canonical.get("href").strip() if canonical and canonical.get("href") else url

    # text blob for phone/email/category detection
    text = soup.get_text("\n", strip=True)
    phones = _extract_phones(text)
    emails = _extract_emails(text)
    address = _extract_address(soup)
    raw_cat = _guess_raw_category(text)

    # fallback name: domain if title empty
    if not title:
        d = urlparse(url).netloc.replace("www.", "")
        title = d

    return {
        "name": title,
        "raw_category": raw_cat,
        "address": address,
        "phone": phones[0] if phones else "",
        "email": emails[0] if emails else "",
        "website": website,
        "source": url,
    }


def scrape_case1_to_raw(query: str, location: str, run_id: str, max_results: int = 20) -> Tuple[List[Dict], str]:
    """
    Case 1 ONLY. Returns (raw_records, saved_json_path).
    """
    _ensure_raw_dir()

    # Build search query (Case1)
    search_q = f"{query} {location} manufacturer contact"
    urls = ddg_search(search_q, max_results=max_results)

    records: List[Dict] = []
    for u in urls:
        try:
            _sleep_polite()
            html = fetch_html(u)
            rec = parse_listing_page(u, html)

            # minimal quality gate: name must exist; at least one of phone/email/address
            if rec.get("name") and (rec.get("phone") or rec.get("email") or rec.get("address")):
                records.append(rec)
        except Exception:
            continue

    out_path = os.path.join(RAW_DIR, f"case1_raw_{run_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records, out_path
