from __future__ import annotations

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import os
import time
import sys
import json
import re
import requests

# -----------------------------
# Path safety
# -----------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# -----------------------------
# Imports
# -----------------------------
from backend.config import (
    DEFAULT_LOCATION,
    DEFAULT_TOP_N,
    TOP_N_CAP,  # should be 300 now
    CASE2_ENABLED as CASE2_ENABLED_DEFAULT,
    CASE2_MAX_LEADERS as CASE2_MAX_LEADERS_DEFAULT,
    CASE2_TIMEOUT_SECS,
)

import backend.scraper as scraper
import backend.miner as miner
import backend.excel_utils as excel_utils

# Case-2 modules
try:
    import backend.scraper_case2 as scraper_case2
except Exception:
    scraper_case2 = None  # type: ignore


# -----------------------------
# Helpers
# -----------------------------
def _safe_top_n(top_n: Any, default: int, cap: int) -> int:
    try:
        n = int(top_n)
    except Exception:
        n = default
    if n <= 0:
        n = default
    return max(1, min(n, cap))


def _read_bytes(path: str) -> bytes | None:
    try:
        if path and os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
    except Exception:
        return None
    return None


def _clean_url(u: str) -> str:
    u = (u or "").strip()
    lu = u.lower()
    if not u or "googleusercontent.com" in lu or "google.com/url" in lu:
        return ""
    if u.startswith(("mailto:", "tel:", "javascript:")):
        return ""
    if u.startswith("www."):
        u = "https://" + u
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    return u


# -----------------------------
# Email helpers
# -----------------------------
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "live.com",
    "icloud.com", "aol.com", "proton.me", "protonmail.com", "zoho.com",
}
_PREFERRED_PREFIX = (
    "info@", "contact@", "admin@", "office@", "support@", "help@",
    "admissions@", "enquiry@", "inquiry@",
)


def _pick_best_email_from_html(html: str) -> str:
    if not html:
        return ""
    emails = [e.lower() for e in _EMAIL_RE.findall(html)]
    if not emails:
        return ""

    filtered: List[str] = []
    for e in emails:
        try:
            dom = e.split("@", 1)[1].strip().lower()
        except Exception:
            continue
        if dom in _FREE_EMAIL_DOMAINS:
            continue
        filtered.append(e)

    if not filtered:
        return ""

    for p in _PREFERRED_PREFIX:
        for e in filtered:
            if e.startswith(p):
                return e

    return filtered[0]


def _scrape_contact_email_light(website: str, timeout: int) -> str:
    if not website:
        return ""
    try:
        r = requests.get(
            website,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=max(6, int(timeout or 10)),
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return ""
        return _pick_best_email_from_html(r.text or "")
    except Exception:
        return ""


# -----------------------------
# Case-2 buckets (strict schema)
# -----------------------------
BUCKETS = [
    "Executive Leadership",
    "Technology / Operations",
    "Finance / Administration",
    "Business Development / Growth",
    "Marketing / Branding",
]


def _empty_case2_management() -> Dict[str, Dict[str, str]]:
    # keep schema stable for miner/excel
    return {b: {"name": "", "designation": ""} for b in BUCKETS}


def _normalize_case2_management(mgmt: Any) -> Dict[str, Dict[str, str]]:
    """
    Ensure mgmt is always in bucketed dict format:
      {bucket: {"name": "", "designation": ""}}
    Accepts dict payloads from scraper_case2 and sanitizes them.
    """
    out = _empty_case2_management()
    if not isinstance(mgmt, dict):
        return out

    for b in BUCKETS:
        v = mgmt.get(b)
        if isinstance(v, dict):
            nm = (v.get("name") or "").strip()
            dg = (v.get("designation") or v.get("role") or "").strip()
            if nm and dg:
                out[b]["name"] = nm
                out[b]["designation"] = dg
    return out


def _has_leadership_strict(mgmt: Dict[str, Dict[str, str]]) -> bool:
    d0 = mgmt.get("Executive Leadership") or {}
    return bool((d0.get("name") or "").strip() and (d0.get("designation") or "").strip())


def _apply_case2_management_to_row(
    row: Dict[str, Any],
    mgmt: Dict[str, Dict[str, str]],
    email: str = "",
) -> None:
    row["case2_management"] = mgmt
    row["Leadership Found"] = "Yes" if _has_leadership_strict(mgmt) else "No"
    if email and not (row.get("Contact Email") or "").strip():
        row["Contact Email"] = email


# -----------------------------
# SAFE Case-1 scraper wrapper
# -----------------------------
def _scrape_case1_safe(
    query: str,
    location: str,
    place: str,
    run_id: str,
    max_results: int,
) -> Tuple[List[Dict[str, Any]], str]:

    # Preferred: scraper.scrape_case1_to_raw supports (place) and expansion (<=300)
    if hasattr(scraper, "scrape_case1_to_raw"):
        try:
            return scraper.scrape_case1_to_raw(
                query=query,
                location=location,
                run_id=run_id,
                max_results=max_results,
                place=place,
            )
        except TypeError:
            merged_query = " ".join([x for x in [query, place] if x]).strip()
            return scraper.scrape_case1_to_raw(
                query=merged_query,
                location=location,
                run_id=run_id,
                max_results=max_results,
            )

    # Fallback: single context (still no duplicates guarantee only if upstream returns unique)
    merged_query = " ".join([x for x in [query, place] if x]).strip()
    results = scraper.scrape_google_places(
        query=merged_query,
        location=location,
        max_results=max_results,
    )

    raw_dir = os.path.join("data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    out_path = os.path.join(raw_dir, f"raw_{run_id}.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return results, out_path


# -----------------------------
# PIPELINE (EXPORT)
# -----------------------------
def run_case1_pipeline(
    query: str,
    location: Optional[str] = None,
    place: str = "",
    top_n: int = DEFAULT_TOP_N,
    use_gpt: bool = False,
    debug: bool = True,
    case2_enabled: Optional[bool] = None,
    case2_max_leaders: Optional[int] = None,
) -> Dict[str, Any]:

    if case2_enabled is None:
        case2_enabled = bool(CASE2_ENABLED_DEFAULT)

    if case2_max_leaders is None:
        try:
            case2_max_leaders = int(CASE2_MAX_LEADERS_DEFAULT or 5)
        except Exception:
            case2_max_leaders = 5
    case2_max_leaders = max(1, min(int(case2_max_leaders), 5))

    location = (location or DEFAULT_LOCATION).strip()
    query = (query or "").strip()
    place = (place or "").strip()

    if not query:
        raise ValueError("Query is empty.")

    # ✅ now supports up to 300 because TOP_N_CAP updated in config.py
    top_n = _safe_top_n(top_n, DEFAULT_TOP_N, TOP_N_CAP)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if debug:
        print(f"\n🚀 Query={query} | Place={place} | Location={location} | cap={top_n}")
        print(f"🧠 Case-2 enabled={case2_enabled} max_leaders={case2_max_leaders}")

    # -----------------------------
    # Case-1: Google Places
    # -----------------------------
    raw_records, raw_path = _scrape_case1_safe(
        query=query,
        location=location,
        place=place,
        run_id=ts,
        max_results=top_n,
    )

    if debug:
        print(f"📊 Raw pulled: {len(raw_records)}")

    cleaned_rows, stats = miner.mine_case1_records(raw_records=raw_records)
    cleaned_rows = (cleaned_rows or [])[:top_n]

    # Ensure baseline keys exist
    for row in cleaned_rows:
        row.setdefault("Leadership Found", "No")
        row.setdefault("case2_management", _empty_case2_management())

    # -----------------------------
    # Case-2: Leadership enrichment
    # -----------------------------
    if case2_enabled and cleaned_rows:
        start = time.time()
        global_timeout = 600  # 10 min guard

        for i, row in enumerate(cleaned_rows):
            if time.time() - start > global_timeout:
                if debug:
                    print("🛑 Global Timeout reached. Stopping Case-2 enrichment.")
                break

            website = _clean_url(row.get("Website URL") or "")
            mgmt = _empty_case2_management()
            email = ""

            if website and scraper_case2 is not None and hasattr(scraper_case2, "run_discovery_sync"):
                try:
                    payload, email2 = scraper_case2.run_discovery_sync(website, row.get("Company Name", ""))
                    # payload expected: {"case2_management": {...}}
                    mgmt = _normalize_case2_management((payload or {}).get("case2_management"))
                    email = (email2 or "").strip()
                except Exception:
                    mgmt = _empty_case2_management()
                    email = ""

            # Fallback email scrape (homepage only)
            if website and not email and not (row.get("Contact Email") or "").strip():
                email = _scrape_contact_email_light(website, int(CASE2_TIMEOUT_SECS or 10))

            _apply_case2_management_to_row(row, mgmt, email)

            if debug:
                ok = "✅" if row.get("Leadership Found") == "Yes" else "⚠️"
                print(f"{ok} [{i+1}/{len(cleaned_rows)}] {row.get('Company Name','')}")

    # -----------------------------
    # Export
    # -----------------------------
    os.makedirs("data/output", exist_ok=True)
    excel_path = os.path.join("data/output", f"case1_{ts}.xlsx")
    excel_utils.write_case1_excel(rows=cleaned_rows, out_path=excel_path)

    # ✅ Stats keys: provide BOTH names to avoid UI mismatch bugs
    with_leadership = sum(1 for r in cleaned_rows if r.get("Leadership Found") == "Yes")

    return {
        "excel_path": excel_path,
        "excel_bytes": _read_bytes(excel_path),
        "cleaned_rows": cleaned_rows,
        "stats": {
            "clean_count": len(cleaned_rows),
            "with_leadership": with_leadership,  # backend canonical
            "with_leaders": with_leadership,     # UI compatibility (your UI used this earlier)
        },
        "raw_path": raw_path,
    }
