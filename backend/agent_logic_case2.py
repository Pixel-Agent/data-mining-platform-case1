# backend/agent_logic_case2.py
# Case 2 — SCRAPING-FIRST (Top Management + Contact Email)
# -------------------------------------------------------
# Streamlit-safe | DB-safe | Excel-safe
#
# Primary path:
#   backend/scraper_case2.py  -> run_discovery_sync() -> (payload, email)
#
# Output formats:
# 1) output["case2_leaders"] = [{"name": "...", "role": "..."}]  (legacy friendly)
# 2) output["case2_management"] = {
#      "Executive Leadership": {"name","designation","email","phone","linkedin"},
#      ...
#    } (bucket dict)
#
# NOTE:
# - Excel final schema is handled by miner/excel_utils (Name 1..5 / Designation 1..5)
# - This module only enriches and returns normalized Case-2 payload.

from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
import re
import json

from backend.config import (
    CASE2_ENABLED,
    CASE2_MAX_LEADERS,
)

# SCRAPING-FIRST module (payload-first)
from backend.scraper_case2 import run_discovery_sync

# Optional DB cache (72h TTL) — safe import
try:
    from backend import db  # type: ignore
except Exception:
    db = None  # type: ignore


# -----------------------------
# Limits / helpers
# -----------------------------
def _max_leaders() -> int:
    try:
        return max(1, min(int(CASE2_MAX_LEADERS or 5), 5))
    except Exception:
        return 5


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", ("" if s is None else str(s)).strip())


def _safe_json_load(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


# -----------------------------
# Buckets (FINAL)
# -----------------------------
BUCKETS = [
    "Executive Leadership",
    "Technology / Operations",
    "Finance / Administration",
    "Business Development / Growth",
    "Marketing / Branding",
]


def _empty_management() -> Dict[str, Dict[str, str]]:
    base = {"name": "", "designation": "", "email": "", "phone": "", "linkedin": ""}
    return {b: dict(base) for b in BUCKETS}


def _leadership_found_strict(mgmt: Dict[str, Dict[str, str]]) -> bool:
    """
    STRICT rule (as per master prompt):
    Leadership Found = Yes only if Executive Leadership has BOTH:
      - name
      - designation
    """
    d0 = mgmt.get("Executive Leadership") or {}
    return bool(_norm(d0.get("name", "")) and _norm(d0.get("designation", "")))


# -----------------------------
# Role normalization -> 5 buckets (only used if payload has only leaders list)
# -----------------------------
_BUCKET_RULES: List[Tuple[str, List[str]]] = [
    (
        "Executive Leadership",
        [
            "founder", "co-founder", "cofounder", "ceo", "chief executive", "managing director",
            "md", "director", "executive director", "chairman", "chairperson", "president",
            "principal", "dean", "medical director", "clinical director", "owner", "proprietor",
        ],
    ),
    (
        "Technology / Operations",
        [
            "cto", "chief technology", "cio", "chief information", "coo", "chief operating",
            "operations", "it head", "technical", "plant head", "head of operations", "administrator",
        ],
    ),
    (
        "Finance / Administration",
        [
            "cfo", "chief financial", "finance", "accounts", "controller", "treasurer",
            "admin", "administration", "hr head", "human resources", "compliance",
        ],
    ),
    (
        "Business Development / Growth",
        [
            "business development", "bd", "growth", "strategy", "partnership", "sales head",
            "admissions", "placement", "revenue", "commercial",
        ],
    ),
    (
        "Marketing / Branding",
        [
            "cmo", "chief marketing", "marketing", "brand", "communications", "pr",
            "digital marketing", "outreach", "social media",
        ],
    ),
]


def _map_role_to_bucket(role: str) -> str:
    r = _norm(role).lower()
    if not r:
        return ""
    for bucket, keys in _BUCKET_RULES:
        for k in keys:
            if k in r:
                return bucket
    return ""  # strict: do not guess


def _clean_leaders_list(value: Any, max_leaders: int = 5) -> List[Dict[str, str]]:
    """
    Normalize leaders -> [{"name":"...","role":"..."}] strict.
    Accepts:
      - list[dict]
      - dict {"leaders":[...]} or {"leaders_raw":[...]}
      - JSON string of above
    """
    if max_leaders <= 0:
        max_leaders = 5

    parsed = _safe_json_load(value)
    if parsed is not None:
        value = parsed

    if isinstance(value, dict):
        value = value.get("leaders_raw") or value.get("leaders") or []

    if not isinstance(value, list):
        return []

    out: List[Dict[str, str]] = []
    seen = set()
    for it in value:
        if not isinstance(it, dict):
            continue
        nm = _norm(it.get("name", ""))
        rl = _norm(it.get("role", "")) or _norm(it.get("designation", ""))
        if not nm or not rl:
            continue
        key = nm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": nm, "role": rl})
        if len(out) >= max_leaders:
            break
    return out


def _leaders_to_management(leaders: List[Dict[str, str]], email: str = "") -> Dict[str, Dict[str, str]]:
    """
    Convert leaders list -> 5 bucket dict.
    Strict: only fill a bucket if mapping is confident (keyword match).
    """
    mgmt = _empty_management()
    seen_buckets = set()

    for it in leaders or []:
        name = _norm(it.get("name", ""))
        role = _norm(it.get("role", "")) or _norm(it.get("designation", ""))
        if not name or not role:
            continue

        bucket = _map_role_to_bucket(role)
        if not bucket:
            continue
        if bucket in seen_buckets:
            continue

        mgmt[bucket]["name"] = name
        mgmt[bucket]["designation"] = role

        # safe default: only attach email to Executive bucket
        if bucket == "Executive Leadership" and email:
            mgmt[bucket]["email"] = _norm(email)

        seen_buckets.add(bucket)

        if len(seen_buckets) >= 5:
            break

    return mgmt


def _normalize_management_from_payload(payload: Any, email: str = "") -> Dict[str, Dict[str, str]]:
    """
    scraper_case2 payload-first normalizer:
      payload["case2_management"] is authoritative if present.

    Expected mgmt shape:
      {bucket: {"name":"", "designation":"", ...}} (extra keys allowed)
    """
    base = _empty_management()

    if not isinstance(payload, dict):
        return base

    mgmt = payload.get("case2_management")
    if isinstance(mgmt, str):
        mgmt = _safe_json_load(mgmt)

    if isinstance(mgmt, dict):
        for b in BUCKETS:
            v = mgmt.get(b)
            if isinstance(v, dict):
                nm = _norm(v.get("name", ""))
                dg = _norm(v.get("designation", "")) or _norm(v.get("role", ""))
                if nm and dg:
                    base[b]["name"] = nm
                    base[b]["designation"] = dg

                    # pass-through optional fields if present
                    base[b]["email"] = _norm(v.get("email", "")) or base[b]["email"]
                    base[b]["phone"] = _norm(v.get("phone", "")) or base[b]["phone"]
                    base[b]["linkedin"] = _norm(v.get("linkedin", "")) or base[b]["linkedin"]

        # if still no explicit email in mgmt but email discovered, attach to Executive
        if email and not base["Executive Leadership"]["email"]:
            base["Executive Leadership"]["email"] = _norm(email)

        return base

    # fallback: leaders list in payload
    leaders = _clean_leaders_list(payload, max_leaders=_max_leaders())
    return _leaders_to_management(leaders, email=email)


# ------------------------------------------------------------
# Optional cache helpers (safe no-crash)
# ------------------------------------------------------------
def _cache_get(cache_key: str) -> Optional[Dict[str, Any]]:
    if not db:
        return None
    fn = getattr(db, "get_case2_cache", None)
    if callable(fn):
        try:
            return fn(cache_key)
        except Exception:
            return None
    return None


def _cache_set(cache_key: str, payload: Dict[str, Any]) -> None:
    if not db:
        return
    fn = getattr(db, "save_case2_cache", None)
    if callable(fn):
        try:
            fn(cache_key, payload)
        except Exception:
            pass


# ------------------------------------------------------------
# Public API (SCRAPING-FIRST)
# ------------------------------------------------------------
def run_case2_enrichment(
    company_name: str,
    website_url: str,
    cache_key: str = "",
) -> Dict[str, Any]:
    """
    Main Case-2 entry (SCRAPING-FIRST).
    Returns a dict safe for Streamlit + pipeline.

    Output keys:
      - case2_leaders: list[{"name","role"}]
      - case2_email: str
      - case2_management: 5-bucket dict (name/designation/email/phone/linkedin)
      - Leadership Found: "Yes"/"No" (STRICT Executive rule)
    """
    out: Dict[str, Any] = {
        "case2_leaders": [],
        "case2_email": "",
        "case2_management": _empty_management(),
        "Leadership Found": "No",
    }

    if not CASE2_ENABLED:
        return out

    website_url = _norm(website_url)
    company_name = _norm(company_name)

    if not website_url:
        return out

    # 1) Cache (optional)
    if cache_key:
        cached = _cache_get(cache_key)
        if isinstance(cached, dict):
            # Make sure required keys exist (avoid old cache shape breaking caller)
            cached.setdefault("case2_leaders", [])
            cached.setdefault("case2_email", "")
            cached.setdefault("case2_management", _empty_management())
            cached.setdefault("Leadership Found", "No")
            return cached

    # 2) Scrape payload + email (scraper_case2)
    payload: Dict[str, Any] = {}
    email: str = ""
    try:
        # preferred keyword signature
        payload, email = run_discovery_sync(website=website_url, company_name=company_name)
    except TypeError:
        # backward positional support
        payload, email = run_discovery_sync(website_url, company_name)

    email = _norm(email)

    # leaders list (legacy friendly)
    leaders = _clean_leaders_list(payload.get("leaders_raw") if isinstance(payload, dict) else payload, max_leaders=_max_leaders())
    out["case2_leaders"] = leaders
    out["case2_email"] = email

    # management buckets
    mgmt = _normalize_management_from_payload(payload, email=email)
    out["case2_management"] = mgmt
    out["Leadership Found"] = "Yes" if _leadership_found_strict(mgmt) else "No"

    # 3) Save cache (optional)
    if cache_key:
        _cache_set(cache_key, out)

    return out


# ------------------------------------------------------------
# Backward compatibility wrapper (legacy)
# ------------------------------------------------------------
def run_case2_top_management(company_name: str, website_url: str) -> Dict[str, Any]:
    """
    Legacy wrapper kept for older code paths.
    It returns:
      - case2_leaders (list)
      - flat "Leader i Name/Role" columns (1..5)
    """
    output: Dict[str, Any] = {}
    max_leaders = _max_leaders()

    # Always initialize legacy columns
    for i in range(1, 6):
        output[f"Leader {i} Name"] = ""
        output[f"Leader {i} Role"] = ""

    if not CASE2_ENABLED:
        output["case2_leaders"] = []
        return output

    data = run_case2_enrichment(
        company_name=company_name or "",
        website_url=website_url or "",
        cache_key="",  # legacy wrapper doesn't force caching
    )

    leaders = (data.get("case2_leaders") or [])[:max_leaders]
    output["case2_leaders"] = leaders

    for idx, leader in enumerate(leaders[:5]):
        col = idx + 1
        output[f"Leader {col} Name"] = _norm(leader.get("name", "")) or ""
        output[f"Leader {col} Role"] = _norm(leader.get("role", "")) or ""

    return output
