from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

# Database Path Configuration
DB_PATH = Path(__file__).parent.parent / "data" / "db.sqlite3"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(dt: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(dt)
    except Exception:
        return None


def _norm_text(x: Any) -> str:
    s = "" if x is None else str(x)
    return " ".join(s.strip().split())


def _norm_email(x: Any) -> str:
    e = _norm_text(x).lower()
    if e in ("n/a", "na", "none", "null", "-", "--"):
        return ""
    return e


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
# Case-2 bucket schema (FINAL)
# -----------------------------
BUCKETS = [
    "Executive Leadership",
    "Technology / Operations",
    "Finance / Administration",
    "Business Development / Growth",
    "Marketing / Branding",
]


def _empty_case2_management() -> Dict[str, Dict[str, str]]:
    return {b: {"name": "", "designation": ""} for b in BUCKETS}


def _is_case2_management_dict(x: Any) -> bool:
    if not isinstance(x, dict):
        return False
    return any(k in x for k in BUCKETS)


def _normalize_case2_management(raw: Any) -> Dict[str, Dict[str, str]]:
    """
    Accepts:
      - dict bucket style
      - JSON string bucket style
      - wrapper dict {"case2_management": {...}}
    Returns canonical bucket dict.
    """
    base = _empty_case2_management()

    parsed = _safe_json_load(raw)
    if parsed is not None:
        raw = parsed

    if isinstance(raw, dict) and "case2_management" in raw:
        raw = raw.get("case2_management")

    if not _is_case2_management_dict(raw):
        return base

    for b in BUCKETS:
        v = raw.get(b) if isinstance(raw, dict) else None
        if isinstance(v, dict):
            nm = _norm_text(v.get("name", ""))
            dg = _norm_text(v.get("designation", "")) or _norm_text(v.get("role", ""))
            if nm and dg:
                base[b]["name"] = nm
                base[b]["designation"] = dg

    return base


def _map_role_to_bucket(role: str) -> str:
    r = _norm_text(role).lower()
    if not r:
        return ""

    # Executive
    if any(k in r for k in [
        "founder", "co-founder", "cofounder", "ceo", "chief executive",
        "managing director", "executive director", "director",
        "chairman", "chairperson", "president", "owner", "proprietor",
        "principal", "dean", "medical director", "clinical director",
    ]):
        return "Executive Leadership"

    # Tech/Ops
    if any(k in r for k in [
        "cto", "chief technology", "cio", "chief information",
        "coo", "chief operating", "operations", "it", "technical",
        "head of operations", "plant head",
    ]):
        return "Technology / Operations"

    # Finance/Admin
    if any(k in r for k in [
        "cfo", "chief financial", "finance", "accounts", "controller",
        "treasurer", "admin", "administration", "hr", "human resources",
        "compliance",
    ]):
        return "Finance / Administration"

    # Business/Growth
    if any(k in r for k in [
        "business development", "bd", "growth", "strategy",
        "partnership", "sales", "revenue", "commercial",
        "admissions", "placement",
    ]):
        return "Business Development / Growth"

    # Marketing/Brand
    if any(k in r for k in [
        "cmo", "chief marketing", "marketing", "brand",
        "communications", "pr", "digital marketing", "outreach",
        "social media",
    ]):
        return "Marketing / Branding"

    return ""


def _norm_leaders_list(raw: Any, max_leaders: int = 5) -> List[Dict[str, str]]:
    """
    Normalize list-style leaders to: [{"name": "...", "role": "..."}]
    Accepts:
      - list[dict]
      - dict {"leaders":[...]}
      - json-string of above
      - legacy key: designation
    """
    try:
        max_i = max(1, min(int(max_leaders or 5), 10))
    except Exception:
        max_i = 5

    parsed = _safe_json_load(raw)
    if parsed is not None:
        raw = parsed

    if isinstance(raw, dict) and "leaders" in raw:
        raw = raw.get("leaders")

    if not isinstance(raw, list):
        return []

    out: List[Dict[str, str]] = []
    seen = set()
    for it in raw:
        if not isinstance(it, dict):
            continue
        nm = _norm_text(it.get("name", ""))
        rl = _norm_text(it.get("role", "")) or _norm_text(it.get("designation", ""))
        if not nm or not rl:
            continue
        key = nm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": nm, "role": rl})
        if len(out) >= max_i:
            break
    return out


def _leaders_to_case2_management(leaders: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    mgmt = _empty_case2_management()
    used = set()

    for it in leaders or []:
        nm = _norm_text(it.get("name", ""))
        rl = _norm_text(it.get("role", "")) or _norm_text(it.get("designation", ""))
        if not nm or not rl:
            continue
        b = _map_role_to_bucket(rl)
        if not b or b in used:
            continue
        mgmt[b]["name"] = nm
        mgmt[b]["designation"] = rl
        used.add(b)
        if len(used) >= 5:
            break

    return mgmt


def _has_any_management(mgmt: Dict[str, Dict[str, str]]) -> bool:
    for b in BUCKETS:
        d = mgmt.get(b) or {}
        if _norm_text(d.get("name", "")) and _norm_text(d.get("designation", "")):
            return True
    return False


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]  # type: ignore[index]
    return cols


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, col_def: str) -> None:
    cols = _table_columns(conn, table)
    if col in cols:
        return
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")


def init_db() -> None:
    """
    Initializes the database schema with Case-2 bucket support.
    Safe for existing DBs (adds missing columns).
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cached_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key TEXT NOT NULL,
            result_type TEXT NOT NULL,
            result_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    # Core business table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS business (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            industry TEXT,
            website_url TEXT UNIQUE,
            has_website INTEGER,
            google_rating REAL,
            rating_count INTEGER,
            contact_phone TEXT,
            contact_email TEXT,
            case2_leaders_json TEXT,          -- legacy list
            case2_management_json TEXT,       -- ✅ bucket dict
            query_tag TEXT,
            timestamp TEXT,
            place_id TEXT,
            address TEXT,
            source_url TEXT
        )
        """
    )

    # Safe migrations (older DBs)
    try:
        _ensure_column(conn, "business", "contact_email", "TEXT")
        _ensure_column(conn, "business", "case2_leaders_json", "TEXT")
        _ensure_column(conn, "business", "case2_management_json", "TEXT")
        _ensure_column(conn, "business", "query_tag", "TEXT")
        _ensure_column(conn, "business", "timestamp", "TEXT")
        _ensure_column(conn, "business", "place_id", "TEXT")
        _ensure_column(conn, "business", "address", "TEXT")
        _ensure_column(conn, "business", "source_url", "TEXT")
        _ensure_column(conn, "business", "rating_count", "INTEGER")
        _ensure_column(conn, "business", "google_rating", "REAL")
    except Exception:
        pass

    cur.execute("CREATE INDEX IF NOT EXISTS idx_cached_cachekey ON cached_results(cache_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_business_querytag ON business(query_tag)")

    conn.commit()
    conn.close()


# -----------------------------
# Generic Caching & History
# -----------------------------
def make_cache_key(query: str, location: str = "", place: str = "", top_n: int = 0, case2_enabled: bool = False) -> str:
    payload = {
        "query": _norm_text(query).lower(),
        "location": _norm_text(location).lower(),
        "place": _norm_text(place).lower(),
        "top_n": int(top_n or 0),
        "case2_enabled": bool(case2_enabled),
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def cache_results(cache_key: str, result_type: str, results: Any) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cached_results (cache_key, result_type, result_json, created_at) VALUES (?, ?, ?, ?)",
        (cache_key, result_type, json.dumps(results, ensure_ascii=False), _now_iso()),
    )
    conn.commit()
    conn.close()


def get_cached_results(cache_key: str, result_type: str) -> Optional[Any]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT result_json FROM cached_results WHERE cache_key = ? AND result_type = ? ORDER BY id DESC LIMIT 1",
        (cache_key, result_type),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return _safe_json_load(row["result_json"])


# -----------------------------
# Case-2 Dedicated Cache (72h TTL)
# -----------------------------
CASE2_CACHE_TYPE = "case2_enrichment"


def save_case2_cache(cache_key: str, payload: Dict[str, Any]) -> None:
    if not cache_key:
        return
    cache_results(cache_key=cache_key, result_type=CASE2_CACHE_TYPE, results=payload)


def get_case2_cache(cache_key: str, ttl_hours: int = 72) -> Optional[Dict[str, Any]]:
    if not cache_key:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT result_json, created_at
        FROM cached_results
        WHERE cache_key = ? AND result_type = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (cache_key, CASE2_CACHE_TYPE),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    created_at = _parse_iso(row["created_at"] or "")
    if not created_at:
        return None

    ttl = timedelta(hours=max(1, int(ttl_hours or 72)))
    if datetime.now(timezone.utc) - created_at > ttl:
        return None

    payload = _safe_json_load(row["result_json"])
    return payload if isinstance(payload, dict) else None


def make_case2_cache_key(place_id: str = "", website_url: str = "") -> str:
    pid = _norm_text(place_id)
    web = _norm_text(website_url).lower()
    if pid:
        return f"case2:place_id:{pid}"
    if web:
        return f"case2:website:{web}"
    return ""


# -----------------------------
# Data Insertion Logic
# -----------------------------
def insert_business_list(business_list: List[Dict[str, Any]], query_tag: str) -> None:
    """
    Stores Case-2 in bucket format (case2_management_json).
    Keeps legacy case2_leaders_json too (optional).
    """
    if not business_list:
        return

    conn = get_conn()
    cur = conn.cursor()
    ts = _now_iso()

    for b in business_list:
        name = _norm_text(b.get("Company Name") or b.get("company_name") or b.get("name") or "Unknown")

        web_raw = b.get("Website URL") or b.get("website_url") or b.get("website") or ""
        web = _norm_text(web_raw)
        website_url = web if web else None  # NULL if missing

        industry = _norm_text(b.get("Industry") or b.get("industry") or "Business")
        rating = b.get("Google Rating") or b.get("google_rating") or b.get("rating")

        # ✅ FINAL KEY: Rating Count (but accept old keys too)
        rating_count = (
            b.get("Rating Count")
            or b.get("rating_count")
            or b.get("Reviews")
            or b.get("userRatingCount")
            or b.get("google_rating_count")
        )

        phone = _norm_text(b.get("Contact Phone") or b.get("phone") or b.get("Phone") or "")
        email = _norm_email(b.get("Contact Email") or b.get("email") or "")

        place_id = _norm_text(b.get("Place ID") or b.get("google_place_id") or b.get("place_id") or b.get("id") or "")
        address = _norm_text(b.get("Address") or b.get("formattedAddress") or b.get("address") or "")
        source_url = _norm_text(b.get("Source URL") or b.get("googleMapsUri") or b.get("url") or "")

        # normalize rating_count
        try:
            if rating_count in (None, "", "null", "None"):
                rating_count = None
            else:
                rating_count = int(float(str(rating_count).replace(",", "").strip()))
        except Exception:
            rating_count = None

        # normalize rating
        try:
            if rating in (None, "", "null", "None"):
                rating = None
            else:
                rating = float(str(rating).replace(",", "").strip())
        except Exception:
            rating = None

        # ---- Case-2: bucket-first
        raw_mgmt = b.get("case2_management")
        mgmt = _normalize_case2_management(raw_mgmt)

        # fallback: leaders list -> buckets
        if not _has_any_management(mgmt):
            leaders_raw = (
                b.get("case2_leaders")
                or b.get("leaders")
                or b.get("case2_leaders_json")
                or []
            )
            leaders = _norm_leaders_list(leaders_raw, max_leaders=5)
            if leaders:
                mgmt = _leaders_to_case2_management(leaders)

        case2_management_json = json.dumps(mgmt, ensure_ascii=False)

        # legacy list derived from mgmt
        legacy_list: List[Dict[str, str]] = []
        for buck in BUCKETS:
            d = mgmt.get(buck) or {}
            nm = _norm_text(d.get("name", ""))
            dg = _norm_text(d.get("designation", ""))
            if nm and dg:
                legacy_list.append({"name": nm, "role": dg})
        case2_leaders_json = json.dumps(legacy_list, ensure_ascii=False)

        try:
            if website_url is not None:
                cur.execute(
                    """
                    INSERT INTO business (
                        company_name, industry, website_url, has_website,
                        google_rating, rating_count, contact_phone, contact_email,
                        case2_leaders_json, case2_management_json,
                        query_tag, timestamp,
                        place_id, address, source_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(website_url) DO UPDATE SET
                        company_name=excluded.company_name,
                        industry=excluded.industry,
                        has_website=excluded.has_website,
                        google_rating=excluded.google_rating,
                        rating_count=excluded.rating_count,
                        contact_phone=excluded.contact_phone,
                        contact_email=excluded.contact_email,
                        case2_leaders_json=excluded.case2_leaders_json,
                        case2_management_json=excluded.case2_management_json,
                        query_tag=excluded.query_tag,
                        timestamp=excluded.timestamp,
                        place_id=excluded.place_id,
                        address=excluded.address,
                        source_url=excluded.source_url
                    """,
                    (
                        name,
                        industry,
                        website_url,
                        1,
                        rating,
                        rating_count,
                        phone,
                        email,
                        case2_leaders_json,
                        case2_management_json,
                        query_tag,
                        ts,
                        place_id,
                        address,
                        source_url,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO business (
                        company_name, industry, website_url, has_website,
                        google_rating, rating_count, contact_phone, contact_email,
                        case2_leaders_json, case2_management_json,
                        query_tag, timestamp,
                        place_id, address, source_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        industry,
                        None,
                        0,
                        rating,
                        rating_count,
                        phone,
                        email,
                        case2_leaders_json,
                        case2_management_json,
                        query_tag,
                        ts,
                        place_id,
                        address,
                        source_url,
                    ),
                )
        except Exception as e:
            print(f"[DB_ERR] Failed to insert/update {name}: {e}")

    conn.commit()
    conn.close()


def fetch_businesses_by_query(query_tag: str) -> List[Dict[str, Any]]:
    """
    Returns rows normalized for miner/excel:
      - includes case2_management (bucket dict)
      - exposes FINAL keys: Rating Count (not Reviews)
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM business WHERE query_tag = ? ORDER BY id DESC", (query_tag,))
    rows = cur.fetchall()
    conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)

        d["Company Name"] = _norm_text(d.get("company_name") or "Unknown")
        d["Industry"] = _norm_text(d.get("industry") or "Business")
        d["Website URL"] = _norm_text(d.get("website_url") or "")
        d["Has Website"] = "Yes" if d.get("has_website") else "No"
        d["Google Rating"] = d.get("google_rating")

        # ✅ FINAL KEY
        d["Rating Count"] = d.get("rating_count")

        d["Contact Phone"] = _norm_text(d.get("contact_phone") or "")
        d["Contact Email"] = _norm_email(d.get("contact_email") or "")

        d["Place ID"] = _norm_text(d.get("place_id") or "")
        d["Address"] = _norm_text(d.get("address") or "")
        d["Source Name"] = "Google Places"
        d["Source URL"] = _norm_text(d.get("source_url") or "")

        # Case-2 bucket first
        mgmt = _normalize_case2_management(d.get("case2_management_json"))
        if not _has_any_management(mgmt):
            legacy_leaders = _norm_leaders_list(d.get("case2_leaders_json") or [], max_leaders=5)
            if legacy_leaders:
                mgmt = _leaders_to_case2_management(legacy_leaders)

        d["case2_management"] = mgmt

        # optional list view too (for any old code)
        leaders_list: List[Dict[str, str]] = []
        for buck in BUCKETS:
            x = mgmt.get(buck) or {}
            nm = _norm_text(x.get("name", ""))
            dg = _norm_text(x.get("designation", ""))
            if nm and dg:
                leaders_list.append({"name": nm, "role": dg})
        d["case2_leaders"] = leaders_list

        out.append(d)

    return out


def add_search_history(query: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO search_history (query, created_at) VALUES (?, ?)",
        (_norm_text(query), _now_iso()),
    )
    conn.commit()
    conn.close()
