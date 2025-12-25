from __future__ import annotations

import os
import re
import json
import time
from typing import Dict, List, Tuple, Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Playwright SYNC (Windows safe)
try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None  # type: ignore


# =========================================================
# ======================= CASE 1 ==========================
# Google Places ONLY (New Places API - searchText)
# =========================================================

GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
RAW_DIR = os.path.join("data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

GOOGLE_PAGE_SIZE = 20

# ✅ UPDATED CAP (NEW REQUIREMENT)
MAX_CAP_RESULTS = 300

# Safety delays (avoid rate-limit)
_PAGE_TOKEN_DELAY_SECS = 2.0
_CONTEXT_DELAY_SECS = 0.6

# Expansion knobs (safe defaults)
_MAX_CONTEXTS = 35              # ✅ increased (more subareas/contexts to reach 300)
_STUCK_WINDOW = 4               # after N contexts, if gains too low -> try variants
_STUCK_MIN_GAIN = 8             # "too low" new uniques threshold
_VARIANTS_ENABLED = True
_MAX_VARIANTS = 8               # ✅ increased (still controlled, no spam)


def _dedupe_places_by_id(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dedupe by Places 'id' (best unique key)."""
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        pid = (it or {}).get("id") or ""
        if not pid:
            out.append(it)
            continue
        if pid in seen:
            continue
        seen.add(pid)
        out.append(it)
    return out


def _normalize_location_text(location: str) -> str:
    return re.sub(r"\s+", " ", (location or "").strip())


def _guess_city(location: str) -> str:
    loc = _normalize_location_text(location).lower()
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if not parts:
        return loc
    return parts[0]


def _city_subareas(city_key: str) -> List[str]:
    """
    Seed subareas for dense cities.
    ✅ Expanded lists to help reach 300 in Tier-1 cities.
    """
    city_key = (city_key or "").lower().strip()
    seeds: Dict[str, List[str]] = {
        "mumbai": [
            "Andheri", "Bandra", "Borivali", "Powai", "Goregaon", "Malad", "Kandivali",
            "Dadar", "Lower Parel", "Worli", "BKC", "Vikhroli", "Ghatkopar", "Kurla",
            "Navi Mumbai", "Vashi", "Belapur", "Airoli", "Thane", "Mulund",
        ],
        "delhi": [
            "Connaught Place", "Karol Bagh", "Rohini", "Pitampura", "Rajouri Garden",
            "Janakpuri", "Dwarka", "Saket", "Lajpat Nagar", "Nehru Place",
            "Okhla", "Mayur Vihar", "Shahdara", "Noida", "Gurgaon", "Faridabad",
        ],
        "new delhi": [
            "Connaught Place", "Karol Bagh", "Rohini", "Dwarka", "Saket",
            "Nehru Place", "Okhla", "Lajpat Nagar", "Janakpuri",
        ],
        "bengaluru": [
            "Koramangala", "HSR Layout", "Indiranagar", "Whitefield", "Marathahalli",
            "Electronic City", "Bellandur", "Sarjapur Road", "JP Nagar", "Jayanagar",
            "MG Road", "Hebbal", "Yelahanka", "Rajajinagar", "Basavanagudi",
        ],
        "bangalore": [
            "Koramangala", "HSR Layout", "Indiranagar", "Whitefield", "Marathahalli",
            "Electronic City", "Bellandur", "Sarjapur Road", "JP Nagar", "Jayanagar",
            "MG Road", "Hebbal", "Yelahanka", "Rajajinagar", "Basavanagudi",
        ],
        "hyderabad": [
            "Hitech City", "Gachibowli", "Madhapur", "Kondapur", "Secunderabad",
            "Jubilee Hills", "Banjara Hills", "Begumpet", "Kukatpally", "Ameerpet",
            "Uppal", "LB Nagar",
        ],
        "chennai": [
            "T Nagar", "Guindy", "Velachery", "Anna Nagar", "OMR", "Tambaram",
            "Adyar", "Porur", "Nungambakkam", "Kodambakkam", "Perungudi",
        ],
        "pune": [
            "Hinjewadi", "Wakad", "Baner", "Balewadi", "Aundh", "Kothrud",
            "Shivajinagar", "Camp", "Kharadi", "Viman Nagar", "Hadapsar",
            "Magarpatta", "Pimpri", "Chinchwad", "Nigdi", "Pashan",
        ],
        "ahmedabad": [
            "SG Highway", "Navrangpura", "Satellite", "Vastrapur", "Prahlad Nagar",
            "Bodakdev", "Thaltej", "Maninagar",
        ],
        "kolkata": [
            "Salt Lake", "New Town", "Park Street", "Ballygunge", "Howrah",
            "Garia", "Behala",
        ],
        "gurugram": [
            "Cyber City", "Udyog Vihar", "Golf Course Road", "Sohna Road", "Sector 44",
            "Sector 45", "Sector 48",
        ],
        "noida": [
            "Sector 62", "Sector 63", "Sector 16", "Sector 18", "Sector 15",
            "Greater Noida",
        ],
    }
    return seeds.get(city_key, [])


def _build_search_contexts(location: str, place: str = "") -> List[str]:
    """
    Build multiple location contexts to expand coverage:
    - base location
    - place + location (if provided)
    - seeded subareas for known dense cities
    """
    base = _normalize_location_text(location)
    p = _normalize_location_text(place)

    contexts: List[str] = []
    if base:
        contexts.append(base)
    if p:
        contexts.append(f"{p}, {base}" if base else p)

    city = _guess_city(base)
    for s in _city_subareas(city):
        contexts.append(f"{s}, {base}" if base else s)

    # Dedup contexts + cap
    seen: set[str] = set()
    uniq: List[str] = []
    for c in contexts:
        key = c.lower().strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(c)

    return uniq[:_MAX_CONTEXTS]


def _build_query_variants(query: str) -> List[str]:
    """
    Keyword variants fallback: helps reach 300 in dense cities when query is too narrow.
    Safe: small list + used only if we get stuck.
    """
    q = _normalize_location_text(query)
    if not q:
        return []

    low = q.lower()
    variants: List[str] = []

    # IT/Software expansions
    if any(k in low for k in ["software", "it ", " it", "technology", "tech", "saas", "developer", "development"]):
        variants.extend([
            q,
            "IT services",
            "Software company",
            "Software development company",
            "Technology company",
            "IT consulting",
            "IT solutions",
            "Web development",
            "App development",
        ])

    # Marketing expansions
    elif any(k in low for k in ["marketing", "agency", "advertis", "digital", "branding"]):
        variants.extend([
            q,
            "Digital marketing agency",
            "Marketing agency",
            "Advertising agency",
            "Branding agency",
            "Social media agency",
        ])

    # Default generic expansions (minimal)
    else:
        variants.extend([
            q,
            f"{q} services",
            f"{q} company",
            f"{q} near me",
        ])

    # Dedup + cap
    seen: set[str] = set()
    out: List[str] = []
    for v in variants:
        v2 = _normalize_location_text(v)
        if not v2:
            continue
        k = v2.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(v2)
        if len(out) >= _MAX_VARIANTS:
            break
    return out


def scrape_google_places(
    query: str,
    location: str,
    max_results: int = 40,
) -> List[Dict[str, Any]]:
    """
    Single-context Places searchText (kept for backward compatibility).
    """
    if not GOOGLE_PLACES_API_KEY:
        raise RuntimeError("❌ GOOGLE_PLACES_API_KEY missing")

    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": (
            "places.displayName,"
            "places.formattedAddress,"
            "places.rating,"
            "places.userRatingCount,"
            "places.websiteUri,"
            "places.nationalPhoneNumber,"
            "places.internationalPhoneNumber,"
            "places.id,"
            "places.googleMapsUri"
        ),
    }

    payload: Dict[str, Any] = {
        "textQuery": f"{query} in {location}",
        "pageSize": min(GOOGLE_PAGE_SIZE, max_results),
    }

    out: List[Dict[str, Any]] = []

    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        js = r.json()

        out.extend(js.get("places", []))

        if len(out) >= max_results:
            break

        token = js.get("nextPageToken")
        if not token:
            break

        payload["pageToken"] = token
        time.sleep(_PAGE_TOKEN_DELAY_SECS)

    return out[:max_results]


def scrape_google_places_expand(
    query: str,
    location: str,
    place: str = "",
    max_results: int = 300,
) -> List[Dict[str, Any]]:
    """
    Multi-context expansion + variant fallback (only if stuck).
    ✅ real-only, strict dedupe, returns min(unique_available, max_results)
    """
    max_results = int(max_results)
    max_results = max(1, min(max_results, MAX_CAP_RESULTS))

    contexts = _build_search_contexts(location=location, place=place)
    variants = _build_query_variants(query) if _VARIANTS_ENABLED else [query]
    if not variants:
        variants = [query]

    collected: List[Dict[str, Any]] = []
    last_gains: List[int] = []

    def _unique_count() -> int:
        return len(collected)

    # Start with user's intent; add variants only if stuck.
    active_variants: List[str] = [variants[0]]
    fallback_variants: List[str] = variants[1:]

    for idx_ctx, ctx in enumerate(contexts):
        collected = _dedupe_places_by_id(collected)
        if _unique_count() >= max_results:
            break

        remaining = max_results - _unique_count()
        # Keep request sane; token pages are limited anyway.
        per_ctx_limit = min(max(20, remaining), 60)

        before = _unique_count()

        # Try current active variants for this context
        for qv in active_variants:
            if _unique_count() >= max_results:
                break
            try:
                res = scrape_google_places(query=qv, location=ctx, max_results=per_ctx_limit)
            except Exception:
                res = []
            if res:
                collected.extend(res)
                collected = _dedupe_places_by_id(collected)

            # Small delay between variant calls (rate-limit friendly)
            time.sleep(0.25)

        after = _unique_count()
        gain = max(0, after - before)
        last_gains.append(gain)
        if len(last_gains) > _STUCK_WINDOW:
            last_gains.pop(0)

        # If stuck and we still have fallback variants, activate one more variant
        if (
            _VARIANTS_ENABLED
            and fallback_variants
            and len(last_gains) >= _STUCK_WINDOW
            and sum(last_gains) < _STUCK_MIN_GAIN
        ):
            next_v = fallback_variants.pop(0)
            if next_v and next_v not in active_variants:
                active_variants.append(next_v)
            last_gains = []

        # Delay between contexts
        if idx_ctx < len(contexts) - 1:
            time.sleep(_CONTEXT_DELAY_SECS)

    return collected[:max_results]


def scrape_case1_to_raw(
    query: str,
    location: str,
    run_id: str,
    max_results: int = 40,
    place: str = "",  # ✅ optional, backward compatible
) -> Tuple[List[Dict[str, Any]], str]:
    """
    ✅ Backward compatible:
      - existing callers with (query, location, run_id, max_results) still work
    ✅ New behavior:
      - auto expands contexts + variant fallback to try reaching up to max_results (<=300)
    """
    max_results = max(1, min(int(max_results), MAX_CAP_RESULTS))

    raw = scrape_google_places_expand(
        query=query,
        location=location,
        place=place,
        max_results=max_results,
    )

    out_path = os.path.join(RAW_DIR, f"raw_{run_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

    return raw, out_path


# =========================================================
# ======================= CASE 2 ==========================
# Website Leadership Scraping
# Static -> Dynamic -> Internal Crawl
# (UNCHANGED)
# =========================================================

MAX_PAGES = 7
DEPTH_LIMIT = 2
TIMEOUT_SECS = 25

PAGE_KEYWORDS = ["team", "leadership", "management", "board", "people", "about"]
BLOCKLIST = ["blog", "news", "career", "job", "privacy", "terms", "cookie", "press", "event"]

ROLE_KEYWORDS = [
    "ceo", "founder", "director", "chairman", "president",
    "cto", "cfo", "coo", "chief", "head", "manager", "lead",
]

NAME_RE = re.compile(r"^[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,4}$", re.ASCII)


def _clean_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("www."):
        u = "https://" + u
    if not u.startswith("http"):
        u = "https://" + u
    return u


def _same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc == urlparse(b).netloc
    except Exception:
        return False


def _fetch(url: str, timeout: int = 12) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def _is_valid_name(name: str) -> bool:
    if not name or len(name) < 4:
        return False
    if any(ch.isdigit() for ch in name):
        return False
    return bool(NAME_RE.match(name.strip()))


def _is_valid_role(role: str) -> bool:
    r = (role or "").lower()
    return any(k in r for k in ROLE_KEYWORDS)


def _extract_from_jsonld(soup: BeautifulSoup) -> List[Dict[str, str]]:
    leaders: List[Dict[str, str]] = []
    for sc in soup.find_all("script", type=re.compile("ld\\+json", re.I)):
        try:
            data = json.loads(sc.get_text())
        except Exception:
            continue

        nodes = data if isinstance(data, list) else [data]
        for n in nodes:
            if isinstance(n, dict) and n.get("@type") == "Person":
                name = (n.get("name") or "").strip()
                role = (n.get("jobTitle") or "").strip()
                if _is_valid_name(name) and _is_valid_role(role):
                    leaders.append({"name": name, "role": role})
    return leaders


def _extract_from_dom(soup: BeautifulSoup) -> List[Dict[str, str]]:
    leaders: List[Dict[str, str]] = []

    containers = soup.select(
        "section, article, li, "
        "[class*='team'], [class*='member'], [class*='profile'], "
        "[class*='card'], [class*='person'], [class*='director']"
    )

    for c in containers:
        h = c.find(["h1", "h2", "h3", "strong", "b"])
        if not h:
            continue

        name = h.get_text(" ", strip=True)
        if not _is_valid_name(name):
            continue

        role = ""
        for r in c.find_all(["span", "p", "small", "div"], limit=8):
            t = r.get_text(" ", strip=True)
            if _is_valid_role(t):
                role = t
                break

        if role:
            leaders.append({"name": name, "role": role})

    return leaders


def _discover_pages(home: str) -> List[str]:
    pages = [home]
    html = _fetch(home)
    if not html:
        return pages

    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = urljoin(home, a["href"])
        if not _same_domain(home, href):
            continue
        low = href.lower()
        if any(b in low for b in BLOCKLIST):
            continue
        if any(k in low for k in PAGE_KEYWORDS):
            pages.append(href)
        if len(pages) >= MAX_PAGES:
            break

    return pages


def scrape_leadership_smart(website: str) -> List[Dict[str, str]]:
    website = _clean_url(website)
    if not website:
        return []

    start = time.time()
    pages = _discover_pages(website)
    seen = set()

    for url in pages:
        if time.time() - start > TIMEOUT_SECS:
            break
        if url in seen:
            continue
        seen.add(url)

        # A) STATIC
        html = _fetch(url)
        if html:
            soup = BeautifulSoup(html, "lxml")
            leaders = _extract_from_jsonld(soup) + _extract_from_dom(soup)
            if leaders:
                return leaders[:5]

        # B) DYNAMIC
        if sync_playwright:
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.goto(url, timeout=60000)
                    html = page.content()
                    browser.close()

                soup = BeautifulSoup(html, "lxml")
                leaders = _extract_from_jsonld(soup) + _extract_from_dom(soup)
                if leaders:
                    return leaders[:5]
            except Exception:
                pass

    return []
