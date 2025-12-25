#scraper_case2.py
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set, Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from backend.config import OPENAI_API_KEY, CASE2_MAX_LEADERS

# Playwright is optional (dynamic + XHR fallback)
try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:
    sync_playwright = None  # type: ignore

# OpenAI OPTIONAL (last fallback only)
try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# ---------------------------------------------------------
# Global "disable OpenAI" latch (prevents spam on 401/429)
# ---------------------------------------------------------
_OPENAI_DISABLED: bool = False
_OPENAI_DISABLED_REASON: str = ""


# -----------------------------
# Case-2 constants (hard rules)
# -----------------------------
MAX_INTERNAL_PAGES = 8          # 6–8 pages
MAX_CRAWL_DEPTH = 2             # depth <= 2
PER_COMPANY_TIMEOUT_SECS = 25

# Confidence threshold: no garbage
MIN_CONFIDENCE = 0.65

# XHR capture constraints
MAX_XHR_BYTES = 1_200_000  # ignore huge payloads
MAX_XHR_HITS = 10          # don't collect too many responses

# Paths to probe quickly (seed)
DISCOVERY_PATHS = [
    "/team",
    "/leadership",
    "/management",
    "/board",
    "/about",
    "/people",
    "/our-team",
    "/our-leadership",
    "/administration",
    "/directors",
]

LEADERSHIP_LINK_KEYWORDS = [
    "team",
    "leadership",
    "management",
    "board",
    "people",
    "executive",
    "founder",
    "directors",
    "who-we-are",
    "about",
    "administration",
    "staff",
]

CONTACT_LINK_KEYWORDS = [
    "contact",
    "contact-us",
    "reach-us",
    "support",
    "help",
    "enquiry",
    "inquiry",
    "admissions",
]

# Keep roles broad (domain-agnostic)
ROLE_KEYWORDS = [
    "ceo", "chief executive",
    "cto", "chief technology",
    "coo", "chief operating",
    "cfo", "chief financial",
    "cmo", "chief marketing",
    "cio", "chief information",
    "chro", "chief human",
    "cpo", "chief product",
    "cro", "chief revenue",
    "cso", "chief strategy",
    "founder", "co-founder", "cofounder", "founding",
    "owner", "proprietor",
    "president",
    "vice president", "vp", "svp", "evp",
    "managing director", "director", "executive director",
    "partner", "managing partner", "principal",
    "chairman", "chairperson", "trustee",
    "dean", "registrar", "headmaster", "headmistress",
    "medical director", "clinical director",
    "head of", "department head",
]

ROLE_RE = re.compile(r"(" + "|".join(re.escape(k) for k in ROLE_KEYWORDS) + r")", re.I)

# human-name heuristic: allow Dr/Mr, initials with dots, 2–5 tokens, no digits
NAME_TOKEN = r"(?:[A-Z][a-z]+|[A-Z]\.)"
NAME_RE = re.compile(
    rf"^(?:(?:Dr|Mr|Ms|Mrs)\.?\s+)?{NAME_TOKEN}(?:\s+{NAME_TOKEN}){{1,4}}(?:\s+(?:Jr\.?|Sr\.?))?$"
)

BAD_NAME_WORDS = {
    "team", "leadership", "management", "board", "about", "company",
    "careers", "privacy", "terms", "cookies", "support", "contact", "press", "news",
    "solutions", "services", "products", "pricing", "blog", "resources",
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

# -----------------------------
# Buckets (for your NEW Excel schema)
# -----------------------------
BUCKETS = [
    "Executive Leadership",
    "Technology / Operations",
    "Finance / Administration",
    "Business Development / Growth",
    "Marketing / Branding",
]

BUCKET_TO_EXCEL_PREFIX = {
    "Executive Leadership": "Executive",
    "Technology / Operations": "Tech/Ops",
    "Finance / Administration": "Finance/Admin",
    "Business Development / Growth": "Business/Growth",
    "Marketing / Branding": "Marketing/Brand",
}

_BUCKET_RULES = [
    ("Executive Leadership", [
        "founder", "co-founder", "cofounder", "ceo", "chief executive",
        "managing director", "executive director", "director",
        "chairman", "chairperson", "president", "owner", "proprietor",
        "principal", "dean", "medical director", "clinical director",
    ]),
    ("Technology / Operations", [
        "cto", "chief technology", "cio", "chief information",
        "coo", "chief operating", "operations", "it", "technical",
        "head of operations", "plant head",
    ]),
    ("Finance / Administration", [
        "cfo", "chief financial", "finance", "accounts", "controller",
        "treasurer", "admin", "administration", "hr", "human resources",
        "compliance",
    ]),
    ("Business Development / Growth", [
        "business development", "bd", "growth", "strategy",
        "partnership", "sales", "revenue", "commercial",
        "admissions", "placement",
    ]),
    ("Marketing / Branding", [
        "cmo", "chief marketing", "marketing", "brand",
        "communications", "pr", "digital marketing", "outreach",
        "social media",
    ]),
]


@dataclass
class LeaderCandidate:
    name: str
    role: str
    source_url: str
    evidence: str
    confidence: float


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _max_leaders() -> int:
    try:
        return max(1, min(int(CASE2_MAX_LEADERS or 5), 5))
    except Exception:
        return 5


def _normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url.lstrip("/")
    try:
        p = urlparse(url)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or "/"
        return urlunparse((scheme, netloc, path, "", p.query or "", ""))
    except Exception:
        return url


def _base_domain(url: str) -> str:
    try:
        n = (urlparse(url).netloc or "").lower()
        return n.lstrip("www.")
    except Exception:
        return ""


def _same_domain(a: str, b: str) -> bool:
    da, db = _base_domain(a), _base_domain(b)
    return bool(da) and da == db


def _deadline_remaining(deadline_ts: float) -> float:
    return max(0.0, deadline_ts - time.time())


def _fetch_static(url: str, timeout_s: float) -> Optional[str]:
    try:
        r = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=max(3.0, min(12.0, timeout_s)),
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return None
        ct = (r.headers.get("Content-Type") or "").lower()
        if ct and ("text/html" not in ct) and ("application/xhtml+xml" not in ct):
            return None
        return r.text or ""
    except Exception:
        return None


def _looks_js_shell(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    if "__next_data__" in low or 'id="__next"' in low or "react-root" in low:
        return True
    if len(html) < 2500:
        return True
    return False


def _fetch_dynamic_and_xhr(url: str, timeout_s: float) -> Tuple[Optional[str], List[str]]:
    """
    Dynamic render with Playwright + capture XHR/Fetch JSON responses (HXR).
    Returns: (page_html, json_texts[])
    """
    if sync_playwright is None:
        return None, []

    nav_timeout_ms = int(max(5000, min(16000, timeout_s * 1000)))

    # IMPORTANT: headless=False reduces bot-detection for many sites
    headless_mode = False

    captured_json: List[str] = []

    def maybe_store_json(text: str) -> None:
        if not text:
            return
        if len(text) > MAX_XHR_BYTES:
            return
        # quick filter: must look like JSON object/array
        t = text.lstrip()
        if not (t.startswith("{") or t.startswith("[")):
            return
        captured_json.append(text)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless_mode,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            ctx = browser.new_context(
                user_agent=DEFAULT_HEADERS["User-Agent"],
                viewport={"width": 1366, "height": 768},
                java_script_enabled=True,
            )
            # basic stealth: webdriver undefined
            try:
                ctx.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )
            except Exception:
                pass

            page = ctx.new_page()
            page.set_default_navigation_timeout(nav_timeout_ms)
            page.set_default_timeout(nav_timeout_ms)

            # capture XHR/Fetch
            def on_response(resp):
                try:
                    if len(captured_json) >= MAX_XHR_HITS:
                        return
                    ct = (resp.headers.get("content-type") or "").lower()
                    if "application/json" in ct or "text/json" in ct or "application/ld+json" in ct:
                        txt = resp.text()
                        maybe_store_json(txt)
                        return
                    # sometimes APIs return text/plain but JSON body
                    if "text/plain" in ct or "application/octet-stream" in ct:
                        txt = resp.text()
                        maybe_store_json(txt)
                        return
                except Exception:
                    return

            page.on("response", on_response)

            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(900)

                # light scroll
                for _ in range(2):
                    page.mouse.wheel(0, 2000)
                    page.wait_for_timeout(700)

                html = page.content()
                return html, captured_json[:MAX_XHR_HITS]
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass

    except Exception:
        return None, captured_json[:MAX_XHR_HITS]


def _looks_like_human_name(name: str) -> bool:
    name = _norm(name)
    if not name:
        return False
    if any(ch.isdigit() for ch in name):
        return False
    low = name.lower().strip()
    if low in BAD_NAME_WORDS:
        return False
    if len(name) > 80:
        return False
    tokens = name.split()
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    if name.isupper() and all(t.isalpha() for t in tokens):
        return True
    return bool(NAME_RE.match(name))


def _role_matches(role: str) -> bool:
    role = _norm(role)
    if not role:
        return False
    return bool(ROLE_RE.search(role))


def _dedupe_leaders(items: List[LeaderCandidate], max_n: int) -> List[LeaderCandidate]:
    seen = set()
    out: List[LeaderCandidate] = []
    for it in sorted(items, key=lambda x: x.confidence, reverse=True):
        name = _norm(it.name)
        role = _norm(it.role)
        if not name or not role:
            continue
        key = (name.lower(), role.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
        if len(out) >= max_n:
            break
    return out


def _score_candidate(page_url: str, name: str, role: str, evidence: str) -> float:
    score = 0.0
    u = (page_url or "").lower()
    if any(k in u for k in ["team", "leadership", "management", "board", "people", "administration", "directors"]):
        score += 0.40
    if _looks_like_human_name(name):
        score += 0.25
    if _role_matches(role):
        score += 0.25
    if len(_norm(evidence)) > 220:
        score -= 0.25
    if len(re.split(r"[.!?]+", _norm(evidence))) >= 5:
        score -= 0.20
    return max(0.0, min(1.0, score))


def _extract_jsonld_people(html: str, page_url: str) -> List[LeaderCandidate]:
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    scripts = soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)})

    found: List[LeaderCandidate] = []

    def walk(obj):
        if obj is None:
            return
        if isinstance(obj, list):
            for x in obj:
                walk(x)
            return
        if isinstance(obj, dict):
            if "@graph" in obj:
                walk(obj.get("@graph"))
            t = obj.get("@type")
            types: List[str] = []
            if isinstance(t, list):
                types = [str(x).lower() for x in t]
            elif t:
                types = [str(t).lower()]

            if "person" in types:
                name = _norm(str(obj.get("name") or ""))
                role = _norm(str(obj.get("jobTitle") or obj.get("roleName") or ""))
                if _looks_like_human_name(name) and _role_matches(role):
                    evidence = f"{name} — {role}"
                    conf = _score_candidate(page_url, name, role, evidence)
                    found.append(
                        LeaderCandidate(
                            name=name, role=role, source_url=page_url,
                            evidence=evidence, confidence=conf
                        )
                    )

            for k in ("employee", "employees", "member", "members", "founder", "founders"):
                if k in obj:
                    walk(obj.get(k))
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)

    for s in scripts:
        raw = s.get_text(strip=True) or ""
        if not raw:
            continue
        try:
            data = json.loads(raw)
            walk(data)
        except Exception:
            continue

    return [c for c in found if c.confidence >= MIN_CONFIDENCE]


def _tight_pair_from_text(text: str) -> Optional[Tuple[str, str]]:
    t = _norm(text)
    if not t or len(t) < 8 or len(t) > 180:
        return None

    m = re.match(r"^(.{3,80}?)[\-\–\—,:]\s*(.{3,100})$", t)
    if not m:
        return None
    name = _norm(m.group(1))
    role = _norm(m.group(2))

    if not (_looks_like_human_name(name) and _role_matches(role)):
        return None
    return name, role


def _extract_html_people_strict(html: str, page_url: str) -> List[LeaderCandidate]:
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "form"]):
        try:
            tag.decompose()
        except Exception:
            pass

    candidates: List[LeaderCandidate] = []

    containers = soup.select(
        "article, section, li, "
        "[class*='team'], [class*='member'], [class*='lead'], [class*='profile'], [class*='staff'], "
        "[class*='card'], [class*='person'], [class*='director']"
    )

    for el in containers[:420]:
        txt = _norm(el.get_text(" ", strip=True))
        if not txt or len(txt) < 12 or len(txt) > 420:
            continue

        lines = re.split(r"[\n|•·]+", txt)
        lines = [_norm(x) for x in lines if _norm(x)]
        if not lines:
            continue

        h = el.find(["h1", "h2", "h3", "h4", "strong", "b"])
        name = _norm(h.get_text(" ", strip=True)) if h else ""
        if _looks_like_human_name(name):
            role = ""
            for node in el.find_all(["p", "span", "div", "small"], limit=10):
                t = _norm(node.get_text(" ", strip=True))
                if 3 <= len(t) <= 100 and _role_matches(t):
                    role = t
                    break
            if role:
                evidence = f"{name} — {role}"
                conf = _score_candidate(page_url, name, role, evidence)
                if conf >= MIN_CONFIDENCE:
                    candidates.append(LeaderCandidate(name=name, role=role, source_url=page_url, evidence=evidence, confidence=conf))
                    continue

        for ln in lines[:10]:
            pair = _tight_pair_from_text(ln)
            if not pair:
                continue
            n, r = pair
            evidence = ln
            conf = _score_candidate(page_url, n, r, evidence)
            if conf >= MIN_CONFIDENCE:
                candidates.append(LeaderCandidate(name=n, role=r, source_url=page_url, evidence=evidence, confidence=conf))

    return candidates


def _extract_all_candidates(html: str, page_url: str) -> List[LeaderCandidate]:
    out = _extract_jsonld_people(html, page_url)
    if out:
        return out
    return _extract_html_people_strict(html, page_url)


# -----------------------------
# XHR parsing (HXR)
# -----------------------------
def _walk_json(obj: Any) -> List[Dict[str, str]]:
    """
    Walk any JSON and extract potential (name, role) pairs from dict nodes:
      - {"name": "...", "title": "..."} etc.
    This is strict: both must pass heuristics.
    """
    out: List[Dict[str, str]] = []

    def visit(x: Any):
        if x is None:
            return
        if isinstance(x, list):
            for it in x:
                visit(it)
            return
        if isinstance(x, dict):
            # common keys
            name = None
            role = None

            for nk in ["name", "fullName", "personName"]:
                v = x.get(nk)
                if isinstance(v, str) and v.strip():
                    name = v.strip()
                    break

            for rk in ["title", "jobTitle", "designation", "role", "position"]:
                v = x.get(rk)
                if isinstance(v, str) and v.strip():
                    role = v.strip()
                    break

            if name and role and _looks_like_human_name(name) and _role_matches(role):
                out.append({"name": _norm(name), "role": _norm(role)})

            # recurse
            for v in x.values():
                if isinstance(v, (dict, list)):
                    visit(v)

    visit(obj)
    return out


def _extract_from_xhr_json(json_texts: List[str], source_url: str) -> List[LeaderCandidate]:
    cands: List[LeaderCandidate] = []
    for txt in (json_texts or [])[:MAX_XHR_HITS]:
        try:
            data = json.loads(txt)
        except Exception:
            continue
        pairs = _walk_json(data)
        for p in pairs:
            name = _norm(p.get("name", ""))
            role = _norm(p.get("role", ""))
            if not name or not role:
                continue
            evidence = f"[XHR] {name} — {role}"
            conf = _score_candidate(source_url, name, role, evidence)
            if conf >= MIN_CONFIDENCE:
                cands.append(LeaderCandidate(name=name, role=role, source_url=source_url, evidence=evidence, confidence=conf))
    return cands


# -----------------------------
# Internal crawling
# -----------------------------
def _discover_internal_pages(home_url: str, kind: str, deadline_ts: float) -> List[str]:
    home_url = _normalize_url(home_url)
    if not home_url:
        return []

    keywords = LEADERSHIP_LINK_KEYWORDS if kind == "leadership" else CONTACT_LINK_KEYWORDS

    def is_candidate(u: str, anchor_text: str) -> bool:
        probe = " ".join([u.lower(), (anchor_text or "").lower()])
        return any(k in probe for k in keywords)

    queue: List[Tuple[str, int]] = []
    seen: Set[str] = set()

    def push(u: str, depth: int) -> None:
        u = _normalize_url(u)
        if not u:
            return
        if u in seen:
            return
        if not _same_domain(u, home_url):
            return

        low = u.lower()
        if any(x in low for x in ["/blog", "/news", "/events", "/careers", "/jobs", "/privacy", "/terms"]):
            return

        seen.add(u)
        queue.append((u, depth))

    push(home_url, 0)
    for p in (DISCOVERY_PATHS if kind == "leadership" else ["/contact", "/contact-us", "/support", "/help"]):
        push(urljoin(home_url, p), 1)

    results: List[str] = []
    while queue and len(results) < MAX_INTERNAL_PAGES and _deadline_remaining(deadline_ts) > 1.0:
        url, depth = queue.pop(0)
        results.append(url)

        if depth >= MAX_CRAWL_DEPTH:
            continue

        html = _fetch_static(url, timeout_s=_deadline_remaining(deadline_ts))
        if not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "javascript:")):
                continue
            full = _normalize_url(urljoin(url, href))
            if not full or not _same_domain(full, home_url):
                continue

            text = _norm(a.get_text(" ", strip=True))
            aria = _norm(a.get("aria-label", "")) if hasattr(a, "get") else ""
            title = _norm(a.get("title", "")) if hasattr(a, "get") else ""
            anchor = " ".join([text, aria, title]).strip()

            if is_candidate(full, anchor):
                push(full, depth + 1)

            if len(results) + len(queue) >= MAX_INTERNAL_PAGES:
                break

    ordered: List[str] = []
    seen2: Set[str] = set()
    for u in results:
        if u not in seen2:
            seen2.add(u)
            ordered.append(u)

    return ordered[:MAX_INTERNAL_PAGES]


# -----------------------------
# Role -> bucket mapping
# -----------------------------
def _map_role_to_bucket(role: str) -> str:
    r = _norm(role).lower()
    if not r:
        return ""
    for bucket, keys in _BUCKET_RULES:
        for k in keys:
            if k in r:
                return bucket
    return ""


def _empty_case2_management() -> Dict[str, Dict[str, str]]:
    return {b: {"name": "", "designation": ""} for b in BUCKETS}


def _leaders_to_case2_management(leaders: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    mgmt = _empty_case2_management()
    used = set()

    for it in leaders or []:
        name = _norm(it.get("name", ""))
        role = _norm(it.get("role", "")) or _norm(it.get("designation", ""))
        if not name or not role:
            continue
        bucket = _map_role_to_bucket(role)
        if not bucket or bucket in used:
            continue
        mgmt[bucket]["name"] = name
        mgmt[bucket]["designation"] = role
        used.add(bucket)

    return mgmt


def _leadership_found(case2_management: Dict[str, Dict[str, str]]) -> bool:
    for b in BUCKETS:
        d = case2_management.get(b) or {}
        if _norm(d.get("name", "")) and _norm(d.get("designation", "")):
            return True
    return False


# -----------------------------
# OpenAI fallback (optional)
# -----------------------------
def _openai_available() -> bool:
    if _OPENAI_DISABLED:
        return False
    if not OPENAI_API_KEY or not str(OPENAI_API_KEY).strip():
        return False
    if OpenAI is None:
        return False
    return True


def _disable_openai(reason: str) -> None:
    global _OPENAI_DISABLED, _OPENAI_DISABLED_REASON
    _OPENAI_DISABLED = True
    _OPENAI_DISABLED_REASON = (reason or "").strip()[:200]


def _should_disable_openai_from_exception(e: Exception) -> Optional[str]:
    msg = (str(e) or "").lower()
    status = getattr(e, "status_code", None)
    try:
        status_int = int(status) if status is not None else None
    except Exception:
        status_int = None

    if "insufficient_quota" in msg or "exceeded your current quota" in msg:
        return "OpenAI quota exhausted (429 insufficient_quota)."
    if "invalid api key" in msg or "incorrect api key" in msg:
        return "OpenAI key invalid (401)."
    if status_int == 401:
        return "OpenAI authentication failed (401)."
    if status_int == 403:
        return "OpenAI permission denied (403)."
    if status_int == 429:
        if "rate limit" in msg:
            return None
        return "OpenAI 429 (quota/limits)."
    return None


def _make_client() -> "OpenAI":
    return OpenAI(api_key=str(OPENAI_API_KEY).strip())


def _safe_list_of_leaders(obj: object, max_n: int) -> List[Dict[str, str]]:
    leaders: List[Dict[str, str]] = []
    if isinstance(obj, list):
        items = obj
    elif isinstance(obj, dict):
        items = obj.get("leaders", [])
    else:
        items = []
    if not isinstance(items, list):
        return []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = _norm(str(it.get("name", "")))
        role = _norm(str(it.get("role", "")))
        if name and role:
            leaders.append({"name": name, "role": role})
    return leaders[:max_n]


# ---------------------------------------------------------
# Leaders: Static -> Dynamic -> XHR -> OpenAI (optional)
# ---------------------------------------------------------
def scrape_management_from_website(
    website: str,
    company_name: str = "",
    max_leaders: int = 5,
) -> Dict[str, Any]:
    """
    STRICT scraping-first.
    Returns dict:
      {
        "case2_management": {bucket: {name, designation}},
        "Leadership Found": "Yes/No",
        "leaders_raw": [{"name","role"}]   # optional debug/internal use
      }
    Never raises.
    """
    website = _normalize_url(website)
    if not website:
        mgmt = _empty_case2_management()
        return {"case2_management": mgmt, "Leadership Found": "No", "leaders_raw": []}

    max_leaders = max(1, min(int(max_leaders or 5), 5))
    deadline = time.time() + PER_COMPANY_TIMEOUT_SECS

    pages = _discover_internal_pages(website, kind="leadership", deadline_ts=deadline)

    all_candidates: List[LeaderCandidate] = []
    xhr_candidates: List[LeaderCandidate] = []

    for page_url in pages:
        if _deadline_remaining(deadline) <= 0.8:
            break

        # A) static extract
        html = _fetch_static(page_url, timeout_s=_deadline_remaining(deadline))
        if html:
            all_candidates.extend(_extract_all_candidates(html, page_url))

        strong = [c for c in all_candidates if c.confidence >= MIN_CONFIDENCE]
        if len(strong) >= max_leaders:
            break

        # B) dynamic + XHR when JS shell / weak
        if _deadline_remaining(deadline) <= 4.0:
            continue
        if html and not _looks_js_shell(html):
            continue

        dyn_html, json_texts = _fetch_dynamic_and_xhr(page_url, timeout_s=_deadline_remaining(deadline))

        if dyn_html:
            all_candidates.extend(_extract_all_candidates(dyn_html, page_url))

        if json_texts:
            xhr_candidates.extend(_extract_from_xhr_json(json_texts, page_url))

        # merge strong check
        merged = all_candidates + xhr_candidates
        strong2 = [c for c in merged if c.confidence >= MIN_CONFIDENCE]
        if len(strong2) >= max_leaders:
            break

    merged_final = _dedupe_leaders(all_candidates + xhr_candidates, max_n=max_leaders)
    if merged_final:
        leaders = [{"name": _norm(c.name), "role": _norm(c.role)} for c in merged_final]
        mgmt = _leaders_to_case2_management(leaders)
        return {
            "case2_management": mgmt,
            "Leadership Found": "Yes" if _leadership_found(mgmt) else "No",
            "leaders_raw": leaders,
        }

    # C) OpenAI fallback (last)
    if not _openai_available():
        mgmt = _empty_case2_management()
        return {"case2_management": mgmt, "Leadership Found": "No", "leaders_raw": []}

    c_name = _norm(company_name) or "this organization"
    try:
        client = _make_client()
        prompt = f"""
You are extracting top management for an organization.

Organization: {c_name}
Website: {website}

Return ONLY JSON in this schema:
{{
  "leaders": [
    {{ "name": "Full Name", "role": "Role/Title" }}
  ]
}}

Rules:
- Max {max_leaders} leaders
- Only real people with leadership titles (Founder/CEO/Director/Principal/Dean/CXO)
- If unsure, return {{ "leaders": [] }}
""".strip()

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return valid JSON only. No markdown."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )

        text = resp.choices[0].message.content or "{}"
        data = json.loads(text)
        leaders = _safe_list_of_leaders(data, max_n=max_leaders)

        # hard validate
        clean: List[Dict[str, str]] = []
        seen = set()
        for it in leaders:
            n = _norm(it.get("name", ""))
            r = _norm(it.get("role", ""))
            if not (_looks_like_human_name(n) and _role_matches(r)):
                continue
            k = (n.lower(), r.lower())
            if k in seen:
                continue
            seen.add(k)
            clean.append({"name": n, "role": r})
            if len(clean) >= max_leaders:
                break

        mgmt = _leaders_to_case2_management(clean)
        return {
            "case2_management": mgmt,
            "Leadership Found": "Yes" if _leadership_found(mgmt) else "No",
            "leaders_raw": clean,
        }

    except Exception as e:
        reason = _should_disable_openai_from_exception(e)
        if reason:
            _disable_openai(reason)
            print(f"⚠️ [OPENAI_DISABLED] {reason} (first hit on: {c_name})")
        mgmt = _empty_case2_management()
        return {"case2_management": mgmt, "Leadership Found": "No", "leaders_raw": []}


# ---------------------------------------------------------
# Email (same as your logic, kept)
# ---------------------------------------------------------
def scrape_contact_email_from_website(website: str, company_name: str = "") -> str:
    website = _normalize_url(website)
    if not website:
        return ""

    deadline = time.time() + PER_COMPANY_TIMEOUT_SECS
    pages = _discover_internal_pages(website, kind="contact", deadline_ts=deadline)

    def pick_best_email(found: List[str]) -> str:
        preferred_prefix = (
            "info@", "contact@", "admin@", "office@", "support@", "help@",
            "sales@", "hr@", "careers@", "admissions@", "enquiry@", "inquiry@"
        )
        emails = []
        for e in found:
            e = _norm(e).lower()
            m = EMAIL_REGEX.search(e) if e else None
            if not m:
                continue
            emails.append(m.group(0).lower())
        if not emails:
            return ""
        for p in preferred_prefix:
            for e in emails:
                if e.startswith(p):
                    return e
        return emails[0]

    for page_url in pages:
        if _deadline_remaining(deadline) <= 0.8:
            break

        html = _fetch_static(page_url, timeout_s=_deadline_remaining(deadline))
        if html:
            best = pick_best_email(EMAIL_REGEX.findall(html))
            if best:
                return best

        if _deadline_remaining(deadline) <= 4.0:
            continue
        if html and not _looks_js_shell(html):
            continue

        dyn_html, json_texts = _fetch_dynamic_and_xhr(page_url, timeout_s=_deadline_remaining(deadline))
        if dyn_html:
            best = pick_best_email(EMAIL_REGEX.findall(dyn_html))
            if best:
                return best
        # (XHR emails not reliable; skip)

    return ""


# ---------------------------------------------------------
# Sync wrapper (Streamlit safe)
# ---------------------------------------------------------
def run_discovery_sync(website: str, company_name: str) -> Tuple[Dict[str, Any], str]:
    """
    Returns:
      (mgmt_payload, email)
    where mgmt_payload contains:
      case2_management + Leadership Found + leaders_raw
    """
    payload = scrape_management_from_website(
        website=website,
        company_name=company_name,
        max_leaders=_max_leaders(),
    )
    email = scrape_contact_email_from_website(website=website, company_name=company_name)
    return payload, (email or "")
