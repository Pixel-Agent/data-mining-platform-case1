from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import os


# ---------------------------------------------------------
# Helper functions for environment variables
# ---------------------------------------------------------
def _env_int(key: str, default: int) -> int:
    try:
        return int(str(os.getenv(key, default)).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, default)).strip())
    except Exception:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    return str(os.getenv(key, str(default))).strip().lower() in {"true", "1", "yes", "y"}


def _env_str(key: str, default: str = "") -> str:
    try:
        return str(os.getenv(key, default) or default).strip()
    except Exception:
        return default


# ---------------------------------------------------------
# Case 1: Search Settings (Google Places)
# ---------------------------------------------------------
DEFAULT_LOCATION = _env_str("DEFAULT_LOCATION", "Pune, Maharashtra")
DEFAULT_TOP_N = _env_int("DEFAULT_TOP_N", 20)

# ✅ UI hard cap (what user can request); scraper may have its own caps.
# Set to 300 to support big-city expansion runs (REAL unique only, no duplicates).
TOP_N_CAP = _env_int("TOP_N_CAP", 300)

# Google Places API key
GOOGLE_PLACES_API_KEY = _env_str("GOOGLE_PLACES_API_KEY", "")

# NOTE:
# If you want 300+ results:
# - increase TOP_N_CAP (this file)
# - AND update backend/scraper.py MAX_CAP_RESULTS + expansion strategy (multi-context/variants)
# - AND update UI max_value (st.number_input / slider)


# ---------------------------------------------------------
# Case 2: Leadership Extraction (Scraping-first)
# ---------------------------------------------------------
CASE2_ENABLED = _env_bool("CASE2_ENABLED", True)

# (Legacy) Output cap; some old modules still read this
CASE2_MAX_LEADERS = _env_int("CASE2_MAX_LEADERS", 5)

# Per-company timeout seconds (default ~25s)
CASE2_TIMEOUT_SECS = _env_int("CASE2_TIMEOUT_SECS", 25)

# (Legacy) Max pages to try (older scraper used this)
# ✅ Align legacy with new internal crawl max (avoid accidental lower caps)
CASE2_MAX_PAGES = _env_int("CASE2_MAX_PAGES", 8)

# Max HTML bytes to keep per page (avoid huge pages)
CASE2_MAX_BYTES = _env_int("CASE2_MAX_BYTES", 900_000)

# UA for both requests + Playwright contexts
CASE2_USER_AGENT = _env_str(
    "CASE2_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
)

# -----------------------------
# NEW: internal crawling controls (FINAL)
# -----------------------------
# max internal pages visited per company (6–8 recommended)
CASE2_MAX_INTERNAL_PAGES = _env_int("CASE2_MAX_INTERNAL_PAGES", 8)
# internal crawl depth (<=2)
CASE2_MAX_CRAWL_DEPTH = _env_int("CASE2_MAX_CRAWL_DEPTH", 2)
# strict confidence threshold to block garbage (>=0.65 recommended)
CASE2_MIN_CONFIDENCE = _env_float("CASE2_MIN_CONFIDENCE", 0.65)

# -----------------------------
# Optional toggles for fallbacks
# -----------------------------
# If False -> dynamic layer skipped even if Playwright installed
CASE2_ENABLE_PLAYWRIGHT = _env_bool("CASE2_ENABLE_PLAYWRIGHT", True)

# ✅ Reliability knobs for Playwright (prevents hanging on Windows)
CASE2_PLAYWRIGHT_HEADLESS = _env_bool("CASE2_PLAYWRIGHT_HEADLESS", True)
CASE2_PLAYWRIGHT_NAV_TIMEOUT_MS = _env_int("CASE2_PLAYWRIGHT_NAV_TIMEOUT_MS", 20_000)
CASE2_PLAYWRIGHT_WAIT_UNTIL = _env_str("CASE2_PLAYWRIGHT_WAIT_UNTIL", "domcontentloaded")

# If True -> allow XHR harvesting layer (only if implemented in code)
# ✅ Default ON because your master prompt requires the XHR fallback step.
CASE2_ENABLE_XHR = _env_bool("CASE2_ENABLE_XHR", True)

# Role keywords (comma-separated) used by scraper_case2 ROLE_WORDS_RE
CASE2_ROLE_KEYWORDS = _env_str(
    "CASE2_ROLE_KEYWORDS",
    ",".join(
        [
            "ceo",
            "chief executive",
            "founder",
            "co-founder",
            "managing director",
            "director",
            "owner",
            "president",
            "cto",
            "cfo",
            "chairman",
            "coo",
            "vp",
            "vice president",
            "head of",
            "principal",
            "dean",
            "chancellor",
            "vice chancellor",
            "registrar",
            "partner",
            "executive",
            "lead",
        ]
    ),
)

# Discovery blocklist for links/pages (comma-separated substrings)
CASE2_DISCOVERY_BLOCKLIST = _env_str(
    "CASE2_DISCOVERY_BLOCKLIST",
    ",".join(
        [
            "privacy",
            "terms",
            "cookie",
            "careers",
            "jobs",
            "blog",
            "news",
            "press",
            "events",
            "webinar",
            "login",
            "signup",
            "register",
            "support",
            "help",
            "docs",
            "documentation",
            "pricing",
            "partners",
            "solutions",
            "products",
            "services",
        ]
    ),
)


# ---------------------------------------------------------
# OpenAI (OPTIONAL fallback ONLY)
# ---------------------------------------------------------
OPENAI_API_KEY = _env_str("OPENAI_API_KEY", "")


# ---------------------------------------------------------
# Debugging & Reliability
# ---------------------------------------------------------
DEBUG_MODE = _env_bool("DEBUG_MODE", True)
MAX_RETRIES = _env_int("MAX_RETRIES", 2)


# ---------------------------------------------------------
# Legacy / Backward Compatibility (DO NOT TOUCH)
# ---------------------------------------------------------
CASE2_TOTAL_TIMEOUT_SECS = _env_int("CASE2_TOTAL_TIMEOUT_SECS", 600)
CASE2_MAX_SECONDARY_ORGS = _env_int("CASE2_MAX_SECONDARY_ORGS", 100)
