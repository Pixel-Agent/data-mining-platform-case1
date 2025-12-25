from __future__ import annotations

import re
import json
import asyncio
from typing import List, Dict, Any, Optional

from backend.config import OPENAI_API_KEY

# OpenAI is optional at runtime (must not crash project if missing)
try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


# -----------------------------
# Helpers
# -----------------------------
def _norm(s: Any) -> str:
    s = "" if s is None else str(s)
    s = re.sub(r"\s+", " ", s.strip())
    return s


def _safe_json_load(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def _clean_leaders(obj: Any, max_leaders: int = 5) -> List[Dict[str, str]]:
    """
    Accepts:
      - {"leaders": [...]}
      - [...]
    Returns:
      - [{"name": "...", "role": "..."}]
    """
    if max_leaders <= 0:
        max_leaders = 5

    items: Any
    if isinstance(obj, dict):
        items = obj.get("leaders", [])
    elif isinstance(obj, list):
        items = obj
    else:
        items = []

    if not isinstance(items, list):
        return []

    out: List[Dict[str, str]] = []
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        name = _norm(it.get("name"))
        role = _norm(it.get("role")) or _norm(it.get("designation"))
        if not name or not role:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": name, "role": role})
        if len(out) >= max_leaders:
            break
    return out


def _should_disable_for_error(e: Exception) -> bool:
    """
    If quota/billing/auth related, we should not spam or crash.
    Treat 401/403/429 and "insufficient_quota" as disable conditions.
    """
    msg = (str(e) or "").lower()

    if "insufficient_quota" in msg or "exceeded your current quota" in msg:
        return True

    # auth / billing / permission
    if "invalid api key" in msg or "incorrect api key" in msg or "unauthorized" in msg:
        return True
    if "billing" in msg and ("required" in msg or "details" in msg or "account" in msg):
        return True
    if "permission" in msg and "denied" in msg:
        return True

    # status-code heuristics (string-based + attr-based)
    if "error code: 401" in msg or "401" in msg and "auth" in msg:
        return True
    if "error code: 403" in msg or "403" in msg and "permission" in msg:
        return True
    if "error code: 429" in msg or ("429" in msg and ("quota" in msg or "rate" in msg or "limit" in msg)):
        return True

    status = getattr(e, "status_code", None)
    try:
        status_i = int(status) if status is not None else None
    except Exception:
        status_i = None

    if status_i in (401, 403, 429):
        return True

    return False


# ---------------------------------------------------------
# OpenAI-backed client (keeps class name for compatibility)
# ---------------------------------------------------------
class GeminiClient:
    """
    Backward-compatible client wrapper.
    Previously Gemini-based; now uses OpenAI (if enabled).
    MUST NOT crash if key/quota missing.
    """

    def __init__(self) -> None:
        self.model_id: str = "gpt-4o-mini"
        self._disabled: bool = False
        self.client: Any = None  # lazily created

    def is_enabled(self) -> bool:
        if self._disabled:
            return False
        key = (OPENAI_API_KEY or "").strip()
        if not key:
            return False
        if OpenAI is None:
            return False
        return True

    def _ensure_client(self) -> Optional[Any]:
        if not self.is_enabled():
            return None
        if self.client is not None:
            return self.client
        try:
            self.client = OpenAI(api_key=(OPENAI_API_KEY or "").strip())  # type: ignore[misc]
            return self.client
        except Exception:
            self.client = None
            return None

    async def discovery_search_async(self, company_name: str, website: str) -> List[Dict[str, str]]:
        """
        LLM-only leadership extraction (no website scraping here).
        Returns: [{"name":"...","role":"..."}]
        Never raises.
        """
        client = self._ensure_client()
        if client is None:
            return []

        company_name = _norm(company_name) or "this company"
        website = _norm(website)
        if not website:
            return []

        prompt = f"""
Extract current top management of the company.

Company: {company_name}
Website: {website}

Return ONLY JSON:
{{
  "leaders": [
    {{ "name": "Full Name", "role": "Role/Title" }}
  ]
}}

Rules:
- Max 5
- Focus on CEO, Founder, Co-Founder, Managing Director, Director, CTO, COO, CFO
- If unsure, return {{ "leaders": [] }}
""".strip()

        try:
            def _call():
                return client.chat.completions.create(
                    model=self.model_id,
                    messages=[
                        {"role": "system", "content": "Return valid JSON only. No markdown."},
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.2,
                )

            resp = await asyncio.to_thread(_call)
            text = (resp.choices[0].message.content or "{}").strip()
            data = _safe_json_load(text) or {}
            return _clean_leaders(data, max_leaders=5)

        except Exception as e:
            if _should_disable_for_error(e):
                self._disabled = True
                return []
            return []

    def clean_leadership_data(self, raw_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        OPTIONAL LLM cleaning: remove junk/menu items/slogans and normalize roles.
        Input: list of dicts
        Output: list of dicts [{"name","role"}]
        Never raises.
        """
        if not raw_data:
            return []

        client = self._ensure_client()
        if client is None:
            return raw_data[:5]

        prompt = f"""
Clean and normalize this leadership list.

Rules:
- Remove navigation/menu items, slogans, page headings, locations, departments.
- Keep only real people.
- Ensure each item has "name" and "role".
- Max 5.

Return ONLY JSON:
{{
  "leaders": [
    {{ "name": "Full Name", "role": "Role/Title" }}
  ]
}}

Data:
{json.dumps(raw_data, ensure_ascii=False)}
""".strip()

        try:
            resp = client.chat.completions.create(
                model=self.model_id,
                messages=[
                    {"role": "system", "content": "Return valid JSON only. No markdown."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
            )
            text = (resp.choices[0].message.content or "{}").strip()
            data = _safe_json_load(text) or {}
            cleaned = _clean_leaders(data, max_leaders=5)
            return cleaned or raw_data[:5]

        except Exception as e:
            if _should_disable_for_error(e):
                self._disabled = True
                return raw_data[:5]
            return raw_data[:5]

    def normalize_top_level_management(self, raw_title: str) -> str:
        """
        Rule-based bucket mapping for consistent dashboard categorization.
        """
        title = (raw_title or "").strip()
        if not title:
            return "General Management"
        t = re.sub(r"\s+", " ", title).lower()

        # Executive Leadership
        if re.search(r"\b(founder|ceo\b|director|president|md\b|chairman|principal|chief executive)\b", t):
            return "Executive Leadership"
        # Finance / Admin
        if re.search(r"\b(cfo\b|finance|accounts|admin|hr\b|human resources|legal|compliance)\b", t):
            return "Finance / Administration"
        # Tech / Ops
        if re.search(r"\b(cto\b|technology|it\b|engineering|operations|ops\b|production|technical)\b", t):
            return "Technology / Operations"
        # Growth / BD
        if re.search(r"\b(sales|growth|business development|bd\b|strategy|revenue|commercial)\b", t):
            return "Business Development / Growth"
        # Marketing
        if re.search(r"\b(marketing|brand|branding|pr\b|communications|digital)\b", t):
            return "Marketing / Branding"

        return "Other Management"
