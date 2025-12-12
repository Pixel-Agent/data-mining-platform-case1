# --- Case 1 Agent API ---

from typing import List, Dict, Any, Optional, Tuple
import re

def _norm_phone(p: str) -> str:
    p = (p or "").strip()
    p = re.sub(r"[^\d+]", "", p)
    return p

def _has_website(w: str) -> bool:
    w = (w or "").strip()
    return bool(w) and ("http" in w or "." in w)

def mine_case1_records(raw_records: List[Dict[str, Any]], gpt_client=None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Case 1 ONLY: returns (cleaned_rows, stats)
    cleaned_rows: list of dict (final columns ready for excel)
    """
    cleaned = []
    seen = set()

    for r in raw_records:
        name = (r.get("name") or "").strip()
        addr = (r.get("address") or "").strip()
        phone = _norm_phone(r.get("phone") or "")
        website = (r.get("website") or "").strip()
        raw_cat = (r.get("raw_category") or r.get("category") or "").strip()

        if not name:
            continue

        # simple dedupe key
        key = (name.lower(), addr.lower(), phone)
        if key in seen:
            continue
        seen.add(key)

        primary_cat = ""
        if gpt_client is not None and getattr(gpt_client, "is_enabled", lambda: False)():
            primary_cat = gpt_client.classify_primary_category_case1(
                name=name,
                raw_category=raw_cat,
                address=addr,
            )

        cleaned.append({
            "Name": name,
            "Primary Category": primary_cat or raw_cat or "Other",
            "Address": addr,
            "Phone": phone,
            "Email": (r.get("email") or "").strip(),
            "Website": website,
            "Has Website": "Yes" if _has_website(website) else "No",
            "Source": (r.get("source") or "").strip(),
        })

    stats = {
        "raw_count": len(raw_records),
        "clean_count": len(cleaned),
        "with_website": sum(1 for x in cleaned if x["Has Website"] == "Yes"),
        "no_website": sum(1 for x in cleaned if x["Has Website"] == "No"),
    }

    return cleaned, stats
