from __future__ import annotations

from typing import List, Dict, Any, Tuple
import re
import json


# -----------------------------
# Normalization Helpers
# -----------------------------
def _norm(s: Any) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip())


def _safe_json(x: Any) -> Any:
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return None
    return None


# -----------------------------
# Buckets (SOURCE ONLY)
# -----------------------------
BUCKETS_ORDER = [
    "Executive Leadership",
    "Technology / Operations",
    "Finance / Administration",
    "Business Development / Growth",
    "Marketing / Branding",
]


def _empty_names() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for i in range(1, 6):
        out[f"Name {i}"] = ""
        out[f"Designation {i}"] = ""
    return out


def _flatten_case2_management(case2_management: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """
    Converts bucketed management into:
      Name 1 / Designation 1 ... Name 5 / Designation 5
    Order strictly follows BUCKETS_ORDER
    """
    out = _empty_names()
    idx = 1

    for bucket in BUCKETS_ORDER:
        if idx > 5:
            break
        data = case2_management.get(bucket) or {}
        name = _norm(data.get("name", ""))
        role = _norm(data.get("designation", ""))
        if name and role:
            out[f"Name {idx}"] = name
            out[f"Designation {idx}"] = role
            idx += 1

    return out


def _leadership_found(flat: Dict[str, str]) -> str:
    return "Yes" if flat.get("Name 1") and flat.get("Designation 1") else "No"


def _dedupe_key(place_id: str, company: str, address: str) -> str:
    """
    Primary: place_id
    Fallback: normalized company+address
    """
    pid = _norm(place_id)
    if pid:
        return f"pid::{pid}"

    c = _norm(company).lower()
    a = _norm(address).lower()
    if c and a:
        return f"na::{c}||{a}"
    if c:
        return f"n::{c}"
    return ""


# -----------------------------
# MAIN MINER
# -----------------------------
def mine_case1_records(raw_records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for r in raw_records or []:
        company = _norm(
            r.get("Company Name")
            or (r.get("displayName") or {}).get("text")
            or r.get("name")
        )

        website = _norm(
            r.get("Website URL")
            or r.get("websiteUri")
            or r.get("website")
        )

        place_id = _norm(
            r.get("Place ID")
            or r.get("google_place_id")
            or r.get("place_id")
            or r.get("id")
        )

        address = _norm(r.get("Address") or r.get("formattedAddress") or "")

        # ✅ DEDUPE (critical for 150 cap accuracy)
        key = _dedupe_key(place_id=place_id, company=company, address=address)
        if key and key in seen:
            continue
        if key:
            seen.add(key)

        # -----------------------------
        # Case-2 handling
        # -----------------------------
        raw_case2 = (
            r.get("case2_management")
            or (r.get("case2_payload") or {}).get("case2_management")
        )

        case2_mgmt = _safe_json(raw_case2) or {}
        if not isinstance(case2_mgmt, dict):
            case2_mgmt = {}

        flat_leaders = _flatten_case2_management(case2_mgmt)

        row: Dict[str, Any] = {
            "Company Name": company or "Unknown",
            "Industry": _norm(r.get("Industry") or r.get("primaryType") or "Business"),
            "Google Rating": r.get("Google Rating") or r.get("rating"),
            "Rating Count": r.get("Reviews") or r.get("userRatingCount"),
            "Has Website": "Yes" if website else "No",
            "Website URL": website,
            "Contact Phone": _norm(
                r.get("Contact Phone")
                or r.get("internationalPhoneNumber")
                or r.get("nationalPhoneNumber")
                or ""
            ),
            "Contact Email": _norm(r.get("Contact Email") or r.get("email") or ""),
            "Address": address,
            "Place ID": place_id,
            "Source Name": _norm(r.get("Source Name") or "Google Places"),
            "Source URL": _norm(r.get("Source URL") or r.get("googleMapsUri") or ""),
        }

        # Inject flattened leaders
        row.update(flat_leaders)
        row["Leadership Found"] = _leadership_found(flat_leaders)

        # Keep for DB/debug
        row["case2_management"] = case2_mgmt

        cleaned.append(row)

    stats = {
        "total": len(cleaned),
        "with_website": sum(1 for x in cleaned if (x.get("Website URL") or "").strip()),
        "with_leadership": sum(1 for x in cleaned if x.get("Leadership Found") == "Yes"),
        "deduped_out": (len(raw_records or []) - len(cleaned)),
    }

    return cleaned, stats
