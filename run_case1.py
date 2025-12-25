# run_case1.py

from backend.db import (
    init_db,
    get_cached_results,
    cache_raw_results,
    insert_business_list,
    add_search_history,
)
from backend.scraper1 import scrape_case1_to_raw
from backend.miner import clean_and_process
from backend.excel_utils import generate_excel_from_business_list

import uuid
import os

# Initialize DB (creates it if missing)
init_db()


def run_case1(query, location="Pune", use_cache=True):
    print("PIPELINE START — CASE 1")
    print("Query:", query)

    # ---------------------------------
    # 1) CHECK CACHE
    # ---------------------------------
    if use_cache:
        cached = get_cached_results(query)
        if cached:
            print("Using cached results…")
            cleaned = clean_and_process(cached)
            insert_business_list(cleaned, query_tag=query)
            out = generate_excel_from_business_list(
                cleaned, f"case1_{safe_filename(query)}.xlsx"
            )
            return out

    # ---------------------------------
    # 2) SCRAPE NEW DATA
    # ---------------------------------
    print("Scraping fresh data from IndiaMART…")

    run_id = str(uuid.uuid4())[:8]

    raw_records, raw_json_path = scrape_case1_to_raw(
        query=query,
        location=location,
        run_id=run_id,
        max_results=40,
        pages=2,
        debug=True
    )

    print(f"Scraper returned: {len(raw_records)} records")
    print(f"Raw JSON saved to: {raw_json_path}")

    # Save raw in DB cache
    cache_raw_results(query, raw_records)
    add_search_history(query)

    # ---------------------------------
    # 3) CLEAN + PROCESS
    # ---------------------------------
    cleaned = clean_and_process(raw_records)

    # Store in DB
    insert_business_list(cleaned, query_tag=query)

    # ---------------------------------
    # 4) GENERATE EXCEL
    # ---------------------------------
    out = generate_excel_from_business_list(
        cleaned, f"case1_{safe_filename(query)}.xlsx"
    )

    print("Excel generated:", out)
    return out


def safe_filename(s):
    keep = "".join(c for c in s if c.isalnum() or c in (" ", "_", "-")).rstrip()
    return keep.replace(" ", "_")[:80]


if __name__ == "__main__":
    res = run_case1("Manufacturing industries")
    print("FINAL OUTPUT:", res)
