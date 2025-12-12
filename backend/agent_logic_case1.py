# backend/agent_logic_case1.py
from __future__ import annotations
from typing import Dict, Any, Optional
from datetime import datetime
import os

from backend.config import DEFAULT_LOCATION
from backend.gpt_client import GPTClient
from backend import scraper, miner, excel_utils


def run_case1_pipeline(query: str, location: Optional[str] = None, use_gpt: bool = True) -> Dict[str, Any]:
    """
    Case 1 ONLY pipeline:
      scraper -> miner -> excel
    """
    location = location or DEFAULT_LOCATION
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_records, raw_path = scraper.scrape_case1_to_raw(query=query, location=location, run_id=ts)

    gpt = GPTClient() if use_gpt else None
    cleaned_rows, stats = miner.mine_case1_records(raw_records=raw_records, gpt_client=gpt)

    os.makedirs("data/output", exist_ok=True)
    excel_path = os.path.join("data/output", f"case1_{ts}.xlsx")
    excel_utils.write_case1_excel(rows=cleaned_rows, out_path=excel_path)

    return {"raw_path": raw_path, "excel_path": excel_path, "stats": stats}
