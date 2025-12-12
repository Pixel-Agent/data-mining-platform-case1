# --- Case 1 Agent API ---

from typing import List, Dict, Any
import pandas as pd

def write_case1_excel(rows: List[Dict[str, Any]], out_path: str) -> None:
    df = pd.DataFrame(rows)

    # Safety: ensure columns order
    cols = ["Name", "Primary Category", "Address", "Phone", "Website", "Has Website", "Source"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]

    df_with = df[df["Has Website"] == "Yes"].copy()
    df_no = df[df["Has Website"] == "No"].copy()

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="All Results")
        df_with.to_excel(writer, index=False, sheet_name="With Website")
        df_no.to_excel(writer, index=False, sheet_name="No Website")
