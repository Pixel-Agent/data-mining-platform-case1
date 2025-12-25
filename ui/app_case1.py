from __future__ import annotations

import os
import sys
import base64
from typing import Optional, Dict, Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components  # noqa: F401

# -----------------------------
# Project Root (FIX)
# -----------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# -----------------------------
# Backend Loading
# -----------------------------
try:
    from backend.agent_logic_case1 import run_case1_pipeline
except ImportError:
    try:
        from agent_logic_case1 import run_case1_pipeline  # type: ignore
    except ImportError as e:
        st.error(f"❌ Critical Error: Backend modules not found. {e}")
        st.info(f"Make sure 'backend/agent_logic_case1.py' exists in {PROJECT_ROOT}")
        st.stop()

# -----------------------------
# Config (for dynamic UI caps)
# -----------------------------
try:
    from backend.config import TOP_N_CAP as UI_TOP_N_CAP
except Exception:
    UI_TOP_N_CAP = 300  # safe fallback

# -----------------------------
# UI Helpers & Formatting
# -----------------------------
def _b64_image(path: str) -> str:
    try:
        if not path or not os.path.exists(path):
            return ""
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return ""


def _ensure_role_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Align leadership columns for consistent display.

    Your FINAL schema is:
      Name 1 / Designation 1 ... Name 5 / Designation 5
    But older code sometimes had:
      Leader 1 Role, Leader 1 Designation, etc.
    So we normalize a few common variations into Name/Designation format.
    """
    if df is None or df.empty:
        return df

    rename_map = {}

    # old -> new
    for i in range(1, 6):
        # if someone used Leader columns
        if f"Leader {i} Name" in df.columns and f"Name {i}" not in df.columns:
            rename_map[f"Leader {i} Name"] = f"Name {i}"
        if f"Leader {i} Role" in df.columns and f"Designation {i}" not in df.columns:
            rename_map[f"Leader {i} Role"] = f"Designation {i}"
        if f"Leader {i} Designation" in df.columns and f"Designation {i}" not in df.columns:
            rename_map[f"Leader {i} Designation"] = f"Designation {i}"

    # apply once
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;700;800&display=swap');

        :root{
          --bg: #030305;
          --panel: rgba(18, 18, 26, 0.75);
          --stroke: rgba(255,255,255,0.08);
          --text: #f4f4f7;
          --accent: #7c3aed;
          --radius: 24px;
        }

        .stApp {
            background: radial-gradient(circle at 50% -20%, #1e1b4b 0%, #030305 80%);
            font-family: 'Plus Jakarta Sans', sans-serif;
            color: var(--text);
        }

        .card{
          background: var(--panel); border: 1px solid var(--stroke);
          border-radius: var(--radius); padding: 1.8rem;
          backdrop-filter: blur(20px); margin-bottom: 1.5rem;
          box-shadow: 0 10px 40px rgba(0,0,0,0.6);
        }

        @keyframes pulse-ai {
            0% { box-shadow: 0 0 0 0 rgba(124, 58, 237, 0.6); }
            70% { box-shadow: 0 0 0 15px rgba(124, 58, 237, 0); }
            100% { box-shadow: 0 0 0 0 rgba(124, 58, 237, 0); }
        }
        .ai-active {
            animation: pulse-ai 2s infinite;
            border: 1px solid var(--accent) !important;
        }

        .px-title {
            font-size: clamp(38px, 6vw, 72px);
            font-weight: 900;
            background: linear-gradient(90deg, #fff, #a78bfa, #7c3aed);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-align: center;
            letter-spacing: -2px;
        }

        .px-nav{
          position: fixed; top: 0; left: 0; right: 0; z-index: 1000;
          padding: 12px 40px; background: rgba(3,3,5,0.85);
          backdrop-filter: blur(18px); border-bottom: 1px solid var(--stroke);
        }

        .logline {
            display: flex; align-items: center; gap: 12px; padding: 12px;
            background: rgba(255,255,255,0.04); border-radius: 14px; margin: 8px 0;
            border: 1px solid rgba(255,255,255,0.05);
        }
        .dot { height: 10px; width: 10px; background: var(--accent); border-radius: 50%; box-shadow: 0 0 10px var(--accent); }

        #MainMenu, footer, header { visibility: hidden; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _navbar():
    assets_dir = os.path.join(PROJECT_ROOT, "ui", "assets")
    logo_path = os.path.join(assets_dir, "pixel11_logo.jpeg")
    logo_b64 = _b64_image(logo_path)
    logo_html = (
        f'<img src="data:image/png;base64,{logo_b64}" style="height:50px; border-radius:10px;"/>'
        if logo_b64
        else ""
    )
    st.markdown(
        f"""
        <div class="px-nav">
            <div style="display:flex; align-items:center; gap:20px;">
                {logo_html}
                <div style="font-size:24px; font-weight:950; letter-spacing:-1px;">
                    PIXEL11 <span style="color:#7c3aed;">ENGINE</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------
# Main Application
# -----------------------------
def main():
    st.set_page_config(page_title="Pixel11 Engine", layout="wide")
    _inject_css()
    _navbar()

    if "started" not in st.session_state:
        st.session_state.started = False
    if "results" not in st.session_state:
        st.session_state.results = None
        st.session_state.df = pd.DataFrame()

    st.markdown('<div style="padding: 5rem 0 2rem 0;">', unsafe_allow_html=True)
    st.markdown('<div class="px-title">Discovery Reimagined.</div>', unsafe_allow_html=True)
    st.markdown(
        '<div style="text-align:center; opacity:0.5; letter-spacing:3px; font-weight:600;">GOOGLE PLACES • HYBRID LEADERSHIP MINING</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if not st.session_state.started:
        c1, c2, c3 = st.columns([1, 1.8, 1])
        with c2:
            if st.button("⚡ INITIALIZE ENGINE", use_container_width=True):
                st.session_state.started = True
                st.rerun()
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### 🛠️ Extraction Protocol")

        c1, c2, c3 = st.columns(3)
        with c1:
            loc = st.text_input("📍 Location", value="Pune")
        with c2:
            area = st.text_input("🏢 Area (Optional)")
        with c3:
            q = st.text_input("🔍 Business Niche", value="Software Companies")

        with st.expander("⚙️ Advanced Mining Settings"):
            a1, a2, a3 = st.columns(3)
            with a1:
                # ✅ cap is now config-driven (300 if you set TOP_N_CAP=300)
                limit = st.number_input("Max Results", 1, int(UI_TOP_N_CAP), 20)
            with a2:
                case2_on = st.toggle("Enable Case-2 Leadership (Hybrid)", value=True)
            with a3:
                lead_count = st.number_input("Leaders / Org", 1, 5, 5)

        if st.button("🚀 EXECUTE PIPELINE", use_container_width=True):
            if not loc or not q:
                st.error("Input Required: Define Location and Query.")
            else:
                status_placeholder = st.empty()
                with status_placeholder:
                    st.markdown(f'<div class="card {"ai-active" if case2_on else ""}">', unsafe_allow_html=True)
                    st.markdown("#### ⚙️ Processing Sequence")
                    st.markdown(
                        '<div class="logline"><div class="dot"></div>Initializing Mining Sequence...</div>',
                        unsafe_allow_html=True,
                    )
                    st.markdown("</div>", unsafe_allow_html=True)

                try:
                    with st.spinner("Mining Deep Intelligence..."):
                        res = run_case1_pipeline(
                            query=q,
                            location=loc,
                            place=area,
                            top_n=int(limit),
                            debug=True,
                            case2_enabled=bool(case2_on),
                            case2_max_leaders=int(lead_count),
                        )

                        st.session_state.results = res
                        st.session_state.df = _ensure_role_cols(pd.DataFrame(res.get("cleaned_rows", [])))
                        status_placeholder.empty()
                        st.balloons()
                except Exception as e:
                    status_placeholder.empty()
                    st.error(f"❌ Pipeline Failed: {str(e)}")

        st.markdown("</div>", unsafe_allow_html=True)

    if st.session_state.results:
        res = st.session_state.results
        stats = res.get("stats", {}) or {}

        main_col, side_col = st.columns([3.2, 1])
        with main_col:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.dataframe(st.session_state.df, use_container_width=True, height=550)
            st.markdown("</div>", unsafe_allow_html=True)

        with side_col:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.metric("Total Found", stats.get("clean_count", 0))

            # ✅ backend now returns both: with_leadership + with_leaders (safe)
            leaders_val = stats.get("with_leadership", stats.get("with_leaders", 0))
            st.metric("With Leaders", leaders_val)

            st.markdown("</div>", unsafe_allow_html=True)

            if res.get("excel_bytes"):
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.download_button(
                    label="📥 DOWNLOAD REPORT (.XLSX)",
                    data=res["excel_bytes"],
                    file_name="mining_report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
