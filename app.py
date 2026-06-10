"""
ETL Automator — Informatica to BigQuery + Airflow Migration Platform
Built by Srinivas Punugu
"""

import streamlit as st
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

st.set_page_config(
    page_title="ETL Automator — by Srinivas Punugu",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
  .stApp { background: #0a0f1e; color: #e2e8f0; }

  .app-header {
    background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
    border-bottom: 1px solid #6366f133;
    padding: 20px 32px 16px;
    margin: -1rem -1rem 1.5rem -1rem;
  }
  .app-header .title { font-size: 30px; font-weight: 700; color: #fff; letter-spacing: -0.5px; }
  .app-header .title span { color: #818cf8; }
  .app-header .subtitle { font-size: 13px; color: #94a3b8; margin-top: 4px; }
  .app-header .author { font-size: 12px; color: #6366f1; margin-top: 2px; font-weight: 600; }

  .tab-header { border-left: 3px solid #6366f1; padding-left: 16px; margin-bottom: 24px; }
  .tab-header h2 { margin: 0 0 4px 0; font-size: 22px; color: #f1f5f9; }
  .tab-header p  { margin: 0; color: #94a3b8; font-size: 14px; }

  .stTabs [data-baseweb="tab-list"] { gap: 4px; background: #0f172a; padding: 8px 8px 0; border-bottom: 1px solid #1e293b; }
  .stTabs [data-baseweb="tab"] { background: transparent; border: 1px solid #1e293b; border-bottom: none; border-radius: 8px 8px 0 0; color: #94a3b8; font-size: 13px; font-weight: 500; padding: 8px 18px; }
  .stTabs [aria-selected="true"] { background: #1e293b !important; color: #f1f5f9 !important; border-color: #334155 !important; }
  .stTabs [data-baseweb="tab-panel"] { background: #0f172a; border: 1px solid #1e293b; border-top: none; border-radius: 0 8px 8px 8px; padding: 24px; }

  .stButton > button[kind="primary"] { background: linear-gradient(135deg, #6366f1, #4f46e5) !important; border: none !important; color: white !important; font-weight: 600 !important; border-radius: 8px !important; }
  .stButton > button[kind="primary"]:hover { box-shadow: 0 4px 20px rgba(99,102,241,0.4) !important; }
  .stButton > button { border-radius: 8px !important; border: 1px solid #334155 !important; background: #1e293b !important; color: #e2e8f0 !important; }

  [data-testid="metric-container"] { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px !important; }
  [data-testid="stMetricLabel"] { color: #94a3b8 !important; font-size: 12px !important; }
  [data-testid="stMetricValue"] { color: #f1f5f9 !important; font-size: 24px !important; font-weight: 700 !important; }

  .stTextInput > div > div > input, .stSelectbox > div > div { background: #1e293b !important; border: 1px solid #334155 !important; border-radius: 8px !important; color: #e2e8f0 !important; }
  .streamlit-expanderHeader { background: #1e293b !important; border-radius: 8px !important; color: #e2e8f0 !important; }
  code { font-family: 'JetBrains Mono', monospace !important; }
  [data-testid="stSidebar"] { background: #0f172a !important; border-right: 1px solid #1e293b; }
  .stDownloadButton > button { background: linear-gradient(135deg, #1d4ed8, #1e40af) !important; border: none !important; color: white !important; font-weight: 600 !important; border-radius: 8px !important; }
  hr { border-color: #1e293b !important; }
</style>
""", unsafe_allow_html=True)

# ── Session State Defaults ─────────────────────────────────────────────────────
for k, v in {
    "connected": False, "folders": [], "workflows": [],
    "selected_folder": None, "selected_workflow": None,
    "wf_merged_xml": None, "wf_parsed": None, "wf_session_io": {},
    "curr_sessions": [], "curr_worklets": [], "curr_mappings": [],
    "curr_sources": [], "curr_targets": [], "curr_lookups": [],
    "row_count": 0, "complexity": "Low",
    "generated_sqls": None, "generated_dag": None,
    "dataset_map": {}, "bq_project": "your-gcp-project",
    "logic_results": None, "rowcount_results": None,
    "schema_results": None, "validation_steps": set(),
    "demo_mode": True,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:16px 0 24px;">
        <div style="font-size:36px;">⚡</div>
        <div style="font-weight:700;font-size:16px;color:#f1f5f9;">ETL Automator</div>
        <div style="font-size:11px;color:#6366f1;font-weight:600;">by Srinivas Punugu</div>
        <div style="font-size:10px;color:#475569;margin-top:2px;">Informatica → BigQuery + Airflow</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Navigation to other apps ──────────────────────────────────────────────
    st.markdown("#### 🌐 Navigation")
    if st.button("🏠 Home / Landing Page", use_container_width=True, key="nav_home"):
        st.page_link("Home.py", label="Home")
        st.stop()
    if st.button("🌐 All Migrations", use_container_width=True, key="nav_all"):
        st.page_link("pages/All_Migrations.py", label="All Migrations")
        st.stop()

    st.markdown("---")
    st.markdown("#### ⚙️ Settings")
    demo_mode = st.toggle("Demo Mode", value=st.session_state.get("demo_mode", True),
                          help="Use sample data — no Informatica needed", key="sidebar_demo")
    st.session_state.demo_mode = demo_mode
    os.environ["DEMO_MODE"] = "true" if demo_mode else "false"
    if demo_mode:
        st.info("🎭 Running with sample data", icon="ℹ️")
    else:
        st.warning("⚠️ Real mode: needs Informatica + GCP", icon="⚠️")

    st.markdown("---")
    st.markdown("#### 📍 Progress")
    for label, done in [
        ("1. Repository Export",  bool(st.session_state.get("wf_merged_xml"))),
        ("2. Lineage Analyzed",   bool(st.session_state.get("curr_sessions"))),
        ("3. Schema Mapped",      bool(st.session_state.get("dataset_map"))),
        ("4. SQL + DAG Built",    bool(st.session_state.get("generated_sqls"))),
        ("5. Validation Done",    len(st.session_state.get("validation_steps", set())) >= 3),
    ]:
        st.markdown(f"{'✅' if done else '⬜'} {label}")

    if st.session_state.get("selected_workflow"):
        st.markdown("---")
        st.markdown("#### 📋 Active Workflow")
        st.code(st.session_state.selected_workflow, language=None)
        badge = st.session_state.get("complexity", "Low")
        st.markdown(f"Complexity: {'🟢' if badge=='Low' else '🟡' if badge=='Medium' else '🟠' if badge=='High' else '🔴'} **{badge}**")
        rc = st.session_state.get("row_count", 0)
        if rc:
            st.markdown(f"Last Run: `{rc:,}` rows")

    st.markdown("---")
    if st.button("🔄 Reset All", use_container_width=True, key="reset_all"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    st.markdown("""
    <div style="text-align:center;padding-top:24px;color:#475569;font-size:11px;">
        ETL Automator v1.0<br>
        Built by <strong style="color:#6366f1;">Srinivas Punugu</strong>
    </div>
    """, unsafe_allow_html=True)

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
    <div class="title">⚡ ETL <span>Automator</span></div>
    <div class="subtitle">Informatica PowerCenter → Google BigQuery + Apache Airflow</div>
    <div class="author">Built by Srinivas Punugu</div>
</div>
""", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────────
from pages import (
    tab0_config, tab1_repository, tab2_lineage, tab3_schema,
    tab4_converter, tab5_validation, tab6_ai_assistant, tab7_cicd
)

t0, t1, t2, t3, t4, t5, t6, t7 = st.tabs([
    "⚙️ 0. Configuration",
    "🗂️ 1. Repository Explorer",
    "🔗 2. Lineage Analysis",
    "🧬 3. Schema Analyzer",
    "⚡ 4. SQL Converter",
    "✅ 5. Validation",
    "🤖 6. AI Assistant",
    "🚀 7. CI/CD Pipeline",
])

with t0: tab0_config.render()
with t1: tab1_repository.render()
with t2: tab2_lineage.render()
with t3: tab3_schema.render()
with t4: tab4_converter.render()
with t5: tab5_validation.render()
with t6: tab6_ai_assistant.render()
with t7: tab7_cicd.render()
