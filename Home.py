"""
ETL Automator — Landing Page
Built by Srinivas Punugu

Shows both tools:
1. Informatica → GCP Migrator (full 8-tab app)
2. Universal ETL Converter (any source → any target)
"""

import streamlit as st

st.set_page_config(
    page_title="ETL Automator — by Srinivas Punugu",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
  .stApp { background: #0a0f1e; color: #e2e8f0; }

  .hero {
    background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
    border-bottom: 1px solid #6366f133;
    padding: 60px 40px 50px;
    text-align: center;
    margin: -1rem -1rem 2rem -1rem;
  }
  .hero-title {
    font-size: 48px; font-weight: 800; color: #fff;
    letter-spacing: -1px; margin-bottom: 12px;
  }
  .hero-title span { color: #818cf8; }
  .hero-subtitle {
    font-size: 18px; color: #94a3b8; max-width: 600px;
    margin: 0 auto 8px; line-height: 1.6;
  }
  .hero-author {
    font-size: 13px; color: #6366f1; font-weight: 600; margin-top: 8px;
  }
  .hero-badges {
    display: flex; justify-content: center; gap: 12px;
    flex-wrap: wrap; margin-top: 20px;
  }
  .badge {
    background: #1e293b; border: 1px solid #334155;
    color: #94a3b8; padding: 4px 14px; border-radius: 20px; font-size: 12px;
  }

  .tool-card {
    background: #0f172a;
    border: 1.5px solid #1e293b;
    border-radius: 16px;
    padding: 32px;
    transition: transform 0.2s, box-shadow 0.2s, border-color 0.2s;
    height: 100%;
    cursor: pointer;
  }
  .tool-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 40px rgba(0,0,0,0.5);
  }
  .tool-card-1 { border-top: 4px solid #6366f1; }
  .tool-card-1:hover { border-color: #6366f1; box-shadow: 0 12px 40px rgba(99,102,241,0.2); }
  .tool-card-2 { border-top: 4px solid #22c55e; }
  .tool-card-2:hover { border-color: #22c55e; box-shadow: 0 12px 40px rgba(34,197,94,0.2); }

  .tool-icon { font-size: 48px; margin-bottom: 16px; }
  .tool-title { font-size: 22px; font-weight: 700; color: #f1f5f9; margin-bottom: 8px; }
  .tool-subtitle { font-size: 13px; color: #64748b; margin-bottom: 20px; }
  .tool-desc { font-size: 14px; color: #94a3b8; line-height: 1.7; margin-bottom: 20px; }

  .feature-list { list-style: none; padding: 0; margin: 0 0 24px 0; }
  .feature-list li {
    font-size: 13px; color: #94a3b8; padding: 4px 0;
    border-bottom: 1px solid #1e293b;
  }
  .feature-list li:last-child { border-bottom: none; }

  .tag {
    display: inline-block; padding: 2px 10px; border-radius: 10px;
    font-size: 11px; font-weight: 600; margin: 2px;
  }
  .tag-purple { background: #6366f122; color: #818cf8; border: 1px solid #6366f133; }
  .tag-green  { background: #22c55e22; color: #86efac; border: 1px solid #22c55e33; }
  .tag-blue   { background: #3b82f622; color: #93c5fd; border: 1px solid #3b82f633; }
  .tag-orange { background: #f97316; color: white; font-size: 10px; padding: 2px 8px; }

  .stats-bar {
    display: flex; justify-content: center; gap: 48px;
    flex-wrap: wrap; padding: 24px 0;
    border-top: 1px solid #1e293b;
    border-bottom: 1px solid #1e293b;
    margin: 8px 0 32px;
  }
  .stat { text-align: center; }
  .stat-num { font-size: 28px; font-weight: 700; color: #6366f1; }
  .stat-label { font-size: 11px; color: #64748b; margin-top: 2px; }

  .stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #6366f1, #4f46e5) !important;
    border: none !important; color: white !important;
    font-weight: 700 !important; border-radius: 10px !important;
    font-size: 15px !important; padding: 12px 24px !important;
  }
  .stButton > button {
    border-radius: 10px !important; border: 1px solid #334155 !important;
    background: #1e293b !important; color: #e2e8f0 !important;
    font-weight: 600 !important;
  }
  hr { border-color: #1e293b !important; }
  [data-testid="stSidebar"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="hero-title">⚡ ETL <span>Automator</span></div>
    <div class="hero-subtitle">
        Enterprise-grade ETL migration platform — from legacy systems
        to modern cloud data warehouses
    </div>
    <div class="hero-author">Built by Srinivas Punugu</div>
    <div class="hero-badges">
        <span class="badge">🔴 Informatica PowerCenter</span>
        <span class="badge">🔷 SSIS</span>
        <span class="badge">🟢 Talend</span>
        <span class="badge">🔵 DataStage</span>
        <span class="badge">→</span>
        <span class="badge">☁️ BigQuery</span>
        <span class="badge">❄️ Snowflake</span>
        <span class="badge">🔷 Synapse</span>
        <span class="badge">🔥 Databricks</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Stats ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="stats-bar">
    <div class="stat"><div class="stat-num">8</div><div class="stat-label">ETL Tools Supported</div></div>
    <div class="stat"><div class="stat-num">10</div><div class="stat-label">Source Databases</div></div>
    <div class="stat"><div class="stat-num">6</div><div class="stat-label">Cloud Targets</div></div>
    <div class="stat"><div class="stat-num">8</div><div class="stat-label">App Modules</div></div>
    <div class="stat"><div class="stat-num">3</div><div class="stat-label">AI Providers</div></div>
    <div class="stat"><div class="stat-num">100%</div><div class="stat-label">Open & Free</div></div>
</div>
""", unsafe_allow_html=True)

# ── Tool Cards ────────────────────────────────────────────────────────────────
st.markdown("## 🛠️ Choose Your Tool")
st.markdown("<p style='color:#64748b;margin-bottom:24px;'>Select the migration tool that fits your use case</p>",
            unsafe_allow_html=True)

col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown("""
    <div class="tool-card tool-card-1">
        <div class="tool-icon">🔴⚡</div>
        <div class="tool-title">Informatica → GCP Migrator</div>
        <div class="tool-subtitle">Full end-to-end Informatica PowerCenter migration</div>
        <div class="tool-desc">
            Purpose-built for migrating Informatica PowerCenter ETL workflows
            to Google Cloud Platform (BigQuery + Cloud Composer/Airflow).
            Reduces weeks of manual migration to hours.
        </div>
        <ul class="feature-list">
            <li>🗂️ Repository Explorer — browse folders, workflows, export XML</li>
            <li>🔗 Lineage Analysis — visual data flow with session run stats</li>
            <li>🧬 Schema Analyzer — Teradata → BigQuery type mapping + DDL</li>
            <li>⚡ SQL Converter — auto-generate BigQuery SQL + Airflow DAG</li>
            <li>✅ Validation — row count, logic, schema diff checks</li>
            <li>🤖 AI Assistant — Claude / ChatGPT / Gemini chat support</li>
            <li>🚀 CI/CD Pipeline — MR → Jenkins → GCS → Git → Merge</li>
            <li>⚙️ Configuration — all connection settings in one place</li>
        </ul>
        <div>
            <span class="tag tag-purple">Informatica XML</span>
            <span class="tag tag-purple">BigQuery SQL</span>
            <span class="tag tag-purple">Airflow DAG</span>
            <span class="tag tag-purple">Jenkins CI/CD</span>
            <span class="tag tag-orange">🎭 Demo Ready</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🚀 Open Informatica → GCP Migrator",
                 type="primary", use_container_width=True,
                 key="btn_infa"):
        st.page_link("app.py", label="Open ETL Migrator")
        st.stop()

with col2:
    st.markdown("""
    <div class="tool-card tool-card-2">
        <div class="tool-icon">🌐✨</div>
        <div class="tool-title">Universal ETL Converter</div>
        <div class="tool-subtitle">Any ETL tool → Any cloud platform</div>
        <div class="tool-desc">
            Convert ETL jobs from any source tool and any source database
            to any modern cloud data platform. Handles SQL dialect conversion,
            type mapping, and orchestration code generation automatically.
        </div>
        <ul class="feature-list">
            <li>🔴 Informatica, 🔷 SSIS, 🟢 Talend, 🔵 DataStage, 🟠 Ab Initio</li>
            <li>🗄️ Teradata, SQL Server, Oracle, MySQL, PostgreSQL, DB2, SAP HANA</li>
            <li>☁️ BigQuery + Airflow — Google Cloud</li>
            <li>❄️ Snowflake + dbt — Multi-cloud</li>
            <li>🔷 Azure Synapse + ADF — Microsoft Azure</li>
            <li>🟠 Redshift + Glue — AWS</li>
            <li>🔥 Databricks + Spark — Multi-cloud</li>
            <li>🌊 Delta Lake + Prefect — Multi-cloud</li>
        </ul>
        <div>
            <span class="tag tag-green">Any Source</span>
            <span class="tag tag-green">Any Target</span>
            <span class="tag tag-blue">SQL Conversion</span>
            <span class="tag tag-blue">Type Mapping</span>
            <span class="tag tag-orange">NEW ✨</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🌐 Open Universal ETL Converter",
                 use_container_width=True,
                 key="btn_universal"):
        st.page_link("pages/All_Migrations.py", label="Open All Migrations")
        st.stop()

st.markdown("---")

# ── How it works ──────────────────────────────────────────────────────────────
st.markdown("## 🔄 How It Works")

steps = [
    ("1", "🔌", "Connect", "Connect to your source ETL tool or paste your SQL/XML"),
    ("2", "🔍", "Analyze", "App parses metadata, lineage, schema, and transformation logic"),
    ("3", "⚡", "Convert", "Auto-generates target SQL + orchestration code"),
    ("4", "✅", "Validate", "Row count checks, schema diff, logic verification"),
    ("5", "🚀", "Deploy", "CI/CD pipeline: MR → Jenkins → GCS → Merge to main"),
]

step_cols = st.columns(5)
for col, (num, icon, title, desc) in zip(step_cols, steps):
    with col:
        st.markdown(f"""
        <div style="text-align:center;padding:16px 8px;">
            <div style="background:#6366f1;color:white;width:32px;height:32px;
                        border-radius:50%;display:flex;align-items:center;justify-content:center;
                        font-weight:700;font-size:14px;margin:0 auto 12px;">{num}</div>
            <div style="font-size:24px;">{icon}</div>
            <div style="font-size:13px;font-weight:700;color:#f1f5f9;margin-top:8px;">{title}</div>
            <div style="font-size:11px;color:#64748b;margin-top:4px;line-height:1.5;">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("---")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;padding:24px 0;color:#475569;font-size:12px;">
    <div style="font-size:14px;font-weight:600;color:#6366f1;margin-bottom:8px;">
        ⚡ ETL Automator v1.0
    </div>
    Built by <strong style="color:#818cf8;">Srinivas Punugu</strong> &nbsp;|&nbsp;
    Informatica PowerCenter → BigQuery + Airflow &nbsp;|&nbsp;
    Universal ETL Migration Platform<br><br>
    <span style="font-size:11px;">
        🔗 GitHub: github.com/spunugu/informatica &nbsp;|&nbsp;
        🌐 Live: informatica-x9bmxhy9dwvrh7ajixgqfe.streamlit.app
    </span>
</div>
""", unsafe_allow_html=True)
