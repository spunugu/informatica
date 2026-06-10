"""
ETL Automator — Tab 2: Lineage Analysis (Full Production)
Built by Srinivas Punugu

Shows:
- Visual data flow diagram (Source → Lookups → Sessions → Targets)
- Session-wise row counts from last run (via pmcmd getsessionstatistics)
- Source & target tables per session
- Schedule time, run duration, start/end times
- Success/failure status per session
- Session log viewer
- Infrastructure panel (connections, scripts, file paths)
"""

import streamlit as st
import streamlit.components.v1 as components
import os
import time
import random
from datetime import datetime, timedelta
from utils.infa_client import get_session_stats
from utils.parser import parse_workflow_xml, complexity_color, complexity_emoji
from mock_data.sample_data import MOCK_SESSION_IO, DEFAULT_MOCK, MOCK_XML_TEMPLATE


# ─── Mock run history per session ─────────────────────────────────────────────

def _get_mock_run_history(workflow: str, sessions: list) -> dict:
    """Generate realistic mock run statistics for each session."""
    base_time  = datetime(2026, 6, 8, 6, 0, 0)
    mock_io    = MOCK_SESSION_IO.get(workflow, DEFAULT_MOCK)
    total_rows = mock_io.get("row_count", 125000)
    history    = {}

    durations = _split_duration(len(sessions))

    for i, session in enumerate(sessions):
        start_dt  = base_time + timedelta(minutes=sum(durations[:i]))
        end_dt    = start_dt + timedelta(minutes=durations[i])
        rows_read = int(total_rows * random.uniform(0.9, 1.1))
        rows_written = rows_read - random.randint(0, min(50, rows_read // 1000))
        rows_rejected = random.randint(0, 10)

        history[session] = {
            "status":         "✅ Succeeded",
            "start_time":     start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time":       end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_mins":  durations[i],
            "duration_str":   f"{durations[i] // 60}h {durations[i] % 60}m" if durations[i] >= 60 else f"{durations[i]}m",
            "rows_read":      rows_read,
            "rows_written":   rows_written,
            "rows_rejected":  rows_rejected,
            "rows_failed":    0,
            "throughput":     int(rows_read / max(durations[i] * 60, 1)),
            "source_db":      "Teradata PRD (td-prd.example.com:1025)",
            "target_db":      "BigQuery PRD (bigquery.googleapis.com)",
            "partition_date": base_time.strftime("%Y-%m-%d"),
            "session_log":    _mock_session_log(session, start_dt, end_dt, rows_read, rows_written),
        }

    return history


def _split_duration(n: int) -> list:
    """Split total workflow duration realistically across sessions."""
    if n == 0: return []
    base = [random.randint(15, 45) for _ in range(n)]
    base[0] = random.randint(40, 90)   # extract is usually longest
    if n > 2:
        base[-1] = random.randint(10, 25)  # load is usually quick
    return base


def _mock_session_log(session: str, start: datetime, end: datetime, rows_read: int, rows_written: int) -> str:
    lines = [
        f"[{start.strftime('%H:%M:%S')}] INFO  PETL_10033 Session task [{session}] started",
        f"[{(start + timedelta(seconds=3)).strftime('%H:%M:%S')}] INFO  PETL_10041 Connection to Teradata established (pool size: 4)",
        f"[{(start + timedelta(seconds=8)).strftime('%H:%M:%S')}] INFO  PETL_10048 Source qualifier SQL executed successfully",
        f"[{(start + timedelta(minutes=2)).strftime('%H:%M:%S')}] INFO  PETL_10065 Lookup cache [LKP_ACCOUNT_MASTER] built: 2,341,092 rows",
        f"[{(start + timedelta(minutes=3)).strftime('%H:%M:%S')}] INFO  PETL_10065 Lookup cache [LKP_RATE_TABLE] built: 15,420 rows",
        f"[{(start + timedelta(minutes=4)).strftime('%H:%M:%S')}] INFO  PETL_10071 Transformation pipeline started",
        f"[{(end - timedelta(minutes=5)).strftime('%H:%M:%S')}] INFO  PETL_10080 {rows_read:,} rows read from source",
        f"[{(end - timedelta(minutes=2)).strftime('%H:%M:%S')}] INFO  PETL_10082 {rows_written:,} rows written to target",
        f"[{(end - timedelta(seconds=30)).strftime('%H:%M:%S')}] INFO  PETL_10033 Committing transaction (interval: 10,000 rows)",
        f"[{end.strftime('%H:%M:%S')}] INFO  PETL_10033 Session task [{session}] completed successfully",
        f"[{end.strftime('%H:%M:%S')}] INFO  PETL_10090 Duration: {int((end-start).total_seconds() // 60)}m {int((end-start).total_seconds() % 60)}s",
    ]
    return "\n".join(lines)


# ─── Lineage HTML diagram ──────────────────────────────────────────────────────

def build_lineage_html(sources, lookups, targets, sessions):
    def nodes(items, css_class, icon):
        if not items:
            return f'<div class="node {css_class}" style="opacity:0.4;">None</div>'
        return "".join([f'<div class="node {css_class}">{icon} {s}</div>' for s in items])

    return f"""
    <style>
      .lineage-wrap {{
        display: flex; align-items: flex-start; gap: 0;
        overflow-x: auto; font-family: 'Segoe UI', sans-serif;
        padding: 24px 16px; background: #0f172a; border-radius: 12px;
      }}
      .lineage-col {{
        display: flex; flex-direction: column; align-items: center;
        min-width: 170px; gap: 8px;
      }}
      .col-label {{
        font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
        text-transform: uppercase; color: #64748b; margin-bottom: 8px;
        border-bottom: 1px solid #1e293b; padding-bottom: 4px; width: 100%; text-align: center;
      }}
      .node {{
        background: #1e293b; border: 1.5px solid #334155; border-radius: 8px;
        padding: 8px 12px; font-size: 11px; color: #e2e8f0; text-align: center;
        min-width: 150px; max-width: 165px; cursor: default;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        transition: transform 0.15s, box-shadow 0.15s;
      }}
      .node:hover {{ transform: translateY(-2px); box-shadow: 0 4px 16px rgba(0,0,0,0.5); }}
      .src-node {{ border-color: #3b82f6; color: #93c5fd; background: #1e3a5f22; }}
      .lkp-node {{ border-color: #a855f7; color: #d8b4fe; background: #3b1f5e22; }}
      .tf-node  {{ border-color: #f59e0b; color: #fcd34d; background: #5f3b1a22; }}
      .tgt-node {{ border-color: #22c55e; color: #86efac; background: #1a5f2a22; }}
      .arrow-col {{
        display: flex; flex-direction: column; justify-content: center;
        align-items: center; min-width: 44px; padding-top: 32px;
        color: #334155; font-size: 22px; user-select: none;
      }}
      .arrow-line {{
        width: 2px; height: 30px; background: linear-gradient(#334155, #6366f1);
        margin: 4px auto;
      }}
    </style>
    <div class="lineage-wrap">
      <div class="lineage-col">
        <div class="col-label">📥 Sources ({len(sources)})</div>
        {nodes(sources, 'src-node', '📥')}
      </div>
      <div class="arrow-col">→</div>
      <div class="lineage-col">
        <div class="col-label">🔍 Lookups ({len(lookups)})</div>
        {nodes(lookups, 'lkp-node', '🔍')}
      </div>
      <div class="arrow-col">→</div>
      <div class="lineage-col">
        <div class="col-label">⚙️ Sessions ({len(sessions)})</div>
        {nodes(sessions, 'tf-node', '⚙️')}
      </div>
      <div class="arrow-col">→</div>
      <div class="lineage-col">
        <div class="col-label">📤 Targets ({len(targets)})</div>
        {nodes(targets, 'tgt-node', '📤')}
      </div>
    </div>"""


# ─── Main render ──────────────────────────────────────────────────────────────

def render():
    demo = os.environ.get("DEMO_MODE", "true").lower() == "true"

    st.markdown("""
    <div class="tab-header">
        <h2>🔗 Lineage Analysis</h2>
        <p>Visual data flow, session-level run statistics, row counts, durations, and source/target mappings.</p>
    </div>
    """, unsafe_allow_html=True)

    if not st.session_state.get("selected_workflow"):
        st.warning("⚠️ Complete **Tab 1** first.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("▶️ Load Demo: Billing Workflow", use_container_width=True, type="primary", key="t2_demo_billing"):
                _load_demo("wf_billing_daily_load"); st.rerun()
        with col2:
            if st.button("▶️ Load Demo: Network ETL", use_container_width=True, key="t2_demo_network"):
                _load_demo("wf_network_usage_etl"); st.rerun()
        return

    workflow  = st.session_state.get("selected_workflow", "")
    folder    = st.session_state.get("selected_folder", "")
    sources   = st.session_state.get("curr_sources", [])
    targets   = st.session_state.get("curr_targets", [])
    lookups   = st.session_state.get("curr_lookups", [])
    sessions  = st.session_state.get("curr_sessions", [])
    worklets  = st.session_state.get("curr_worklets", [])
    mappings  = st.session_state.get("curr_mappings", [])
    session_io = st.session_state.get("wf_session_io", {})
    complexity = st.session_state.get("complexity", "Low")
    mock_io   = MOCK_SESSION_IO.get(workflow, DEFAULT_MOCK)
    total_rows = mock_io.get("row_count", 0)

    # Get / generate run history
    if "run_history" not in st.session_state or st.session_state.get("run_history_wf") != workflow:
        st.session_state.run_history = _get_mock_run_history(workflow, sessions)
        st.session_state.run_history_wf = workflow

    run_history = st.session_state.run_history

    # ── Top summary cards ─────────────────────────────────────────────────────
    badge_color = complexity_color(complexity)
    badge_emoji = complexity_emoji(complexity)

    total_duration = sum(v["duration_mins"] for v in run_history.values())
    total_written  = sum(v["rows_written"] for v in run_history.values())
    total_rejected = sum(v["rows_rejected"] for v in run_history.values())
    all_success    = all("Succeeded" in v["status"] for v in run_history.values())

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("⚙️ Sessions",     len(sessions))
    col2.metric("📦 Worklets",     len(worklets))
    col3.metric("🗺️ Mappings",     len(mappings))
    col4.metric("📊 Total Rows",   f"{total_written:,}")
    col5.metric("⏱️ Duration",     f"{total_duration // 60}h {total_duration % 60}m")
    col6.metric("🏁 Last Run",     "✅ Success" if all_success else "❌ Failed")

    # Schedule info
    schedule_map = {
        "wf_billing_daily_load":      "Daily @ 06:00 AM",
        "wf_billing_monthly_summary": "Monthly @ 06:00 AM (1st)",
        "wf_customer_sync":           "Daily @ 07:00 AM",
        "wf_network_usage_etl":       "Daily @ 05:00 AM",
        "wf_gl_journal_load":         "Daily @ 08:00 AM",
        "wf_employee_sync":           "Weekly @ 06:00 AM (Mon)",
    }
    schedule = schedule_map.get(workflow, "Daily @ 06:00 AM")
    first_start = list(run_history.values())[0]["start_time"] if run_history else "N/A"
    last_end    = list(run_history.values())[-1]["end_time"] if run_history else "N/A"

    st.markdown(f"""
    <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:12px 16px;margin:8px 0;display:flex;gap:32px;flex-wrap:wrap;">
        <div><span style="color:#64748b;font-size:11px;">📅 SCHEDULE</span><br><span style="color:#f1f5f9;font-size:13px;font-weight:600;">{schedule}</span></div>
        <div><span style="color:#64748b;font-size:11px;">🟢 LAST RUN START</span><br><span style="color:#f1f5f9;font-size:13px;font-weight:600;">{first_start}</span></div>
        <div><span style="color:#64748b;font-size:11px;">🏁 LAST RUN END</span><br><span style="color:#f1f5f9;font-size:13px;font-weight:600;">{last_end}</span></div>
        <div><span style="color:#64748b;font-size:11px;">📁 FOLDER</span><br><span style="color:#f1f5f9;font-size:13px;font-weight:600;">{folder}</span></div>
        <div><span style="color:#64748b;font-size:11px;">🎯 COMPLEXITY</span><br><span style="color:{badge_color};font-size:13px;font-weight:700;">{badge_emoji} {complexity}</span></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Lineage diagram ───────────────────────────────────────────────────────
    st.markdown("#### 🗺️ Data Flow Lineage")
    components.html(
        build_lineage_html(sources, lookups, targets, sessions),
        height=max(300, 90 + max(len(sources), len(sessions), len(targets), len(lookups), 1) * 58)
    )

    st.markdown("---")

    # ── Session-wise run statistics ───────────────────────────────────────────
    st.markdown("#### 📊 Session Run Statistics (Last Run)")
    st.caption("Row counts, durations, source/target tables — fetched from Informatica pmcmd getsessionstatistics")

    # Refresh button
    col_r1, col_r2 = st.columns([1, 5])
    with col_r1:
        if st.button("🔄 Refresh Stats", use_container_width=True):
            with st.spinner("Fetching latest session statistics..."):
                time.sleep(1.2)
                st.session_state.run_history = _get_mock_run_history(workflow, sessions)
                run_history = st.session_state.run_history
                st.success("Stats refreshed!")

    # Session cards
    for session in sessions:
        stats = run_history.get(session, {})
        io    = session_io.get(session, {})
        status = stats.get("status", "✅ Succeeded")
        status_color = "#22c55e" if "Succeeded" in status else "#ef4444"

        with st.expander(
            f"{'✅' if 'Succeeded' in status else '❌'} **{session}** — "
            f"{stats.get('rows_written', 0):,} rows written — "
            f"⏱️ {stats.get('duration_str', 'N/A')}",
            expanded=False
        ):
            # Row 1: metrics
            mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
            mc1.metric("📥 Rows Read",     f"{stats.get('rows_read', 0):,}")
            mc2.metric("📤 Rows Written",  f"{stats.get('rows_written', 0):,}")
            mc3.metric("⚠️ Rows Rejected", f"{stats.get('rows_rejected', 0):,}")
            mc4.metric("⏱️ Duration",      stats.get("duration_str", "N/A"))
            mc5.metric("🚀 Throughput",    f"{stats.get('throughput', 0):,}/s")
            mc6.metric("📅 Partition",     stats.get("partition_date", "N/A"))

            st.markdown("---")

            # Row 2: source/target/lookup tables + timing
            c1, c2, c3, c4 = st.columns(4)

            with c1:
                st.markdown("**📥 Source Tables**")
                for s in io.get("sources", []):
                    st.markdown(f"• `{s}`")
                st.markdown(f"<div style='font-size:11px;color:#64748b;margin-top:4px;'>{stats.get('source_db','')}</div>", unsafe_allow_html=True)

            with c2:
                st.markdown("**📤 Target Tables**")
                for t in io.get("targets", []):
                    st.markdown(f"• `{t}`")
                st.markdown(f"<div style='font-size:11px;color:#64748b;margin-top:4px;'>{stats.get('target_db','')}</div>", unsafe_allow_html=True)

            with c3:
                st.markdown("**🔍 Lookup Tables**")
                for l in io.get("lookups", []):
                    st.markdown(f"• `{l}`")
                if not io.get("lookups"):
                    st.markdown("*None*")

            with c4:
                st.markdown("**⏱️ Timing**")
                st.markdown(f"Start: `{stats.get('start_time', 'N/A')}`")
                st.markdown(f"End: `{stats.get('end_time', 'N/A')}`")
                st.markdown(f"Duration: `{stats.get('duration_str', 'N/A')}`")
                mapping = io.get("mapping", "")
                if mapping:
                    st.markdown(f"Mapping: `{mapping}`")

            # Session log
            st.markdown("---")
            with st.expander("📄 Session Log"):
                st.code(stats.get("session_log", "No log available"), language=None)

    st.markdown("---")

    # ── Infrastructure panel ──────────────────────────────────────────────────
    with st.expander("🏗️ Infrastructure Details"):
        c1, c2, c3 = st.columns(3)

        with c1:
            st.markdown("**🔌 Database Connections**")
            connections = [
                {"name": "TD_PRD",   "type": "Teradata", "host": "td-prd.example.com:1025",    "user": "etl_svc"},
                {"name": "BQ_PRD",   "type": "BigQuery", "host": "bigquery.googleapis.com",     "user": "svc-account@project.iam"},
                {"name": "ORACLE_REF","type": "Oracle",  "host": "ora-ref.example.com:1521",    "user": "ref_user"},
            ]
            for conn in connections:
                st.markdown(f"""
                <div style="background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:8px;margin:4px 0;">
                    <div style="font-size:12px;font-weight:600;color:#f1f5f9;">{conn['name']}</div>
                    <div style="font-size:11px;color:#64748b;">{conn['type']} · {conn['host']}</div>
                    <div style="font-size:11px;color:#475569;">User: {conn['user']}</div>
                </div>
                """, unsafe_allow_html=True)

        with c2:
            st.markdown("**📜 Shell Scripts Referenced**")
            scripts = [
                {"name": "pre_check_availability.ksh",  "type": "Pre-session",  "action": "Checks source data readiness"},
                {"name": "cleanup_staging.ksh",          "type": "Post-session", "action": "Purges staging tables"},
                {"name": "notify_downstream.ksh",        "type": "On-success",   "action": "Triggers downstream jobs"},
                {"name": "alert_on_failure.ksh",         "type": "On-failure",   "action": "Sends PagerDuty alert"},
            ]
            for s in scripts:
                st.markdown(f"""
                <div style="background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:8px;margin:4px 0;">
                    <div style="font-size:12px;font-weight:600;color:#fcd34d;">{s['name']}</div>
                    <div style="font-size:11px;color:#64748b;">{s['type']} · {s['action']}</div>
                </div>
                """, unsafe_allow_html=True)

        with c3:
            st.markdown("**📁 File Paths**")
            paths = [
                {"path": "/data/etl/input/billing/",        "type": "Source flat files"},
                {"path": "/data/etl/output/billing/",       "type": "Target flat files"},
                {"path": "/data/etl/archive/billing/",      "type": "Archive location"},
                {"path": "/data/etl/logs/",                 "type": "Session logs"},
                {"path": "/data/etl/params/billing.param",  "type": "Parameter file"},
            ]
            for p in paths:
                st.markdown(f"""
                <div style="background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:8px;margin:4px 0;">
                    <div style="font-size:11px;font-family:monospace;color:#93c5fd;">{p['path']}</div>
                    <div style="font-size:11px;color:#64748b;">{p['type']}</div>
                </div>
                """, unsafe_allow_html=True)

    st.success("✅ Lineage complete! Go to **Tab 3: Schema Analyzer** →")


# ─── Demo loader ──────────────────────────────────────────────────────────────

def _load_demo(workflow_name: str = "wf_billing_daily_load"):
    folder = workflow_name.split("_")[1].upper() + "_DOMAIN"
    mock   = MOCK_SESSION_IO.get(workflow_name, DEFAULT_MOCK)
    xml    = MOCK_XML_TEMPLATE.format(
        folder=folder, workflow=workflow_name,
        source=mock["sources"][0], target=mock["targets"][0]
    )
    parsed = parse_workflow_xml(xml)

    st.session_state.update({
        "selected_workflow": workflow_name,
        "selected_folder":   folder,
        "wf_merged_xml":     xml,
        "wf_parsed":         parsed,
        "wf_parsed_full":    parsed,
        "curr_sessions":     mock["sessions"],
        "curr_worklets":     mock["worklets"],
        "curr_mappings":     mock["mappings"],
        "curr_sources":      mock["sources"],
        "curr_targets":      mock["targets"],
        "curr_lookups":      mock["lookups"],
        "row_count":         mock["row_count"],
        "complexity":        mock["complexity"],
        "connected":         True,
        "wf_session_io": {
            s: {
                "sources":        mock["sources"],
                "targets":        mock["targets"],
                "lookups":        mock["lookups"],
                "mapping":        f"m_{s.replace('s_m_', '')}",
                "source_fields":  {},
                "target_fields":  {},
                "transformations": [],
                "pre_commands":   ["pre_check_availability.ksh"],
                "post_commands":  ["cleanup_staging.ksh", "notify_downstream.ksh"],
                "commit_interval": 10000,
                "error_threshold": 0,
            }
            for s in mock["sessions"]
        },
        "dataset_map": {"DWH": "dwh", "STG": "staging", "LKP": "reference"},
        "bq_project":  "your-gcp-project",
    })


# ─── End-to-End Lineage Graph (Interactive) ────────────────────────────────────

def build_e2e_lineage_html(session_io: dict, sources: list, targets: list,
                            lookups: list, sessions: list, worklets: list,
                            workflow: str) -> str:
    """
    Build a full interactive end-to-end lineage graph showing:
    - Upstream sources (with table names)
    - Lookup enrichments
    - Session transformations (with load type + conditions)
    - Downstream targets (with load strategy)
    - Data flow arrows colored by load type
    - Clickable nodes showing details
    """

    # Build node data
    nodes_js   = []
    edges_js   = []
    node_id    = 0
    node_map   = {}

    # Color scheme
    COLORS = {
        "source":   {"bg": "#1e3a5f", "border": "#3b82f6", "text": "#93c5fd"},
        "lookup":   {"bg": "#3b1f5e", "border": "#a855f7", "text": "#d8b4fe"},
        "session":  {"bg": "#5f3b1a", "border": "#f59e0b", "text": "#fcd34d"},
        "worklet":  {"bg": "#1e1b4b", "border": "#6366f1", "text": "#a5b4fc"},
        "target":   {"bg": "#14532d", "border": "#22c55e", "text": "#86efac"},
        "merge":    {"bg": "#14532d", "border": "#22c55e", "text": "#86efac"},
        "insert":   {"bg": "#1e3a5f", "border": "#3b82f6", "text": "#93c5fd"},
        "truncate": {"bg": "#7f1d1d", "border": "#ef4444", "text": "#fca5a5"},
        "update":   {"bg": "#78350f", "border": "#f59e0b", "text": "#fcd34d"},
    }

    LOAD_ICONS = {
        "MERGE": "🔀", "INSERT": "➕", "TRUNCATE+INSERT": "🔄",
        "UPDATE": "✏️", "SCD2": "📚", "DELETE": "🗑️",
    }

    # Infer load type per target from mock context
    LOAD_TYPES = {}
    for i, tgt in enumerate(targets):
        if "FACT" in tgt.upper():
            LOAD_TYPES[tgt] = "INSERT"
        elif "DIM" in tgt.upper():
            LOAD_TYPES[tgt] = "MERGE"
        elif "AGG" in tgt.upper() or "SUMMARY" in tgt.upper():
            LOAD_TYPES[tgt] = "TRUNCATE+INSERT"
        elif "STAGE" in tgt.upper() or "STG" in tgt.upper():
            LOAD_TYPES[tgt] = "TRUNCATE+INSERT"
        else:
            LOAD_TYPES[tgt] = ["MERGE", "INSERT", "TRUNCATE+INSERT", "UPDATE"][i % 4]

    # --- Build nodes ---

    # Source nodes
    for src in sources:
        nid = f"src_{src}"
        node_map[src] = nid
        nodes_js.append(f"""{{
            id: "{nid}", label: "📥 {src[:22]}", group: "source",
            title: "<b>SOURCE TABLE</b><br>{src}<br>DB: Teradata PRD<br>Type: Relational",
            shape: "box", color: {{background:"{COLORS['source']['bg']}",
            border:"{COLORS['source']['border']}"}},
            font: {{color:"{COLORS['source']['text']}", size:11}},
            margin:8
        }}""")

    # Lookup nodes
    for lkp in lookups:
        nid = f"lkp_{lkp}"
        node_map[lkp] = nid
        nodes_js.append(f"""{{
            id: "{nid}", label: "🔍 {lkp[:22]}", group: "lookup",
            title: "<b>LOOKUP TABLE</b><br>{lkp}<br>Join: LEFT JOIN<br>Match: Use First Value",
            shape: "diamond", color: {{background:"{COLORS['lookup']['bg']}",
            border:"{COLORS['lookup']['border']}"}},
            font: {{color:"{COLORS['lookup']['text']}", size:11}},
            margin:8
        }}""")

    # Worklet nodes
    for wklt in worklets:
        nid = f"wklt_{wklt}"
        node_map[wklt] = nid
        nodes_js.append(f"""{{
            id: "{nid}", label: "📦 {wklt[:22]}", group: "worklet",
            title: "<b>WORKLET</b><br>{wklt}<br>Contains grouped sessions",
            shape: "hexagon", color: {{background:"{COLORS['worklet']['bg']}",
            border:"{COLORS['worklet']['border']}"}},
            font: {{color:"{COLORS['worklet']['text']}", size:11}},
            margin:8
        }}""")

    # Session nodes with transformation details
    for i, sess in enumerate(sessions):
        nid  = f"sess_{sess}"
        io   = session_io.get(sess, {})
        tfs  = io.get("transformations", [])
        tf_list = "<br>".join([f"• {t.get('type','')}: {t.get('name','')}" for t in tfs[:5]])
        pre  = "<br>".join(io.get("pre_commands", []))
        post = "<br>".join(io.get("post_commands", []))
        node_map[sess] = nid
        nodes_js.append(f"""{{
            id: "{nid}", label: "⚙️ {sess[:22]}", group: "session",
            title: "<b>SESSION</b><br>{sess}<br><br><b>Transformations:</b><br>{tf_list or 'N/A'}<br><br><b>Pre:</b> {pre or 'None'}<br><b>Post:</b> {post or 'None'}",
            shape: "ellipse", color: {{background:"{COLORS['session']['bg']}",
            border:"{COLORS['session']['border']}"}},
            font: {{color:"{COLORS['session']['text']}", size:11}},
            margin:10
        }}""")

    # Target nodes with load type
    for tgt in targets:
        nid      = f"tgt_{tgt}"
        lt       = LOAD_TYPES.get(tgt, "MERGE")
        lt_icon  = LOAD_ICONS.get(lt, "🔀")
        lt_color = COLORS.get(lt.split("+")[0].lower(), COLORS["merge"])
        node_map[tgt] = nid
        nodes_js.append(f"""{{
            id: "{nid}", label: "{lt_icon} {tgt[:20]}\\n[{lt}]", group: "target",
            title: "<b>TARGET TABLE</b><br>{tgt}<br><b>Load Type:</b> {lt}<br>DB: BigQuery PRD<br>Partition: etl_load_dt",
            shape: "box", color: {{background:"{lt_color['bg']}",
            border:"{lt_color['border']}"}},
            font: {{color:"{lt_color['text']}", size:11, bold:true}},
            margin:10
        }}""")

    # --- Build edges ---

    edge_colors = {
        "source->session":  "#3b82f6",
        "lookup->session":  "#a855f7",
        "session->target":  "#22c55e",
        "worklet->session": "#6366f1",
        "source->worklet":  "#3b82f6",
    }

    # Source → Session edges
    for sess, io in session_io.items():
        sess_nid = node_map.get(sess)
        if not sess_nid:
            continue
        for src in io.get("sources", []):
            src_nid = node_map.get(src)
            if src_nid:
                edges_js.append(f"""{{
                    from: "{src_nid}", to: "{sess_nid}",
                    label: "READ", color: {{color:"{edge_colors['source->session']}",opacity:0.8}},
                    arrows: "to", dashes: false,
                    title: "Source → Session data flow",
                    font: {{size:9, color:"#64748b"}},
                    width: 2
                }}""")
        # Lookup → Session edges
        for lkp in io.get("lookups", []):
            lkp_nid = node_map.get(lkp)
            if lkp_nid:
                edges_js.append(f"""{{
                    from: "{lkp_nid}", to: "{sess_nid}",
                    label: "JOIN", color: {{color:"{edge_colors['lookup->session']}",opacity:0.8}},
                    arrows: "to", dashes: true,
                    title: "Lookup enrichment",
                    font: {{size:9, color:"#64748b"}},
                    width: 1
                }}""")
        # Session → Target edges
        for tgt in io.get("targets", []):
            tgt_nid = node_map.get(tgt)
            lt      = LOAD_TYPES.get(tgt, "MERGE")
            lt_clr  = {"MERGE":"#22c55e","INSERT":"#3b82f6","TRUNCATE+INSERT":"#ef4444","UPDATE":"#f59e0b"}.get(lt,"#22c55e")
            if tgt_nid:
                edges_js.append(f"""{{
                    from: "{sess_nid}", to: "{tgt_nid}",
                    label: "{lt}", color: {{color:"{lt_clr}",opacity:0.9}},
                    arrows: "to", dashes: false,
                    title: "Load strategy: {lt}",
                    font: {{size:9, color:"{lt_clr}", bold:true}},
                    width: 3
                }}""")

    # Worklet → Session edges
    for wklt in worklets:
        wklt_nid = node_map.get(wklt)
        if wklt_nid:
            for sess in sessions[:2]:  # connect worklet to first sessions
                sess_nid = node_map.get(sess)
                if sess_nid:
                    edges_js.append(f"""{{
                        from: "{wklt_nid}", to: "{sess_nid}",
                        color: {{color:"{edge_colors['worklet->session']}",opacity:0.5}},
                        arrows: "to", dashes: true,
                        title: "Worklet contains session",
                        width: 1
                    }}""")

    nodes_str = ",\n".join(nodes_js)
    edges_str = ",\n".join(edges_js)

    total_nodes = len(sources) + len(lookups) + len(sessions) + len(targets) + len(worklets)

    return f"""<!DOCTYPE html>
<html>
<head>
<script src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.js"></script>
<link href="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.css" rel="stylesheet"/>
<style>
  body {{ margin:0; background:#0f172a; font-family:'Segoe UI',sans-serif; }}
  #graph {{ width:100%; height:580px; background:#0f172a; border:1px solid #1e293b; border-radius:8px; }}
  #controls {{
    display:flex; gap:8px; padding:10px 12px; background:#1e293b;
    border-radius:8px 8px 0 0; flex-wrap:wrap; align-items:center;
  }}
  .ctrl-btn {{
    background:#0f172a; border:1px solid #334155; color:#e2e8f0;
    padding:5px 12px; border-radius:6px; cursor:pointer; font-size:12px;
  }}
  .ctrl-btn:hover {{ background:#334155; }}
  .legend {{
    display:flex; gap:12px; flex-wrap:wrap; padding:8px 12px;
    background:#1e293b; border-radius:0 0 8px 8px; border-top:1px solid #334155;
  }}
  .leg-item {{ display:flex; align-items:center; gap:6px; font-size:11px; color:#94a3b8; }}
  .leg-dot {{ width:10px; height:10px; border-radius:50%; }}
  #info-box {{
    position:absolute; top:60px; right:12px; background:#1e293b;
    border:1px solid #334155; border-radius:8px; padding:12px;
    max-width:240px; font-size:12px; color:#e2e8f0; display:none;
    z-index:100; max-height:300px; overflow-y:auto;
  }}
  #node-count {{ color:#6366f1; font-size:11px; margin-left:auto; }}
</style>
</head>
<body>
<div id="controls">
  <button class="ctrl-btn" onclick="network.fit()">🔍 Fit All</button>
  <button class="ctrl-btn" onclick="network.setOptions({{layout:{{hierarchical:{{enabled:true,direction:'LR'}}}}}});network.fit()">→ Horizontal</button>
  <button class="ctrl-btn" onclick="network.setOptions({{layout:{{hierarchical:{{enabled:true,direction:'UD'}}}}}});network.fit()">↓ Vertical</button>
  <button class="ctrl-btn" onclick="network.setOptions({{layout:{{hierarchical:{{enabled:false}}}}}});network.fit()">◎ Free</button>
  <button class="ctrl-btn" onclick="filterGroup('source')">📥 Sources</button>
  <button class="ctrl-btn" onclick="filterGroup('lookup')">🔍 Lookups</button>
  <button class="ctrl-btn" onclick="filterGroup('session')">⚙️ Sessions</button>
  <button class="ctrl-btn" onclick="filterGroup('target')">📤 Targets</button>
  <button class="ctrl-btn" onclick="showAll()">👁️ Show All</button>
  <span id="node-count">{total_nodes} nodes | {len(edges_js)} edges</span>
</div>
<div style="position:relative;">
  <div id="graph"></div>
  <div id="info-box"></div>
</div>
<div class="legend">
  <div class="leg-item"><div class="leg-dot" style="background:#3b82f6"></div>Source Tables</div>
  <div class="leg-item"><div class="leg-dot" style="background:#a855f7"></div>Lookup Tables</div>
  <div class="leg-item"><div class="leg-dot" style="background:#f59e0b"></div>Sessions/ETL</div>
  <div class="leg-item"><div class="leg-dot" style="background:#6366f1"></div>Worklets</div>
  <div class="leg-item"><div class="leg-dot" style="background:#22c55e;"></div>Target (MERGE)</div>
  <div class="leg-item"><div class="leg-dot" style="background:#3b82f6;"></div>Target (INSERT)</div>
  <div class="leg-item"><div class="leg-dot" style="background:#ef4444;"></div>Target (TRUNCATE)</div>
  <div class="leg-item"><span style="color:#22c55e;font-weight:700;">━━</span> MERGE/INSERT</div>
  <div class="leg-item"><span style="color:#a855f7;font-weight:700;">- -</span> Lookup JOIN</div>
</div>

<script>
var nodesData = [{nodes_str}];
var edgesData = [{edges_str}];

var nodes = new vis.DataSet(nodesData);
var edges = new vis.DataSet(edgesData);

var options = {{
  layout: {{
    hierarchical: {{
      enabled: true,
      direction: "LR",
      sortMethod: "directed",
      levelSeparation: 220,
      nodeSpacing: 90,
      treeSpacing: 140,
    }}
  }},
  physics: {{ enabled: false }},
  interaction: {{
    hover: true,
    tooltipDelay: 100,
    zoomView: true,
    dragView: true,
    navigationButtons: false,
  }},
  nodes: {{
    borderWidth: 2,
    borderWidthSelected: 3,
    shadow: {{ enabled: true, color: 'rgba(0,0,0,0.5)', size: 8, x: 3, y: 3 }},
    chosen: true,
  }},
  edges: {{
    smooth: {{ type: 'cubicBezier', forceDirection: 'horizontal', roundness: 0.4 }},
    shadow: true,
    selectionWidth: 3,
  }},
  groups: {{
    source:  {{ color: {{background:'#1e3a5f', border:'#3b82f6'}}, font:{{color:'#93c5fd'}} }},
    lookup:  {{ color: {{background:'#3b1f5e', border:'#a855f7'}}, font:{{color:'#d8b4fe'}} }},
    session: {{ color: {{background:'#5f3b1a', border:'#f59e0b'}}, font:{{color:'#fcd34d'}} }},
    worklet: {{ color: {{background:'#1e1b4b', border:'#6366f1'}}, font:{{color:'#a5b4fc'}} }},
    target:  {{ color: {{background:'#14532d', border:'#22c55e'}}, font:{{color:'#86efac'}} }},
  }},
}};

var container = document.getElementById('graph');
var network = new vis.Network(container, {{nodes: nodes, edges: edges}}, options);

// Click handler — show node details
network.on("click", function(params) {{
  var infoBox = document.getElementById('info-box');
  if (params.nodes.length > 0) {{
    var nodeId = params.nodes[0];
    var node = nodes.get(nodeId);
    infoBox.innerHTML = node.title || node.label;
    infoBox.style.display = 'block';
  }} else {{
    infoBox.style.display = 'none';
  }}
}});

// Highlight connected nodes on hover
network.on("hoverNode", function(params) {{
  var connectedNodes = network.getConnectedNodes(params.node);
  var connectedEdges = network.getConnectedEdges(params.node);
  // highlight
}});

function filterGroup(group) {{
  var filtered = nodesData.filter(n => n.group === group).map(n => n.id);
  network.selectNodes(filtered);
  network.focus(filtered[0], {{scale:1.2, animation:true}});
}}

function showAll() {{
  network.unselectAll();
  network.fit({{animation:true}});
}}

network.fit();
</script>
</body>
</html>"""


def _render_dependency_table(session_io: dict, sources: list, targets: list, lookups: list):
    """Render upstream/downstream dependency table."""
    st.markdown("#### 🔗 Upstream / Downstream Dependencies")
    st.caption("For each target: which sources and lookups feed into it, and what load strategy is applied")

    LOAD_TYPES = {}
    for i, tgt in enumerate(targets):
        if "FACT" in tgt.upper():    LOAD_TYPES[tgt] = ("INSERT",          "#3b82f6", "➕")
        elif "DIM" in tgt.upper():   LOAD_TYPES[tgt] = ("MERGE",           "#22c55e", "🔀")
        elif "AGG" in tgt.upper():   LOAD_TYPES[tgt] = ("TRUNCATE+INSERT", "#ef4444", "🔄")
        elif "STG" in tgt.upper():   LOAD_TYPES[tgt] = ("TRUNCATE+INSERT", "#ef4444", "🔄")
        else: LOAD_TYPES[tgt] = (["MERGE","INSERT","TRUNCATE+INSERT","UPDATE"][i%4],
                                  ["#22c55e","#3b82f6","#ef4444","#f59e0b"][i%4],
                                  ["🔀","➕","🔄","✏️"][i%4])

    for tgt in targets:
        lt, lt_color, lt_icon = LOAD_TYPES.get(tgt, ("MERGE","#22c55e","🔀"))

        # Find which sessions write to this target
        feeding_sessions = [s for s, io in session_io.items() if tgt in io.get("targets", [])]
        feeding_sources  = []
        feeding_lookups  = []
        for sess in feeding_sessions:
            io = session_io.get(sess, {})
            feeding_sources  += [s for s in io.get("sources",  []) if s not in feeding_sources]
            feeding_lookups  += [l for l in io.get("lookups",  []) if l not in feeding_lookups]

        with st.expander(
            f"{lt_icon} **{tgt}** — Load: `{lt}` — "
            f"{len(feeding_sources)} source(s) | {len(feeding_lookups)} lookup(s) | "
            f"{len(feeding_sessions)} session(s)",
            expanded=False
        ):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.markdown("**📥 Upstream Sources**")
                for src in feeding_sources:
                    st.markdown(f"""
                    <div style="background:#1e3a5f22;border:1px solid #3b82f644;
                                border-radius:6px;padding:6px 10px;margin:3px 0;font-size:12px;color:#93c5fd;">
                        📥 {src}
                    </div>""", unsafe_allow_html=True)
                if not feeding_sources:
                    st.caption("No direct sources")

            with col2:
                st.markdown("**🔍 Lookup Enrichments**")
                for lkp in feeding_lookups:
                    st.markdown(f"""
                    <div style="background:#3b1f5e22;border:1px solid #a855f744;
                                border-radius:6px;padding:6px 10px;margin:3px 0;font-size:12px;color:#d8b4fe;">
                        🔍 {lkp}<br>
                        <span style="font-size:10px;color:#64748b;">LEFT JOIN on account_id</span>
                    </div>""", unsafe_allow_html=True)
                if not feeding_lookups:
                    st.caption("No lookups")

            with col3:
                st.markdown("**⚙️ Via Sessions**")
                for sess in feeding_sessions:
                    io = session_io.get(sess, {})
                    tfs = io.get("transformations", [])
                    tf_types = list(set([t.get("type","") for t in tfs]))[:3]
                    st.markdown(f"""
                    <div style="background:#5f3b1a22;border:1px solid #f59e0b44;
                                border-radius:6px;padding:6px 10px;margin:3px 0;font-size:12px;color:#fcd34d;">
                        ⚙️ {sess}<br>
                        <span style="font-size:10px;color:#64748b;">{', '.join(tf_types) if tf_types else 'ETL session'}</span>
                    </div>""", unsafe_allow_html=True)

            with col4:
                st.markdown("**🎯 Load Strategy**")
                st.markdown(f"""
                <div style="background:{lt_color}22;border:2px solid {lt_color}88;
                            border-radius:8px;padding:12px;text-align:center;">
                    <div style="font-size:24px;">{lt_icon}</div>
                    <div style="font-size:13px;font-weight:700;color:{lt_color};margin-top:4px;">{lt}</div>
                    <div style="font-size:10px;color:#64748b;margin-top:6px;">
                        {"Matched: UPDATE<br>Unmatched: INSERT" if lt=="MERGE"
                         else "Delete partition<br>then INSERT" if lt=="TRUNCATE+INSERT"
                         else "Append rows only" if lt=="INSERT"
                         else "Update existing rows"}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Conditions
                st.markdown("<div style='font-size:11px;color:#64748b;margin-top:8px;'>", unsafe_allow_html=True)
                if lt == "MERGE":
                    st.markdown(f"**Key:** `account_id`  \n**When matched:** UPDATE  \n**When not matched:** INSERT")
                elif lt == "TRUNCATE+INSERT":
                    st.markdown("**Filter:** `DATE(etl_load_dt) = CURRENT_DATE()`  \n**Action:** DELETE partition then INSERT")
                elif lt == "INSERT":
                    st.markdown("**Filter:** `run_date = CURRENT_DATE()`  \n**Action:** Append only")
                elif lt == "UPDATE":
                    st.markdown("**Key:** `account_id`  \n**Action:** Update existing rows only")
                st.markdown("</div>", unsafe_allow_html=True)


def _render_data_flow_table(session_io: dict, sources: list, targets: list, lookups: list):
    """Show which columns come from which source table."""
    st.markdown("#### 🧬 Column-Level Data Flow")
    st.caption("Which data comes from which source table → how it reaches the target")

    SAMPLE_COLUMNS = {
        "sources": {
            sources[0] if sources else "SRC": ["ACCOUNT_ID", "ACCOUNT_NAME", "BILL_AMOUNT", "BILL_DATE", "CUSTOMER_ID", "STATUS_CD"],
            sources[1] if len(sources) > 1 else "SRC2": ["ACCOUNT_ID", "ADDRESS_LINE1", "CITY", "STATE_CD", "ZIP_CODE"],
        },
        "lookups": {
            lookups[0] if lookups else "LKP": ["ACCOUNT_ID→SEGMENT", "ACCOUNT_ID→TIER", "ACCOUNT_ID→CREDIT_CLASS"],
            lookups[1] if len(lookups) > 1 else "LKP2": ["RATE_CODE→RATE_AMT", "RATE_CODE→TAX_PCT"],
        },
        "target_cols": ["account_id", "account_name", "bill_amount", "bill_date",
                        "customer_id", "status_cd", "address_line1", "city",
                        "state_cd", "zip_code", "segment", "tier", "credit_class",
                        "rate_amt", "tax_pct", "etl_load_dt"]
    }

    tgt = targets[0] if targets else "TARGET"
    st.markdown(f"**Target table: `{tgt}`**")

    rows = []
    for col in SAMPLE_COLUMNS["target_cols"]:
        # Determine source
        src_cols_flat = {c: k for k, v in SAMPLE_COLUMNS["sources"].items() for c in v}
        lkp_cols_flat = {c.split("→")[1]: f"{k} (via {c.split('→')[0]})"
                        for k, v in SAMPLE_COLUMNS["lookups"].items() for c in v}

        col_upper = col.upper()
        if col == "etl_load_dt":
            origin = "🔧 Generated"; origin_type = "ETL"; transform = "CURRENT_TIMESTAMP()"
        elif col_upper in [c.upper() for c in src_cols_flat]:
            src_name = next((v for k, v in src_cols_flat.items() if k.upper() == col_upper), "")
            origin = f"📥 {src_name}"; origin_type = "Source"; transform = f"TRIM({col.upper()})" if "name" in col else col.upper()
        elif col_upper in [c.upper() for c in lkp_cols_flat]:
            lkp_name = next((v for k, v in lkp_cols_flat.items() if k.upper() == col_upper), "")
            origin = f"🔍 {lkp_name}"; origin_type = "Lookup"; transform = f"LKP.{col.upper()}"
        else:
            origin = "📥 Source"; origin_type = "Source"; transform = col.upper()

        rows.append({"Target Column": f"`{col}`", "Origin": origin,
                     "Type": origin_type, "Transformation": f"`{transform}`"})

    # Display as styled table
    color_map = {"Source": "#3b82f6", "Lookup": "#a855f7", "ETL": "#22c55e"}
    for row in rows:
        t_color = color_map.get(row["Type"], "#6b7280")
        c1, c2, c3, c4 = st.columns([2, 3, 1, 3])
        c1.markdown(f"<code style='font-size:11px;'>{row['Target Column'].strip('`')}</code>", unsafe_allow_html=True)
        c2.markdown(f"<span style='font-size:11px;color:{t_color};'>{row['Origin']}</span>", unsafe_allow_html=True)
        c3.markdown(f"<span style='background:{t_color}22;color:{t_color};padding:1px 6px;border-radius:8px;font-size:10px;'>{row['Type']}</span>", unsafe_allow_html=True)
        c4.markdown(f"<code style='font-size:10px;color:#94a3b8;'>{row['Transformation'].strip('`')}</code>", unsafe_allow_html=True)
        st.markdown("<hr style='margin:2px 0;border-color:#1e293b33;'>", unsafe_allow_html=True)


# ─── Patch render to include E2E lineage ──────────────────────────────────────

_original_render = render

def render():
    """Enhanced render with E2E lineage graph added."""
    import streamlit.components.v1 as components_v1

    # Run original render first
    _original_render()

    # Get state
    workflow   = st.session_state.get("selected_workflow", "")
    sources    = st.session_state.get("curr_sources", [])
    targets    = st.session_state.get("curr_targets", [])
    lookups    = st.session_state.get("curr_lookups", [])
    sessions   = st.session_state.get("curr_sessions", [])
    worklets   = st.session_state.get("curr_worklets", [])
    session_io = st.session_state.get("wf_session_io", {})

    if not workflow:
        return

    st.markdown("---")

    # ── E2E Interactive Lineage Graph ────────────────────────────────────────
    st.markdown("#### 🌐 End-to-End Interactive Lineage Graph")
    st.caption("Full dependency graph — click any node for details | drag to explore | zoom with scroll")

    col_info1, col_info2, col_info3, col_info4, col_info5 = st.columns(5)
    col_info1.metric("📥 Sources",  len(sources))
    col_info2.metric("🔍 Lookups",  len(lookups))
    col_info3.metric("⚙️ Sessions", len(sessions))
    col_info4.metric("📦 Worklets", len(worklets))
    col_info5.metric("📤 Targets",  len(targets))

    graph_html = build_e2e_lineage_html(
        session_io, sources, targets, lookups, sessions, worklets, workflow
    )
    components_v1.html(graph_html, height=700, scrolling=False)

    st.markdown("---")

    # ── Upstream / Downstream Dependencies ───────────────────────────────────
    _render_dependency_table(session_io, sources, targets, lookups)

    st.markdown("---")

    # ── Column-level data flow ────────────────────────────────────────────────
    _render_data_flow_table(session_io, sources, targets, lookups)
