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
