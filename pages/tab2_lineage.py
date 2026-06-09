"""Tab 2: Lineage Analysis"""

import streamlit as st
import os
from mock_data.sample_data import MOCK_SESSION_IO, DEFAULT_MOCK, MOCK_XML_TEMPLATE
from utils.parser import parse_workflow_xml, get_complexity_badge, complexity_color, complexity_emoji


def build_lineage_html(sources, lookups, targets, sessions):
    src_items = "".join([f'<div class="node src-node">📥 {s}</div>' for s in sources])
    lkp_items = "".join([f'<div class="node lkp-node">🔍 {l}</div>' for l in lookups])
    tf_items  = "".join([f'<div class="node tf-node">⚙️ {s}</div>' for s in sessions])
    tgt_items = "".join([f'<div class="node tgt-node">📤 {t}</div>' for t in targets])
    return f"""
    <style>
      .lineage-wrap{{display:flex;align-items:flex-start;gap:0;overflow-x:auto;font-family:'Segoe UI',sans-serif;padding:20px 10px;background:#0f172a;border-radius:12px;}}
      .lineage-col{{display:flex;flex-direction:column;align-items:center;min-width:160px;gap:10px;}}
      .col-label{{font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#94a3b8;margin-bottom:6px;}}
      .node{{background:#1e293b;border:1.5px solid #334155;border-radius:8px;padding:8px 14px;font-size:12px;color:#e2e8f0;text-align:center;min-width:140px;max-width:155px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
      .src-node{{border-color:#3b82f6;color:#93c5fd;}}
      .lkp-node{{border-color:#a855f7;color:#d8b4fe;}}
      .tf-node{{border-color:#f59e0b;color:#fcd34d;}}
      .tgt-node{{border-color:#22c55e;color:#86efac;}}
      .arrow-col{{display:flex;flex-direction:column;justify-content:center;align-items:center;min-width:40px;color:#475569;font-size:20px;padding-top:28px;}}
    </style>
    <div class="lineage-wrap">
      <div class="lineage-col"><div class="col-label">Sources</div>{src_items or '<div class="node src-node">None</div>'}</div>
      <div class="arrow-col">→</div>
      <div class="lineage-col"><div class="col-label">Lookups</div>{lkp_items or '<div style="color:#475569;font-size:12px;padding-top:20px;">None</div>'}</div>
      <div class="arrow-col">→</div>
      <div class="lineage-col"><div class="col-label">Sessions</div>{tf_items or '<div class="node tf-node">Transform</div>'}</div>
      <div class="arrow-col">→</div>
      <div class="lineage-col"><div class="col-label">Targets</div>{tgt_items or '<div class="node tgt-node">None</div>'}</div>
    </div>"""


def render():
    st.markdown("""<div class="tab-header"><h2>🔗 Lineage Analysis</h2>
    <p>Visual data flow: Sources → Lookups → Sessions → Targets extracted from Informatica XML.</p></div>""", unsafe_allow_html=True)

    if not st.session_state.get("selected_workflow"):
        st.warning("⚠️ Complete **Tab 1** first.")
        if st.button("▶️ Load Demo Data"):
            _load_demo(); st.rerun()
        return

    sources  = st.session_state.get("curr_sources", [])
    targets  = st.session_state.get("curr_targets", [])
    lookups  = st.session_state.get("curr_lookups", [])
    sessions = st.session_state.get("curr_sessions", [])
    workflow = st.session_state.get("selected_workflow", "")
    row_count = st.session_state.get("row_count", 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Workflow", workflow.replace("wf_","").replace("_"," ").title())
    col2.metric("Sessions", len(sessions))
    col3.metric("Total Objects", len(sessions)+len(st.session_state.get("curr_worklets",[]))+len(st.session_state.get("curr_mappings",[])))
    col4.metric("Last Run Rows", f"{row_count:,}" if row_count else "N/A")

    st.markdown("---")
    st.markdown("#### 🗺️ Data Flow Lineage")
    st.components.v1.html(build_lineage_html(sources, lookups, targets, sessions),
                          height=max(280, 80 + max(len(sources), len(sessions), len(targets), 1) * 55))

    st.markdown("---")
    st.markdown("#### 📋 Session-Level Mapping")
    for session, io in st.session_state.get("wf_session_io", {}).items():
        with st.expander(f"⚙️ {session}"):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("**📥 Sources**")
                for s in io.get("sources", []): st.markdown(f"• `{s}`")
            with c2:
                st.markdown("**📤 Targets**")
                for t in io.get("targets", []): st.markdown(f"• `{t}`")
            with c3:
                st.markdown("**🔍 Lookups**")
                for l in io.get("lookups", []): st.markdown(f"• `{l}`")

    st.success("✅ Lineage done! Go to **Tab 3: Schema Analyzer** →")


def _load_demo():
    wf = "wf_billing_daily_load"; folder = "BILLING_DOMAIN"
    mock = MOCK_SESSION_IO.get(wf, DEFAULT_MOCK)
    xml = MOCK_XML_TEMPLATE.format(folder=folder, workflow=wf, source=mock["sources"][0], target=mock["targets"][0])
    parsed = parse_workflow_xml(xml)
    st.session_state.update({
        "selected_workflow": wf, "selected_folder": folder,
        "wf_merged_xml": xml, "wf_parsed": parsed,
        "curr_sessions": mock["sessions"], "curr_worklets": mock["worklets"],
        "curr_mappings": mock["mappings"], "curr_sources": mock["sources"],
        "curr_targets": mock["targets"], "curr_lookups": mock["lookups"],
        "row_count": mock["row_count"], "complexity": mock["complexity"],
        "connected": True,
        "wf_session_io": {s: {"sources": mock["sources"], "targets": mock["targets"], "lookups": mock["lookups"]} for s in mock["sessions"]}
    })
