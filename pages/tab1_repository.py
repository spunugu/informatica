"""Tab 1: Repository Explorer"""

import streamlit as st
import time, os
from utils.infa_client import connect_repository, list_folders, list_workflows, export_workflow_xml
from utils.parser import parse_workflow_xml, get_complexity_badge, complexity_color
from mock_data.sample_data import MOCK_SESSION_IO, DEFAULT_MOCK


def render():
    demo = os.environ.get("DEMO_MODE", "true").lower() == "true"

    st.markdown("""
    <div class="tab-header">
        <h2>🗂️ Repository Explorer</h2>
        <p>Connect to Informatica PowerCenter, browse folders & workflows, and export XML definitions.</p>
    </div>
    """, unsafe_allow_html=True)

    if demo:
        st.info("🎭 **DEMO MODE** — Using sample data. Toggle real mode in the sidebar.", icon="ℹ️")

    with st.expander("🔌 Repository Connection", expanded=not st.session_state.get("connected", False)):
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            host = st.text_input("Repository Host", value="infa-repo.example.com" if demo else "", placeholder="hostname or IP")
        with col2:
            port = st.text_input("Port", value="6005")
        with col3:
            repo = st.text_input("Repository", value="ETLRepo" if demo else "")
        col4, col5 = st.columns(2)
        with col4:
            user = st.text_input("Username", value="admin" if demo else "")
        with col5:
            password = st.text_input("Password", type="password", value="••••••••" if demo else "")

        if st.button("🔗 Connect & Fetch All Folders", type="primary", use_container_width=True):
            with st.spinner("Connecting to repository..."):
                result = connect_repository(host, port, repo, user, password)
                if result["success"]:
                    st.session_state.connected = True
                    folders = list_folders()
                    st.session_state.folders = folders
                    st.success(f"✅ {result['message']} — Found **{len(folders)} folders**")
                    st.rerun()
                else:
                    st.error(f"❌ {result['message']}")

    if not st.session_state.get("connected", False):
        if st.button("▶️ Quick Connect (Demo)", use_container_width=True):
            with st.spinner("Connecting..."):
                time.sleep(0.8)
                st.session_state.connected = True
                st.session_state.folders = list_folders()
                st.rerun()
        return

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        folders = st.session_state.get("folders", [])
        selected_folder = st.selectbox("📁 Select Folder", folders, key="sel_folder")
    with col2:
        if st.button("📋 Fetch Workflows", use_container_width=True):
            with st.spinner("Fetching workflows..."):
                st.session_state.workflows = list_workflows(selected_folder)
                st.session_state.selected_folder = selected_folder
        workflows = st.session_state.get("workflows", [])
        selected_workflow = st.selectbox("⚙️ Select Workflow", workflows if workflows else ["— fetch workflows first —"], key="sel_workflow")

    if workflows and selected_workflow and not selected_workflow.startswith("—"):
        st.markdown("---")
        if st.button("🚀 Fetch Dependencies & Export XML", type="primary", use_container_width=True):
            with st.spinner(f"Exporting {selected_workflow}..."):
                xml_text = export_workflow_xml(selected_folder, selected_workflow)
                parsed = parse_workflow_xml(xml_text)
                mock = MOCK_SESSION_IO.get(selected_workflow, DEFAULT_MOCK)

                p_sessions = [s.name for s in parsed.sessions] if parsed.sessions else mock["sessions"]
                p_worklets = [w.name for w in parsed.worklets] if parsed.worklets else mock["worklets"]
                p_mappings = [m.name for m in parsed.mappings] if parsed.mappings else mock["mappings"]
                p_sources  = [s.name for s in parsed.sources]  if parsed.sources  else mock["sources"]
                p_targets  = [t.name for t in parsed.targets]  if parsed.targets  else mock["targets"]
                p_lookups  = []
                for m in parsed.mappings:
                    for lkp in m.get_lookup_transforms():
                        lkp_name = lkp.attributes.get("Lookup table name", lkp.name)
                        if lkp_name and lkp_name not in p_lookups:
                            p_lookups.append(lkp_name)
                if not p_lookups:
                    p_lookups = mock["lookups"]

                st.session_state.wf_merged_xml   = xml_text
                st.session_state.wf_parsed        = parsed
                st.session_state.wf_parsed_full   = parsed
                st.session_state.curr_sessions    = p_sessions
                st.session_state.curr_worklets    = p_worklets
                st.session_state.curr_mappings    = p_mappings
                st.session_state.curr_sources     = p_sources
                st.session_state.curr_targets     = p_targets
                st.session_state.curr_lookups     = p_lookups
                st.session_state.wf_session_io    = parsed.session_io if parsed.session_io else {
                    s: {"sources": mock["sources"], "targets": mock["targets"], "lookups": mock["lookups"],
                        "mapping": f"m_{s.replace('s_m_','')}", "source_fields": {}, "target_fields": {},
                        "transformations": [], "pre_commands": [], "post_commands": [],
                        "commit_interval": 10000, "error_threshold": 0}
                    for s in p_sessions
                }
                st.session_state.selected_workflow = selected_workflow
                st.session_state.selected_folder   = selected_folder
                st.session_state.row_count          = mock.get("row_count", 0)
                st.session_state.complexity         = parsed.complexity_badge if parsed.complexity_badge else get_complexity_badge(p_sessions, p_worklets, p_mappings)

        if st.session_state.get("wf_parsed") is not None:
            badge = st.session_state.get("complexity", "Low")
            badge_color = complexity_color(badge)
            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:12px;margin:16px 0 8px;">
                <h3 style="margin:0;">📊 <code>{selected_workflow}</code></h3>
                <span style="background:{badge_color};color:white;padding:4px 12px;border-radius:20px;font-weight:700;font-size:13px;">{badge}</span>
            </div>""", unsafe_allow_html=True)

            metrics = [
                ("🔄 Sessions", st.session_state.get("curr_sessions", [])),
                ("📦 Worklets", st.session_state.get("curr_worklets", [])),
                ("🗺️ Mappings", st.session_state.get("curr_mappings", [])),
                ("📥 Sources", st.session_state.get("curr_sources", [])),
                ("📤 Targets", st.session_state.get("curr_targets", [])),
                ("🔍 Lookups", st.session_state.get("curr_lookups", [])),
            ]
            cols = st.columns(6)
            for col, (label, items) in zip(cols, metrics):
                with col:
                    st.metric(label, len(items))
                    if items:
                        with st.expander("View"):
                            for item in items:
                                st.markdown(f"• `{item}`")

            with st.expander("📄 Raw XML Preview"):
                st.code(st.session_state.get("wf_merged_xml", "")[:2000], language="xml")

            st.success("✅ Done! Go to **Tab 2: Lineage Analysis** →")
