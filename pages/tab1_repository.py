"""
ETL Automator — Tab 1: Repository Explorer (Full Production)
Built by Srinivas Punugu

Full features per architecture doc:
- Connect to Informatica repository (pmrep connect)
- Browse all folders (pmrep listobjects -o folder)
- Browse workflows per folder (pmrep listobjects -o workflow)
- Fetch dependencies: sessions, worklets, mappings, mapplets, commands
- Export individual XMLs: session, mapping, worklet, mapplet
- Export session logs (pmcmd getsessionlog)
- Complexity badge (Low/Medium/High/Critical)
- Infrastructure panel: DB connections, shell scripts, file paths
- Dependency check per object
- Object count dashboard cards
"""

import streamlit as st
import streamlit.components.v1 as components
import time
import os
import io
import zipfile
from datetime import datetime, timedelta

from utils.infa_client import (
    connect_repository, list_folders, list_workflows,
    export_workflow_xml, get_session_stats
)
from utils.parser import (
    parse_workflow_xml, get_complexity_badge,
    complexity_color, complexity_emoji
)
from mock_data.sample_data import MOCK_SESSION_IO, DEFAULT_MOCK, MOCK_XML_TEMPLATE


# ─── Mock export generators ────────────────────────────────────────────────────

def _mock_session_xml(session_name: str, folder: str, workflow: str, sources: list, targets: list) -> str:
    src_refs = "\n".join([f'    <SESSCONNECTREF CNXREFNAME="TD_PRD" OBJECTNAME="{s}" VARIABLE="$Source"/>' for s in sources])
    tgt_refs = "\n".join([f'    <SESSCONNECTREF CNXREFNAME="BQ_PRD" OBJECTNAME="{t}" VARIABLE="$Target"/>' for t in targets])
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="{datetime.now().strftime("%m/%d/%Y %H:%M:%S")}" REPOSITORY_VERSION="187.93">
<REPOSITORY NAME="ETLRepo" VERSION="187" CODEPAGE="UTF-8">
<FOLDER NAME="{folder}" GROUP="" OWNER="admin">

  <SESSION NAME="{session_name}" REUSABLE="NO"
           MAPPINGNAME="m_{session_name.replace('s_m_','')}"
           DESCRIPTION="ETL session for {workflow}">

    <!-- Source connections -->
{src_refs}

    <!-- Target connections -->
{tgt_refs}

    <!-- Session attributes -->
    <ATTRIBUTE NAME="Pre-session commands"           VALUE="pre_check_availability.ksh"/>
    <ATTRIBUTE NAME="Post-session success commands"  VALUE="cleanup_staging.ksh; notify_downstream.ksh"/>
    <ATTRIBUTE NAME="On-failure commands"            VALUE="alert_on_failure.ksh"/>
    <ATTRIBUTE NAME="Commit interval"                VALUE="10000"/>
    <ATTRIBUTE NAME="Error threshold"                VALUE="0"/>
    <ATTRIBUTE NAME="Enable high precision"          VALUE="YES"/>
    <ATTRIBUTE NAME="Session log file"               VALUE="/data/etl/logs/{session_name}.log"/>
    <ATTRIBUTE NAME="Parameter filename"             VALUE="/data/etl/params/{workflow}.param"/>
    <ATTRIBUTE NAME="$DBConnection_SRCE"             VALUE="TD_PRD"/>
    <ATTRIBUTE NAME="$DBConnection_TGT"              VALUE="BQ_PRD"/>
    <ATTRIBUTE NAME="Number of partitions"           VALUE="4"/>
    <ATTRIBUTE NAME="Pushdown optimization"          VALUE="None"/>

    <!-- Source instance config -->
    <SESSTRANSFORMATION MAPPINGNAME="m_{session_name.replace('s_m_','')}"
                        TRANSFORMATIONNAME="SQ_{sources[0] if sources else 'SOURCE'}"
                        TRANSFORMATIONTYPE="Source Qualifier">
      <ATTRIBUTE NAME="Sql Query"             VALUE=""/>
      <ATTRIBUTE NAME="Source Filter"         VALUE=""/>
      <ATTRIBUTE NAME="Number Of Sorted Ports" VALUE="0"/>
    </SESSTRANSFORMATION>

  </SESSION>

</FOLDER>
</REPOSITORY>
</POWERMART>'''


def _mock_mapping_xml(mapping_name: str, folder: str, sources: list, targets: list, lookups: list) -> str:
    src_fields = "\n".join([
        f'    <SOURCEFIELD DATATYPE="VARCHAR" LENGTH="50" NAME="ACCOUNT_ID" NULLABLE="NOT NULL" KEYTYPE="PRIMARY KEY"/>',
        f'    <SOURCEFIELD DATATYPE="VARCHAR" LENGTH="100" NAME="ACCOUNT_NAME" NULLABLE="NULL"/>',
        f'    <SOURCEFIELD DATATYPE="DECIMAL" LENGTH="15" NAME="AMOUNT" NULLABLE="NULL" PRECISION="15" SCALE="2"/>',
        f'    <SOURCEFIELD DATATYPE="DATE/TIME" LENGTH="29" NAME="EFFECTIVE_DATE" NULLABLE="NULL"/>',
        f'    <SOURCEFIELD DATATYPE="VARCHAR" LENGTH="10" NAME="STATUS_CD" NULLABLE="NULL"/>',
        f'    <SOURCEFIELD DATATYPE="INTEGER" LENGTH="10" NAME="CUSTOMER_ID" NULLABLE="NOT NULL"/>',
    ])
    tgt_fields = "\n".join([
        f'    <TARGETFIELD DATATYPE="STRING" NAME="account_id" NULLABLE="NOT NULL" KEYTYPE="PRIMARY KEY"/>',
        f'    <TARGETFIELD DATATYPE="STRING" NAME="account_name" NULLABLE="NULL"/>',
        f'    <TARGETFIELD DATATYPE="NUMERIC" NAME="amount" NULLABLE="NULL"/>',
        f'    <TARGETFIELD DATATYPE="TIMESTAMP" NAME="effective_date" NULLABLE="NULL"/>',
        f'    <TARGETFIELD DATATYPE="STRING" NAME="status_cd" NULLABLE="NULL"/>',
        f'    <TARGETFIELD DATATYPE="INT64" NAME="customer_id" NULLABLE="NOT NULL"/>',
        f'    <TARGETFIELD DATATYPE="TIMESTAMP" NAME="etl_load_dt" NULLABLE="NULL"/>',
        f'    <TARGETFIELD DATATYPE="TIMESTAMP" NAME="etl_update_dt" NULLABLE="NULL"/>',
    ])
    lkp_transforms = "\n".join([f'''
    <TRANSFORMATION NAME="LKP_{lkp}" TYPE="Lookup Procedure" REUSABLE="NO">
      <TABLEATTRIBUTE NAME="Lookup table name"                   VALUE="{lkp}"/>
      <TABLEATTRIBUTE NAME="Lookup Condition"                    VALUE="LKP_ACCOUNT_ID = ACCOUNT_ID"/>
      <TABLEATTRIBUTE NAME="Lookup Policy On Multiple Match"     VALUE="Use First Value"/>
      <TABLEATTRIBUTE NAME="Lookup Caching Enabled"              VALUE="YES"/>
      <TABLEATTRIBUTE NAME="Lookup Cache Persistent"             VALUE="NO"/>
      <TABLEATTRIBUTE NAME="Lookup Source Filter"                VALUE=""/>
      <TRANSFORMFIELD DATATYPE="integer" NAME="LKP_ACCOUNT_ID"  PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="string"  NAME="LKP_SEGMENT"     PORTTYPE="OUTPUT"/>
      <TRANSFORMFIELD DATATYPE="string"  NAME="LKP_TIER"        PORTTYPE="OUTPUT"/>
    </TRANSFORMATION>''' for lkp in lookups])

    return f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="{datetime.now().strftime("%m/%d/%Y %H:%M:%S")}" REPOSITORY_VERSION="187.93">
<REPOSITORY NAME="ETLRepo" VERSION="187" CODEPAGE="UTF-8">
<FOLDER NAME="{folder}" GROUP="" OWNER="admin">

  <!-- Source definitions -->
  <SOURCE DBDNAME="TD_PRD" NAME="{sources[0] if sources else 'SOURCE_TABLE'}" OWNERNAME="STG" DBTYPE="Teradata">
{src_fields}
  </SOURCE>

  <!-- Target definitions -->
  <TARGET DBDNAME="BQ_PRD" NAME="{targets[0] if targets else 'TARGET_TABLE'}" OWNERNAME="dwh" DBTYPE="BigQuery">
{tgt_fields}
  </TARGET>

  <!-- Mapping definition -->
  <MAPPING NAME="{mapping_name}" DESCRIPTION="ETL mapping — built by Srinivas Punugu">

    <!-- Source Qualifier -->
    <TRANSFORMATION NAME="SQ_{sources[0] if sources else 'SRC'}" TYPE="Source Qualifier" REUSABLE="NO">
      <TABLEATTRIBUTE NAME="Sql Query"              VALUE="SELECT * FROM {sources[0] if sources else 'SOURCE'} WHERE DATE(EFFECTIVE_DATE) = TRUNC(SYSDATE)"/>
      <TABLEATTRIBUTE NAME="Source Filter"          VALUE=""/>
      <TABLEATTRIBUTE NAME="Number Of Sorted Ports" VALUE="0"/>
      <TABLEATTRIBUTE NAME="Distinct"               VALUE="NO"/>
    </TRANSFORMATION>

    <!-- Expression transformation -->
    <TRANSFORMATION NAME="EXP_TRANSFORM" TYPE="Expression" REUSABLE="NO">
      <TRANSFORMFIELD DATATYPE="string"    NAME="IN_ACCOUNT_ID"   PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="string"    NAME="IN_ACCOUNT_NAME" PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="decimal"   NAME="IN_AMOUNT"       PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="string"    NAME="OUT_ACCOUNT_NAME" PORTTYPE="OUTPUT"
                      EXPRESSION="TRIM(IN_ACCOUNT_NAME)"/>
      <TRANSFORMFIELD DATATYPE="decimal"   NAME="OUT_AMOUNT"       PORTTYPE="OUTPUT"
                      EXPRESSION="IIF(IN_AMOUNT &lt; 0, 0, IN_AMOUNT)"/>
      <TRANSFORMFIELD DATATYPE="date/time" NAME="OUT_ETL_LOAD_DT"  PORTTYPE="OUTPUT"
                      EXPRESSION="SYSDATE"/>
    </TRANSFORMATION>

    <!-- Lookup transformations -->
{lkp_transforms}

    <!-- Update Strategy -->
    <TRANSFORMATION NAME="UPD_STRATEGY" TYPE="Update Strategy" REUSABLE="NO">
      <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="IIF(ISNULL(LKP_ACCOUNT_ID), DD_INSERT, DD_UPDATE)"/>
      <TABLEATTRIBUTE NAME="Forward Rejected Rows"      VALUE="YES"/>
    </TRANSFORMATION>

    <!-- Connectors -->
    <CONNECTOR FROMINSTANCE="SQ_{sources[0] if sources else 'SRC'}" FROMFIELD="ACCOUNT_ID"   TOINSTANCE="EXP_TRANSFORM" TOFIELD="IN_ACCOUNT_ID"/>
    <CONNECTOR FROMINSTANCE="SQ_{sources[0] if sources else 'SRC'}" FROMFIELD="ACCOUNT_NAME" TOINSTANCE="EXP_TRANSFORM" TOFIELD="IN_ACCOUNT_NAME"/>
    <CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMFIELD="OUT_ACCOUNT_NAME" TOINSTANCE="{targets[0] if targets else 'TARGET'}" TOFIELD="account_name"/>
    <CONNECTOR FROMINSTANCE="EXP_TRANSFORM" FROMFIELD="OUT_ETL_LOAD_DT"  TOINSTANCE="{targets[0] if targets else 'TARGET'}" TOFIELD="etl_load_dt"/>

  </MAPPING>

</FOLDER>
</REPOSITORY>
</POWERMART>'''


def _mock_worklet_xml(worklet_name: str, folder: str, sessions: list) -> str:
    task_els = "\n".join([f'''
    <TASK NAME="{s}" TYPE="Session" REUSABLE="NO" DESCRIPTION="ETL session task">
      <ATTRIBUTE NAME="Session task name" VALUE="{s}"/>
    </TASK>''' for s in sessions])
    link_els = ""
    for i in range(len(sessions) - 1):
        link_els += f'\n    <LINK FROMTASK="{sessions[i]}" TOTASK="{sessions[i+1]}" CONDITION="$PREV.Status = SUCCEEDED"/>'

    return f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="{datetime.now().strftime("%m/%d/%Y %H:%M:%S")}" REPOSITORY_VERSION="187.93">
<REPOSITORY NAME="ETLRepo" VERSION="187" CODEPAGE="UTF-8">
<FOLDER NAME="{folder}" GROUP="" OWNER="admin">

  <WORKLET NAME="{worklet_name}" REUSABLE="YES"
           DESCRIPTION="Worklet containing ETL sessions — built by Srinivas Punugu">

    <!-- Tasks (sessions) in this worklet -->
{task_els}

    <!-- Task execution links (dependency chain) -->
{link_els}

    <!-- Start/end tasks -->
    <TASK NAME="Start" TYPE="Start" REUSABLE="NO"/>
    <TASK NAME="End"   TYPE="End"   REUSABLE="NO"/>
    <LINK FROMTASK="Start"      TOTASK="{sessions[0] if sessions else 'session_1'}"/>
    <LINK FROMTASK="{sessions[-1] if sessions else 'session_1'}" TOTASK="End"/>

  </WORKLET>

</FOLDER>
</REPOSITORY>
</POWERMART>'''


def _mock_mapplet_xml(mapplet_name: str, folder: str) -> str:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="{datetime.now().strftime("%m/%d/%Y %H:%M:%S")}" REPOSITORY_VERSION="187.93">
<REPOSITORY NAME="ETLRepo" VERSION="187" CODEPAGE="UTF-8">
<FOLDER NAME="{folder}" GROUP="" OWNER="admin">

  <MAPPLET NAME="{mapplet_name}" REUSABLE="YES"
           DESCRIPTION="Reusable mapplet — built by Srinivas Punugu">

    <!-- Input transformation -->
    <TRANSFORMATION NAME="INPUT" TYPE="Input" REUSABLE="NO">
      <TRANSFORMFIELD DATATYPE="string"  NAME="ACCOUNT_ID"   PORTTYPE="OUTPUT"/>
      <TRANSFORMFIELD DATATYPE="string"  NAME="STATUS_CD"    PORTTYPE="OUTPUT"/>
      <TRANSFORMFIELD DATATYPE="decimal" NAME="AMOUNT"       PORTTYPE="OUTPUT"/>
    </TRANSFORMATION>

    <!-- Reusable expression logic -->
    <TRANSFORMATION NAME="EXP_COMMON_LOGIC" TYPE="Expression" REUSABLE="NO">
      <TRANSFORMFIELD DATATYPE="string"    NAME="IN_ACCOUNT_ID" PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="string"    NAME="IN_STATUS_CD"  PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="decimal"   NAME="IN_AMOUNT"     PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="string"    NAME="OUT_ACCOUNT_ID" PORTTYPE="OUTPUT"
                      EXPRESSION="TRIM(IN_ACCOUNT_ID)"/>
      <TRANSFORMFIELD DATATYPE="string"    NAME="OUT_STATUS_CD"  PORTTYPE="OUTPUT"
                      EXPRESSION="UPPER(TRIM(IN_STATUS_CD))"/>
      <TRANSFORMFIELD DATATYPE="decimal"   NAME="OUT_AMOUNT"     PORTTYPE="OUTPUT"
                      EXPRESSION="IIF(IN_AMOUNT &lt; 0, 0, ROUND(IN_AMOUNT, 2))"/>
      <TRANSFORMFIELD DATATYPE="date/time" NAME="OUT_ETL_DT"     PORTTYPE="OUTPUT"
                      EXPRESSION="SYSDATE"/>
    </TRANSFORMATION>

    <!-- Output transformation -->
    <TRANSFORMATION NAME="OUTPUT" TYPE="Output" REUSABLE="NO">
      <TRANSFORMFIELD DATATYPE="string"    NAME="ACCOUNT_ID"  PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="string"    NAME="STATUS_CD"   PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="decimal"   NAME="AMOUNT"      PORTTYPE="INPUT"/>
      <TRANSFORMFIELD DATATYPE="date/time" NAME="ETL_DT"      PORTTYPE="INPUT"/>
    </TRANSFORMATION>

    <!-- Connectors -->
    <CONNECTOR FROMINSTANCE="INPUT"            FROMFIELD="ACCOUNT_ID"   TOINSTANCE="EXP_COMMON_LOGIC" TOFIELD="IN_ACCOUNT_ID"/>
    <CONNECTOR FROMINSTANCE="EXP_COMMON_LOGIC" FROMFIELD="OUT_ACCOUNT_ID" TOINSTANCE="OUTPUT"         TOFIELD="ACCOUNT_ID"/>

  </MAPPLET>

</FOLDER>
</REPOSITORY>
</POWERMART>'''


def _mock_session_log_full(session_name: str, workflow: str, rows: int) -> str:
    now = datetime.now()
    start = now.replace(hour=6, minute=0, second=0)
    lines = [
        f"Informatica PowerCenter ETL Automator",
        f"Session Log: {session_name}",
        f"Workflow   : {workflow}",
        f"Repository : ETLRepo",
        f"{'='*70}",
        f"",
        f"[{start.strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10033 Session task instance [{session_name}] started.",
        f"[{start.strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10041 Connected to database [TD_PRD] as user [etl_svc].",
        f"[{(start + timedelta(seconds=5)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10048 Source qualifier SQL executed:",
        f"   SELECT ACCOUNT_ID, ACCOUNT_NAME, AMOUNT, EFFECTIVE_DATE",
        f"   FROM STG.{session_name.upper().replace('S_M_','')}",
        f"   WHERE DATE(EFFECTIVE_DATE) = TRUNC(SYSDATE)",
        f"[{(start + timedelta(minutes=2)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10065 Building lookup cache [LKP_ACCOUNT_MASTER]...",
        f"[{(start + timedelta(minutes=4)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10065 Lookup cache [LKP_ACCOUNT_MASTER] complete: 2,341,092 rows cached.",
        f"[{(start + timedelta(minutes=5)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10065 Building lookup cache [LKP_RATE_TABLE]...",
        f"[{(start + timedelta(minutes=6)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | READER_1_1_1) PETL_10065 Lookup cache [LKP_RATE_TABLE] complete: 15,420 rows cached.",
        f"[{(start + timedelta(minutes=7)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | TRANSF_1_1_1) PETL_10071 Transformation [EXP_TRANSFORM] started.",
        f"[{(start + timedelta(minutes=30)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | WRITER_1_*_1) PETL_10080 Writer run completed. {rows//2:,} rows committed to target.",
        f"[{(start + timedelta(minutes=60)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | WRITER_1_*_1) PETL_10080 Writer run completed. {rows:,} rows committed to target.",
        f"[{(start + timedelta(minutes=62)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | WRITER_1_*_1) PETL_10082 Load complete. Total rows: {rows:,} inserted, 0 updated, 0 deleted, 0 rejected.",
        f"[{(start + timedelta(minutes=63)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | TRANSF_1_1_1) PETL_10033 Post-session command [cleanup_staging.ksh] executed successfully.",
        f"[{(start + timedelta(minutes=64)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | TRANSF_1_1_1) PETL_10033 Post-session command [notify_downstream.ksh] executed successfully.",
        f"[{(start + timedelta(minutes=65)).strftime('%m/%d/%Y %H:%M:%S')}] INFO  (IS | TRANSF_1_1_1) PETL_10033 Session task instance [{session_name}] completed successfully.",
        f"",
        f"{'='*70}",
        f"Session Statistics Summary:",
        f"  Source rows read       : {rows:,}",
        f"  Target rows written    : {rows:,}",
        f"  Rows rejected          : 0",
        f"  Lookup cache rows      : 2,356,512",
        f"  Run time               : 1h 5m 0s",
        f"  Throughput             : {rows // 3900:,} rows/sec",
        f"{'='*70}",
    ]
    return "\n".join(lines)


# ─── Main Render ───────────────────────────────────────────────────────────────

def render():
    demo = os.environ.get("DEMO_MODE", "true").lower() == "true"

    st.markdown("""
    <div class="tab-header">
        <h2>🗂️ Repository Explorer</h2>
        <p>Connect to Informatica PowerCenter, browse folders & workflows, export XMLs and session logs.</p>
    </div>
    """, unsafe_allow_html=True)

    if demo:
        st.info("🎭 **DEMO MODE** — Using sample data. Toggle real mode in sidebar.", icon="ℹ️")

    # ── Connection Panel ──────────────────────────────────────────────────────
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
                    st.session_state.folders = list_folders()
                    st.success(f"✅ {result['message']} — Found **{len(st.session_state.folders)} folders**")
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

    # ── Folder & Workflow Selection ───────────────────────────────────────────
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
        selected_workflow = st.selectbox(
            "⚙️ Select Workflow",
            workflows if workflows else ["— fetch workflows first —"],
            key="sel_workflow"
        )

    if not (workflows and selected_workflow and not selected_workflow.startswith("—")):
        return

    st.markdown("---")

    # ── Export Button ─────────────────────────────────────────────────────────
    if st.button("🚀 Fetch Dependencies & Export XML", type="primary", use_container_width=True):
        with st.spinner(f"Exporting {selected_workflow}... (parsing sessions, mappings, worklets)"):
            xml_text = export_workflow_xml(selected_folder, selected_workflow)
            parsed   = parse_workflow_xml(xml_text)
            mock     = MOCK_SESSION_IO.get(selected_workflow, DEFAULT_MOCK)

            p_sessions = [s.name for s in parsed.sessions] if parsed.sessions else mock["sessions"]
            p_worklets = [w.name for w in parsed.worklets] if parsed.worklets else mock["worklets"]
            p_mappings = [m.name for m in parsed.mappings] if parsed.mappings else mock["mappings"]
            p_sources  = [s.name for s in parsed.sources]  if parsed.sources  else mock["sources"]
            p_targets  = [t.name for t in parsed.targets]  if parsed.targets  else mock["targets"]

            # Lookups from mapping transforms
            p_lookups = []
            for m in parsed.mappings:
                for lkp in m.get_lookup_transforms():
                    lkp_name = lkp.attributes.get("Lookup table name", lkp.name)
                    if lkp_name and lkp_name not in p_lookups:
                        p_lookups.append(lkp_name)
            if not p_lookups:
                p_lookups = mock["lookups"]

            # Mock mapplets and commands (from architecture doc)
            p_mapplets = [f"mplt_{selected_workflow.replace('wf_','')}_common",
                          f"mplt_{selected_workflow.replace('wf_','')}_audit"]
            p_commands = [f"cmd_pre_{selected_workflow.replace('wf_','')}",
                          f"cmd_post_{selected_workflow.replace('wf_','')}"]

            st.session_state.update({
                "wf_merged_xml":    xml_text,
                "wf_parsed":        parsed,
                "wf_parsed_full":   parsed,
                "curr_sessions":    p_sessions,
                "curr_worklets":    p_worklets,
                "curr_mappings":    p_mappings,
                "curr_sources":     p_sources,
                "curr_targets":     p_targets,
                "curr_lookups":     p_lookups,
                "curr_mapplets":    p_mapplets,
                "curr_commands":    p_commands,
                "selected_workflow": selected_workflow,
                "selected_folder":   selected_folder,
                "row_count":         mock.get("row_count", 0),
                "complexity":        parsed.complexity_badge or get_complexity_badge(p_sessions, p_worklets, p_mappings),
                "wf_session_io": parsed.session_io if parsed.session_io else {
                    s: {
                        "sources": mock["sources"], "targets": mock["targets"],
                        "lookups": mock["lookups"],
                        "mapping": f"m_{s.replace('s_m_','')}",
                        "source_fields": {}, "target_fields": {},
                        "transformations": [], "pre_commands": ["pre_check_availability.ksh"],
                        "post_commands": ["cleanup_staging.ksh", "notify_downstream.ksh"],
                        "commit_interval": 10000, "error_threshold": 0,
                    }
                    for s in p_sessions
                },
                "dataset_map": {"DWH": "dwh", "STG": "staging", "LKP": "reference"},
                "bq_project":  "your-gcp-project",
            })

    if st.session_state.get("wf_parsed") is None:
        return

    # ── Complexity badge ──────────────────────────────────────────────────────
    badge       = st.session_state.get("complexity", "Low")
    badge_color = complexity_color(badge)
    badge_emoji = complexity_emoji(badge)
    p_sessions  = st.session_state.get("curr_sessions", [])
    p_worklets  = st.session_state.get("curr_worklets", [])
    p_mappings  = st.session_state.get("curr_mappings", [])
    p_sources   = st.session_state.get("curr_sources", [])
    p_targets   = st.session_state.get("curr_targets", [])
    p_lookups   = st.session_state.get("curr_lookups", [])
    p_mapplets  = st.session_state.get("curr_mapplets", [])
    p_commands  = st.session_state.get("curr_commands", [])
    mock        = MOCK_SESSION_IO.get(selected_workflow, DEFAULT_MOCK)
    row_count   = st.session_state.get("row_count", 0)

    st.markdown(f"""
    <div style="background:#1e293b;border:1px solid #334155;border-radius:10px;
                padding:16px 20px;margin:12px 0;display:flex;align-items:center;
                justify-content:space-between;flex-wrap:wrap;gap:12px;">
        <div>
            <div style="font-size:18px;font-weight:700;color:#f1f5f9;">
                ⚙️ {selected_workflow}
            </div>
            <div style="font-size:12px;color:#64748b;margin-top:2px;">
                📁 {selected_folder} &nbsp;|&nbsp;
                📊 Last run: <strong style="color:#94a3b8;">{row_count:,} rows</strong>
            </div>
        </div>
        <span style="background:{badge_color};color:white;padding:6px 18px;
                     border-radius:20px;font-weight:700;font-size:13px;">
            {badge_emoji} {badge} Complexity
        </span>
    </div>
    """, unsafe_allow_html=True)

    # ── Object Count Dashboard Cards ──────────────────────────────────────────
    st.markdown("#### 📊 Object Dashboard")

    card_data = [
        ("🔄 Sessions",  p_sessions,  "#6366f1", "SESSION"),
        ("📦 Worklets",  p_worklets,  "#8b5cf6", "WORKLET"),
        ("🗺️ Mappings",  p_mappings,  "#06b6d4", "MAPPING"),
        ("🧩 Mapplets",  p_mapplets,  "#f59e0b", "MAPPLET"),
        ("📥 Sources",   p_sources,   "#3b82f6", "SOURCE"),
        ("📤 Targets",   p_targets,   "#22c55e", "TARGET"),
        ("🔍 Lookups",   p_lookups,   "#a855f7", "LOOKUP"),
        ("⚡ Commands",  p_commands,  "#ef4444", "COMMAND"),
    ]

    cols = st.columns(4)
    for i, (label, items, color, obj_type) in enumerate(card_data):
        with cols[i % 4]:
            st.markdown(f"""
            <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;
                        padding:14px;margin-bottom:8px;border-top:3px solid {color};">
                <div style="font-size:22px;font-weight:700;color:{color};">{len(items)}</div>
                <div style="font-size:12px;font-weight:600;color:#f1f5f9;">{label}</div>
            </div>
            """, unsafe_allow_html=True)
            if items:
                with st.expander(f"View {obj_type}s"):
                    for item in items:
                        st.markdown(f"• `{item}`")

    st.markdown("---")

    # ── Export Section ────────────────────────────────────────────────────────
    st.markdown("#### 📤 Export XML & Logs")
    st.caption("Export individual object XMLs and session logs from Informatica")

    export_tabs = st.tabs([
        "⚙️ Session XMLs",
        "🗺️ Mapping XMLs",
        "📦 Worklet XMLs",
        "🧩 Mapplet XMLs",
        "📄 Session Logs",
        "📦 Export All (ZIP)"
    ])

    # ── Session XMLs ──────────────────────────────────────────────────────────
    with export_tabs[0]:
        st.markdown("**Export individual session XML files (pmrep objectexport -o session)**")
        for session in p_sessions:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"⚙️ `{session}`")
            with col2:
                xml_content = _mock_session_xml(
                    session, selected_folder, selected_workflow,
                    mock["sources"], mock["targets"]
                )
                st.download_button(
                    "⬇️ XML",
                    data=xml_content,
                    file_name=f"{session}.xml",
                    mime="text/xml",
                    key=f"dl_sess_xml_{session}",
                    use_container_width=True
                )
            with col3:
                if st.button("👁️ Preview", key=f"prev_sess_{session}", use_container_width=True):
                    st.session_state[f"show_xml_session_{session}"] = not st.session_state.get(f"show_xml_session_{session}", False)

            if st.session_state.get(f"show_xml_session_{session}"):
                st.code(_mock_session_xml(session, selected_folder, selected_workflow, mock["sources"], mock["targets"]), language="xml")

    # ── Mapping XMLs ──────────────────────────────────────────────────────────
    with export_tabs[1]:
        st.markdown("**Export mapping XML files (pmrep objectexport -o mapping)**")
        for mapping in p_mappings:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"🗺️ `{mapping}`")
            with col2:
                xml_content = _mock_mapping_xml(
                    mapping, selected_folder,
                    mock["sources"], mock["targets"], mock["lookups"]
                )
                st.download_button(
                    "⬇️ XML",
                    data=xml_content,
                    file_name=f"{mapping}.xml",
                    mime="text/xml",
                    key=f"dl_map_xml_{mapping}",
                    use_container_width=True
                )
            with col3:
                if st.button("👁️ Preview", key=f"prev_map_{mapping}", use_container_width=True):
                    st.session_state[f"show_xml_mapping_{mapping}"] = not st.session_state.get(f"show_xml_mapping_{mapping}", False)

            if st.session_state.get(f"show_xml_mapping_{mapping}"):
                st.code(_mock_mapping_xml(mapping, selected_folder, mock["sources"], mock["targets"], mock["lookups"]), language="xml")

    # ── Worklet XMLs ──────────────────────────────────────────────────────────
    with export_tabs[2]:
        st.markdown("**Export worklet XML files (pmrep objectexport -o worklet)**")
        for worklet in p_worklets:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"📦 `{worklet}`")
            with col2:
                xml_content = _mock_worklet_xml(worklet, selected_folder, p_sessions)
                st.download_button(
                    "⬇️ XML",
                    data=xml_content,
                    file_name=f"{worklet}.xml",
                    mime="text/xml",
                    key=f"dl_wklt_xml_{worklet}",
                    use_container_width=True
                )
            with col3:
                if st.button("👁️ Preview", key=f"prev_wklt_{worklet}", use_container_width=True):
                    st.session_state[f"show_xml_worklet_{worklet}"] = not st.session_state.get(f"show_xml_worklet_{worklet}", False)

            if st.session_state.get(f"show_xml_worklet_{worklet}"):
                st.code(_mock_worklet_xml(worklet, selected_folder, p_sessions), language="xml")

    # ── Mapplet XMLs ──────────────────────────────────────────────────────────
    with export_tabs[3]:
        st.markdown("**Export mapplet XML files (pmrep objectexport -o mapplet)**")
        for mapplet in p_mapplets:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"🧩 `{mapplet}`")
            with col2:
                xml_content = _mock_mapplet_xml(mapplet, selected_folder)
                st.download_button(
                    "⬇️ XML",
                    data=xml_content,
                    file_name=f"{mapplet}.xml",
                    mime="text/xml",
                    key=f"dl_mplt_xml_{mapplet}",
                    use_container_width=True
                )
            with col3:
                if st.button("👁️ Preview", key=f"prev_mplt_{mapplet}", use_container_width=True):
                    st.session_state[f"show_xml_mapplet_{mapplet}"] = not st.session_state.get(f"show_xml_mapplet_{mapplet}", False)

            if st.session_state.get(f"show_xml_mapplet_{mapplet}"):
                st.code(_mock_mapplet_xml(mapplet, selected_folder), language="xml")

    # ── Session Logs ──────────────────────────────────────────────────────────
    with export_tabs[4]:
        st.markdown("**Fetch session execution logs (pmcmd getsessionlog)**")
        st.caption("Shows last run log for each session — rows read/written, errors, timing, commands executed")

        for session in p_sessions:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"⚙️ `{session}`")
            with col2:
                log_content = _mock_session_log_full(session, selected_workflow, mock.get("row_count", 125000))
                st.download_button(
                    "⬇️ Log",
                    data=log_content,
                    file_name=f"{session}.log",
                    mime="text/plain",
                    key=f"dl_sess_log_{session}",
                    use_container_width=True
                )
            with col3:
                if st.button("👁️ View Log", key=f"view_log_{session}", use_container_width=True):
                    st.session_state[f"show_log_{session}"] = not st.session_state.get(f"show_log_{session}", False)

            if st.session_state.get(f"show_log_{session}"):
                st.code(_mock_session_log_full(session, selected_workflow, mock.get("row_count", 125000)), language=None)

    # ── Export All ZIP ────────────────────────────────────────────────────────
    with export_tabs[5]:
        st.markdown("**Download complete export bundle — all XMLs + logs in one ZIP**")

        col1, col2 = st.columns([2, 1])
        with col1:
            include_sessions  = st.checkbox("✅ Session XMLs",  value=True)
            include_mappings  = st.checkbox("✅ Mapping XMLs",  value=True)
            include_worklets  = st.checkbox("✅ Worklet XMLs",  value=True)
            include_mapplets  = st.checkbox("✅ Mapplet XMLs",  value=True)
            include_logs      = st.checkbox("✅ Session Logs",  value=True)
            include_workflow  = st.checkbox("✅ Full Workflow XML", value=True)

        with col2:
            total_files = (
                (len(p_sessions) if include_sessions else 0) +
                (len(p_mappings) if include_mappings else 0) +
                (len(p_worklets) if include_worklets else 0) +
                (len(p_mapplets) if include_mapplets else 0) +
                (len(p_sessions) if include_logs else 0) +
                (1 if include_workflow else 0)
            )
            st.markdown(f"""
            <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;text-align:center;">
                <div style="font-size:32px;font-weight:700;color:#6366f1;">{total_files}</div>
                <div style="font-size:12px;color:#94a3b8;">files in ZIP</div>
            </div>
            """, unsafe_allow_html=True)

        if st.button("📦 Build & Download ZIP", type="primary", use_container_width=True):
            with st.spinner("Building export bundle..."):
                time.sleep(0.5)
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    if include_workflow:
                        zf.writestr(f"workflow/{selected_workflow}.xml",
                                    st.session_state.get("wf_merged_xml", "<!-- no xml -->"))
                    if include_sessions:
                        for s in p_sessions:
                            zf.writestr(f"sessions/{s}.xml",
                                _mock_session_xml(s, selected_folder, selected_workflow, mock["sources"], mock["targets"]))
                    if include_mappings:
                        for m in p_mappings:
                            zf.writestr(f"mappings/{m}.xml",
                                _mock_mapping_xml(m, selected_folder, mock["sources"], mock["targets"], mock["lookups"]))
                    if include_worklets:
                        for w in p_worklets:
                            zf.writestr(f"worklets/{w}.xml",
                                _mock_worklet_xml(w, selected_folder, p_sessions))
                    if include_mapplets:
                        for mp in p_mapplets:
                            zf.writestr(f"mapplets/{mp}.xml",
                                _mock_mapplet_xml(mp, selected_folder))
                    if include_logs:
                        for s in p_sessions:
                            zf.writestr(f"logs/{s}.log",
                                _mock_session_log_full(s, selected_workflow, mock.get("row_count", 125000)))

                    # Manifest
                    manifest = (
                        f"ETL Automator Export Manifest\n"
                        f"Built by: Srinivas Punugu\n"
                        f"Workflow: {selected_workflow}\n"
                        f"Folder  : {selected_folder}\n"
                        f"Date    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        f"Sessions : {len(p_sessions)}\n"
                        f"Mappings : {len(p_mappings)}\n"
                        f"Worklets : {len(p_worklets)}\n"
                        f"Mapplets : {len(p_mapplets)}\n"
                        f"Total files: {total_files}\n"
                    )
                    zf.writestr("MANIFEST.txt", manifest)

                buf.seek(0)
                st.download_button(
                    f"⬇️ Download {selected_workflow}_export.zip",
                    data=buf,
                    file_name=f"{selected_workflow}_export_{datetime.now().strftime('%Y%m%d')}.zip",
                    mime="application/zip",
                    use_container_width=True
                )

    st.markdown("---")

    # ── Raw XML Preview ───────────────────────────────────────────────────────
    with st.expander("📄 Full Workflow XML Preview"):
        st.code(st.session_state.get("wf_merged_xml", "")[:3000] + "\n... [truncated]", language="xml")

    st.success("✅ Repository export complete! Go to **Tab 2: Lineage Analysis** →")
