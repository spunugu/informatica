"""Tab 3: Schema Analyzer"""

import streamlit as st
import pandas as pd

MOCK_SCHEMA = {
    "sources": [
        {"column": "ACCOUNT_ID",   "td_type": "VARCHAR(50)",  "nullable": "NOT NULL", "bq_type": "STRING",    "status": "✅ Mapped"},
        {"column": "ACCOUNT_NAME", "td_type": "VARCHAR(100)", "nullable": "NULL",     "bq_type": "STRING",    "status": "✅ Mapped"},
        {"column": "BILL_AMOUNT",  "td_type": "DECIMAL(15,2)","nullable": "NULL",     "bq_type": "NUMERIC",   "status": "✅ Mapped"},
        {"column": "BILL_DATE",    "td_type": "DATE/TIME",    "nullable": "NULL",     "bq_type": "TIMESTAMP", "status": "✅ Mapped"},
        {"column": "CUSTOMER_ID",  "td_type": "INTEGER",      "nullable": "NOT NULL", "bq_type": "INT64",     "status": "✅ Mapped"},
        {"column": "LEGACY_REF_ID","td_type": "CHAR(10)",     "nullable": "NULL",     "bq_type": "STRING",    "status": "⚠️ Deprecated"},
    ],
    "targets": [
        {"column": "account_id",   "bq_type": "STRING",    "mode": "REQUIRED", "description": "Unique account identifier"},
        {"column": "account_name", "bq_type": "STRING",    "mode": "NULLABLE", "description": "Account name"},
        {"column": "bill_amount",  "bq_type": "NUMERIC",   "mode": "NULLABLE", "description": "Billing amount"},
        {"column": "bill_date",    "bq_type": "TIMESTAMP", "mode": "NULLABLE", "description": "Billing timestamp"},
        {"column": "customer_id",  "bq_type": "INT64",     "mode": "REQUIRED", "description": "FK to customer dim"},
        {"column": "etl_load_dt",  "bq_type": "TIMESTAMP", "mode": "NULLABLE", "description": "ETL load timestamp"},
    ]
}

DATASET_SUGGESTIONS = {"STG": "staging", "SRC": "staging", "DWH": "dwh", "LKP": "reference", "REF": "reference", "AGG": "aggregates", "RPT": "reporting"}


def render():
    st.markdown("""<div class="tab-header"><h2>🧬 Schema Analyzer</h2>
    <p>Map Teradata column types to BigQuery equivalents. Flag deprecated columns and type mismatches.</p></div>""", unsafe_allow_html=True)

    if not st.session_state.get("selected_workflow"):
        st.warning("⚠️ Complete **Tab 1** first."); return

    sources = st.session_state.get("curr_sources", [])
    targets = st.session_state.get("curr_targets", [])

    st.markdown("#### 🗂️ Dataset Mapping")
    all_schemas = set()
    for name in sources + targets + st.session_state.get("curr_lookups", []):
        prefix = name.split("_")[0].upper() if "_" in name else name[:3].upper()
        all_schemas.add(prefix)

    cols = st.columns(min(len(all_schemas), 4)) if all_schemas else st.columns(1)
    dataset_map = {}
    for i, schema in enumerate(sorted(all_schemas)):
        with cols[i % len(cols)]:
            dataset_map[schema] = st.text_input(f"`{schema}` → BQ Dataset", value=DATASET_SUGGESTIONS.get(schema, schema.lower()), key=f"ds_{schema}")

    st.markdown("---")
    st.markdown("#### 📥 Source Schema (Teradata → BigQuery)")
    df_src = pd.DataFrame(MOCK_SCHEMA["sources"])
    df_src.columns = ["Column", "Teradata Type", "Nullable", "BigQuery Type", "Status"]

    def color_status(val):
        if "✅" in str(val): return "background-color:#14532d;color:#86efac"
        elif "⚠️" in str(val): return "background-color:#78350f;color:#fcd34d"
        return "background-color:#7f1d1d;color:#fca5a5"

    st.dataframe(df_src.style.map(color_status, subset=["Status"]), use_container_width=True, hide_index=True)

    st.markdown("#### 📤 Target Schema (BigQuery)")
    df_tgt = pd.DataFrame(MOCK_SCHEMA["targets"])
    df_tgt.columns = ["Column", "BQ Type", "Mode", "Description"]
    st.dataframe(df_tgt, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### 🏗️ BigQuery DDL Preview")
    tgt_name = targets[0].lower() if targets else "target_table"
    project = "your-gcp-project"
    dataset = dataset_map.get("DWH", "dwh")
    ddl_lines = [f"CREATE OR REPLACE TABLE `{project}.{dataset}.{tgt_name}` ("]
    for row in MOCK_SCHEMA["targets"]:
        mode = "NOT NULL" if row["mode"] == "REQUIRED" else ""
        ddl_lines.append(f"    {row['column']:25s} {row['bq_type']:12s} {mode},  -- {row['description']}")
    ddl_lines[-1] = ddl_lines[-1].replace(",  --", "  --")
    ddl_lines += [")", f"PARTITION BY DATE(bill_date)", f"CLUSTER BY account_id;"]
    st.code("\n".join(ddl_lines), language="sql")

    st.session_state.dataset_map = dataset_map
    st.session_state.bq_project = project

    col1, col2, col3 = st.columns(3)
    col1.metric("✅ Mapped", sum(1 for r in MOCK_SCHEMA["sources"] if "✅" in r["status"]))
    col2.metric("⚠️ Warnings", sum(1 for r in MOCK_SCHEMA["sources"] if "⚠️" in r["status"]))
    col3.metric("❌ Issues", sum(1 for r in MOCK_SCHEMA["sources"] if "❌" in r["status"]))

    st.success("✅ Schema done! Go to **Tab 4: SQL Converter** →")
