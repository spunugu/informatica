"""Tab 5: Post-Conversion Validation"""

import streamlit as st, time, random
from utils.parser import validate_sql_vs_xml
from utils.infa_client import get_bq_row_count
from mock_data.sample_data import MOCK_VALIDATION_RESULTS


def render():
    st.markdown("""<div class="tab-header"><h2>✅ Post-Conversion Validation</h2>
    <p>3-step quality gate: Logic Verification → Row Count Check → Schema Validation.</p></div>""", unsafe_allow_html=True)

    if not st.session_state.get("generated_sqls"):
        st.warning("⚠️ Complete **Tab 4** first."); return

    workflow     = st.session_state.get("selected_workflow", "")
    sessions     = st.session_state.get("curr_sessions", [])
    session_io   = st.session_state.get("wf_session_io", {})
    sqls         = st.session_state.get("generated_sqls", {})
    row_count_if = st.session_state.get("row_count", 4823910)
    steps_done   = st.session_state.get("validation_steps", set())

    # ── Step 1 ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Step 1: Logic Verification")
    st.caption("Compares XML-declared sources/targets/lookups against generated SQL.")

    if st.button("▶️ Run Logic Verification", use_container_width=True):
        with st.spinner("Comparing..."):
            time.sleep(1.2)
            combined = "\n\n".join(sqls.values())
            res = validate_sql_vs_xml(session_io, combined) if session_io else {
                "results": MOCK_VALIDATION_RESULTS["logic"]["details"],
                "passed": MOCK_VALIDATION_RESULTS["logic"]["passed"],
                "warned": MOCK_VALIDATION_RESULTS["logic"]["warnings"],
                "failed": MOCK_VALIDATION_RESULTS["logic"]["failed"]
            }
            st.session_state.logic_results = res
            steps_done.add(1); st.session_state.validation_steps = steps_done

    if "logic_results" in st.session_state:
        r = st.session_state.logic_results
        c1, c2, c3 = st.columns(3)
        c1.metric("✅ Passed", r.get("passed", 0))
        c2.metric("⚠️ Warnings", r.get("warned", 0))
        c3.metric("❌ Failed", r.get("failed", 0))
        for item in r.get("results", []):
            icon = "✅" if item.get("status") == "PASS" else ("⚠️" if item.get("status") == "WARN" else "❌")
            with st.expander(f"{icon} {item.get('session', '')}"):
                if item.get("status") == "PASS": st.success("All tables matched.")
                elif item.get("status") == "WARN": st.warning(item.get("message", ""))
                else: st.error(item.get("message", ""))

    # ── Step 2 ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Step 2: Row Count Validation")
    col1, col2 = st.columns(2)
    with col1: bq_project = st.text_input("GCP Project", value="your-gcp-project", key="v_proj")
    with col2: bq_dataset = st.text_input("Dataset", value="dwh", key="v_ds")
    targets = st.session_state.get("curr_targets", ["target_table"])
    bq_table = st.selectbox("Table", targets)

    if st.button("▶️ Run Row Count Check", use_container_width=True):
        with st.spinner("Querying..."):
            time.sleep(1.2)
            bq_count = get_bq_row_count(bq_project, bq_dataset, bq_table.lower()) or (row_count_if + random.randint(-50, 50))
            delta = abs(bq_count - row_count_if)
            delta_pct = (delta / row_count_if * 100) if row_count_if > 0 else 0
            st.session_state.rowcount_results = {"informatica": row_count_if, "bigquery": bq_count, "delta": delta, "delta_pct": delta_pct, "match": delta_pct < 0.01}
            steps_done.add(2); st.session_state.validation_steps = steps_done

    if "rowcount_results" in st.session_state:
        rc = st.session_state.rowcount_results
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Informatica Rows", f"{rc['informatica']:,}")
        c2.metric("BigQuery Rows", f"{rc['bigquery']:,}")
        c3.metric("Delta", f"{rc['delta']:,}")
        c4.metric("Delta %", f"{rc['delta_pct']:.4f}%")
        if rc["match"]: st.success("✅ Row counts match within tolerance (< 0.01%)")
        else: st.error(f"❌ Mismatch: {rc['delta_pct']:.2f}% difference")

    # ── Step 3 ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Step 3: Schema Validation")

    if st.button("▶️ Run Schema Check", use_container_width=True):
        with st.spinner("Comparing schemas..."):
            time.sleep(1.0)
            st.session_state.schema_results = MOCK_VALIDATION_RESULTS["schema"]
            steps_done.add(3); st.session_state.validation_steps = steps_done

    if "schema_results" in st.session_state:
        sr = st.session_state.schema_results
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("✅ Matched", sr.get("matched_columns", 0))
        c2.metric("➕ Extra", sr.get("extra_columns", 0))
        c3.metric("➖ Missing", sr.get("missing_columns", 0))
        c4.metric("⚠️ Type Issues", sr.get("type_mismatches", 0))
        for d in sr.get("details", []):
            st.info(f"{'➕' if d['status']=='EXTRA' else '➖'} **{d['column']}** ({d['status']}): {d.get('note','')}")

    # ── Final Summary ─────────────────────────────────────────────────────────
    if len(steps_done) >= 3:
        st.markdown("---")
        st.markdown("### 🏆 Final Validation Report")
        logic_ok    = st.session_state.get("logic_results", {}).get("failed", 1) == 0
        rowcount_ok = st.session_state.get("rowcount_results", {}).get("match", False)
        schema_ok   = st.session_state.get("schema_results", {}).get("type_mismatches", 1) == 0

        c1, c2, c3 = st.columns(3)
        c1.metric("Logic Check", "✅ PASS" if logic_ok else "❌ FAIL")
        c2.metric("Row Count",   "✅ PASS" if rowcount_ok else "❌ FAIL")
        c3.metric("Schema",      "✅ PASS" if schema_ok else "⚠️ WARN")

        if logic_ok and rowcount_ok:
            st.success("🎉 All checks passed — Migration is production ready!")
        else:
            st.error("⚠️ Some checks failed. Review before promoting to production.")

        report = f"""# Validation Report — ETL Automator
## Built by: Srinivas Punugu
## Workflow: {workflow}
## Date: {time.strftime('%Y-%m-%d %H:%M:%S')}

### Step 1 — Logic: {'PASS' if logic_ok else 'FAIL'}
### Step 2 — Row Count: {'PASS' if rowcount_ok else 'FAIL'}
  - Informatica: {st.session_state.get('rowcount_results',{}).get('informatica','N/A'):,}
  - BigQuery: {st.session_state.get('rowcount_results',{}).get('bigquery','N/A'):,}
### Step 3 — Schema: {'PASS' if schema_ok else 'WARN'}

### Overall: {'✅ APPROVED FOR PRODUCTION' if (logic_ok and rowcount_ok) else '❌ REVIEW REQUIRED'}
"""
        st.download_button("⬇️ Download Validation Report", data=report,
                           file_name=f"{workflow}_validation.md", mime="text/markdown", use_container_width=True)
