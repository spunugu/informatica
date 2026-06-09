"""
ETL Automator — Tab 7: CI/CD Pipeline
Built by Srinivas Punugu

Full pipeline:
1. Raise Merge Request (GitLab/GitHub MR)
2. Assign reviewer
3. Self-approve MR (pre-prod)
4. Trigger Jenkins build
5. Deploy SQL files to GCS paths
6. Deploy DAG to Airflow GCS bucket
7. Merge to main branch
8. Pipeline status dashboard
"""

import streamlit as st
import time
import random
import json
from datetime import datetime


# ─── Mock pipeline state helpers ──────────────────────────────────────────────

def _init_pipeline():
    if "pipeline" not in st.session_state:
        st.session_state.pipeline = {
            "mr_id":          None,
            "mr_url":         None,
            "mr_title":       None,
            "mr_status":      None,   # open, approved, merged
            "mr_assignee":    None,
            "mr_reviewer":    None,
            "jenkins_build":  None,
            "jenkins_status": None,   # pending, running, success, failed
            "jenkins_url":    None,
            "gcs_deployed":   False,
            "git_merged":     False,
            "deploy_log":     [],
            "current_step":   0,      # 0=idle,1=mr,2=review,3=approve,4=jenkins,5=gcs,6=merged
        }


def _log(message: str, level: str = "INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    icon = {"INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "❌", "WARN": "⚠️", "RUN": "🔄"}.get(level, "ℹ️")
    st.session_state.pipeline["deploy_log"].append(f"[{ts}] {icon} {message}")


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ─── Step renderers ────────────────────────────────────────────────────────────

def _render_pipeline_status():
    """Visual pipeline progress bar."""
    p = st.session_state.pipeline
    step = p["current_step"]

    steps = [
        ("📝", "Create MR",      1),
        ("👥", "Assign Review",  2),
        ("✅", "Approve MR",     3),
        ("🔨", "Jenkins Build",  4),
        ("☁️", "Deploy GCS",     5),
        ("🚀", "Merge to Main",  6),
    ]

    cols = st.columns(len(steps))
    for col, (icon, label, s) in zip(cols, steps):
        if step > s:
            color = "#22c55e"
            status = "Done"
        elif step == s:
            color = "#6366f1"
            status = "Active"
        else:
            color = "#334155"
            status = "Pending"

        col.markdown(f"""
        <div style="text-align:center;background:{color}22;border:1.5px solid {color};
                    border-radius:8px;padding:10px 4px;">
            <div style="font-size:22px;">{icon}</div>
            <div style="font-size:11px;font-weight:600;color:{'#f1f5f9' if step>=s else '#64748b'};
                        margin-top:4px;">{label}</div>
            <div style="font-size:10px;color:{color};margin-top:2px;">{status}</div>
        </div>
        """, unsafe_allow_html=True)


def _render_step1_create_mr():
    st.markdown("### 📝 Step 1: Create Merge Request")

    workflow  = st.session_state.get("selected_workflow", "wf_billing_daily_load")
    sqls      = st.session_state.get("generated_sqls", {})
    dag       = st.session_state.get("generated_dag", "")
    p         = st.session_state.pipeline

    if p["mr_id"]:
        st.success(f"✅ MR already created — **{p['mr_title']}** ({p['mr_url']})")
        return

    if not sqls:
        st.warning("⚠️ Generate SQL + DAG in **Tab 4** first before raising an MR.")
        return

    col1, col2 = st.columns(2)
    with col1:
        mr_title = st.text_input(
            "MR Title",
            value=f"feat: migrate {workflow} to BigQuery + Airflow",
            key="mr_title_input"
        )
        source_branch = st.text_input("Source Branch", value=f"feature/migrate-{workflow.replace('wf_','')}", key="mr_src")
        target_branch = st.selectbox("Target Branch", ["develop", "pre-prod", "main"], key="mr_tgt")

    with col2:
        mr_desc = st.text_area(
            "MR Description",
            value=f"""## Migration: {workflow}

### What changed
- Generated BigQuery SQL for {len(sqls)} session(s)
- Generated Airflow DAG replacing Informatica scheduler
- Validated row counts and schema

### Sessions migrated
{chr(10).join([f'- `{s}`' for s in sqls.keys()])}

### Checklist
- [x] SQL generated and reviewed
- [x] DAG generated and reviewed
- [x] Row count validation passed
- [x] Schema validation passed
- [ ] Peer review approved
- [ ] Jenkins build passed

Built by: Srinivas Punugu | ETL Automator""",
            height=220,
            key="mr_desc_input"
        )

    col3, col4 = st.columns(2)
    with col3:
        git_repo = st.text_input("Git Repository", value="spunugu/informatica", key="mr_repo")
    with col4:
        labels = st.multiselect(
            "Labels",
            ["migration", "bigquery", "airflow", "etl", "data-engineering", "reviewed"],
            default=["migration", "bigquery", "airflow"],
            key="mr_labels"
        )

    if st.button("📝 Create Merge Request", type="primary", use_container_width=True, key="btn_create_mr"):
        with st.spinner("Creating MR..."):
            time.sleep(1.5)
            mr_id  = random.randint(100, 999)
            mr_url = f"https://github.com/{git_repo}/pull/{mr_id}"

            st.session_state.pipeline.update({
                "mr_id":       mr_id,
                "mr_url":      mr_url,
                "mr_title":    mr_title,
                "mr_status":   "open",
                "current_step": 1,
                "mr_branch":   source_branch,
                "mr_target":   target_branch,
                "mr_repo":     git_repo,
            })

            _log(f"MR #{mr_id} created: {mr_title}", "SUCCESS")
            _log(f"Source: {source_branch} → Target: {target_branch}", "INFO")
            _log(f"Files added: {len(sqls)} SQL files + 1 DAG", "INFO")
            _log(f"MR URL: {mr_url}", "INFO")

            st.success(f"✅ MR #{mr_id} created!")
            st.rerun()


def _render_step2_assign_reviewer():
    st.markdown("### 👥 Step 2: Assign Reviewer")
    p = st.session_state.pipeline

    if not p["mr_id"]:
        st.info("Complete Step 1 first.")
        return

    if p["mr_reviewer"]:
        st.success(f"✅ Assigned to **{p['mr_reviewer']}** for review | Assignee: **{p['mr_assignee']}**")
        return

    # Team members
    team_members = [
        {"name": "Srinivas Punugu",  "role": "Data Engineer (You)",    "avatar": "👨‍💻"},
        {"name": "Rajesh Kumar",     "role": "Senior Data Engineer",   "avatar": "👨‍🔧"},
        {"name": "Priya Sharma",     "role": "ETL Architect",          "avatar": "👩‍💼"},
        {"name": "Ankit Patel",      "role": "BigQuery SME",           "avatar": "👨‍🏫"},
        {"name": "Deepa Nair",       "role": "Data Engineering Lead",  "avatar": "👩‍💻"},
        {"name": "Vikram Singh",     "role": "Airflow SME",            "avatar": "👨‍🚀"},
    ]

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**👤 Assign MR to (owner):**")
        assignee = st.selectbox(
            "Assignee",
            [f"{m['avatar']} {m['name']} — {m['role']}" for m in team_members],
            index=0,
            key="mr_assignee_sel",
            label_visibility="collapsed"
        )
    with col2:
        st.markdown("**👀 Request review from:**")
        reviewer = st.selectbox(
            "Reviewer",
            [f"{m['avatar']} {m['name']} — {m['role']}" for m in team_members],
            index=2,
            key="mr_reviewer_sel",
            label_visibility="collapsed"
        )

    # Show MR diff summary
    sqls = st.session_state.get("generated_sqls", {})
    workflow = st.session_state.get("selected_workflow", "")
    with st.expander("📄 MR Diff Summary"):
        st.markdown(f"**Branch:** `{p.get('mr_branch','feature/migration')}` → `{p.get('mr_target','develop')}`")
        st.markdown(f"**Files changed:** {len(sqls) + 1}")
        for session in sqls.keys():
            st.markdown(f"• ➕ `sql/{session}.sql` — new file")
        dag_name = workflow.replace("wf_", "dag_") + ".py"
        st.markdown(f"• ➕ `dags/{dag_name}` — new file")

    if st.button("👥 Assign & Notify Reviewer", type="primary", use_container_width=True, key="btn_assign"):
        with st.spinner("Assigning and sending notification..."):
            time.sleep(1.0)
            assignee_name = assignee.split("—")[0].strip().split(" ", 1)[1]
            reviewer_name = reviewer.split("—")[0].strip().split(" ", 1)[1]

            st.session_state.pipeline.update({
                "mr_assignee":  assignee_name,
                "mr_reviewer":  reviewer_name,
                "current_step": 2,
            })

            _log(f"MR assigned to: {assignee_name}", "SUCCESS")
            _log(f"Review requested from: {reviewer_name}", "INFO")
            _log(f"Email notification sent to {reviewer_name}", "INFO")
            st.rerun()


def _render_step3_approve_mr():
    st.markdown("### ✅ Step 3: Approve MR (Self-Approve for Pre-Prod)")
    p = st.session_state.pipeline

    if not p["mr_reviewer"]:
        st.info("Complete Step 2 first.")
        return

    if p["mr_status"] == "approved":
        st.success(f"✅ MR #{p['mr_id']} approved by **{p['mr_reviewer']}**")
        return

    # Show MR details
    st.markdown(f"""
    <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;margin:8px 0;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <div style="font-size:15px;font-weight:700;color:#f1f5f9;">
                    🔀 MR #{p['mr_id']}: {p['mr_title']}
                </div>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">
                    {p.get('mr_branch','feature/migration')} → {p.get('mr_target','develop')} &nbsp;|&nbsp;
                    Assignee: {p['mr_assignee']} &nbsp;|&nbsp;
                    Reviewer: {p['mr_reviewer']}
                </div>
            </div>
            <span style="background:#f59e0b;color:#000;padding:4px 12px;border-radius:20px;
                         font-size:11px;font-weight:700;">OPEN</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Review checklist
    st.markdown("**📋 Review Checklist:**")
    checks = {
        "SQL follows BigQuery best practices (partitioning, clustering)": True,
        "MERGE keys correctly defined": True,
        "Lookup joins use LEFT JOIN (not INNER)": True,
        "Audit logging included": True,
        "Airflow DAG has retry logic and SLA": True,
        "No hardcoded credentials or project IDs": True,
        "Row count validation passed (< 0.01% delta)": True,
        "Schema validation passed (no type mismatches)": True,
    }

    all_checked = True
    for check, default in checks.items():
        val = st.checkbox(check, value=default, key=f"chk_{check[:20]}")
        if not val:
            all_checked = False

    # Review comments
    comment = st.text_area(
        "Review Comment",
        value="LGTM ✅ — SQL looks good, DAG structure is clean, row counts match. Approved for pre-prod deploy.",
        key="review_comment",
        height=80
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("✅ Approve MR", type="primary", use_container_width=True,
                     key="btn_approve", disabled=not all_checked):
            with st.spinner("Approving MR..."):
                time.sleep(1.0)
                st.session_state.pipeline.update({
                    "mr_status":    "approved",
                    "current_step": 3,
                })
                _log(f"MR #{p['mr_id']} approved by {p['mr_reviewer']}", "SUCCESS")
                _log(f"Comment: {comment[:80]}", "INFO")
                _log("Pipeline auto-triggered on approval", "INFO")
                st.rerun()
    with col2:
        if st.button("❌ Request Changes", use_container_width=True, key="btn_reject"):
            st.error("Changes requested — fix and re-push.")


def _render_step4_jenkins():
    st.markdown("### 🔨 Step 4: Jenkins CI Build")
    p = st.session_state.pipeline

    if p["mr_status"] != "approved":
        st.info("Complete Step 3 (approve MR) first.")
        return

    if p["jenkins_status"] == "success":
        st.success(f"✅ Jenkins build **#{p['jenkins_build']}** passed!")
        _render_jenkins_result()
        return

    workflow = st.session_state.get("selected_workflow", "")
    sqls     = st.session_state.get("generated_sqls", {})

    # Jenkins config
    col1, col2, col3 = st.columns(3)
    with col1:
        jenkins_url = st.text_input("Jenkins URL", value="https://jenkins.example.com", key="j_url")
    with col2:
        job_name = st.text_input("Job Name", value=f"etl-migrate-{workflow.replace('wf_','')}", key="j_job")
    with col3:
        environment = st.selectbox("Environment", ["pre-prod", "prod"], key="j_env")

    # Pipeline stages preview
    st.markdown("**🔧 Pipeline Stages:**")
    stages = [
        ("📥", "Checkout",          "Clone repo + checkout MR branch"),
        ("🔍", "Lint & Validate",   "SQL lint, DAG syntax check, unit tests"),
        ("🧪", "Integration Tests", "BQ dry-run, Airflow DAG parse test"),
        ("☁️", "Upload to GCS",     f"Push SQL + DAG to gs://your-etl-bucket/"),
        ("📋", "Update Registry",   "Register migration in ETL catalog"),
        ("🔔", "Notify",            "Slack + email on success/failure"),
    ]

    stage_cols = st.columns(len(stages))
    for col, (icon, name, desc) in zip(stage_cols, stages):
        col.markdown(f"""
        <div style="background:#1e293b;border:1px solid #334155;border-radius:6px;
                    padding:8px;text-align:center;font-size:11px;">
            <div style="font-size:18px;">{icon}</div>
            <div style="font-weight:600;color:#f1f5f9;margin-top:4px;">{name}</div>
            <div style="color:#64748b;margin-top:2px;font-size:10px;">{desc[:30]}...</div>
        </div>
        """, unsafe_allow_html=True)

    if st.button("🔨 Trigger Jenkins Build", type="primary", use_container_width=True, key="btn_jenkins"):
        build_num = random.randint(200, 999)
        build_url = f"{jenkins_url}/job/{job_name}/{build_num}"

        st.session_state.pipeline.update({
            "jenkins_build":  build_num,
            "jenkins_url":    build_url,
            "jenkins_status": "running",
            "jenkins_env":    environment,
        })
        _log(f"Jenkins build #{build_num} triggered", "RUN")
        _log(f"Job: {job_name} | Environment: {environment}", "INFO")

        # Simulate build stages
        progress = st.progress(0)
        status_text = st.empty()

        for i, (icon, name, _) in enumerate(stages):
            status_text.markdown(f"**{icon} Running: {name}...**")
            time.sleep(0.8)
            progress.progress((i + 1) / len(stages))
            _log(f"Stage [{name}] completed", "SUCCESS")

        status_text.empty()
        progress.empty()

        st.session_state.pipeline.update({
            "jenkins_status": "success",
            "current_step":   4,
        })
        _log(f"Jenkins build #{build_num} PASSED ✅", "SUCCESS")
        st.rerun()


def _render_jenkins_result():
    p = st.session_state.pipeline
    st.markdown(f"""
    <div style="background:#14532d22;border:1px solid #22c55e;border-radius:8px;padding:12px 16px;">
        <div style="font-size:14px;font-weight:700;color:#22c55e;">
            ✅ Build #{p['jenkins_build']} — SUCCESS
        </div>
        <div style="font-size:12px;color:#64748b;margin-top:4px;">
            Environment: {p.get('jenkins_env','pre-prod')} &nbsp;|&nbsp;
            Duration: ~{random.randint(3,8)}m {random.randint(10,59)}s &nbsp;|&nbsp;
            <a href="{p['jenkins_url']}" style="color:#6366f1;">View Build →</a>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_step5_gcs_deploy():
    st.markdown("### ☁️ Step 5: Deploy to GCS Paths")
    p = st.session_state.pipeline

    if p["jenkins_status"] != "success":
        st.info("Complete Step 4 (Jenkins build) first.")
        return

    if p["gcs_deployed"]:
        st.success("✅ All files deployed to GCS!")
        _render_gcs_paths()
        return

    workflow = st.session_state.get("selected_workflow", "")
    sqls     = st.session_state.get("generated_sqls", {})

    # GCS path config
    st.markdown("**📁 Configure GCS Deployment Paths:**")

    col1, col2 = st.columns(2)
    with col1:
        sql_bucket  = st.text_input("SQL Files Bucket",
            value="gs://your-etl-bucket", key="gcs_sql_bucket")
        sql_path    = st.text_input("SQL Path",
            value=f"sql/{workflow}/", key="gcs_sql_path")
        dag_bucket  = st.text_input("Airflow DAG Bucket",
            value="gs://your-composer-bucket", key="gcs_dag_bucket")
        dag_path    = st.text_input("DAG Path",
            value="dags/", key="gcs_dag_path")
    with col2:
        git_sql_path = st.text_input("Git SQL Path",
            value=f"migrations/sql/{workflow}/", key="git_sql_path")
        git_dag_path = st.text_input("Git DAG Path",
            value="airflow/dags/", key="git_dag_path")
        env_tag      = st.selectbox("Deploy Tag",
            ["pre-prod", "prod", "staging"], key="gcs_env")
        auto_merge   = st.toggle("Auto-merge to main after deploy", value=True, key="gcs_auto_merge")

    # Files to deploy
    st.markdown("**📋 Files to Deploy:**")
    deploy_files = []
    for session in sqls.keys():
        deploy_files.append({
            "file": f"{session}.sql",
            "gcs":  f"{sql_bucket}/{sql_path}{session}.sql",
            "git":  f"{git_sql_path}{session}.sql",
            "type": "SQL"
        })

    dag_name = workflow.replace("wf_", "dag_") + ".py"
    deploy_files.append({
        "file": dag_name,
        "gcs":  f"{dag_bucket}/{dag_path}{dag_name}",
        "git":  f"{git_dag_path}{dag_name}",
        "type": "DAG"
    })

    for f in deploy_files:
        icon = "📄" if f["type"] == "SQL" else "🌊"
        st.markdown(f"""
        <div style="background:#1e293b;border:1px solid #334155;border-radius:6px;
                    padding:8px 12px;margin:4px 0;display:flex;justify-content:space-between;
                    align-items:center;font-size:12px;">
            <span style="color:#f1f5f9;">{icon} <strong>{f['file']}</strong></span>
            <span style="color:#6366f1;font-family:monospace;font-size:11px;">{f['gcs']}</span>
        </div>
        """, unsafe_allow_html=True)

    if st.button("☁️ Deploy to GCS + Git", type="primary", use_container_width=True, key="btn_gcs"):
        progress = st.progress(0)
        status   = st.empty()

        for i, f in enumerate(deploy_files):
            status.markdown(f"**☁️ Uploading `{f['file']}` to GCS...**")
            time.sleep(0.6)
            _log(f"Uploaded: {f['gcs']}", "SUCCESS")
            progress.progress((i + 1) / (len(deploy_files) * 2))

        for i, f in enumerate(deploy_files):
            status.markdown(f"**📁 Committing `{f['file']}` to Git...**")
            time.sleep(0.4)
            _log(f"Git push: {f['git']}", "SUCCESS")
            progress.progress(0.5 + (i + 1) / (len(deploy_files) * 2))

        progress.progress(1.0)
        status.empty()
        progress.empty()

        st.session_state.pipeline.update({
            "gcs_deployed":  True,
            "gcs_paths":     deploy_files,
            "current_step":  5,
            "auto_merge":    auto_merge,
            "gcs_sql_full":  f"{sql_bucket}/{sql_path}",
            "gcs_dag_full":  f"{dag_bucket}/{dag_path}",
        })
        _log(f"All {len(deploy_files)} files deployed to GCS", "SUCCESS")
        _log(f"All {len(deploy_files)} files committed to Git", "SUCCESS")
        st.rerun()


def _render_gcs_paths():
    p = st.session_state.pipeline
    paths = p.get("gcs_paths", [])
    for f in paths:
        icon = "📄" if f["type"] == "SQL" else "🌊"
        st.markdown(f"""
        <div style="background:#14532d22;border:1px solid #22c55e44;border-radius:6px;
                    padding:8px 12px;margin:4px 0;font-size:12px;">
            {icon} <code style="color:#86efac;">{f['gcs']}</code>
        </div>
        """, unsafe_allow_html=True)


def _render_step6_merge():
    st.markdown("### 🚀 Step 6: Merge to Main")
    p = st.session_state.pipeline

    if not p["gcs_deployed"]:
        st.info("Complete Step 5 (GCS deploy) first.")
        return

    if p["git_merged"]:
        st.success(f"✅ MR #{p['mr_id']} merged to **main** branch!")
        return

    workflow  = st.session_state.get("selected_workflow", "")
    sqls      = st.session_state.get("generated_sqls", {})
    auto_merge = p.get("auto_merge", True)

    # Final summary
    st.markdown(f"""
    <div style="background:#1e293b;border:1px solid #334155;border-radius:10px;padding:20px;margin:8px 0;">
        <div style="font-size:16px;font-weight:700;color:#f1f5f9;margin-bottom:12px;">
            🔀 MR #{p['mr_id']} Ready to Merge
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;">
            <div><span style="color:#64748b;">Title:</span>
                 <span style="color:#f1f5f9;">{p['mr_title']}</span></div>
            <div><span style="color:#64748b;">Branch:</span>
                 <span style="color:#6366f1;">{p.get('mr_branch','')} → main</span></div>
            <div><span style="color:#64748b;">Approved by:</span>
                 <span style="color:#22c55e;">✅ {p['mr_reviewer']}</span></div>
            <div><span style="color:#64748b;">Jenkins:</span>
                 <span style="color:#22c55e;">✅ Build #{p['jenkins_build']} passed</span></div>
            <div><span style="color:#64748b;">GCS Deploy:</span>
                 <span style="color:#22c55e;">✅ {len(sqls)+1} files deployed</span></div>
            <div><span style="color:#64748b;">Files:</span>
                 <span style="color:#f1f5f9;">{len(sqls)} SQL + 1 DAG</span></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if auto_merge:
        st.info("🤖 Auto-merge is ON — MR will merge automatically after all checks pass.")

    col1, col2 = st.columns(2)
    with col1:
        merge_strategy = st.selectbox(
            "Merge Strategy",
            ["Squash and merge", "Merge commit", "Rebase and merge"],
            key="merge_strategy"
        )
    with col2:
        delete_branch = st.toggle("Delete source branch after merge", value=True, key="del_branch")

    merge_label = "🤖 Auto-Merge to Main" if auto_merge else "🚀 Merge to Main"
    if st.button(merge_label, type="primary", use_container_width=True, key="btn_merge"):
        with st.spinner("Merging to main..."):
            time.sleep(1.5)
            st.session_state.pipeline.update({
                "git_merged":   True,
                "current_step": 6,
            })
            _log(f"MR #{p['mr_id']} merged to main ({merge_strategy})", "SUCCESS")
            if delete_branch:
                _log(f"Branch {p.get('mr_branch','')} deleted", "INFO")
            _log("🎉 Migration pipeline complete!", "SUCCESS")
            st.rerun()


# ─── Main render ──────────────────────────────────────────────────────────────

def render():
    _init_pipeline()
    p = st.session_state.pipeline

    st.markdown("""
    <div class="tab-header">
        <h2>🚀 CI/CD Pipeline</h2>
        <p>Raise MR → Assign reviewer → Approve → Jenkins build → Deploy to GCS → Merge to main.</p>
    </div>
    """, unsafe_allow_html=True)

    if not st.session_state.get("generated_sqls"):
        st.warning("⚠️ Complete **Tab 4: SQL Converter** first to generate SQL + DAG files.")
        return

    # Pipeline status bar
    _render_pipeline_status()
    st.markdown("---")

    # If complete — show celebration
    if p["git_merged"]:
        workflow = st.session_state.get("selected_workflow", "")
        sqls     = st.session_state.get("generated_sqls", {})
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#14532d,#166534);border:2px solid #22c55e;
                    border-radius:12px;padding:24px;text-align:center;margin:16px 0;">
            <div style="font-size:40px;">🎉</div>
            <div style="font-size:22px;font-weight:700;color:#f0fdf4;margin-top:8px;">
                Migration Complete!
            </div>
            <div style="font-size:14px;color:#86efac;margin-top:8px;">
                {workflow} has been successfully migrated to BigQuery + Airflow
            </div>
            <div style="font-size:13px;color:#4ade80;margin-top:12px;">
                ✅ MR #{p['mr_id']} merged to main &nbsp;|&nbsp;
                ✅ {len(sqls)} SQL files deployed to GCS &nbsp;|&nbsp;
                ✅ Airflow DAG live &nbsp;|&nbsp;
                ✅ Jenkins build #{p['jenkins_build']} passed
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Step sections — always show, disabled if not reached yet
    with st.expander("📝 Step 1: Create Merge Request",
                     expanded=p["current_step"] == 0):
        _render_step1_create_mr()

    with st.expander("👥 Step 2: Assign Reviewer",
                     expanded=p["current_step"] == 1):
        _render_step2_assign_reviewer()

    with st.expander("✅ Step 3: Approve MR",
                     expanded=p["current_step"] == 2):
        _render_step3_approve_mr()

    with st.expander("🔨 Step 4: Trigger Jenkins Build",
                     expanded=p["current_step"] == 3):
        _render_step4_jenkins()

    with st.expander("☁️ Step 5: Deploy to GCS + Git",
                     expanded=p["current_step"] == 4):
        _render_step5_gcs_deploy()

    with st.expander("🚀 Step 6: Merge to Main",
                     expanded=p["current_step"] == 5):
        _render_step6_merge()

    st.markdown("---")

    # Deploy log
    st.markdown("#### 📋 Pipeline Log")
    if p["deploy_log"]:
        log_text = "\n".join(p["deploy_log"])
        st.code(log_text, language=None)
    else:
        st.caption("Pipeline log will appear here as steps complete.")

    # Reset
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔄 Reset Pipeline", use_container_width=True, key="btn_reset_pipeline"):
            st.session_state.pipeline = {}
            _init_pipeline()
            st.rerun()
