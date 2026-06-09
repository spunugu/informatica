"""
ETL Automator — Tab 0: Configuration Settings
Built by Srinivas Punugu

Full configuration for:
- Informatica PowerCenter connection
- Teradata (source) connection
- BigQuery (target) connection
- Google Cloud Storage paths
- Git repository settings
- Jenkins CI/CD settings
- Airflow / Cloud Composer settings
- Notification settings (email, Slack)
- Demo Mode toggle

All defaults pre-filled based on architecture.
Real mode: user fills in actual credentials.
"""

import streamlit as st
import os
import json


# ─── Default config values (from architecture doc) ────────────────────────────

DEFAULTS = {
    # Informatica
    "infa_host":       "infa-repo.example.com",
    "infa_port":       "6005",
    "infa_repo":       "ETLRepo",
    "infa_domain":     "Domain_ETL",
    "infa_user":       "admin",
    "infa_password":   "",
    "infa_bin_path":   r"C:\ProgramData\Informatica\10.5.3\clients\PowerCenterClient\CommandLineUtilities\PC\server\bin",
    "infa_version":    "10.5.3",

    # Teradata
    "td_host":         "td-prd.example.com",
    "td_port":         "1025",
    "td_database":     "VZ_STG",
    "td_user":         "etl_svc",
    "td_password":     "",
    "td_logon_mech":   "LDAP",
    "td_charset":      "UTF16",

    # BigQuery
    "bq_project":      "your-gcp-project",
    "bq_location":     "US",
    "bq_dwh_dataset":  "vz_dwh",
    "bq_stg_dataset":  "vz_staging",
    "bq_ref_dataset":  "vz_reference",
    "bq_audit_dataset":"vz_audit",
    "bq_sa_key_path":  "/secrets/bq-service-account.json",
    "bq_billing_project": "your-billing-project",

    # GCS Paths
    "gcs_sql_bucket":  "gs://your-etl-bucket",
    "gcs_sql_prefix":  "sql/",
    "gcs_dag_bucket":  "gs://your-composer-bucket",
    "gcs_dag_prefix":  "dags/",
    "gcs_archive":     "gs://your-etl-bucket/archive/",
    "gcs_logs":        "gs://your-etl-bucket/logs/",
    "gcs_params":      "gs://your-etl-bucket/params/",

    # Git
    "git_host":        "https://github.com",
    "git_org":         "spunugu",
    "git_repo":        "informatica",
    "git_token":       "",
    "git_sql_path":    "migrations/sql/",
    "git_dag_path":    "airflow/dags/",
    "git_config_path": "config/",
    "git_main_branch": "main",
    "git_dev_branch":  "develop",

    # Jenkins
    "jenkins_url":     "https://jenkins.example.com",
    "jenkins_user":    "admin",
    "jenkins_token":   "",
    "jenkins_job":     "etl-migration-pipeline",
    "jenkins_cred_id": "gcp-service-account",

    # Airflow / Cloud Composer
    "composer_env":    "etl-composer-env",
    "composer_loc":    "us-central1",
    "composer_project":"your-gcp-project",
    "airflow_dag_bucket": "gs://your-composer-bucket/dags/",

    # Notification
    "notify_email":    "srinivas.punugu@example.com",
    "slack_webhook":   "",
    "slack_channel":   "#etl-migrations",

    # Pipeline
    "default_partition_col":   "etl_load_dt",
    "default_load_strategy":   "MERGE",
    "default_commit_interval": "10000",
    "default_error_threshold": "0",
    "default_parallelism":     "4",
}


def _load_config():
    """Load config from session state or defaults."""
    if "app_config" not in st.session_state:
        st.session_state.app_config = DEFAULTS.copy()
    return st.session_state.app_config


def _save_config(cfg: dict):
    st.session_state.app_config = cfg
    # Push key values to session state for use by other tabs
    st.session_state.bq_project  = cfg["bq_project"]
    st.session_state.dataset_map = {
        "DWH": cfg["bq_dwh_dataset"],
        "STG": cfg["bq_stg_dataset"],
        "LKP": cfg["bq_ref_dataset"],
        "AUD": cfg["bq_audit_dataset"],
    }
    os.environ["DEMO_MODE"] = "true" if st.session_state.get("demo_mode", True) else "false"


def _status_badge(connected: bool) -> str:
    if connected:
        return '<span style="background:#14532d;color:#86efac;padding:2px 10px;border-radius:10px;font-size:11px;font-weight:700;">✅ Connected</span>'
    return '<span style="background:#7f1d1d;color:#fca5a5;padding:2px 10px;border-radius:10px;font-size:11px;font-weight:700;">⚪ Not tested</span>'


def render():
    cfg = _load_config()
    demo = st.session_state.get("demo_mode", True)

    st.markdown("""
    <div class="tab-header">
        <h2>⚙️ Configuration</h2>
        <p>Configure all connections — Informatica, Teradata, BigQuery, GCS, Git, Jenkins, Airflow.
           Demo mode uses sample data. Real mode uses your actual credentials.</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Demo / Real Mode Toggle ───────────────────────────────────────────────
    col_tog1, col_tog2 = st.columns([1, 2])
    with col_tog1:
        new_demo = st.toggle(
            "🎭 Demo Mode",
            value=demo,
            help="Demo: uses sample data, no credentials needed. Real: connects to actual systems.",
            key="cfg_demo_toggle"
        )
        if new_demo != demo:
            st.session_state.demo_mode = new_demo
            os.environ["DEMO_MODE"] = "true" if new_demo else "false"
            st.rerun()

    with col_tog2:
        if demo:
            st.info("🎭 **Demo Mode ON** — App uses sample Verizon data. No credentials needed. Toggle off to use real systems.", icon="ℹ️")
        else:
            st.warning("⚠️ **Real Mode ON** — Fill in your actual credentials below. All connections will be live.", icon="⚠️")

    st.markdown("---")

    # ── Config sections as tabs ───────────────────────────────────────────────
    cfg_tabs = st.tabs([
        "🔴 Informatica",
        "🟠 Teradata",
        "🔵 BigQuery",
        "☁️ GCS Paths",
        "📁 Git",
        "🔨 Jenkins",
        "🌊 Airflow",
        "🔔 Notifications",
        "⚡ Pipeline Defaults",
    ])

    # ── Informatica ──────────────────────────────────────────────────────────
    with cfg_tabs[0]:
        st.markdown("### 🔴 Informatica PowerCenter")
        st.caption("Connection details for pmrep and pmcmd CLI tools")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["infa_host"]    = st.text_input("Repository Host", value=cfg["infa_host"], key="infa_host")
            cfg["infa_port"]    = st.text_input("Port", value=cfg["infa_port"], key="infa_port")
            cfg["infa_version"] = st.selectbox("Version", ["10.5.3", "10.4.1", "10.2.0", "9.6.1"],
                index=["10.5.3","10.4.1","10.2.0","9.6.1"].index(cfg.get("infa_version","10.5.3")), key="infa_ver")
        with col2:
            cfg["infa_repo"]    = st.text_input("Repository Name", value=cfg["infa_repo"], key="infa_repo")
            cfg["infa_domain"]  = st.text_input("Domain Name", value=cfg["infa_domain"], key="infa_domain")
            cfg["infa_user"]    = st.text_input("Username", value=cfg["infa_user"], key="infa_user")
        with col3:
            cfg["infa_password"] = st.text_input("Password", type="password",
                value=cfg["infa_password"], key="infa_pwd",
                placeholder="pmrep password" if not demo else "demo-mode")
            cfg["infa_bin_path"] = st.text_input("Binary Path", value=cfg["infa_bin_path"], key="infa_bin")

        st.markdown("**pmrep Commands Preview:**")
        st.code(f"""pmrep connect -r {cfg['infa_repo']} -h {cfg['infa_host']} -o {cfg['infa_port']} -n {cfg['infa_user']} -x [password]
pmrep listobjects -o folder
pmrep listobjects -o workflow -f FOLDER_NAME
pmrep objectexport -n WORKFLOW -o workflow -f FOLDER -m -s -b -r
pmcmd getsessionstatistics -wf WORKFLOW -st SESSION
pmcmd getsessionlog -wf WORKFLOW -st SESSION""", language="bash")

        col_test, _ = st.columns([1, 3])
        with col_test:
            if st.button("🔌 Test Informatica Connection", key="test_infa", use_container_width=True):
                with st.spinner("Testing..."):
                    import time; time.sleep(1)
                    if demo:
                        st.success("✅ Connected to ETLRepo (Demo Mode)")
                    else:
                        st.info("Real connection requires Informatica client installed on Windows.")

    # ── Teradata ─────────────────────────────────────────────────────────────
    with cfg_tabs[1]:
        st.markdown("### 🟠 Teradata Source Database")
        st.caption("Source system where Informatica reads data from")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["td_host"]      = st.text_input("Teradata Host", value=cfg["td_host"], key="td_host")
            cfg["td_port"]      = st.text_input("Port", value=cfg["td_port"], key="td_port")
            cfg["td_logon_mech"]= st.selectbox("Logon Mechanism",
                ["LDAP", "TD2", "TDNEGO", "KRB5"],
                index=["LDAP","TD2","TDNEGO","KRB5"].index(cfg.get("td_logon_mech","LDAP")), key="td_logon")
        with col2:
            cfg["td_database"]  = st.text_input("Default Database", value=cfg["td_database"], key="td_db")
            cfg["td_user"]      = st.text_input("Username", value=cfg["td_user"], key="td_user")
            cfg["td_charset"]   = st.selectbox("Character Set",
                ["UTF16", "UTF8", "ASCII", "LATIN"],
                index=["UTF16","UTF8","ASCII","LATIN"].index(cfg.get("td_charset","UTF16")), key="td_charset")
        with col3:
            cfg["td_password"]  = st.text_input("Password", type="password",
                value=cfg["td_password"], key="td_pwd",
                placeholder="Teradata password" if not demo else "demo-mode")

        st.markdown("**JDBC Connection String:**")
        st.code(f"jdbc:teradata://{cfg['td_host']}/DATABASE={cfg['td_database']},LOGMECH={cfg['td_logon_mech']},CHARSET={cfg['td_charset']}", language="text")

        st.markdown("**Teradata → BigQuery Type Mapping:**")
        type_map = {
            "VARCHAR(n)": "STRING", "CHAR(n)": "STRING",
            "INTEGER": "INT64", "BIGINT": "INT64",
            "DECIMAL(p,s)": "NUMERIC", "FLOAT": "FLOAT64",
            "DATE": "DATE", "DATE/TIME": "TIMESTAMP",
            "BYTE": "BYTES", "CLOB": "STRING",
        }
        cols = st.columns(5)
        for i, (td_type, bq_type) in enumerate(type_map.items()):
            with cols[i % 5]:
                st.markdown(f"""
                <div style="background:#1e293b;border:1px solid #334155;border-radius:6px;
                            padding:8px;text-align:center;margin:4px 0;font-size:11px;">
                    <div style="color:#f59e0b;">{td_type}</div>
                    <div style="color:#64748b;margin:2px 0;">→</div>
                    <div style="color:#22c55e;">{bq_type}</div>
                </div>
                """, unsafe_allow_html=True)

        col_test, _ = st.columns([1, 3])
        with col_test:
            if st.button("🔌 Test Teradata Connection", key="test_td", use_container_width=True):
                with st.spinner("Testing..."):
                    import time; time.sleep(1)
                    if demo:
                        st.success("✅ Connected to td-prd.example.com (Demo Mode)")
                    else:
                        st.info("Install teradatasql Python package and provide credentials.")

    # ── BigQuery ─────────────────────────────────────────────────────────────
    with cfg_tabs[2]:
        st.markdown("### 🔵 Google BigQuery")
        st.caption("Target data warehouse — where migrated data lands")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["bq_project"]         = st.text_input("GCP Project ID", value=cfg["bq_project"], key="bq_proj")
            cfg["bq_billing_project"] = st.text_input("Billing Project ID",
                value=cfg["bq_billing_project"], key="bq_bill",
                help="Usually same as project ID unless using shared billing")
            cfg["bq_location"]        = st.selectbox("Dataset Location",
                ["US", "EU", "us-central1", "us-east1", "europe-west2", "asia-southeast1"],
                index=["US","EU","us-central1","us-east1","europe-west2","asia-southeast1"].index(cfg.get("bq_location","US")),
                key="bq_loc")
        with col2:
            cfg["bq_dwh_dataset"]   = st.text_input("DWH Dataset", value=cfg["bq_dwh_dataset"], key="bq_dwh",
                help="Target DWH tables go here")
            cfg["bq_stg_dataset"]   = st.text_input("Staging Dataset", value=cfg["bq_stg_dataset"], key="bq_stg",
                help="Staging/raw tables")
            cfg["bq_ref_dataset"]   = st.text_input("Reference Dataset", value=cfg["bq_ref_dataset"], key="bq_ref",
                help="Lookup/reference tables")
            cfg["bq_audit_dataset"] = st.text_input("Audit Dataset", value=cfg["bq_audit_dataset"], key="bq_aud",
                help="ETL run logs and audit tables")
        with col3:
            cfg["bq_sa_key_path"] = st.text_input("Service Account Key Path",
                value=cfg["bq_sa_key_path"], key="bq_sa",
                help="Path to GCP service account JSON key file")
            auth_method = st.selectbox("Auth Method",
                ["Service Account JSON", "Application Default Credentials (ADC)", "Workload Identity"],
                key="bq_auth")

        st.markdown("**Dataset Structure:**")
        datasets = [
            (cfg["bq_dwh_dataset"],   "DWH tables (migrated targets)"),
            (cfg["bq_stg_dataset"],   "Staging/raw source tables"),
            (cfg["bq_ref_dataset"],   "Lookup & reference tables"),
            (cfg["bq_audit_dataset"], "ETL run logs & row count audit"),
        ]
        ds_cols = st.columns(4)
        for col, (ds, desc) in zip(ds_cols, datasets):
            with col:
                st.markdown(f"""
                <div style="background:#1e293b;border:1px solid #3b82f6;border-radius:8px;
                            padding:12px;text-align:center;">
                    <div style="font-size:20px;">🗂️</div>
                    <div style="font-size:13px;font-weight:600;color:#93c5fd;margin-top:4px;">{ds}</div>
                    <div style="font-size:10px;color:#64748b;margin-top:2px;">{desc}</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("**BigQuery DDL Preview (Audit Table):**")
        st.code(f"""CREATE TABLE IF NOT EXISTS `{cfg['bq_project']}.{cfg['bq_audit_dataset']}.etl_run_log` (
    workflow_name    STRING,
    session_name     STRING,
    target_table     STRING,
    run_date         DATE,
    rows_src         INT64,
    rows_tgt         INT64,
    delta_rows       INT64,
    status           STRING,
    run_ts           TIMESTAMP
)
PARTITION BY run_date
OPTIONS (description = 'ETL Automator audit log — built by Srinivas Punugu');""", language="sql")

        col_test, _ = st.columns([1, 3])
        with col_test:
            if st.button("🔌 Test BigQuery Connection", key="test_bq", use_container_width=True):
                with st.spinner("Testing..."):
                    import time; time.sleep(1)
                    if demo:
                        st.success(f"✅ Connected to {cfg['bq_project']} (Demo Mode)")
                    else:
                        st.info("Install google-cloud-bigquery and set GOOGLE_APPLICATION_CREDENTIALS.")

    # ── GCS Paths ─────────────────────────────────────────────────────────────
    with cfg_tabs[3]:
        st.markdown("### ☁️ Google Cloud Storage Paths")
        st.caption("Where SQL files, DAGs, logs, and parameters are stored")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**SQL Files (BigQuery scripts)**")
            cfg["gcs_sql_bucket"] = st.text_input("SQL Bucket", value=cfg["gcs_sql_bucket"], key="gcs_sql_b")
            cfg["gcs_sql_prefix"] = st.text_input("SQL Prefix", value=cfg["gcs_sql_prefix"], key="gcs_sql_p")

            st.markdown("**DAG Files (Airflow)**")
            cfg["gcs_dag_bucket"] = st.text_input("DAG Bucket", value=cfg["gcs_dag_bucket"], key="gcs_dag_b")
            cfg["gcs_dag_prefix"] = st.text_input("DAG Prefix", value=cfg["gcs_dag_prefix"], key="gcs_dag_p")

        with col2:
            st.markdown("**Archive & Logs**")
            cfg["gcs_archive"] = st.text_input("Archive Path", value=cfg["gcs_archive"], key="gcs_arch")
            cfg["gcs_logs"]    = st.text_input("Logs Path", value=cfg["gcs_logs"], key="gcs_logs")
            cfg["gcs_params"]  = st.text_input("Params Path", value=cfg["gcs_params"], key="gcs_params")

        st.markdown("**Full Path Preview:**")
        workflow_ex = st.session_state.get("selected_workflow", "wf_billing_daily_load")
        session_ex  = "s_m_billing_extract"
        paths = [
            ("SQL File",   f"{cfg['gcs_sql_bucket']}/{cfg['gcs_sql_prefix']}{workflow_ex}/{session_ex}.sql"),
            ("DAG File",   f"{cfg['gcs_dag_bucket']}/{cfg['gcs_dag_prefix']}{workflow_ex.replace('wf_','dag_')}.py"),
            ("Session Log",f"{cfg['gcs_logs']}{workflow_ex}/{session_ex}.log"),
            ("Param File", f"{cfg['gcs_params']}{workflow_ex}.param"),
            ("Archive",    f"{cfg['gcs_archive']}{workflow_ex}/"),
        ]
        for label, path in paths:
            st.markdown(f"""
            <div style="background:#1e293b;border:1px solid #334155;border-radius:6px;
                        padding:8px 12px;margin:4px 0;display:flex;justify-content:space-between;
                        font-size:12px;">
                <span style="color:#94a3b8;min-width:100px;">{label}</span>
                <code style="color:#93c5fd;">{path}</code>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("**gsutil Deploy Commands Preview:**")
        st.code(f"""# Upload SQL files
gsutil cp sql/*.sql {cfg['gcs_sql_bucket']}/{cfg['gcs_sql_prefix']}{workflow_ex}/

# Upload DAG
gsutil cp dags/*.py {cfg['gcs_dag_bucket']}/{cfg['gcs_dag_prefix']}

# Check deployment
gsutil ls {cfg['gcs_sql_bucket']}/{cfg['gcs_sql_prefix']}{workflow_ex}/

# Rollback (delete deployed files)
gsutil -m rm {cfg['gcs_sql_bucket']}/{cfg['gcs_sql_prefix']}{workflow_ex}/*.sql""", language="bash")

    # ── Git ───────────────────────────────────────────────────────────────────
    with cfg_tabs[4]:
        st.markdown("### 📁 Git Repository")
        st.caption("Source control for SQL files, DAGs, and deployment configs")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["git_host"]  = st.text_input("Git Host", value=cfg["git_host"], key="git_host")
            cfg["git_org"]   = st.text_input("Organization / Username", value=cfg["git_org"], key="git_org")
            cfg["git_repo"]  = st.text_input("Repository Name", value=cfg["git_repo"], key="git_repo_cfg")
        with col2:
            cfg["git_token"]       = st.text_input("Personal Access Token", type="password",
                value=cfg["git_token"], key="git_tok",
                placeholder="ghp_xxxxxxxxxxxx" if not demo else "demo-mode")
            cfg["git_main_branch"] = st.text_input("Main Branch", value=cfg["git_main_branch"], key="git_main")
            cfg["git_dev_branch"]  = st.text_input("Dev Branch", value=cfg["git_dev_branch"], key="git_dev")
        with col3:
            cfg["git_sql_path"]    = st.text_input("SQL Files Path", value=cfg["git_sql_path"], key="git_sql_p")
            cfg["git_dag_path"]    = st.text_input("DAG Files Path", value=cfg["git_dag_path"], key="git_dag_p")
            cfg["git_config_path"] = st.text_input("Config Files Path", value=cfg["git_config_path"], key="git_cfg_p")

        repo_url = f"{cfg['git_host']}/{cfg['git_org']}/{cfg['git_repo']}"
        st.markdown("**Repository Structure Preview:**")
        st.code(f"""{cfg['git_repo']}/
├── {cfg['git_sql_path']}
│   └── wf_billing_daily_load/
│       ├── s_m_billing_extract.sql
│       ├── s_m_billing_transform.sql
│       └── s_m_billing_load.sql
├── {cfg['git_dag_path']}
│   └── dag_billing_daily_load.py
├── {cfg['git_config_path']}
│   └── deploy_wf_billing_daily_load_prod.yaml
└── README.md""", language="text")

        st.markdown("**Git Commands Preview:**")
        st.code(f"""# Clone
git clone {repo_url}

# Create feature branch
git checkout -b feature/migrate-billing_daily_load

# Add migrated files
git add {cfg['git_sql_path']} {cfg['git_dag_path']}
git commit -m "feat: migrate wf_billing_daily_load to BigQuery + Airflow"

# Push and create MR
git push origin feature/migrate-billing_daily_load

# After MR approval — merge to main
git checkout {cfg['git_main_branch']}
git merge feature/migrate-billing_daily_load""", language="bash")

    # ── Jenkins ───────────────────────────────────────────────────────────────
    with cfg_tabs[5]:
        st.markdown("### 🔨 Jenkins CI/CD")
        st.caption("Continuous integration and deployment pipeline")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["jenkins_url"]     = st.text_input("Jenkins URL", value=cfg["jenkins_url"], key="j_url_cfg")
            cfg["jenkins_user"]    = st.text_input("Jenkins Username", value=cfg["jenkins_user"], key="j_user")
            cfg["jenkins_cred_id"] = st.text_input("GCP Credential ID",
                value=cfg["jenkins_cred_id"], key="j_cred",
                help="Jenkins credential ID for GCP service account")
        with col2:
            cfg["jenkins_token"] = st.text_input("API Token", type="password",
                value=cfg["jenkins_token"], key="j_token",
                placeholder="Jenkins API token" if not demo else "demo-mode")
            cfg["jenkins_job"]   = st.text_input("Pipeline Job Name",
                value=cfg["jenkins_job"], key="j_job_cfg")

        st.markdown("**Jenkinsfile Pipeline Preview:**")
        st.code(f"""pipeline {{
    agent any
    environment {{
        GCP_PROJECT = '{cfg["bq_project"]}'
        GCS_BUCKET  = '{cfg["gcs_sql_bucket"]}'
        GIT_REPO    = '{cfg["git_host"]}/{cfg["git_org"]}/{cfg["git_repo"]}'
    }}
    stages {{
        stage('Checkout') {{
            steps {{ git branch: '${{env.BRANCH_NAME}}', url: "${{GIT_REPO}}" }}
        }}
        stage('Lint & Validate') {{
            steps {{
                sh 'python -m py_compile dags/*.py'
                sh 'sqlfluff lint {cfg["git_sql_path"]}**/*.sql --dialect bigquery'
            }}
        }}
        stage('BQ Dry Run') {{
            steps {{
                sh '''for f in {cfg["git_sql_path"]}**/*.sql; do
                    bq query --dry_run --use_legacy_sql=false < $f
                done'''
            }}
        }}
        stage('Upload to GCS') {{
            steps {{
                sh 'gsutil -m cp {cfg["git_sql_path"]}**/*.sql {cfg["gcs_sql_bucket"]}/{cfg["gcs_sql_prefix"]}'
                sh 'gsutil -m cp {cfg["git_dag_path"]}*.py {cfg["gcs_dag_bucket"]}/{cfg["gcs_dag_prefix"]}'
            }}
        }}
        stage('Notify') {{
            steps {{
                emailext to: '{cfg["notify_email"]}',
                         subject: "ETL Migration Build #${{BUILD_NUMBER}} Passed",
                         body: "Workflow deployed successfully"
            }}
        }}
    }}
}}""", language="groovy")

        col_test, _ = st.columns([1, 3])
        with col_test:
            if st.button("🔌 Test Jenkins Connection", key="test_jenkins", use_container_width=True):
                with st.spinner("Testing..."):
                    import time; time.sleep(1)
                    if demo:
                        st.success("✅ Connected to Jenkins (Demo Mode)")
                    else:
                        st.info("Provide Jenkins URL + API token to test real connection.")

    # ── Airflow ───────────────────────────────────────────────────────────────
    with cfg_tabs[6]:
        st.markdown("### 🌊 Apache Airflow / Cloud Composer")
        st.caption("Workflow orchestration — replaces Informatica workflow scheduler")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["composer_project"] = st.text_input("GCP Project", value=cfg["composer_project"], key="comp_proj")
            cfg["composer_env"]     = st.text_input("Composer Environment", value=cfg["composer_env"], key="comp_env")
            cfg["composer_loc"]     = st.selectbox("Location",
                ["us-central1","us-east1","europe-west1","asia-southeast1"],
                index=["us-central1","us-east1","europe-west1","asia-southeast1"].index(cfg.get("composer_loc","us-central1")),
                key="comp_loc")
        with col2:
            cfg["airflow_dag_bucket"] = st.text_input("DAG Bucket", value=cfg["airflow_dag_bucket"], key="af_dag_b")

        st.markdown("**Deploy DAG Command:**")
        st.code(f"""# Deploy DAG to Cloud Composer
gsutil cp dags/dag_billing_daily_load.py {cfg['airflow_dag_bucket']}

# Trigger DAG manually
gcloud composer environments run {cfg['composer_env']} \\
    --location {cfg['composer_loc']} \\
    dags trigger -- dag_billing_daily_load

# Check DAG status
gcloud composer environments run {cfg['composer_env']} \\
    --location {cfg['composer_loc']} \\
    dags list""", language="bash")

        st.markdown("**Airflow → Informatica Equivalents:**")
        equiv = [
            ("Informatica Workflow", "Airflow DAG"),
            ("Informatica Worklet", "Airflow TaskGroup"),
            ("Informatica Session", "BigQueryInsertJobOperator"),
            ("Pre-session command (.ksh)", "PythonOperator (pre-task)"),
            ("Post-session command (.ksh)", "PythonOperator (post-task)"),
            ("Workflow Scheduler", "DAG schedule_interval (cron)"),
            ("Parameter file (.param)", "Airflow Variables / Secrets"),
            ("Informatica Monitor", "Airflow UI + Cloud Monitoring"),
        ]
        for infa, af in equiv:
            col_a, col_arrow, col_b = st.columns([3, 1, 3])
            col_a.markdown(f"<div style='background:#7f1d1d22;border:1px solid #ef444433;border-radius:6px;padding:6px 10px;font-size:12px;color:#fca5a5;'>{infa}</div>", unsafe_allow_html=True)
            col_arrow.markdown("<div style='text-align:center;padding-top:6px;color:#6366f1;'>→</div>", unsafe_allow_html=True)
            col_b.markdown(f"<div style='background:#14532d22;border:1px solid #22c55e33;border-radius:6px;padding:6px 10px;font-size:12px;color:#86efac;'>{af}</div>", unsafe_allow_html=True)

    # ── Notifications ─────────────────────────────────────────────────────────
    with cfg_tabs[7]:
        st.markdown("### 🔔 Notifications")
        st.caption("Email and Slack alerts for pipeline success/failure")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**📧 Email**")
            cfg["notify_email"] = st.text_input("Notification Email",
                value=cfg["notify_email"], key="notif_email")
            email_on_fail    = st.toggle("Email on Failure", value=True, key="em_fail")
            email_on_success = st.toggle("Email on Success", value=False, key="em_succ")

        with col2:
            st.markdown("**💬 Slack**")
            cfg["slack_webhook"] = st.text_input("Slack Webhook URL", type="password",
                value=cfg["slack_webhook"], key="slack_wh",
                placeholder="https://hooks.slack.com/services/...")
            cfg["slack_channel"] = st.text_input("Slack Channel",
                value=cfg["slack_channel"], key="slack_ch")

        st.markdown("**Notification Template Preview:**")
        st.code(f"""Subject: ETL Migration Alert — wf_billing_daily_load
To: {cfg['notify_email']}

✅ Migration pipeline completed successfully
Workflow  : wf_billing_daily_load
Sessions  : s_m_billing_extract, s_m_billing_transform, s_m_billing_load
Rows      : 4,823,910 rows loaded
Duration  : 1h 5m
GCS SQL   : {cfg['gcs_sql_bucket']}/{cfg['gcs_sql_prefix']}wf_billing_daily_load/
GCS DAG   : {cfg['gcs_dag_bucket']}/{cfg['gcs_dag_prefix']}dag_billing_daily_load.py
Jenkins   : Build #324 PASSED
MR        : https://github.com/{cfg['git_org']}/{cfg['git_repo']}/pull/637 — MERGED

Built by ETL Automator — Srinivas Punugu""", language="text")

    # ── Pipeline Defaults ─────────────────────────────────────────────────────
    with cfg_tabs[8]:
        st.markdown("### ⚡ Pipeline Defaults")
        st.caption("Default settings applied to all SQL generation and pipeline runs")

        col1, col2, col3 = st.columns(3)
        with col1:
            cfg["default_load_strategy"]  = st.selectbox("Default Load Strategy",
                ["MERGE", "INSERT_ONLY", "UPDATE_ONLY", "TRUNCATE_INSERT", "SCD_TYPE1", "SCD_TYPE2"],
                index=["MERGE","INSERT_ONLY","UPDATE_ONLY","TRUNCATE_INSERT","SCD_TYPE1","SCD_TYPE2"].index(cfg.get("default_load_strategy","MERGE")),
                key="def_load")
            cfg["default_partition_col"]  = st.selectbox("Default Partition Column",
                ["etl_load_dt", "load_date", "bill_date", "effective_date", "run_date"],
                index=["etl_load_dt","load_date","bill_date","effective_date","run_date"].index(cfg.get("default_partition_col","etl_load_dt")),
                key="def_part")
        with col2:
            cfg["default_commit_interval"] = st.text_input("Commit Interval (rows)",
                value=cfg["default_commit_interval"], key="def_commit")
            cfg["default_error_threshold"] = st.text_input("Error Threshold (rows)",
                value=cfg["default_error_threshold"], key="def_err")
        with col3:
            cfg["default_parallelism"] = st.selectbox("Parallelism (partitions)",
                ["1", "2", "4", "8", "16"],
                index=["1","2","4","8","16"].index(cfg.get("default_parallelism","4")),
                key="def_par")

        st.markdown("**Generated SQL will use these defaults:**")
        st.code(f"""-- Default settings applied to every generated SQL
-- Load Strategy  : {cfg['default_load_strategy']}
-- Partition Col  : {cfg['default_partition_col']}
-- Commit Interval: {cfg['default_commit_interval']} rows
-- Error Threshold: {cfg['default_error_threshold']} rows
-- Parallelism    : {cfg['default_parallelism']} partitions

-- Example MERGE with defaults:
MERGE `{cfg['bq_project']}.{cfg['bq_dwh_dataset']}.target_table` T
USING tmp_session_transformed S
ON (T.account_id = S.account_id)
WHEN MATCHED THEN UPDATE SET T.etl_update_dt = S.etl_update_dt
WHEN NOT MATCHED THEN INSERT (...)
VALUES (...);""", language="sql")

    st.markdown("---")

    # ── Save All Config ───────────────────────────────────────────────────────
    col_save, col_export, col_reset = st.columns(3)

    with col_save:
        if st.button("💾 Save All Configuration", type="primary", use_container_width=True, key="save_cfg"):
            _save_config(cfg)
            st.success("✅ Configuration saved! All tabs will use these settings.")

    with col_export:
        # Export config as JSON (mask passwords)
        export_cfg = {k: ("***" if "password" in k or "token" in k or "key" in k else v)
                     for k, v in cfg.items()}
        st.download_button(
            "⬇️ Export Config (JSON)",
            data=json.dumps(export_cfg, indent=2),
            file_name="etl_automator_config.json",
            mime="application/json",
            use_container_width=True,
            key="export_cfg"
        )

    with col_reset:
        if st.button("🔄 Reset to Defaults", use_container_width=True, key="reset_cfg"):
            st.session_state.app_config = DEFAULTS.copy()
            st.rerun()

    # Auto-save on any change
    _save_config(cfg)
