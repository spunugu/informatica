"""
ETL Automator — Extended Mock Data
Built by Srinivas Punugu
"""

from mock_data.sample_data_base import *

LOAD_TYPE_DESCRIPTIONS = {
    "MERGE": "MERGE WHEN MATCHED → UPDATE, WHEN NOT MATCHED → INSERT. Most common pattern for dimension loads.",
    "INSERT_ONLY": "Append-only load. New rows added, existing rows untouched. Use for fact tables / logs.",
    "UPDATE_ONLY": "Only updates existing rows. Rows not found in target are skipped/rejected.",
    "DELETE": "Removes rows from target matching source keys. Use for soft-delete ETL patterns.",
    "TRUNCATE_INSERT": "Deletes today's partition then re-inserts. Full refresh of daily partition.",
    "SCD_TYPE1": "Slowly Changing Dimension Type 1 — overwrites historical values. No history preserved.",
    "SCD_TYPE2": "Slowly Changing Dimension Type 2 — new row per change, effective_start/end_date columns.",
}

MOCK_SQL_SAMPLES = {
    "default": """-- ETL Automator — BigQuery SQL (Demo)
-- Built by Srinivas Punugu
-- ============================================================

DECLARE run_date DATE DEFAULT CURRENT_DATE();
DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE rows_src INT64 DEFAULT 0;
DECLARE rows_tgt INT64 DEFAULT 0;

-- STEP 1: Extract from Source
CREATE OR REPLACE TEMP TABLE tmp_session_raw AS
SELECT
     account_id
    ,account_name
    ,bill_amount
    ,bill_date
    ,customer_id
    ,status_cd
FROM `your-gcp-project.staging.stg_billing_raw`
WHERE DATE(bill_date) = run_date;

SET rows_src = (SELECT COUNT(*) FROM tmp_session_raw);

-- STEP 2: Lookup enrichment
CREATE OR REPLACE TEMP TABLE tmp_session_lkp1 AS
SELECT
    src.*,
    lkp_1.account_segment,
    lkp_1.account_tier,
    lkp_1.credit_class,
    CURRENT_TIMESTAMP() AS etl_load_dt
FROM tmp_session_raw src
LEFT JOIN (
    SELECT * FROM `your-gcp-project.reference.lkp_account_master`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_date DESC) = 1
) lkp_1 ON src.account_id = lkp_1.account_id;

-- STEP 3: Expression transformations
CREATE OR REPLACE TEMP TABLE tmp_session_transformed AS
SELECT
    account_id,
    TRIM(account_name) AS account_name,
    CASE WHEN bill_amount < 0 THEN 0 ELSE bill_amount END AS bill_amount,
    bill_date,
    customer_id,
    UPPER(status_cd) AS status_cd,
    account_segment,
    account_tier,
    credit_class,
    CURRENT_TIMESTAMP() AS etl_load_dt,
    CURRENT_TIMESTAMP() AS etl_update_dt
FROM tmp_session_lkp1;

-- STEP 4: MERGE into target
MERGE `your-gcp-project.dwh.dwh_billing_fact` T
USING tmp_session_transformed S
ON (T.account_id = S.account_id AND DATE(T.bill_date) = DATE(S.bill_date))
WHEN MATCHED THEN
    UPDATE SET
        T.account_name    = S.account_name,
        T.bill_amount     = S.bill_amount,
        T.status_cd       = S.status_cd,
        T.account_segment = S.account_segment,
        T.account_tier    = S.account_tier,
        T.etl_update_dt   = S.etl_update_dt
WHEN NOT MATCHED THEN
    INSERT (account_id, account_name, bill_amount, bill_date, customer_id,
            status_cd, account_segment, account_tier, credit_class, etl_load_dt, etl_update_dt)
    VALUES (S.account_id, S.account_name, S.bill_amount, S.bill_date, S.customer_id,
            S.status_cd, S.account_segment, S.account_tier, S.credit_class, S.etl_load_dt, S.etl_update_dt);

-- STEP 5: Audit logging
SET rows_tgt = (SELECT COUNT(*) FROM `your-gcp-project.dwh.dwh_billing_fact`
                WHERE DATE(etl_load_dt) = run_date);

INSERT INTO `your-gcp-project.audit.etl_run_log`
    (workflow_name, session_name, target_table, run_date, rows_src, rows_tgt, delta_rows, status, run_ts)
VALUES
    ('wf_billing_daily_load', 's_m_billing_extract',
     'dwh_billing_fact', run_date, rows_src, rows_tgt,
     (rows_src - rows_tgt),
     IF(ABS(rows_src - rows_tgt) <= 100, 'SUCCESS', 'WARN_ROW_MISMATCH'),
     run_ts);
""",
}

MOCK_DAG_SAMPLE = """# ETL Automator — Airflow DAG
# Built by Srinivas Punugu
# Workflow: {workflow}
# Generated: {timestamp}

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {{
    "owner": "srinivas-punugu",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),
}}

PROJECT_ID = "{project}"

with DAG(
    dag_id="{dag_id}",
    default_args=DEFAULT_ARGS,
    description="Migrated from Informatica: {workflow} — by Srinivas Punugu",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl-automator", "migrated"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    def pre_checks(**ctx):
        print(f"Pre-checks for {{ctx['ds']}}")
        return "proceed_run"

    task_pre   = BranchPythonOperator(task_id="pre_checks", python_callable=pre_checks)
    skip_run   = EmptyOperator(task_id="skip_run")
    proceed    = EmptyOperator(task_id="proceed_run")

    task_extract = BigQueryInsertJobOperator(
        task_id="s_m_billing_extract",
        configuration={{"query": {{"query": open("sql/s_m_billing_extract.sql").read(), "useLegacySql": False}}}},
        project_id=PROJECT_ID,
    )
    task_transform = BigQueryInsertJobOperator(
        task_id="s_m_billing_transform",
        configuration={{"query": {{"query": open("sql/s_m_billing_transform.sql").read(), "useLegacySql": False}}}},
        project_id=PROJECT_ID,
    )
    task_load = BigQueryInsertJobOperator(
        task_id="s_m_billing_load",
        configuration={{"query": {{"query": open("sql/s_m_billing_load.sql").read(), "useLegacySql": False}}}},
        project_id=PROJECT_ID,
    )
    task_post = PythonOperator(task_id="post_checks", python_callable=lambda **ctx: print("Done"))

    start >> task_pre >> [proceed, skip_run]
    skip_run >> end
    proceed >> task_extract >> task_transform >> task_load >> task_post >> end
"""
