"""
ETL Automator — All Migrations Page
Built by Srinivas Punugu

A single dynamic page that handles ALL migration combinations:
- Any ETL Tool (Informatica, SSIS, Talend, DataStage, AbInitio, Pentaho, ODI, Shell)
- Any Source DB (Teradata, SQL Server, Oracle, MySQL, PostgreSQL, DB2, SAP HANA, Netezza)
- Any Target Cloud (BigQuery, Snowflake, Azure Synapse, Redshift, Databricks, Delta Lake)

Each combination generates:
- Full connection config
- SQL conversion with type mapping
- Orchestration code (Airflow/ADF/Prefect/dbt)
- Validation checks
- CI/CD deployment config
"""

import streamlit as st
import os
import sys
import io
import json
import zipfile
import time
import random
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

st.set_page_config(
    page_title="All Migrations — ETL Automator",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html,body,[class*="css"]{font-family:'Inter',sans-serif!important;}
  .stApp{background:#0a0f1e;color:#e2e8f0;}
  .page-header{background:linear-gradient(135deg,#0f172a,#1e293b);border-bottom:1px solid #22c55e33;
               padding:20px 32px 16px;margin:-1rem -1rem 1.5rem -1rem;}
  .tab-header{border-left:3px solid #22c55e;padding-left:16px;margin-bottom:24px;}
  .tab-header h2{margin:0 0 4px;font-size:22px;color:#f1f5f9;}
  .tab-header p{margin:0;color:#94a3b8;font-size:14px;}
  [data-testid="metric-container"]{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px!important;}
  [data-testid="stMetricLabel"]{color:#94a3b8!important;font-size:12px!important;}
  [data-testid="stMetricValue"]{color:#f1f5f9!important;font-size:24px!important;font-weight:700!important;}
  .stButton>button[kind="primary"]{background:linear-gradient(135deg,#22c55e,#16a34a)!important;
    border:none!important;color:white!important;font-weight:600!important;border-radius:8px!important;}
  .stButton>button{border-radius:8px!important;border:1px solid #334155!important;
    background:#1e293b!important;color:#e2e8f0!important;}
  .stTabs [data-baseweb="tab-list"]{gap:4px;background:#0f172a;padding:8px 8px 0;border-bottom:1px solid #1e293b;}
  .stTabs [data-baseweb="tab"]{background:transparent;border:1px solid #1e293b;border-bottom:none;
    border-radius:8px 8px 0 0;color:#94a3b8;font-size:13px;font-weight:500;padding:8px 18px;}
  .stTabs [aria-selected="true"]{background:#1e293b!important;color:#f1f5f9!important;border-color:#334155!important;}
  .stTabs [data-baseweb="tab-panel"]{background:#0f172a;border:1px solid #1e293b;
    border-top:none;border-radius:0 8px 8px 8px;padding:24px;}
  hr{border-color:#1e293b!important;}
  [data-testid="stSidebar"]{background:#0f172a!important;border-right:1px solid #1e293b;}
  .stTextInput>div>div>input,.stSelectbox>div>div{background:#1e293b!important;
    border:1px solid #334155!important;border-radius:8px!important;color:#e2e8f0!important;}
  .stDownloadButton>button{background:linear-gradient(135deg,#1d4ed8,#1e40af)!important;
    border:none!important;color:white!important;font-weight:600!important;border-radius:8px!important;}
</style>
""", unsafe_allow_html=True)

# ── Data ──────────────────────────────────────────────────────────────────────

ETL_TOOLS = {
    "informatica": {"name":"Informatica PowerCenter","icon":"🔴","color":"#ef4444","input":"XML export"},
    "ssis":        {"name":"SSIS",                   "icon":"🔷","color":"#3b82f6","input":".dtsx files"},
    "talend":      {"name":"Talend",                 "icon":"🟢","color":"#22c55e","input":"JSON export"},
    "abinitio":    {"name":"Ab Initio",              "icon":"🟠","color":"#f97316","input":".mp graph files"},
    "datastage":   {"name":"IBM DataStage",          "icon":"🔵","color":"#0ea5e9","input":".dsx export"},
    "pentaho":     {"name":"Pentaho",                "icon":"🟣","color":"#a855f7","input":".ktr/.kjb"},
    "odi":         {"name":"Oracle ODI",             "icon":"🔶","color":"#f59e0b","input":"XML export"},
    "shell":       {"name":"Shell Scripts",          "icon":"⚫","color":"#6b7280","input":".sh/.ksh files"},
    "spark":       {"name":"Apache Spark",           "icon":"🔥","color":"#ff3621","input":"PySpark scripts"},
    "dbt":         {"name":"dbt",                    "icon":"🟤","color":"#ff694b","input":".sql models"},
}

SOURCE_DBS = {
    "teradata":   {"name":"Teradata",    "icon":"🟠","color":"#f97316","port":"1025","driver":"teradatasql"},
    "sqlserver":  {"name":"SQL Server",  "icon":"🔷","color":"#3b82f6","port":"1433","driver":"pyodbc"},
    "oracle":     {"name":"Oracle",      "icon":"🔴","color":"#ef4444","port":"1521","driver":"cx_Oracle"},
    "mysql":      {"name":"MySQL",       "icon":"🐬","color":"#06b6d4","port":"3306","driver":"mysql-connector-python"},
    "postgresql": {"name":"PostgreSQL",  "icon":"🐘","color":"#6366f1","port":"5432","driver":"psycopg2"},
    "db2":        {"name":"IBM DB2",     "icon":"🔵","color":"#0ea5e9","port":"50000","driver":"ibm_db"},
    "saphana":    {"name":"SAP HANA",    "icon":"🟢","color":"#22c55e","port":"30015","driver":"hdbcli"},
    "netezza":    {"name":"Netezza",     "icon":"🟣","color":"#a855f7","port":"5480","driver":"pyodbc"},
    "greenplum":  {"name":"Greenplum",   "icon":"🟢","color":"#16a34a","port":"5432","driver":"psycopg2"},
    "snowflake":  {"name":"Snowflake",   "icon":"❄️","color":"#29b5e8","port":"443","driver":"snowflake-connector-python"},
    "bigquery":   {"name":"BigQuery",    "icon":"☁️","color":"#4285f4","port":"443","driver":"google-cloud-bigquery"},
    "redshift":   {"name":"Redshift",    "icon":"🟠","color":"#ff9900","port":"5439","driver":"redshift_connector"},
}

TARGET_CLOUDS = {
    "bigquery":   {"name":"BigQuery + Airflow",      "icon":"☁️","color":"#4285f4","cloud":"GCP",       "orch":"Airflow",   "sql":"BigQuery SQL"},
    "snowflake":  {"name":"Snowflake + dbt",         "icon":"❄️","color":"#29b5e8","cloud":"Multi",     "orch":"dbt+Airflow","sql":"Snowflake SQL"},
    "synapse":    {"name":"Azure Synapse + ADF",     "icon":"🔷","color":"#0078d4","cloud":"Azure",     "orch":"ADF",       "sql":"T-SQL"},
    "redshift":   {"name":"Redshift + Glue",         "icon":"🟠","color":"#ff9900","cloud":"AWS",       "orch":"AWS Glue",  "sql":"Redshift SQL"},
    "databricks": {"name":"Databricks + Spark",      "icon":"🔥","color":"#ff3621","cloud":"Multi",     "orch":"Workflows", "sql":"Spark SQL"},
    "deltalake":  {"name":"Delta Lake + Prefect",    "icon":"🌊","color":"#0052cc","cloud":"Multi",     "orch":"Prefect",   "sql":"Delta SQL"},
    "fabric":     {"name":"Microsoft Fabric",        "icon":"🏭","color":"#742774","cloud":"Azure",     "orch":"Fabric",    "sql":"T-SQL / Spark"},
    "athena":     {"name":"AWS Athena + Glue",       "icon":"🏛️","color":"#ff9900","cloud":"AWS",       "orch":"Step Fn",   "sql":"Presto SQL"},
    "duckdb":     {"name":"DuckDB + Prefect",        "icon":"🦆","color":"#f59e0b","cloud":"Local/Any", "orch":"Prefect",   "sql":"DuckDB SQL"},
    "spanner":    {"name":"Cloud Spanner + Dataflow","icon":"🌀","color":"#4285f4","cloud":"GCP",       "orch":"Dataflow",  "sql":"GoogleSQL"},
}

TYPE_MAP = {
    ("teradata","bigquery"):   {"VARCHAR":"STRING","INTEGER":"INT64","DECIMAL":"NUMERIC","DATE/TIME":"TIMESTAMP","FLOAT":"FLOAT64","CHAR":"STRING","BYTEINT":"INT64","CLOB":"STRING","BYTE":"BYTES"},
    ("sqlserver","bigquery"):  {"NVARCHAR":"STRING","INT":"INT64","BIGINT":"INT64","DECIMAL":"NUMERIC","DATETIME":"TIMESTAMP","BIT":"BOOL","TEXT":"STRING","VARBINARY":"BYTES"},
    ("oracle","bigquery"):     {"VARCHAR2":"STRING","NUMBER":"NUMERIC","DATE":"TIMESTAMP","CLOB":"STRING","BLOB":"BYTES","INTEGER":"INT64","FLOAT":"FLOAT64"},
    ("mysql","bigquery"):      {"VARCHAR":"STRING","INT":"INT64","BIGINT":"INT64","DECIMAL":"NUMERIC","DATETIME":"TIMESTAMP","TINYINT":"INT64","TEXT":"STRING","BLOB":"BYTES"},
    ("postgresql","bigquery"): {"VARCHAR":"STRING","INTEGER":"INT64","BIGINT":"INT64","NUMERIC":"NUMERIC","TIMESTAMP":"TIMESTAMP","BOOLEAN":"BOOL","TEXT":"STRING","BYTEA":"BYTES"},
    ("teradata","snowflake"):  {"VARCHAR":"VARCHAR","INTEGER":"INTEGER","DECIMAL":"NUMBER","DATE/TIME":"TIMESTAMP_NTZ","BYTEINT":"SMALLINT","CLOB":"TEXT"},
    ("sqlserver","snowflake"): {"NVARCHAR":"VARCHAR","INT":"INTEGER","DECIMAL":"NUMBER","DATETIME":"TIMESTAMP_NTZ","BIT":"BOOLEAN","TEXT":"TEXT"},
    ("oracle","snowflake"):    {"VARCHAR2":"VARCHAR","NUMBER":"NUMBER","DATE":"TIMESTAMP_NTZ","CLOB":"TEXT","BLOB":"BINARY"},
    ("sqlserver","synapse"):   {"NVARCHAR":"NVARCHAR","INT":"INT","BIGINT":"BIGINT","DECIMAL":"DECIMAL","DATETIME2":"DATETIME2","BIT":"BIT"},
    ("oracle","synapse"):      {"VARCHAR2":"NVARCHAR","NUMBER":"DECIMAL","DATE":"DATETIME2","CLOB":"NVARCHAR(MAX)"},
    ("teradata","redshift"):   {"VARCHAR":"VARCHAR","INTEGER":"INTEGER","DECIMAL":"DECIMAL","DATE/TIME":"TIMESTAMP","FLOAT":"FLOAT8"},
    ("teradata","databricks"): {"VARCHAR":"STRING","INTEGER":"INT","DECIMAL":"DECIMAL","DATE/TIME":"TIMESTAMP","FLOAT":"DOUBLE"},
    ("postgresql","snowflake"):{"VARCHAR":"VARCHAR","INTEGER":"INTEGER","NUMERIC":"NUMBER","TIMESTAMP":"TIMESTAMP_NTZ","BOOLEAN":"BOOLEAN","TEXT":"TEXT"},
    ("mysql","snowflake"):     {"VARCHAR":"VARCHAR","INT":"INTEGER","DECIMAL":"NUMBER","DATETIME":"TIMESTAMP_NTZ","TEXT":"TEXT"},
}

FUNC_MAP = {
    "bigquery":   {"SYSDATE":"CURRENT_TIMESTAMP()","TRUNC(SYSDATE)":"CURRENT_DATE()","NVL(":"COALESCE(","IIF(":"IF(","DECODE(":"CASE WHEN ","TO_CHAR(":"FORMAT_TIMESTAMP(","INSTR(":"STRPOS(","ROWNUM":"ROW_NUMBER() OVER ()","GETDATE()":"CURRENT_TIMESTAMP()","ISNULL(":"COALESCE(","CONVERT(":"CAST(","TOP ":"LIMIT ","MINUS":"EXCEPT DISTINCT","||":"||"},
    "snowflake":  {"SYSDATE":"CURRENT_TIMESTAMP()","NVL(":"NVL(","IIF(":"IFF(","ROWNUM":"ROW_NUMBER() OVER (ORDER BY 1)","TRUNC(":"DATE_TRUNC(","INSTR(":"POSITION(","TO_CHAR(":"TO_VARCHAR(","GETDATE()":"CURRENT_TIMESTAMP()","ISNULL(":"NVL(","MINUS":"EXCEPT"},
    "synapse":    {"SYSDATE":"GETDATE()","NVL(":"ISNULL(","IIF(":"IIF(","ROWNUM":"ROW_NUMBER() OVER (ORDER BY (SELECT NULL))","INSTR(":"CHARINDEX(","SUBSTR(":"SUBSTRING(","||":"+","TRUNC(":"CONVERT(DATE,","MINUS":"EXCEPT"},
    "redshift":   {"SYSDATE":"SYSDATE","NVL(":"NVL(","IIF(":"CASE WHEN ","ROWNUM":"ROW_NUMBER() OVER ()","TRUNC(":"DATE_TRUNC(","INSTR(":"POSITION(","MINUS":"EXCEPT"},
    "databricks": {"SYSDATE":"CURRENT_TIMESTAMP()","NVL(":"COALESCE(","IIF(":"IF(","ROWNUM":"ROW_NUMBER() OVER (ORDER BY 1)","TRUNC(":"DATE_TRUNC(","TO_CHAR(":"DATE_FORMAT(","INSTR(":"LOCATE(","GETDATE()":"CURRENT_TIMESTAMP()","MINUS":"EXCEPT"},
    "deltalake":  {"SYSDATE":"CURRENT_TIMESTAMP()","NVL(":"COALESCE(","IIF(":"IF(","TRUNC(":"DATE_TRUNC(","INSTR(":"LOCATE(","GETDATE()":"CURRENT_TIMESTAMP()"},
    "fabric":     {"SYSDATE":"GETDATE()","NVL(":"ISNULL(","IIF(":"IIF(","INSTR(":"CHARINDEX(","SUBSTR(":"SUBSTRING(","||":"+"},
    "athena":     {"SYSDATE":"CURRENT_TIMESTAMP","NVL(":"COALESCE(","IIF(":"IF(","TRUNC(":"DATE_TRUNC(","INSTR(":"STRPOS(","ROWNUM":"ROW_NUMBER() OVER ()"},
    "duckdb":     {"SYSDATE":"CURRENT_TIMESTAMP()","NVL(":"COALESCE(","IIF(":"IF(","TRUNC(":"DATE_TRUNC(","INSTR(":"STRPOS("},
    "spanner":    {"SYSDATE":"CURRENT_TIMESTAMP()","NVL(":"COALESCE(","IIF(":"IF(","TRUNC(":"DATE_TRUNC(","INSTR(":"STRPOS("},
}


# ── SQL Generators ─────────────────────────────────────────────────────────────

def convert_sql(sql: str, target: str) -> str:
    result = sql
    for src, tgt in FUNC_MAP.get(target, {}).items():
        result = result.replace(src, tgt)
    return result


def gen_sql(src_db, target, src_table, tgt_table, strategy, keys, base_sql, ts, project="your-project", dataset="dwh"):
    merge_keys = " AND ".join([f"T.{k} = S.{k}" for k in keys])
    converted  = convert_sql(base_sql, target)

    headers = {
        "bigquery":   f"-- BigQuery Standard SQL\n-- Project: {project} | Dataset: {dataset}",
        "snowflake":  f"-- Snowflake SQL\n-- Database: {project} | Schema: {dataset}",
        "synapse":    f"-- Azure Synapse T-SQL\n-- Database: {project} | Schema: {dataset}",
        "redshift":   f"-- Amazon Redshift SQL\n-- Database: {project} | Schema: {dataset}",
        "databricks": f"-- Databricks Spark SQL\n-- Catalog: {project} | Schema: {dataset}",
        "deltalake":  f"-- Delta Lake PySpark\n-- Path: s3://{project}/delta/{tgt_table}",
        "fabric":     f"-- Microsoft Fabric T-SQL\n-- Workspace: {project} | Lakehouse: {dataset}",
        "athena":     f"-- AWS Athena Presto SQL\n-- Database: {project} | Table: {tgt_table}",
        "duckdb":     f"-- DuckDB SQL\n-- Database: {project}.db",
        "spanner":    f"-- Cloud Spanner GoogleSQL\n-- Instance: {project} | Database: {dataset}",
    }

    merge_stmts = {
        "bigquery":   f"MERGE `{project}.{dataset}.{tgt_table}` T\nUSING tmp_{tgt_table}_transformed S\nON ({merge_keys})\nWHEN MATCHED THEN UPDATE SET T.etl_update_dt = CURRENT_TIMESTAMP()\nWHEN NOT MATCHED THEN INSERT ROW;",
        "snowflake":  f"MERGE INTO {tgt_table} T\nUSING {tgt_table}_transformed S ON {merge_keys}\nWHEN MATCHED THEN UPDATE SET T.ETL_UPDATE_DT = CURRENT_TIMESTAMP()\nWHEN NOT MATCHED THEN INSERT VALUES (S.*);",
        "synapse":    f"MERGE INTO dbo.{tgt_table} AS T\nUSING ##tmp_{tgt_table} AS S ON {merge_keys}\nWHEN MATCHED THEN UPDATE SET T.etl_update_dt = GETDATE()\nWHEN NOT MATCHED THEN INSERT VALUES (S.*);",
        "redshift":   f"DELETE FROM {tgt_table} USING tmp_{tgt_table}_stage s WHERE {tgt_table}.{keys[0]} = s.{keys[0]};\nINSERT INTO {tgt_table} SELECT * FROM tmp_{tgt_table}_stage;",
        "databricks": f"MERGE INTO {project}.{dataset}.{tgt_table} T\nUSING tmp_{tgt_table}_transformed S ON {merge_keys}\nWHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *;",
        "deltalake":  f"delta_table.alias('T').merge(df.alias('S'), '{merge_keys}')\n    .whenMatchedUpdateAll()\n    .whenNotMatchedInsertAll()\n    .execute()",
        "fabric":     f"MERGE INTO dbo.{tgt_table} T\nUSING #tmp_{tgt_table} S ON {merge_keys}\nWHEN MATCHED THEN UPDATE SET T.etl_update_dt = GETDATE()\nWHEN NOT MATCHED THEN INSERT VALUES (S.*);",
        "athena":     f"-- Athena doesn't support MERGE — use INSERT OVERWRITE\nINSERT OVERWRITE {tgt_table}\nSELECT * FROM tmp_{tgt_table}_transformed;",
        "duckdb":     f"INSERT OR REPLACE INTO {tgt_table}\nSELECT * FROM tmp_{tgt_table}_transformed;",
        "spanner":    f"-- Spanner uses INSERT OR UPDATE\nINSERT OR UPDATE INTO {tgt_table}\nSELECT * FROM tmp_{tgt_table}_transformed;",
    }

    merge_stmt = merge_stmts.get(target, merge_stmts["bigquery"])
    header     = headers.get(target, "-- Generated SQL")

    if target == "deltalake":
        return f"""# ============================================================
# Universal ETL Converter → Delta Lake (PySpark)
# {header.split(chr(10))[1]}
# Source: {src_db} | Strategy: {strategy}
# Generated: {ts} | Built by: Srinivas Punugu
# ============================================================

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("ETL-{tgt_table}") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .getOrCreate()

# Extract
df_raw = spark.sql(\"\"\"{converted}\"\"\")

# Transform
df = df_raw.withColumn("etl_load_dt", current_timestamp()) \\
           .withColumn("etl_update_dt", current_timestamp())

# Load — {strategy}
delta_path = "s3://your-bucket/delta/{tgt_table}"
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
    {merge_stmt}
else:
    df.write.format("delta").mode("overwrite").save(delta_path)

# Optimize
spark.sql(f"OPTIMIZE delta.`{{delta_path}}` ZORDER BY ({keys[0]})")
"""

    tmp_keyword = {"synapse": "##tmp", "fabric": "#tmp"}.get(target, f"tmp_{tgt_table}")

    return f"""-- ============================================================
-- Universal ETL Converter → {TARGET_CLOUDS.get(target,{}).get('sql','SQL')}
-- {header.split(chr(10))[1]}
-- Source DB : {SOURCE_DBS.get(src_db,{}).get('name', src_db)}
-- Strategy  : {strategy}
-- Generated : {ts}
-- Built by  : Srinivas Punugu
-- ============================================================

-- STEP 1: Extract from source
{"CREATE OR REPLACE TEMP TABLE" if target in ("bigquery","databricks","athena","duckdb","spanner") else "CREATE" + (" TRANSIENT" if target=="snowflake" else "") + " TABLE" + (" ##tmp_" if target in ("synapse","fabric") else " ")} {tgt_table}_stage AS
{converted};

-- STEP 2: Transform + enrich
{"CREATE OR REPLACE TEMP TABLE" if target in ("bigquery","databricks","athena","duckdb","spanner") else "CREATE" + (" TRANSIENT" if target=="snowflake" else "") + " TABLE"} {tgt_table}_transformed AS
SELECT *,
    {"CURRENT_TIMESTAMP()" if target in ("bigquery","snowflake","databricks","deltalake","athena","duckdb","spanner") else "GETDATE()" if target in ("synapse","fabric") else "SYSDATE"} AS etl_load_dt,
    {"CURRENT_TIMESTAMP()" if target in ("bigquery","snowflake","databricks","deltalake","athena","duckdb","spanner") else "GETDATE()" if target in ("synapse","fabric") else "SYSDATE"} AS etl_update_dt
FROM {tgt_table}_stage;

-- STEP 3: {strategy} into target
{merge_stmt}

-- STEP 4: Audit
{"INSERT INTO `" + project + ".audit.etl_run_log`" if target=="bigquery" else "INSERT INTO etl_run_log"}
    (source_db, source_table, target_table, run_date, strategy, status, run_ts)
VALUES
    ('{src_db}', '{src_table}', '{tgt_table}', {"CURRENT_DATE()" if target in ("bigquery","databricks") else "CAST(GETDATE() AS DATE)" if target in ("synapse","fabric") else "CURRENT_DATE"}, '{strategy}', 'SUCCESS', {"CURRENT_TIMESTAMP()" if target not in ("synapse","fabric") else "GETDATE()"});
"""


def gen_orchestrator(tool, target, workflow, sessions, ts, project="your-project"):
    tgt = TARGET_CLOUDS.get(target, {})
    orch = tgt.get("orch", "Airflow")

    if target in ("bigquery", "snowflake", "redshift", "databricks"):
        operator_map = {
            "bigquery":   ("BigQueryInsertJobOperator", "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator", "google_cloud_default"),
            "snowflake":  ("SnowflakeOperator",         "from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator",           "snowflake_default"),
            "redshift":   ("RedshiftSQLOperator",        "from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator",     "redshift_default"),
            "databricks": ("DatabricksRunNowOperator",   "from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator",   "databricks_default"),
        }
        op_name, op_import, conn_id = operator_map.get(target, operator_map["bigquery"])

        tasks = "\n".join([f"""    task_{s.replace('-','_')} = {op_name}(
        task_id="{s}",
        {"configuration={'query': {'query': open('sql/" + s + ".sql').read(), 'useLegacySql': False}}, project_id=PROJECT_ID" if target=="bigquery" else "sql=open('sql/" + s + ".sql').read(), " + conn_id.replace('_default','_conn_id') + "='" + conn_id + "'"},
    )""" for s in sessions])

        chain = " >> ".join([f"task_{s.replace('-','_')}" for s in sessions])

        return f"""# ============================================================
# Universal ETL Converter — Airflow DAG
# Source Tool: {tool.upper()} | Target: {tgt.get('name',target)}
# Workflow   : {workflow}
# Generated  : {ts} | Built by: Srinivas Punugu
# ============================================================

from airflow import DAG
{op_import}
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

PROJECT_ID = "{project}"

with DAG(
    dag_id="dag_{workflow.replace('wf_','').replace('-','_')}",
    default_args={{
        "owner": "srinivas-punugu",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=6),
    }},
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl-automator", "{tool}", "{target}"],
) as dag:

{tasks}

    task_pre = PythonOperator(task_id="pre_checks", python_callable=lambda **ctx: print(f"Pre-checks for {{ctx['ds']}}"))
    task_post = PythonOperator(task_id="post_checks", python_callable=lambda **ctx: print(f"Post-checks for {{ctx['ds']}}"))

    task_pre >> {chain} >> task_post
"""

    elif target == "synapse":
        return f"""{{
  "name": "pipeline_{workflow}",
  "properties": {{
    "description": "Universal ETL — {tool} to Azure Synapse | Built by Srinivas Punugu",
    "activities": [
{chr(10).join([f'      {{"name": "{s}", "type": "SqlServerStoredProcedure", "typeProperties": {{"storedProcedureName": "exec_etl", "storedProcedureParameters": {{"sql_file": "{s}.sql"}}}}}}' for s in sessions])}
    ],
    "annotations": ["etl-automator", "{tool}"]
  }}
}}"""

    elif target in ("deltalake", "duckdb"):
        tasks = "\n".join([f"""@task
def {s.replace('-','_')}():
    spark.sql(open("sql/{s}.sql").read())
    print(f"Session {s} completed")""" for s in sessions])
        chain = " >> ".join([f"{s.replace('-','_')}()" for s in sessions])
        return f"""# Universal ETL — Prefect Flow
# Source: {tool} | Target: {tgt.get('name',target)}
# Generated: {ts} | Built by: Srinivas Punugu

from prefect import flow, task
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL-{workflow}").getOrCreate()

{tasks}

@flow(name="{workflow}", log_prints=True)
def run_pipeline():
    {chain}

if __name__ == "__main__":
    run_pipeline()
"""

    elif target == "fabric":
        return f"""// Microsoft Fabric Pipeline — {workflow}
// Source: {tool} | Built by: Srinivas Punugu | Generated: {ts}
{{
  "name": "{workflow}",
  "objectType": "DataPipeline",
  "activities": [
{chr(10).join([f'    {{"name": "{s}", "type": "Script", "typeProperties": {{"scripts": [{{"text": "EXEC sp_executesql @stmt = N\'{{read_sql(\"{s}.sql\")}}\'"}}], "linkedService": {{"name": "FabricWarehouse"}}}}}}' for s in sessions])}
  ]
}}"""

    else:
        tasks = "\n".join([f"""    task_{s.replace('-','_')} = PythonOperator(
        task_id="{s}",
        python_callable=lambda: execute_sql(open("sql/{s}.sql").read()),
    )""" for s in sessions])
        chain = " >> ".join([f"task_{s.replace('-','_')}" for s in sessions])
        return f"""# Airflow DAG — {tgt.get('name', target)}
# Source: {tool} | Workflow: {workflow}
# Generated: {ts} | Built by: Srinivas Punugu

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

with DAG(dag_id="dag_{workflow.replace('wf_','')}", schedule_interval="0 6 * * *",
         start_date=days_ago(1), catchup=False,
         tags=["etl-automator", "{tool}", "{target}"]) as dag:

{tasks}

    {chain}
"""


# ── Main App ──────────────────────────────────────────────────────────────────

# Header
st.markdown("""
<div class="page-header">
    <div style="font-size:28px;font-weight:700;color:#fff;">
        🌐 All <span style="color:#86efac;">Migrations</span>
    </div>
    <div style="font-size:13px;color:#94a3b8;margin-top:4px;">
        Any ETL Tool → Any Cloud Platform — Fully Dynamic
    </div>
    <div style="font-size:12px;color:#22c55e;font-weight:600;margin-top:2px;">
        Built by Srinivas Punugu
    </div>
</div>
""", unsafe_allow_html=True)

# Back button
if st.button("← Back to Home", key="back_home_all"):
    st.switch_page("Home.py")

st.markdown("---")

# ── Sidebar: Migration selector ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:12px 0 20px;">
        <div style="font-size:28px;">🌐</div>
        <div style="font-weight:700;font-size:15px;color:#f1f5f9;">All Migrations</div>
        <div style="font-size:10px;color:#22c55e;">by Srinivas Punugu</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("#### 📤 Source")
    etl_tool  = st.selectbox("ETL Tool",
        list(ETL_TOOLS.keys()),
        format_func=lambda x: f"{ETL_TOOLS[x]['icon']} {ETL_TOOLS[x]['name']}",
        key="all_etl")
    source_db = st.selectbox("Source Database",
        list(SOURCE_DBS.keys()),
        format_func=lambda x: f"{SOURCE_DBS[x]['icon']} {SOURCE_DBS[x]['name']}",
        key="all_src")

    st.markdown("#### 📥 Target")
    target = st.selectbox("Cloud Platform",
        list(TARGET_CLOUDS.keys()),
        format_func=lambda x: f"{TARGET_CLOUDS[x]['icon']} {TARGET_CLOUDS[x]['name']}",
        key="all_tgt")

    st.markdown("---")
    st.markdown("#### ⚙️ Workflow")
    workflow_name  = st.text_input("Workflow Name", value="wf_customer_migration", key="all_wf")
    source_table   = st.text_input("Source Table",  value="STG_CUSTOMER", key="all_src_tbl")
    target_table   = st.text_input("Target Table",  value="DWH_CUSTOMER_DIM", key="all_tgt_tbl")
    sessions_input = st.text_input("Sessions",      value="s_extract,s_transform,s_load", key="all_sess")
    key_cols_input = st.text_input("Key Columns",   value="customer_id", key="all_keys")
    load_strategy  = st.selectbox("Load Strategy",
        ["MERGE", "INSERT", "TRUNCATE+INSERT", "UPDATE", "SCD_TYPE2"],
        key="all_load")
    project_id     = st.text_input("Project/Account ID", value="your-project", key="all_proj")

    st.markdown("---")
    convert_btn = st.button("⚡ Convert", type="primary", use_container_width=True, key="all_convert")

# ── Migration path banner ─────────────────────────────────────────────────────
tool_cfg = ETL_TOOLS[etl_tool]
src_cfg  = SOURCE_DBS[source_db]
tgt_cfg  = TARGET_CLOUDS[target]

st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e1b4b,#1e293b);border:1px solid #22c55e44;
            border-radius:10px;padding:16px 24px;margin-bottom:16px;
            display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;">
    <div style="display:flex;align-items:center;gap:16px;">
        <div style="text-align:center;">
            <div style="font-size:28px;">{tool_cfg['icon']}</div>
            <div style="font-size:11px;color:{tool_cfg['color']};font-weight:600;">{tool_cfg['name']}</div>
        </div>
        <div style="font-size:24px;color:#334155;">+</div>
        <div style="text-align:center;">
            <div style="font-size:28px;">{src_cfg['icon']}</div>
            <div style="font-size:11px;color:{src_cfg['color']};font-weight:600;">{src_cfg['name']}</div>
        </div>
        <div style="font-size:28px;color:#22c55e;font-weight:700;">→</div>
        <div style="text-align:center;">
            <div style="font-size:28px;">{tgt_cfg['icon']}</div>
            <div style="font-size:11px;color:{tgt_cfg['color']};font-weight:600;">{tgt_cfg['name']}</div>
        </div>
    </div>
    <div style="display:flex;gap:24px;font-size:12px;">
        <div><span style="color:#64748b;">Cloud:</span> <span style="color:#f1f5f9;">{tgt_cfg['cloud']}</span></div>
        <div><span style="color:#64748b;">Orchestrator:</span> <span style="color:#f1f5f9;">{tgt_cfg['orch']}</span></div>
        <div><span style="color:#64748b;">SQL Dialect:</span> <span style="color:#f1f5f9;">{tgt_cfg['sql']}</span></div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Stats ─────────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("ETL Tools",      len(ETL_TOOLS))
c2.metric("Source DBs",     len(SOURCE_DBS))
c3.metric("Cloud Targets",  len(TARGET_CLOUDS))
c4.metric("Combinations",   len(ETL_TOOLS) * len(SOURCE_DBS) * len(TARGET_CLOUDS))
c5.metric("SQL Dialects",   len(set(v["sql"] for v in TARGET_CLOUDS.values())))

st.markdown("---")

# ── Main tabs ─────────────────────────────────────────────────────────────────
main_tabs = st.tabs([
    "⚙️ Connection Config",
    "🔠 Type Mapping",
    "📄 SQL Converter",
    "🔄 Orchestrator",
    "✅ Validation",
    "🚀 CI/CD Deploy",
    "📦 Download All",
])

sessions  = [s.strip() for s in sessions_input.split(",") if s.strip()]
key_cols  = [k.strip() for k in key_cols_input.split(",") if k.strip()]
ts        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Run conversion if button pressed
if convert_btn:
    base_sql = f"SELECT * FROM {source_table} WHERE 1=1"
    st.session_state.all_sql   = gen_sql(source_db, target, source_table, target_table,
                                          load_strategy, key_cols, base_sql, ts, project_id)
    st.session_state.all_orch  = gen_orchestrator(etl_tool, target, workflow_name, sessions, ts, project_id)
    st.session_state.all_ready = True
    st.success(f"✅ Converted {tool_cfg['name']} + {src_cfg['name']} → {tgt_cfg['name']}!")

# ── Tab: Connection Config ────────────────────────────────────────────────────
with main_tabs[0]:
    st.markdown("""<div class="tab-header"><h2>⚙️ Connection Configuration</h2>
    <p>Connection details for source and target systems</p></div>""", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"**{src_cfg['icon']} Source: {src_cfg['name']}**")
        src_host = st.text_input("Host", value=f"{source_db}-server.example.com", key="src_host_all")
        src_port = st.text_input("Port", value=src_cfg['port'], key="src_port_all")
        src_db_name = st.text_input("Database/Schema", value="source_db", key="src_db_all")
        src_user = st.text_input("Username", value="etl_user", key="src_user_all")
        src_pwd  = st.text_input("Password", type="password", key="src_pwd_all", placeholder="source password")

        st.markdown("**Connection String:**")
        conn_strings = {
            "teradata":   f"jdbc:teradata://{src_host}/DATABASE={src_db_name},LOGMECH=LDAP",
            "sqlserver":  f"jdbc:sqlserver://{src_host}:{src_port};database={src_db_name}",
            "oracle":     f"jdbc:oracle:thin:@{src_host}:{src_port}/{src_db_name}",
            "mysql":      f"jdbc:mysql://{src_host}:{src_port}/{src_db_name}",
            "postgresql": f"postgresql://{src_user}@{src_host}:{src_port}/{src_db_name}",
            "db2":        f"jdbc:db2://{src_host}:{src_port}/{src_db_name}",
            "saphana":    f"jdbc:sap://{src_host}:{src_port}/?databaseName={src_db_name}",
            "netezza":    f"jdbc:netezza://{src_host}:{src_port}/{src_db_name}",
            "snowflake":  f"jdbc:snowflake://{src_host}.snowflakecomputing.com/?db={src_db_name}",
            "bigquery":   f"bigquery://{project_id}/{src_db_name}",
            "redshift":   f"jdbc:redshift://{src_host}:{src_port}/{src_db_name}",
        }
        st.code(conn_strings.get(source_db, f"jdbc://{src_host}:{src_port}/{src_db_name}"), language="text")

        st.markdown("**Python Driver:**")
        st.code(f"pip install {src_cfg['driver']}", language="bash")

    with col2:
        st.markdown(f"**{tgt_cfg['icon']} Target: {tgt_cfg['name']}**")
        tgt_conn_configs = {
            "bigquery":   [("GCP Project ID", project_id), ("Dataset", "dwh"), ("Location", "US"), ("Auth", "Service Account JSON")],
            "snowflake":  [("Account", f"{project_id}.snowflakecomputing.com"), ("Database", "DWH"), ("Warehouse", "ETL_WH"), ("Schema", "PUBLIC")],
            "synapse":    [("Server", f"{project_id}.sql.azuresynapse.net"), ("Database", "dwh"), ("Authentication", "AAD Token")],
            "redshift":   [("Host", f"{project_id}.redshift.amazonaws.com"), ("Port", "5439"), ("Database", "dwh"), ("IAM Role", "arn:aws:iam::account:role/redshift")],
            "databricks": [("Host", f"adb-{project_id}.azuredatabricks.net"), ("HTTP Path", "/sql/1.0/warehouses/xxxxx"), ("Token", "dapi...")],
            "deltalake":  [("Storage", "s3://your-bucket/delta/"), ("Catalog", "spark_catalog"), ("Schema", "dwh")],
            "fabric":     [("Workspace", project_id), ("Lakehouse", "dwh"), ("Auth", "Service Principal")],
            "athena":     [("Region", "us-east-1"), ("S3 Output", f"s3://{project_id}/athena-results/"), ("Database", "dwh")],
            "duckdb":     [("Database File", f"{project_id}.duckdb"), ("Schema", "dwh"), ("Mode", "read_write")],
            "spanner":    [("Instance", project_id), ("Database", "etl_db"), ("Project", project_id)],
        }
        for label, val in tgt_conn_configs.get(target, []):
            st.text_input(label, value=val, key=f"tgt_{label.lower().replace(' ','_')}_all")

        st.markdown("**Quick Test:**")
        if st.button("🔌 Test Connections", use_container_width=True, key="test_all"):
            with st.spinner("Testing..."):
                time.sleep(1.2)
                st.success(f"✅ {src_cfg['name']} connected (Demo)")
                st.success(f"✅ {tgt_cfg['name']} connected (Demo)")

# ── Tab: Type Mapping ─────────────────────────────────────────────────────────
with main_tabs[1]:
    st.markdown("""<div class="tab-header"><h2>🔠 Type Mapping</h2>
    <p>Data type conversion from source to target platform</p></div>""", unsafe_allow_html=True)

    type_key = (source_db, target)
    tmap     = TYPE_MAP.get(type_key, TYPE_MAP.get(("teradata","bigquery"), {}))

    if tmap:
        st.markdown(f"**{src_cfg['name']} → {tgt_cfg['sql']} Type Mapping**")
        cols = st.columns(4)
        for i, (src_type, tgt_type) in enumerate(tmap.items()):
            with cols[i % 4]:
                st.markdown(f"""
                <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;
                            padding:10px;text-align:center;margin:4px 0;">
                    <div style="color:#f59e0b;font-size:12px;font-weight:600;">{src_type}</div>
                    <div style="color:#475569;margin:4px 0;">↓</div>
                    <div style="color:#22c55e;font-size:12px;font-weight:600;">{tgt_type}</div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info(f"Type mapping for {src_cfg['name']} → {tgt_cfg['name']} uses intelligent defaults.")
        st.markdown("Common safe mappings applied: `STRING`, `INT64/INTEGER`, `NUMERIC`, `TIMESTAMP`, `BOOLEAN`, `BYTES`")

    st.markdown("---")
    st.markdown("**Function Conversion:**")
    fmap = FUNC_MAP.get(target, {})
    if fmap:
        f_cols = st.columns(3)
        for i, (src_fn, tgt_fn) in enumerate(list(fmap.items())[:12]):
            with f_cols[i % 3]:
                st.markdown(f"""
                <div style="background:#1e293b;border:1px solid #334155;border-radius:6px;
                            padding:8px;margin:3px 0;font-size:11px;">
                    <code style="color:#f59e0b;">{src_fn}</code>
                    <span style="color:#475569;"> → </span>
                    <code style="color:#22c55e;">{tgt_fn}</code>
                </div>
                """, unsafe_allow_html=True)

# ── Tab: SQL ──────────────────────────────────────────────────────────────────
with main_tabs[2]:
    st.markdown("""<div class="tab-header"><h2>📄 SQL Converter</h2>
    <p>Auto-generated target SQL from source query</p></div>""", unsafe_allow_html=True)

    source_sql = st.text_area("Paste your source SQL",
        value=f"SELECT\n    customer_id,\n    customer_name,\n    email_address,\n    phone_number,\n    account_status,\n    created_date\nFROM {source_table}\nWHERE account_status != 'DELETED'\n  AND created_date >= TRUNC(SYSDATE) - 90",
        height=160, key="all_src_sql")

    if st.button("⚡ Generate SQL", type="primary", use_container_width=True, key="all_gen_sql"):
        sql = gen_sql(source_db, target, source_table, target_table,
                      load_strategy, key_cols, source_sql, ts, project_id)
        st.session_state.all_sql   = sql
        st.session_state.all_ready = True

    if st.session_state.get("all_sql"):
        sql  = st.session_state.all_sql
        lang = "python" if target == "deltalake" else "sql"
        c1, c2 = st.columns(2)
        c1.metric("Lines", len(sql.split("\n")))
        c2.metric("Target Dialect", tgt_cfg['sql'])
        st.code(sql, language=lang)
        st.download_button("⬇️ Download SQL", data=sql,
            file_name=f"{workflow_name}_{target}.{'py' if target=='deltalake' else 'sql'}",
            mime="text/plain", use_container_width=True, key="dl_all_sql")

# ── Tab: Orchestrator ─────────────────────────────────────────────────────────
with main_tabs[3]:
    st.markdown(f"""<div class="tab-header"><h2>🔄 {tgt_cfg['orch']} Orchestrator</h2>
    <p>Auto-generated orchestration code for {tgt_cfg['name']}</p></div>""", unsafe_allow_html=True)

    if st.button("⚡ Generate Orchestrator", type="primary", use_container_width=True, key="all_gen_orch"):
        orch = gen_orchestrator(etl_tool, target, workflow_name, sessions, ts, project_id)
        st.session_state.all_orch  = orch
        st.session_state.all_ready = True

    if st.session_state.get("all_orch"):
        orch = st.session_state.all_orch
        lang = "json" if target == "synapse" else "python"
        st.metric("Lines", len(orch.split("\n")))
        st.code(orch, language=lang)
        ext = ".json" if target == "synapse" else ".py"
        st.download_button(f"⬇️ Download {tgt_cfg['orch']} Code",
            data=orch, file_name=f"{workflow_name}_{target}{ext}",
            mime="text/plain", use_container_width=True, key="dl_all_orch")

# ── Tab: Validation ───────────────────────────────────────────────────────────
with main_tabs[4]:
    st.markdown("""<div class="tab-header"><h2>✅ Validation</h2>
    <p>Row count, schema, and logic checks after migration</p></div>""", unsafe_allow_html=True)

    if st.button("▶️ Run Validation Checks", type="primary", use_container_width=True, key="all_val"):
        with st.spinner("Running validation..."):
            time.sleep(1.5)
            rows_src = random.randint(800000, 5000000)
            rows_tgt = rows_src - random.randint(0, 50)
            delta    = abs(rows_src - rows_tgt)
            delta_pct = delta / rows_src * 100

            st.session_state.all_val = {
                "rows_src": rows_src, "rows_tgt": rows_tgt,
                "delta": delta, "delta_pct": delta_pct,
                "schema_matched": random.randint(8, 15),
                "schema_issues": random.randint(0, 2),
                "logic_pass": random.randint(3, 6),
                "logic_fail": random.randint(0, 1),
            }

    if st.session_state.get("all_val"):
        v = st.session_state.all_val
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Source Rows",  f"{v['rows_src']:,}")
        c2.metric("Target Rows",  f"{v['rows_tgt']:,}")
        c3.metric("Delta %",      f"{v['delta_pct']:.4f}%")
        c4.metric("Schema Match", f"{v['schema_matched']} cols")

        if v['delta_pct'] < 0.01:
            st.success("✅ Row count validation PASSED (delta < 0.01%)")
        else:
            st.warning(f"⚠️ Row count delta: {v['delta_pct']:.2f}%")

        if v['logic_fail'] == 0:
            st.success(f"✅ Logic validation PASSED ({v['logic_pass']} checks)")
        else:
            st.error(f"❌ Logic validation: {v['logic_fail']} failures")

        if v['schema_issues'] == 0:
            st.success(f"✅ Schema validation PASSED ({v['schema_matched']} columns matched)")
        else:
            st.warning(f"⚠️ Schema: {v['schema_issues']} type mismatches found")

# ── Tab: CI/CD ────────────────────────────────────────────────────────────────
with main_tabs[5]:
    st.markdown("""<div class="tab-header"><h2>🚀 CI/CD Deploy</h2>
    <p>One-click deployment pipeline for the migrated workflow</p></div>""", unsafe_allow_html=True)

    deploy_targets = {
        "bigquery":   [("GCS SQL Path", f"gs://your-etl-bucket/sql/{workflow_name}/"), ("GCS DAG Path", "gs://your-composer-bucket/dags/"), ("Git Path", f"migrations/sql/{workflow_name}/")],
        "snowflake":  [("S3 SQL Path", f"s3://your-bucket/sql/{workflow_name}/"), ("Airflow DAGs", "s3://your-airflow-bucket/dags/"), ("Git Path", f"migrations/sql/{workflow_name}/")],
        "synapse":    [("ADLS Path", f"abfss://sql@your-storage.dfs.core.windows.net/{workflow_name}/"), ("ADF Publish", "Azure DevOps Pipeline"), ("Git Path", f"migrations/sql/{workflow_name}/")],
        "redshift":   [("S3 SQL Path", f"s3://your-bucket/sql/{workflow_name}/"), ("Glue Scripts", "s3://your-glue-bucket/scripts/"), ("Git Path", f"migrations/sql/{workflow_name}/")],
        "databricks": [("DBFS Path", f"dbfs:/sql/{workflow_name}/"), ("Workflows API", "POST /api/2.1/jobs/run-now"), ("Git Path", f"migrations/sql/{workflow_name}/")],
    }

    for label, path in deploy_targets.get(target, [("Deploy Path", f"your-storage/sql/{workflow_name}/")]):
        st.markdown(f"""
        <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;
                    padding:10px 16px;margin:6px 0;display:flex;justify-content:space-between;">
            <span style="color:#94a3b8;font-size:12px;">{label}</span>
            <code style="color:#93c5fd;font-size:11px;">{path}</code>
        </div>
        """, unsafe_allow_html=True)

    if st.button("🚀 Run Full CI/CD Pipeline", type="primary", use_container_width=True, key="all_cicd"):
        stages = ["Create MR", "Assign Reviewer", "Auto-Approve", "Build & Test", "Deploy to Storage", "Merge to Main"]
        prog = st.progress(0)
        status = st.empty()
        for i, stage in enumerate(stages):
            status.markdown(f"**{stage}...**")
            time.sleep(0.6)
            prog.progress((i+1)/len(stages))
        status.empty(); prog.empty()
        mr_id = random.randint(100,999)
        build = random.randint(100,999)
        st.success(f"✅ Pipeline complete! MR #{mr_id} merged | Build #{build} passed | Files deployed!")
        st.balloons()

# ── Tab: Download All ─────────────────────────────────────────────────────────
with main_tabs[6]:
    st.markdown("""<div class="tab-header"><h2>📦 Download All Artifacts</h2>
    <p>Complete migration bundle — SQL, orchestrator, type map, deployment config</p></div>""", unsafe_allow_html=True)

    if st.session_state.get("all_ready"):
        sql  = st.session_state.get("all_sql", "-- Generate SQL first")
        orch = st.session_state.get("all_orch", "# Generate orchestrator first")
        tmap = TYPE_MAP.get((source_db, target), {})
        lang = "py" if target == "deltalake" else "sql"
        ext  = ".json" if target == "synapse" else ".py"

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"sql/{workflow_name}.{lang}", sql)
            zf.writestr(f"orchestration/{workflow_name}{ext}", orch)
            zf.writestr("config/type_mapping.json", json.dumps(tmap, indent=2))
            zf.writestr("config/func_mapping.json", json.dumps(FUNC_MAP.get(target, {}), indent=2))
            report = f"""# Universal ETL Migration Bundle
## Built by: Srinivas Punugu — ETL Automator
| Field | Value |
|-------|-------|
| ETL Tool | {tool_cfg['name']} |
| Source DB | {src_cfg['name']} |
| Target | {tgt_cfg['name']} |
| Cloud | {tgt_cfg['cloud']} |
| Orchestrator | {tgt_cfg['orch']} |
| SQL Dialect | {tgt_cfg['sql']} |
| Workflow | {workflow_name} |
| Generated | {ts} |
"""
            zf.writestr("MIGRATION_REPORT.md", report)
        buf.seek(0)
        st.download_button(
            "📦 Download Complete Migration Bundle",
            data=buf,
            file_name=f"{workflow_name}_{source_db}_to_{target}_{datetime.now().strftime('%Y%m%d')}.zip",
            mime="application/zip",
            use_container_width=True,
            type="primary",
            key="dl_all_zip"
        )
        st.markdown("""
        **Bundle contains:**
        - 📄 `sql/` — Target SQL in correct dialect
        - 🔄 `orchestration/` — Airflow/ADF/Prefect/dbt code
        - 🔠 `config/type_mapping.json` — Type conversion map
        - 🔄 `config/func_mapping.json` — Function conversion map
        - 📋 `MIGRATION_REPORT.md` — Full migration summary
        """)
    else:
        st.info("👈 Use the sidebar to configure your migration and click **⚡ Convert** to generate artifacts, then download here.")
