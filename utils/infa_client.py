"""
Informatica PowerCenter CLI Wrapper
Switches between real pmrep/pmcmd calls and mock data based on DEMO_MODE
"""

import subprocess, os, time
from typing import Dict, List, Optional
from mock_data.sample_data_base import MOCK_FOLDERS, MOCK_WORKFLOWS, MOCK_SESSION_IO, DEFAULT_MOCK, MOCK_XML_TEMPLATE

INFA_BIN = r"C:\ProgramData\Informatica\10.5.3\clients\PowerCenterClient\CommandLineUtilities\PC\server\bin"


def is_demo_mode() -> bool:
    return os.environ.get("DEMO_MODE", "true").lower() == "true"


def run_pmrep(args, timeout=60):
    if is_demo_mode():
        return {"success": True, "stdout": "MOCK_MODE", "stderr": ""}
    try:
        result = subprocess.run([os.path.join(INFA_BIN, "pmrep.exe")] + args, capture_output=True, text=True, timeout=timeout)
        return {"success": result.returncode == 0, "stdout": result.stdout, "stderr": result.stderr}
    except Exception as e:
        return {"success": False, "stdout": "", "stderr": str(e)}


def connect_repository(host, port, repo, user, password):
    if is_demo_mode():
        time.sleep(1.0)
        return {"success": True, "message": "Connected to ETL Repository (DEMO MODE)"}
    result = run_pmrep(["connect", "-r", repo, "-h", host, "-o", port, "-n", user, "-x", password])
    return {"success": result["success"], "message": result["stdout"] or result["stderr"]}


def list_folders():
    if is_demo_mode():
        time.sleep(0.6)
        return MOCK_FOLDERS
    result = run_pmrep(["listobjects", "-o", "folder"])
    if not result["success"]: return []
    return [l.strip() for l in result["stdout"].strip().split("\n") if l.strip() and not l.startswith("pmrep>")]


def list_workflows(folder):
    if is_demo_mode():
        time.sleep(0.5)
        return MOCK_WORKFLOWS.get(folder, ["wf_sample_workflow_1", "wf_sample_workflow_2"])
    result = run_pmrep(["listobjects", "-o", "workflow", "-f", folder])
    if not result["success"]: return []
    return [l.strip() for l in result["stdout"].strip().split("\n") if l.strip() and not l.startswith("pmrep>")]


def export_workflow_xml(folder, workflow):
    if is_demo_mode():
        time.sleep(1.5)
        mock = MOCK_SESSION_IO.get(workflow, DEFAULT_MOCK)
        return MOCK_XML_TEMPLATE.format(
            folder=folder, workflow=workflow,
            source=mock["sources"][0], target=mock["targets"][0]
        )
    result = run_pmrep(["objectexport", "-n", workflow, "-o", "workflow", "-f", folder, "-m", "-s", "-b", "-r"])
    return result.get("stdout", "")


def get_session_stats(workflow, session):
    if is_demo_mode():
        time.sleep(0.4)
        mock = MOCK_SESSION_IO.get(workflow, DEFAULT_MOCK)
        return {"success": True, "rows_read": mock["row_count"], "rows_written": mock["row_count"], "rows_rejected": 0}
    return {"success": False}


def get_bq_row_count(project, dataset, table):
    if is_demo_mode():
        import random
        return 4823910 + random.randint(-100, 100)
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project)
        result = client.query(f"SELECT COUNT(*) as cnt FROM `{project}.{dataset}.{table}` WHERE DATE(etl_load_dt) = CURRENT_DATE()").result()
        for row in result:
            return row.cnt
    except:
        return None
