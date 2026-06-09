"""
Mock data for demo mode - simulates real Informatica PowerCenter responses
"""

MOCK_FOLDERS = [
    "BILLING_DOMAIN", "CRM_DOMAIN", "NETWORK_DOMAIN",
    "FINANCE_DOMAIN", "HR_DOMAIN", "REPORTING_DOMAIN"
]

MOCK_WORKFLOWS = {
    "BILLING_DOMAIN": ["wf_billing_daily_load", "wf_billing_monthly_summary", "wf_billing_reconciliation", "wf_invoice_generation"],
    "CRM_DOMAIN": ["wf_customer_sync", "wf_account_update", "wf_lead_scoring", "wf_churn_prediction"],
    "NETWORK_DOMAIN": ["wf_network_usage_etl", "wf_tower_stats_load", "wf_outage_tracking"],
    "FINANCE_DOMAIN": ["wf_gl_journal_load", "wf_revenue_recognition", "wf_cost_allocation"],
    "HR_DOMAIN": ["wf_employee_sync", "wf_payroll_etl", "wf_headcount_report"],
    "REPORTING_DOMAIN": ["wf_executive_dashboard", "wf_kpi_aggregation", "wf_data_quality_report"]
}

MOCK_SESSION_IO = {
    "wf_billing_daily_load": {
        "sessions": ["s_m_billing_extract", "s_m_billing_transform", "s_m_billing_load"],
        "worklets": ["wklt_billing_pre_checks", "wklt_billing_post_checks"],
        "mappings": ["m_billing_extract", "m_billing_transform", "m_billing_load"],
        "sources": ["STG_BILLING_RAW", "LKP_ACCOUNT_MASTER", "LKP_RATE_TABLE"],
        "targets": ["DWH_BILLING_FACT", "DWH_BILLING_DAILY_AGG"],
        "lookups": ["LKP_ACCOUNT_MASTER", "LKP_RATE_TABLE", "LKP_TAX_CODES"],
        "complexity": "High",
        "row_count": 4823910
    },
    "wf_billing_monthly_summary": {
        "sessions": ["s_m_monthly_agg", "s_m_monthly_load"],
        "worklets": ["wklt_monthly_pre"],
        "mappings": ["m_monthly_agg", "m_monthly_load"],
        "sources": ["DWH_BILLING_FACT", "LKP_CALENDAR"],
        "targets": ["DWH_BILLING_MONTHLY"],
        "lookups": ["LKP_CALENDAR"],
        "complexity": "Medium",
        "row_count": 152340
    },
    "wf_customer_sync": {
        "sessions": ["s_m_cust_extract", "s_m_cust_cleanse", "s_m_cust_load"],
        "worklets": ["wklt_cust_validation"],
        "mappings": ["m_cust_extract", "m_cust_cleanse", "m_cust_load"],
        "sources": ["SRC_CRM_CUSTOMERS", "SRC_CRM_CONTACTS", "LKP_GEO_CODES"],
        "targets": ["DWH_CUSTOMER_DIM", "DWH_CONTACT_DIM"],
        "lookups": ["LKP_GEO_CODES", "LKP_SEGMENT_CODES"],
        "complexity": "Medium",
        "row_count": 8934210
    },
    "wf_network_usage_etl": {
        "sessions": ["s_m_usage_extract", "s_m_usage_agg", "s_m_usage_load"],
        "worklets": ["wklt_usage_pre", "wklt_usage_post", "wklt_usage_audit"],
        "mappings": ["m_usage_extract", "m_usage_agg", "m_usage_load"],
        "sources": ["SRC_NETWORK_EVENTS", "SRC_TOWER_DATA", "LKP_DEVICE_MASTER"],
        "targets": ["DWH_NETWORK_USAGE_FACT", "DWH_TOWER_AGG"],
        "lookups": ["LKP_DEVICE_MASTER", "LKP_PLAN_CODES", "LKP_TOWER_INFO"],
        "complexity": "Critical",
        "row_count": 98234100
    }
}

DEFAULT_MOCK = {
    "sessions": ["s_m_extract", "s_m_transform", "s_m_load"],
    "worklets": ["wklt_pre_process"],
    "mappings": ["m_extract", "m_transform", "m_load"],
    "sources": ["SRC_TABLE_A", "SRC_TABLE_B"],
    "targets": ["TGT_TABLE_C"],
    "lookups": ["LKP_REF_DATA"],
    "complexity": "Low",
    "row_count": 125000
}

MOCK_XML_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="05/01/2026 09:00:00" REPOSITORY_VERSION="187.93">
<REPOSITORY NAME="ETLRepo" VERSION="187" CODEPAGE="UTF-8">
<FOLDER NAME="{folder}" GROUP="" OWNER="admin">
  <SOURCE DBDNAME="TERADATA_PRD" NAME="{source}" OWNERNAME="STG">
    <SOURCEFIELD DATATYPE="VARCHAR" LENGTH="50" NAME="ACCOUNT_ID" NULLABLE="NOT NULL"/>
    <SOURCEFIELD DATATYPE="VARCHAR" LENGTH="100" NAME="ACCOUNT_NAME" NULLABLE="NULL"/>
    <SOURCEFIELD DATATYPE="DECIMAL" LENGTH="15" NAME="BILL_AMOUNT" NULLABLE="NULL"/>
    <SOURCEFIELD DATATYPE="DATE/TIME" LENGTH="29" NAME="BILL_DATE" NULLABLE="NULL"/>
    <SOURCEFIELD DATATYPE="INTEGER" LENGTH="10" NAME="CUSTOMER_ID" NULLABLE="NOT NULL"/>
  </SOURCE>
  <TARGET DBDNAME="BIGQUERY_PRD" NAME="{target}" OWNERNAME="dwh">
    <TARGETFIELD DATATYPE="STRING" NAME="account_id" NULLABLE="NOT NULL"/>
    <TARGETFIELD DATATYPE="STRING" NAME="account_name" NULLABLE="NULL"/>
    <TARGETFIELD DATATYPE="NUMERIC" NAME="bill_amount" NULLABLE="NULL"/>
    <TARGETFIELD DATATYPE="TIMESTAMP" NAME="bill_date" NULLABLE="NULL"/>
    <TARGETFIELD DATATYPE="INTEGER" NAME="customer_id" NULLABLE="NOT NULL"/>
    <TARGETFIELD DATATYPE="TIMESTAMP" NAME="etl_load_dt" NULLABLE="NULL"/>
  </TARGET>
  <MAPPING NAME="m_{workflow}_extract">
    <TRANSFORMATION NAME="SQ_{source}" TYPE="Source Qualifier">
      <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM {source} WHERE BILL_DATE = TRUNC(SYSDATE)"/>
    </TRANSFORMATION>
    <TRANSFORMATION NAME="LKP_ACCOUNT_MASTER" TYPE="Lookup Procedure">
      <TABLEATTRIBUTE NAME="Lookup table name" VALUE="ACCOUNT_MASTER"/>
    </TRANSFORMATION>
    <TRANSFORMATION NAME="EXP_TRANSFORM" TYPE="Expression">
      <TRANSFORMFIELD EXPRESSION="TRIM(ACCOUNT_NAME)" NAME="OUT_ACCOUNT_NAME"/>
      <TRANSFORMFIELD EXPRESSION="SYSDATE" NAME="OUT_ETL_LOAD_DT"/>
    </TRANSFORMATION>
  </MAPPING>
  <SESSION NAME="s_m_{workflow}_extract" REUSABLE="NO">
    <SESSCONNECTREF CNXREFNAME="TD_PRD" OBJECTNAME="{source}" VARIABLE="$Source"/>
    <SESSCONNECTREF CNXREFNAME="BQ_PRD" OBJECTNAME="{target}" VARIABLE="$Target"/>
  </SESSION>
  <WORKFLOW NAME="{workflow}" REUSABLE="NO" SCHEDULENAME="DAILY_6AM">
    <WORKLET NAME="wklt_pre_checks">
      <TASK NAME="s_m_{workflow}_extract" TYPE="Session" REUSABLE="NO"/>
    </WORKLET>
  </WORKFLOW>
</FOLDER>
</REPOSITORY>
</POWERMART>'''

MOCK_VALIDATION_RESULTS = {
    "logic": {
        "passed": 8, "failed": 1, "warnings": 2,
        "details": [
            {"session": "s_m_billing_extract", "status": "PASS", "sources_matched": True, "targets_matched": True, "lookups_matched": True},
            {"session": "s_m_billing_transform", "status": "PASS", "sources_matched": True, "targets_matched": True, "lookups_matched": True},
            {"session": "s_m_billing_load", "status": "WARN", "message": "1 optional lookup not referenced in generated SQL"},
            {"session": "s_m_monthly_agg", "status": "FAIL", "message": "Target table DWH_BILLING_MONTHLY_V2 not found in generated SQL"},
        ]
    },
    "row_counts": {"informatica": 4823910, "bigquery": 4823910, "match": True, "delta_pct": 0.0},
    "schema": {
        "matched_columns": 47, "missing_columns": 1, "extra_columns": 2, "type_mismatches": 0,
        "details": [
            {"column": "etl_load_dt", "status": "EXTRA", "note": "Added by BQ pipeline"},
            {"column": "legacy_ref_id", "status": "MISSING", "note": "Deprecated column not migrated"},
        ]
    }
}
