# ⚡ ETL Automator

**Built by Srinivas Punugu**

> Automates migration of Informatica PowerCenter ETL workflows to Google BigQuery + Apache Airflow

---

## 🎯 What This Does

Takes Informatica PowerCenter workflows and converts them to:
- **BigQuery SQL** — one file per Informatica session
- **Airflow DAG** — replaces the Informatica workflow scheduler

What used to take weeks manually now takes hours.

---

## 🗂️ 5-Tab Workflow

| Tab | Purpose |
|-----|---------|
| 1️⃣ Repository Explorer | Connect to Informatica, browse & export XML |
| 2️⃣ Lineage Analysis | Visual data flow diagram |
| 3️⃣ Schema Analyzer | Map Teradata → BigQuery types, generate DDL |
| 4️⃣ SQL Converter | Generate BigQuery SQL + Airflow DAG |
| 5️⃣ Validation | 3-step QA: Logic + Row Count + Schema |

---

## 🚀 Run Locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

Open http://localhost:8501 — runs in **Demo Mode** by default.

---

## 🔧 Real Mode

1. Toggle **Demo Mode off** in the sidebar
2. Install Informatica PowerCenter Client (Windows)
3. Configure GCP credentials: `gcloud auth application-default login`
4. Enter repo connection details in Tab 1

---

## 📁 Structure

```
etl_automator/
├── app.py                    ← Main entry point
├── requirements.txt
├── .streamlit/config.toml
├── pages/
│   ├── tab1_repository.py
│   ├── tab2_lineage.py
│   ├── tab3_schema.py
│   ├── tab4_converter.py
│   └── tab5_validation.py
├── utils/
│   ├── parser.py             ← XML parser + SQL/DAG generators
│   └── infa_client.py        ← Informatica CLI wrapper
└── mock_data/
    └── sample_data.py        ← Demo data
```

---

Built by **Srinivas Punugu** · ETL Automator v1.0
