"""ETL Migrator — redirects to main app"""
import streamlit as st

st.set_page_config(
    page_title="ETL Automator — Informatica → GCP",
    page_icon="⚡", layout="wide",
)

st.markdown("## ⚡ ETL Automator — Informatica → GCP")
st.markdown("Click below to open the full Informatica migration tool:")
st.page_link("app.py", label="→ Open ETL Automator", icon="⚡")
