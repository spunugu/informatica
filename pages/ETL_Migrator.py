"""ETL Migrator page — redirects to main app"""
import streamlit as st, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

st.set_page_config(page_title="ETL Automator — Informatica → GCP", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

# Import and run the full app logic inline
exec(open(os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py")).read())
