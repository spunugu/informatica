"""Universal ETL Converter — redirects to All_Migrations page"""
import streamlit as st

st.set_page_config(
    page_title="Universal ETL Converter — by Srinivas Punugu",
    page_icon="🌐", layout="wide",
)

st.markdown("## 🌐 Universal ETL Converter")
st.markdown("Redirecting to All Migrations...")
st.page_link("pages/All_Migrations.py", label="→ Go to All Migrations", icon="🌐")
