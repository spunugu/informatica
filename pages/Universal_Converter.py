"""Universal ETL Converter — redirects to All_Migrations page"""
import streamlit as st

st.set_page_config(
    page_title="Universal ETL Converter — by Srinivas Punugu",
    page_icon="🌐", layout="wide",
)

# Just redirect to All_Migrations which has the full implementation
st.switch_page("pages/All_Migrations.py")
