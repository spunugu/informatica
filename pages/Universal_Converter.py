"""Universal ETL Converter — standalone page"""
import streamlit as st, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

st.set_page_config(
    page_title="Universal ETL Converter — by Srinivas Punugu",
    page_icon="🌐", layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html,body,[class*="css"]{font-family:'Inter',sans-serif!important;}
  .stApp{background:#0a0f1e;color:#e2e8f0;}
  .tab-header{border-left:3px solid #22c55e;padding-left:16px;margin-bottom:24px;}
  .tab-header h2{margin:0 0 4px;font-size:22px;color:#f1f5f9;}
  .tab-header p{margin:0;color:#94a3b8;font-size:14px;}
  [data-testid="metric-container"]{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px!important;}
  [data-testid="stMetricLabel"]{color:#94a3b8!important;font-size:12px!important;}
  [data-testid="stMetricValue"]{color:#f1f5f9!important;font-size:24px!important;font-weight:700!important;}
  .stButton>button[kind="primary"]{background:linear-gradient(135deg,#22c55e,#16a34a)!important;border:none!important;color:white!important;font-weight:600!important;border-radius:8px!important;}
  .stButton>button{border-radius:8px!important;border:1px solid #334155!important;background:#1e293b!important;color:#e2e8f0!important;}
  hr{border-color:#1e293b!important;}
  [data-testid="stSidebar"]{background:#0f172a!important;border-right:1px solid #1e293b;}
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div style="background:linear-gradient(135deg,#0f172a,#1e293b);border-bottom:1px solid #22c55e33;
            padding:20px 32px 16px;margin:-1rem -1rem 1.5rem -1rem;">
    <div style="font-size:28px;font-weight:700;color:#fff;">🌐 Universal ETL <span style="color:#86efac;">Converter</span></div>
    <div style="font-size:13px;color:#94a3b8;margin-top:4px;">Any ETL Tool → Any Cloud Platform</div>
    <div style="font-size:12px;color:#22c55e;margin-top:2px;font-weight:600;">Built by Srinivas Punugu</div>
</div>
""", unsafe_allow_html=True)

# Back button
if st.button("← Back to Home", key="back_home"):
    st.switch_page("Home.py")

st.markdown("---")

# Run the universal converter tab
from pages.tab8_universal import render
render()
