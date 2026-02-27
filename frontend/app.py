"""
============================================================
Streamlit Dashboard — Claims Lakehouse Optimizer
============================================================
WHAT: Visual dashboard showing table health and operations
WHY:  Non-technical people (managers, analysts) need visuals

STORY: The API is the kitchen, this dashboard is the 
       dining room. Customers don't go into the kitchen —
       they see the beautifully presented food (data) 
       on their table (screen).
============================================================
"""

import streamlit as st
import requests
import os

# ============================================================
# Page Configuration
# ============================================================
st.set_page_config(
    page_title="Claims Lakehouse Optimizer",
    page_icon="🏥",
    layout="wide",
)

# API URL — inside Docker, containers use service names
API_URL = os.getenv("API_URL", "http://api:8000")

# ============================================================
# Header
# ============================================================
st.title("🏥 Claims Lakehouse Predictive Optimizer")
st.caption("Automated Delta Lake maintenance for healthcare claims data")
st.divider()

# ============================================================
# Top Metrics Row
# ============================================================
# STORY: Like a hospital dashboard showing key stats at a glance
#        Doctors see: Patients today, Beds available, ER wait time
#        We show: Tables monitored, Operations today, Bytes saved

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="📊 Tables Monitored", value="3")
with col2:
    st.metric(label="⚡ Operations Today", value="0", delta="Starting up")
with col3:
    st.metric(label="💾 Storage Saved", value="0 MB")
with col4:
    st.metric(label="✅ Healthy Tables", value="1 / 3")

st.divider()

# ============================================================
# Table Health Section
# ============================================================
st.header("📋 Table Health Overview")

# Try to fetch data from API
try:
    response = requests.get(f"{API_URL}/api/sample-tables", timeout=5)
    
    if response.status_code == 200:
        data = response.json()
        tables = data.get("tables", [])
        
        for table in tables:
            # Color-code based on health
            if table["small_files_percentage"] > 40:
                status_emoji = "🔴"
                status_text = "Needs Attention"
            elif table["small_files_percentage"] > 25:
                status_emoji = "🟡"  
                status_text = "Warning"
            else:
                status_emoji = "🟢"
                status_text = "Healthy"
            
            with st.container():
                c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 2])
                
                with c1:
                    st.markdown(f"**{status_emoji} {table['table_name']}**")
                with c2:
                    st.metric("Files", table["total_files"])
                with c3:
                    st.metric("Small Files %", f"{table['small_files_percentage']}%")
                with c4:
                    size_mb = table["total_size_bytes"] / (1024 * 1024)
                    st.metric("Size", f"{size_mb:.0f} MB")
                with c5:
                    actions = []
                    if table["needs_optimize"]:
                        actions.append("OPTIMIZE")
                    if table["needs_vacuum"]:
                        actions.append("VACUUM")
                    if table["needs_analyze"]:
                        actions.append("ANALYZE")
                    st.markdown(f"**Needs:** {', '.join(actions) if actions else 'Nothing'}")
                
                st.divider()
    else:
        st.warning(f"API returned status {response.status_code}")
        
except requests.exceptions.ConnectionError:
    st.info("⏳ Waiting for API to start... Refresh the page in a few seconds.")
except Exception as e:
    st.error(f"Error connecting to API: {str(e)}")

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    **Claims Lakehouse Predictive Optimizer**
    
    Simulates Databricks Predictive Optimization 
    for healthcare claims Delta tables.
    
    **What it monitors:**
    - OPTIMIZE (file compaction)
    - VACUUM (storage cleanup)
    - ANALYZE (statistics refresh)
    
    **Data:** CMS SynPUF-style synthetic claims
    """)
    
    st.divider()
    st.header("🔗 Quick Links")
    st.markdown("""
    - [API Docs](http://localhost:8000/docs)
    - [Spark UI](http://localhost:8080)
    """)
    
    st.divider()
    
    # API connection status
    st.header("🔌 Connection Status")
    try:
        r = requests.get(f"{API_URL}/health", timeout=3)
        if r.status_code == 200:
            st.success("API: Connected ✅")
        else:
            st.warning("API: Error")
    except:
        st.error("API: Not Connected ❌")