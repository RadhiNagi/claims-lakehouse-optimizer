"""
============================================================
Streamlit Dashboard — Claims Lakehouse Predictive Optimizer
============================================================
WHAT: Visual dashboard showing REAL table health and recommendations
WHY:  Makes the optimizer's intelligence visible to humans

STORY: In hospitals, doctors look at patient monitors showing 
       heart rate, blood pressure, oxygen levels — all in 
       real-time. THIS dashboard is the "patient monitor" 
       for your Delta Lake tables.
       
       The dashboard calls the API every time you load it.
       The API calls the optimizer brain.
       The optimizer brain scans the Delta tables.
       Fresh results every time!

INTERVIEW: "I built a Streamlit dashboard that provides 
       real-time visibility into Delta table health metrics, 
       optimization recommendations with priority scoring, 
       and operational summaries — similar to the Databricks 
       Predictive Optimization UI."
============================================================
"""

import streamlit as st
import requests
import os
import json

# ============================================================
# Page Configuration
# ============================================================
st.set_page_config(
    page_title="Claims Lakehouse Optimizer",
    page_icon="🏥",
    layout="wide",
)

# API URL — inside Docker, containers find each other by service name
API_URL = os.getenv("API_URL", "http://api:8000")


# ============================================================
# Helper Functions
# ============================================================
def call_api(endpoint: str, timeout: int = 30):
    """
    Call the API and return JSON response.
    
    WHY timeout=30? The first API call starts Spark, 
    which can take 10-20 seconds. After that it's fast.
    """
    try:
        response = requests.get(f"{API_URL}{endpoint}", timeout=timeout)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API error: {response.status_code}")
            return None
    except requests.exceptions.ConnectionError:
        st.warning("⏳ Connecting to API... Please wait and refresh.")
        return None
    except requests.exceptions.Timeout:
        st.warning("⏳ API is starting Spark engine... This takes ~20 seconds on first load. Please refresh.")
        return None
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None


def format_bytes(bytes_val: int) -> str:
    """Convert bytes to human-readable format"""
    if bytes_val >= 1024 * 1024 * 1024:
        return f"{bytes_val / (1024**3):.1f} GB"
    elif bytes_val >= 1024 * 1024:
        return f"{bytes_val / (1024**2):.1f} MB"
    elif bytes_val >= 1024:
        return f"{bytes_val / 1024:.1f} KB"
    return f"{bytes_val} B"


def health_color(small_pct: float) -> str:
    """Return emoji based on small files percentage"""
    if small_pct > 40:
        return "🔴"
    elif small_pct > 25:
        return "🟡"
    return "🟢"


def priority_label(priority: float) -> str:
    """Convert priority number to label"""
    if priority > 0.7:
        return "🔴 HIGH"
    elif priority > 0.4:
        return "🟡 MEDIUM"
    return "🟢 LOW"


# ============================================================
# Header
# ============================================================
st.title("🏥 Claims Lakehouse Predictive Optimizer")
st.caption("Real-time Delta Lake health monitoring & automated maintenance recommendations")
st.divider()

# ============================================================
# Refresh Button
# ============================================================
# WHY a refresh button? Each click triggers a fresh health check.
# In production, this would run on a schedule (every 15 minutes).
col_refresh, col_status = st.columns([1, 5])
with col_refresh:
    refresh = st.button("🔄 Refresh Data", type="primary")

# ============================================================
# TOP METRICS ROW (from /api/summary)
# ============================================================
summary = call_api("/api/summary")

if summary:
    with col_status:
        st.caption(f"Last checked: {summary.get('checked_at', 'N/A')[:19]}")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="📊 Tables Monitored",
            value=summary["total_tables"],
        )
    with col2:
        st.metric(
            label="✅ Healthy Tables",
            value=f"{summary['healthy_tables']} / {summary['total_tables']}",
        )
    with col3:
        st.metric(
            label="⚠️ Need Attention",
            value=summary["tables_needing_attention"],
        )
    with col4:
        st.metric(
            label="📁 Total Files",
            value=summary["total_files"],
        )
    with col5:
        st.metric(
            label="💾 Total Size",
            value=f"{summary['total_size_mb']} MB",
        )
    
    st.divider()

# ============================================================
# TABLE HEALTH SECTION (from /api/tables/health)
# ============================================================
st.header("📋 Table Health Overview")

health_data = call_api("/api/tables/health")

if health_data:
    tables = health_data.get("tables", [])
    
    if not tables:
        st.info("No Delta tables found. Run the data loader first.")
    else:
        for table in tables:
            # Determine health status
            small_pct = table.get("small_files_percentage", 0)
            emoji = health_color(small_pct)
            
            needs = []
            if table.get("needs_optimize"):
                needs.append("🔧 OPTIMIZE")
            if table.get("needs_vacuum"):
                needs.append("🧹 VACUUM")
            if table.get("needs_analyze"):
                needs.append("📊 ANALYZE")
            
            status_text = " • ".join(needs) if needs else "✅ Healthy"
            
            # Table card
            with st.container(border=True):
                # Row 1: Name and status
                name_col, status_col = st.columns([3, 2])
                with name_col:
                    st.subheader(f"{emoji} {table['table_name']}")
                with status_col:
                    st.markdown(f"**Status:** {status_text}")
                
                # Row 2: Metrics
                m1, m2, m3, m4, m5 = st.columns(5)
                
                with m1:
                    st.metric("Total Files", table["total_files"])
                with m2:
                    st.metric("Small Files", table.get("small_files_count", 0))
                with m3:
                    st.metric("Small Files %", f"{small_pct}%")
                with m4:
                    st.metric("Size", format_bytes(table["total_size_bytes"]))
                with m5:
                    st.metric("Rows", f"{table.get('row_count', 0):,}")

    st.divider()

# ============================================================
# RECOMMENDATIONS SECTION (from /api/recommendations)
# ============================================================
st.header("💡 Optimization Recommendations")

rec_data = call_api("/api/recommendations")

if rec_data:
    recommendations = rec_data.get("recommendations", [])
    
    if not recommendations:
        st.success("🎉 All tables are healthy! No actions needed.")
    else:
        st.info(f"Found **{len(recommendations)}** recommendation(s) across {rec_data['tables_analyzed']} tables")
        
        for i, rec in enumerate(recommendations):
            with st.container(border=True):
                # Header row
                h1, h2, h3 = st.columns([3, 1, 1])
                
                with h1:
                    # Operation icon
                    op = rec["operation"]
                    op_icon = "🔧" if op == "OPTIMIZE" else "🧹" if op == "VACUUM" else "📊"
                    st.markdown(f"### {op_icon} {op} → `{rec['table_name']}`")
                
                with h2:
                    st.metric("Priority", priority_label(rec["priority"]))
                
                with h3:
                    st.metric("Est. Cost", f"{rec['estimated_cost_dbu']} DBU")
                
                # Details
                st.markdown(f"**Reason:** {rec['reason']}")
                st.markdown(f"**Expected Improvement:** {rec['estimated_improvement']}")
    
    st.divider()

# ============================================================
# ARCHITECTURE INFO (for interviews!)
# ============================================================
with st.expander("🏗️ System Architecture", expanded=False):
    st.markdown("""
    **Data Pipeline:**
```
    CMS SynPUF CSV → PySpark → Delta Lake (partitioned by month)
```
    
    **Optimization Engine:**
```
    Health Monitor → Threshold Analysis → Priority Scoring → Recommendations
```
    
    **Technology Stack:**
    - **Data Format:** Delta Lake on Apache Spark
    - **API:** FastAPI with auto-generated Swagger docs
    - **Dashboard:** Streamlit with real-time API integration
    - **Database:** PostgreSQL for operations metadata
    - **Infrastructure:** Docker Compose (3 services)
    - **Data:** CMS SynPUF-style healthcare claims (10K records)
    
    **Optimization Operations:**
    - **OPTIMIZE:** Compacts small files into optimal 128MB files
    - **VACUUM:** Removes old unreferenced files to save storage
    - **ANALYZE:** Refreshes table statistics for better query plans
    """)

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    **Claims Lakehouse Predictive Optimizer**
    
    Simulates Databricks Predictive Optimization 
    for healthcare claims Delta tables.
    """)
    
    st.divider()
    
    st.header("🔗 Quick Links")
    st.markdown("""
    - [📖 API Docs](http://localhost:8000/docs)
    - [📊 API Summary](http://localhost:8000/api/summary)
    - [💡 API Recommendations](http://localhost:8000/api/recommendations)
    """)
    
    st.divider()
    
    # Connection status
    st.header("🔌 Status")
    try:
        r = requests.get(f"{API_URL}/health", timeout=5)
        if r.status_code == 200:
            st.success("API: Connected ✅")
        else:
            st.warning("API: Error ⚠️")
    except:
        st.error("API: Not Connected ❌")
    
    st.divider()
    
    st.header("📊 Quick Stats")
    if summary:
        st.markdown(f"""
        - **Tables:** {summary['total_tables']}
        - **Files:** {summary['total_files']}
        - **Size:** {summary['total_size_mb']} MB
        - **Recommendations:** {summary['total_recommendations']}
        """)