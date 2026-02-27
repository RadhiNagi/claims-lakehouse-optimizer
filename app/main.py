"""
============================================================
Claims Lakehouse Predictive Optimizer — Main API
============================================================
WHAT: The FastAPI application that powers everything
WHY:  This is the ENTRY POINT — when Docker starts the 
      container, THIS file runs first.

STORY: This is the FRONT DOOR of your restaurant.
       When a customer (user/dashboard) wants something:
       - "Show me table health" → GET /api/health
       - "What needs optimizing?" → GET /api/recommendations
       - "Run OPTIMIZE on table X" → POST /api/operations/run
       
       FastAPI handles the request, talks to the database 
       and Spark, and returns the answer.

INTERVIEW TIP: "I used FastAPI because it auto-generates 
       API documentation (Swagger UI) and validates all 
       inputs with Pydantic — reducing bugs and making 
       the API self-documenting."
============================================================
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

from app.config import APP_NAME, APP_VERSION

# ============================================================
# Create the FastAPI application
# ============================================================
app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="""
    ## Claims Lakehouse Predictive Optimizer
    
    Simulates Databricks Predictive Optimization for Delta Lake tables.
    
    **What it does:**
    - Monitors Delta table health (file count, sizes, staleness)
    - Recommends OPTIMIZE, VACUUM, and ANALYZE operations
    - Tracks operations history and cost savings
    
    **Based on:** 
    - Databricks Predictive Optimization
    - CMS SynPUF healthcare claims data structure
    """,
)

# ============================================================
# CORS Middleware
# ============================================================
# WHAT: Allows the Streamlit frontend to talk to this API
# WHY:  By default, browsers BLOCK requests between different 
#       ports (8501 → 8000). CORS says "it's okay, let them talk."
# STORY: Like a security guard at the restaurant door.
#        Without CORS: "Sorry, you're from Port 8501, not allowed!"
#        With CORS: "Port 8501 is on the guest list, come in!"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # In production, restrict this!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Root Endpoint (Health Check)
# ============================================================
@app.get("/")
def root():
    """
    WHAT: Basic health check — "is the API alive?"
    WHEN: First thing you check when something seems broken
    
    STORY: Like calling a restaurant and asking 
           "Are you open today?" — simple yes/no.
    """
    return {
        "name": APP_NAME,
        "version": APP_VERSION,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "message": "Welcome to the Claims Lakehouse Optimizer API! Visit /docs for Swagger UI."
    }


# ============================================================
# Health Check for Docker/Kubernetes
# ============================================================
@app.get("/health")
def health_check():
    """
    WHAT: Detailed health check
    WHY:  Docker and Kubernetes use this to know if the 
          container is healthy. If this fails, they restart it.
    
    INTERVIEW TIP: "I included health checks so the container
          orchestrator can automatically restart unhealthy services."
    """
    return {
        "status": "healthy",
        "api": True,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ============================================================
# Sample Data Endpoint (Temporary — will be replaced in Step 8)
# ============================================================
@app.get("/api/sample-tables")
def get_sample_tables():
    """
    WHAT: Returns sample table health data for testing
    WHY:  We need something to show in the dashboard 
          BEFORE we connect to real Delta tables
    
    This will be replaced with real data in Step 8.
    """
    return {
        "tables": [
            {
                "table_name": "fact_claim_line",
                "total_files": 150,
                "small_files_count": 65,
                "small_files_percentage": 43.3,
                "total_size_bytes": 524288000,
                "needs_optimize": True,
                "needs_vacuum": False,
                "needs_analyze": True,
            },
            {
                "table_name": "dim_provider",
                "total_files": 12,
                "small_files_count": 2,
                "small_files_percentage": 16.7,
                "total_size_bytes": 10485760,
                "needs_optimize": False,
                "needs_vacuum": True,
                "needs_analyze": False,
            },
            {
                "table_name": "dim_beneficiary",
                "total_files": 25,
                "small_files_count": 10,
                "small_files_percentage": 40.0,
                "total_size_bytes": 52428800,
                "needs_optimize": True,
                "needs_vacuum": False,
                "needs_analyze": True,
            },
        ]
    }