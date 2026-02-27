"""
============================================================
Claims Lakehouse Predictive Optimizer — Main API
============================================================
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

from app.config import APP_NAME, APP_VERSION
from app.api.routes_tables import router as tables_router

# Create FastAPI app
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

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routes
app.include_router(tables_router)


@app.get("/")
def root():
    """Root health check"""
    return {
        "name": APP_NAME,
        "version": APP_VERSION,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "table_health": "/api/tables/health",
            "recommendations": "/api/recommendations",
            "summary": "/api/summary",
        }
    }


@app.get("/health")
def health_check():
    """Health check for Docker/Kubernetes"""
    return {
        "status": "healthy",
        "api": True,
        "timestamp": datetime.utcnow().isoformat(),
    }