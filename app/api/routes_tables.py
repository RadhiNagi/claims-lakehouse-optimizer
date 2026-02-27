"""
============================================================
Table Health & Optimization API Routes
============================================================
WHAT: API endpoints that expose the optimizer's intelligence
WHY:  The dashboard (and external tools) need a way to 
      ASK the optimizer for table health and recommendations

ENDPOINTS:
  GET  /api/tables/health          → Health of all tables
  GET  /api/tables/{name}/health   → Health of one table
  GET  /api/recommendations        → What needs fixing
  POST /api/operations/optimize    → Run OPTIMIZE
  GET  /api/summary                → Dashboard summary

STORY: These endpoints are the MENU at your restaurant.
       Customers (dashboard) look at the menu and order:
       "I'd like the table health report, please!"
       The waiter (FastAPI) takes the order to the kitchen 
       (optimizer) and brings back the result.
============================================================
"""

import os
from datetime import datetime
from typing import List
from fastapi import APIRouter, HTTPException

from app.config import DATA_DELTA_PATH
from app.core.spark_manager import get_spark
from app.core.health_monitor import DeltaHealthMonitor
from app.core.optimizer_engine import PredictiveOptimizer

# Create a router (a group of related endpoints)
# prefix="/api" means all these endpoints start with /api/
router = APIRouter(prefix="/api", tags=["Table Operations"])


@router.get("/tables/health")
def get_all_tables_health():
    """
    Get health status of ALL Delta tables.
    
    WHAT HAPPENS:
    1. Get SparkSession (reuses existing one)
    2. Health Monitor scans all Delta tables
    3. Optimizer analyzes each table
    4. Returns combined health + recommendations
    
    USED BY: Dashboard's "Table Health Overview" section
    """
    spark = get_spark()
    
    # Step 1: Monitor checks all tables
    monitor = DeltaHealthMonitor(spark, DATA_DELTA_PATH)
    health_reports = monitor.check_all_tables()
    
    # Step 2: Optimizer analyzes health
    optimizer = PredictiveOptimizer()
    for health in health_reports:
        optimizer.analyze(health)
    
    # Step 3: Return results
    return {
        "tables": [h.to_dict() for h in health_reports],
        "checked_at": datetime.utcnow().isoformat(),
        "total_tables": len(health_reports),
    }


@router.get("/tables/{table_name}/health")
def get_table_health(table_name: str):
    """
    Get health status of ONE specific table.
    
    Example: GET /api/tables/fact_claim_line/health
    
    USED BY: When you click on a table in the dashboard
    """
    spark = get_spark()
    
    monitor = DeltaHealthMonitor(spark, DATA_DELTA_PATH)
    health = monitor.check_table(table_name)
    
    if health.file_metrics.total_files == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found or has no data files"
        )
    
    # Also get recommendations for this table
    optimizer = PredictiveOptimizer()
    recommendations = optimizer.analyze(health)
    
    return {
        "health": health.to_dict(),
        "recommendations": [r.to_dict() for r in recommendations],
    }


@router.get("/recommendations")
def get_recommendations():
    """
    Get ALL optimization recommendations across all tables.
    
    Returns recommendations sorted by priority (highest first).
    This is THE KEY ENDPOINT — it answers:
    "What should I fix first?"
    
    USED BY: Dashboard's recommendations panel
    
    INTERVIEW: "This endpoint runs the full predictive 
               optimization pipeline: health monitoring, 
               threshold analysis, priority scoring, and 
               cost estimation — all in one API call."
    """
    spark = get_spark()
    
    # Monitor all tables
    monitor = DeltaHealthMonitor(spark, DATA_DELTA_PATH)
    health_reports = monitor.check_all_tables()
    
    # Get recommendations
    optimizer = PredictiveOptimizer()
    recommendations = optimizer.analyze_all(health_reports)
    
    return {
        "recommendations": [r.to_dict() for r in recommendations],
        "total_recommendations": len(recommendations),
        "tables_analyzed": len(health_reports),
        "analyzed_at": datetime.utcnow().isoformat(),
    }


@router.get("/summary")
def get_dashboard_summary():
    """
    Get summary metrics for the dashboard top bar.
    
    Returns the 4 big numbers shown at the top:
    - Total tables monitored
    - Tables needing attention
    - Total size
    - Recommendations count
    
    USED BY: Dashboard header metrics (those big numbers!)
    """
    spark = get_spark()
    
    monitor = DeltaHealthMonitor(spark, DATA_DELTA_PATH)
    health_reports = monitor.check_all_tables()
    
    optimizer = PredictiveOptimizer()
    recommendations = optimizer.analyze_all(health_reports)
    
    # Calculate summary
    total_tables = len(health_reports)
    tables_needing_attention = sum(
        1 for h in health_reports
        if h.needs_optimize or h.needs_vacuum or h.needs_analyze
    )
    healthy_tables = total_tables - tables_needing_attention
    total_size = sum(h.file_metrics.total_size_bytes for h in health_reports)
    total_files = sum(h.file_metrics.total_files for h in health_reports)
    
    return {
        "total_tables": total_tables,
        "healthy_tables": healthy_tables,
        "tables_needing_attention": tables_needing_attention,
        "total_recommendations": len(recommendations),
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "total_files": total_files,
        "checked_at": datetime.utcnow().isoformat(),
    }