"""
============================================================
Test the Optimizer: Health Check + Recommendations
============================================================
Run this to see the optimizer in action!
It checks all Delta tables and shows what needs fixing.
============================================================
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark():
    """Create SparkSession"""
    builder = (
        SparkSession.builder
        .appName("OptimizerTest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("=" * 60)
    print("PREDICTIVE OPTIMIZER — TEST RUN")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Setup paths
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    delta_path = os.path.join(base_path, "data", "delta")
    
    # Create Spark
    spark = create_spark()
    
    try:
        # Import our modules
        sys.path.insert(0, base_path)
        from app.core.health_monitor import DeltaHealthMonitor
        from app.core.optimizer_engine import PredictiveOptimizer
        
        # Step 1: Health Monitor checks all tables
        monitor = DeltaHealthMonitor(spark, delta_path)
        health_reports = monitor.check_all_tables()
        
        # Step 2: Optimizer analyzes health reports
        print("\n" + "=" * 60)
        print("OPTIMIZER ANALYSIS")
        print("=" * 60)
        
        optimizer = PredictiveOptimizer(
            small_file_threshold_pct=30.0,
            vacuum_retention_days=7,
            stats_staleness_hours=24.0,
        )
        
        recommendations = optimizer.analyze_all(health_reports)
        
        # Step 3: Display results
        print("\n" + "=" * 60)
        print(f"RECOMMENDATIONS ({len(recommendations)} actions needed)")
        print("=" * 60)
        
        if not recommendations:
            print("\n  ✅ All tables are healthy! No actions needed.")
        else:
            for i, rec in enumerate(recommendations, 1):
                print(f"\n  {'='*50}")
                print(f"  Recommendation #{i}")
                print(f"  {'='*50}")
                print(f"  Table:       {rec.table_name}")
                print(f"  Operation:   {rec.operation.value}")
                print(f"  Priority:    {rec.priority} {'🔴 HIGH' if rec.priority > 0.7 else '🟡 MEDIUM' if rec.priority > 0.4 else '🟢 LOW'}")
                print(f"  Reason:      {rec.reason}")
                print(f"  Est. Cost:   {rec.estimated_cost_dbu} DBU")
                print(f"  Improvement: {rec.estimated_improvement}")
        
        # Summary table
        print("\n" + "=" * 60)
        print("TABLE HEALTH SUMMARY")
        print("=" * 60)
        print(f"  {'Table':<25} {'Files':<8} {'Small%':<10} {'Size':<12} {'Status'}")
        print(f"  {'-'*25} {'-'*8} {'-'*10} {'-'*12} {'-'*20}")
        
        for h in health_reports:
            size_str = f"{h.file_metrics.total_size_bytes / (1024*1024):.1f} MB"
            status_parts = []
            if h.needs_optimize:
                status_parts.append("OPTIMIZE")
            if h.needs_vacuum:
                status_parts.append("VACUUM")
            if h.needs_analyze:
                status_parts.append("ANALYZE")
            status = ", ".join(status_parts) if status_parts else "✅ Healthy"
            
            pct = h.file_metrics.small_files_percentage
            emoji = "🔴" if pct > 40 else "🟡" if pct > 25 else "🟢"
            
            print(f"  {emoji} {h.table_name:<23} {h.file_metrics.total_files:<8} "
                  f"{pct:<10.1f} {size_str:<12} {status}")
        
        print("\n" + "=" * 60)
        print("TEST COMPLETE")
        print("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()