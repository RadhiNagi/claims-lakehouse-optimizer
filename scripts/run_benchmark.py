"""
============================================================
BENCHMARK: Before vs After Optimization
============================================================
WHAT: Runs queries BEFORE optimization, runs all operations,
      then runs queries AFTER — measuring the difference.

THIS IS YOUR INTERVIEW DEMO SCRIPT!
  "Let me show you the before/after impact..."
  
Output shows:
  BEFORE: 179 files, query took 4.2 seconds
  [runs OPTIMIZE + VACUUM + ANALYZE]
  AFTER:  24 files, query took 0.8 seconds
  IMPROVEMENT: 81% faster, 155 files reduced
============================================================
"""

import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


def create_spark():
    builder = (
        SparkSession.builder
        .appName("OptimizationBenchmark")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_benchmark_queries(spark, table_path: str, label: str) -> dict:
    """
    Run a set of typical healthcare analytics queries 
    and measure their execution time.
    
    These are queries that hospital analysts run daily:
    1. Total claims by month (monthly reporting)
    2. Top diagnosis codes (clinical analytics)
    3. Average payment by place of service (financial)
    4. Claims for specific patient (patient lookup)
    5. High-value claims filter (fraud detection)
    """
    print(f"\n  📊 Running benchmark queries ({label})...")
    
    df = spark.read.format("delta").load(table_path)
    df.createOrReplaceTempView("claims")
    
    queries = {
        "Monthly claim totals": """
            SELECT SERVICE_MONTH, COUNT(*) as claim_count, 
                   ROUND(SUM(CLM_PMT_AMT), 2) as total_paid
            FROM claims 
            GROUP BY SERVICE_MONTH 
            ORDER BY SERVICE_MONTH
        """,
        "Top 5 diagnoses": """
            SELECT ICD10_DGNS_CD_1, DGNS_DESC, COUNT(*) as frequency,
                   ROUND(AVG(CLM_PMT_AMT), 2) as avg_payment
            FROM claims 
            GROUP BY ICD10_DGNS_CD_1, DGNS_DESC
            ORDER BY frequency DESC 
            LIMIT 5
        """,
        "Payment by service place": """
            SELECT PLACE_OF_SRVC_DESC, 
                   COUNT(*) as claims,
                   ROUND(AVG(CLM_ALLOWED_AMT), 2) as avg_allowed,
                   ROUND(AVG(CLM_PMT_AMT), 2) as avg_paid
            FROM claims 
            GROUP BY PLACE_OF_SRVC_DESC
            ORDER BY claims DESC
        """,
        "Single patient lookup": """
            SELECT CLM_ID, CLM_FROM_DT, ICD10_DGNS_CD_1, 
                   CLM_PMT_AMT, PLACE_OF_SRVC_DESC
            FROM claims 
            WHERE DESYNPUF_ID = 'P000042'
            ORDER BY CLM_FROM_DT
        """,
        "High-value claims": """
            SELECT CLM_ID, DESYNPUF_ID, CLM_PMT_AMT, 
                   ICD10_DGNS_CD_1, PLACE_OF_SRVC_DESC
            FROM claims 
            WHERE CLM_PMT_AMT > 5000
            ORDER BY CLM_PMT_AMT DESC
            LIMIT 10
        """,
    }
    
    results = {}
    total_time = 0
    
    for query_name, sql in queries.items():
        # Run each query 3 times and take average (more accurate)
        times = []
        for run in range(3):
            start = time.time()
            result = spark.sql(sql).collect()
            elapsed = time.time() - start
            times.append(elapsed)
        
        avg_time = sum(times) / len(times)
        total_time += avg_time
        results[query_name] = round(avg_time, 4)
        print(f"     {query_name}: {avg_time:.4f}s (avg of 3 runs)")
    
    results["TOTAL"] = round(total_time, 4)
    print(f"     {'─' * 40}")
    print(f"     TOTAL: {total_time:.4f}s")
    
    return results


def count_files(table_path: str) -> tuple:
    """Count parquet files and total size"""
    total_files = 0
    total_bytes = 0
    for root, dirs, files in os.walk(table_path):
        if "_delta_log" in root:
            continue
        for f in files:
            if f.endswith(".parquet"):
                fp = os.path.join(root, f)
                total_files += 1
                total_bytes += os.path.getsize(fp)
    return total_files, total_bytes


def main():
    print("=" * 70)
    print("  CLAIMS LAKEHOUSE OPTIMIZER — BENCHMARK REPORT")
    print("  Before vs After Optimization")
    print("=" * 70)
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    delta_path = os.path.join(base_path, "data", "delta")
    table_path = os.path.join(delta_path, "fact_claim_line")
    
    spark = create_spark()
    
    try:
        # Add project to path for imports
        sys.path.insert(0, base_path)
        from app.core.operations_executor import OperationsExecutor
        
        # ============================================
        # PHASE 1: BEFORE Optimization
        # ============================================
        print("\n" + "=" * 70)
        print("  PHASE 1: BEFORE OPTIMIZATION")
        print("=" * 70)
        
        files_before, bytes_before = count_files(table_path)
        print(f"\n  📁 Files: {files_before}")
        print(f"  💾 Size: {bytes_before / 1024:.1f} KB")
        
        before_results = run_benchmark_queries(spark, table_path, "BEFORE")
        
        # ============================================
        # PHASE 2: RUN OPTIMIZATION
        # ============================================
        print("\n" + "=" * 70)
        print("  PHASE 2: RUNNING OPTIMIZATIONS")
        print("=" * 70)
        
        executor = OperationsExecutor(spark, delta_path)
        
        # Run all three operations
        optimize_result = executor.run_optimize("fact_claim_line")
        vacuum_result = executor.run_vacuum("fact_claim_line", retention_hours=0)
        analyze_result = executor.run_analyze("fact_claim_line")
        
        # ============================================
        # PHASE 3: AFTER Optimization
        # ============================================
        print("\n" + "=" * 70)
        print("  PHASE 3: AFTER OPTIMIZATION")
        print("=" * 70)
        
        files_after, bytes_after = count_files(table_path)
        print(f"\n  📁 Files: {files_after}")
        print(f"  💾 Size: {bytes_after / 1024:.1f} KB")
        
        after_results = run_benchmark_queries(spark, table_path, "AFTER")
        
        # ============================================
        # PHASE 4: COMPARISON REPORT
        # ============================================
        print("\n" + "=" * 70)
        print("  📊 BENCHMARK COMPARISON REPORT")
        print("=" * 70)
        
        # File metrics
        files_reduced = files_before - files_after
        files_pct = (files_reduced / files_before * 100) if files_before > 0 else 0
        bytes_saved = bytes_before - bytes_after
        
        print(f"\n  FILE METRICS:")
        print(f"  {'Metric':<25} {'Before':<15} {'After':<15} {'Change'}")
        print(f"  {'─' * 70}")
        print(f"  {'Files':<25} {files_before:<15} {files_after:<15} "
              f"↓ {files_reduced} ({files_pct:.0f}% reduction)")
        print(f"  {'Size (KB)':<25} {bytes_before/1024:<15.1f} {bytes_after/1024:<15.1f} "
              f"{'↓' if bytes_saved > 0 else '↑'} {abs(bytes_saved)/1024:.1f} KB")
        
        # Query performance
        print(f"\n  QUERY PERFORMANCE:")
        print(f"  {'Query':<30} {'Before (s)':<15} {'After (s)':<15} {'Improvement'}")
        print(f"  {'─' * 75}")
        
        total_improvement = 0
        for query_name in before_results:
            if query_name == "TOTAL":
                continue
            before_time = before_results[query_name]
            after_time = after_results.get(query_name, before_time)
            
            if before_time > 0:
                improvement = ((before_time - after_time) / before_time) * 100
            else:
                improvement = 0
            total_improvement += improvement
            
            arrow = "✅ ↓" if improvement > 0 else "↔️"
            print(f"  {query_name:<30} {before_time:<15.4f} {after_time:<15.4f} "
                  f"{arrow} {abs(improvement):.1f}%")
        
        # Total
        before_total = before_results["TOTAL"]
        after_total = after_results["TOTAL"]
        total_pct = ((before_total - after_total) / before_total * 100) if before_total > 0 else 0
        
        print(f"  {'─' * 75}")
        print(f"  {'TOTAL':<30} {before_total:<15.4f} {after_total:<15.4f} "
              f"{'✅ ↓' if total_pct > 0 else '↔️'} {abs(total_pct):.1f}%")
        
        # Operations summary
        print(f"\n  OPERATIONS EXECUTED:")
        print(f"  {'Operation':<15} {'Duration':<15} {'Files Changed':<20} {'Status'}")
        print(f"  {'─' * 65}")
        
        for op in [optimize_result, vacuum_result, analyze_result]:
            print(f"  {op.operation:<15} {op.duration_seconds:<15.2f} "
                  f"{op.files_reduced:<20} {op.status}")
        
        # Final summary
        print(f"\n  {'=' * 70}")
        print(f"  📋 EXECUTIVE SUMMARY")
        print(f"  {'=' * 70}")
        print(f"  • Files reduced: {files_before} → {files_after} "
              f"({files_pct:.0f}% fewer files)")
        print(f"  • Query performance: {total_pct:.1f}% improvement")
        print(f"  • Storage impact: {abs(bytes_saved)/1024:.1f} KB")
        print(f"  • Operations: OPTIMIZE + VACUUM + ANALYZE in "
              f"{optimize_result.duration_seconds + vacuum_result.duration_seconds + analyze_result.duration_seconds:.1f}s")
        print(f"  {'=' * 70}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()