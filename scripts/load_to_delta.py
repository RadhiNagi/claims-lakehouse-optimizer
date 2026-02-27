"""
============================================================
Load Healthcare Claims CSV → Delta Lake Tables
============================================================
WHAT: Reads CSV files and writes them as Delta Lake tables
WHY:  Delta tables support OPTIMIZE, VACUUM, ANALYZE — 
      the operations our predictive optimizer manages

STORY: This script is the "intake desk" at the hospital.
       Raw patient forms (CSV) come in → get processed, 
       validated, organized → stored in proper medical 
       records system (Delta Lake).

INTERVIEW TIP: "I used PySpark with Delta Lake to create 
       a medallion architecture: raw CSVs as bronze layer, 
       Delta tables as silver layer. This is the standard 
       pattern at Databricks and most modern data teams."

WHAT IS MEDALLION ARCHITECTURE?
       Bronze = raw data (CSV, as-is)        ← data/raw/
       Silver = cleaned, structured (Delta)   ← data/delta/
       Gold   = aggregated for analytics      ← (future step)
       
       This is how Netflix, Walmart, and hospitals organize data!
============================================================
"""

import os
import sys
import shutil
from datetime import datetime

# ============================================================
# PySpark + Delta Lake Setup
# ============================================================
# STORY: These imports are like hiring your kitchen staff
#   - SparkSession: the head chef (manages everything)
#   - configure_spark_with_delta_pip: installs Delta Lake tools
#   - DeltaTable: lets us do OPTIMIZE, VACUUM, etc later

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DateType
)
from delta import configure_spark_with_delta_pip, DeltaTable


def create_spark_session():
    """
    Create a SparkSession with Delta Lake support.
    
    WHAT: SparkSession is the entry point to all Spark functionality.
    WHY:  You need it to read files, process data, write tables.
    
    STORY: Think of SparkSession as the KITCHEN MANAGER.
           You tell the manager what you need:
           "Read this CSV" → manager assigns a cook
           "Write to Delta" → manager assigns another cook
           If you have 4 CPU cores, the manager can run 4 cooks 
           simultaneously = parallel processing = FAST!
           
    INTERVIEW: "local[*]" means use all CPU cores on this machine.
               In production, this would point to a Spark cluster
               or Databricks workspace.
    """
    print("Creating SparkSession with Delta Lake support...")
    
    builder = (
        SparkSession.builder
        .appName("HealthcarClaimsLoader")
        .master("local[*]")
        # Delta Lake configurations
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Performance settings for local mode
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        # Delta Lake defaults
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"  SparkSession created: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    return spark


# ============================================================
# SECTION 1: Load Beneficiaries (Patients)
# ============================================================
def load_beneficiaries(spark, raw_path, delta_path):
    """
    Load beneficiaries CSV into Delta Lake table.
    
    STORY: These are patient records. In a real hospital,
           this would be millions of records from the 
           Medicare enrollment system.
    """
    print("\n" + "=" * 60)
    print("LOADING: Beneficiaries (Patients)")
    print("=" * 60)
    
    csv_path = os.path.join(raw_path, "beneficiaries.csv")
    table_path = os.path.join(delta_path, "dim_beneficiary")
    
    # Read CSV with schema enforcement
    # WHY define schema? Without it, Spark guesses types (often wrong!)
    # With schema: "I KNOW this column is an integer" → no surprises
    print(f"  Reading: {csv_path}")
    df = (
        spark.read
        .option("header", "true")      # First row is column names
        .option("inferSchema", "false") # Don't guess, we'll define types
        .csv(csv_path)
    )
    
    # Cast columns to correct types
    # WHY: CSV treats everything as strings. We need proper types
    #      for accurate analytics (SUM, AVG, GROUP BY, etc.)
    df = (
        df
        .withColumn("BENE_BIRTH_DT", F.to_date("BENE_BIRTH_DT", "yyyy-MM-dd"))
        .withColumn("BENE_SEX_IDENT_CD", F.col("BENE_SEX_IDENT_CD").cast(IntegerType()))
        .withColumn("BENE_RACE_CD", F.col("BENE_RACE_CD").cast(IntegerType()))
        .withColumn("BENE_COUNTY_CD", F.col("BENE_COUNTY_CD").cast(IntegerType()))
        .withColumn("SP_ALZHDMTA", F.col("SP_ALZHDMTA").cast(IntegerType()))
        .withColumn("SP_CHF", F.col("SP_CHF").cast(IntegerType()))
        .withColumn("SP_DIABETES", F.col("SP_DIABETES").cast(IntegerType()))
    )
    
    # Show sample data (for verification)
    print(f"  Records: {df.count()}")
    print(f"  Schema:")
    df.printSchema()
    print(f"  Sample (first 3 rows):")
    df.show(3, truncate=False)
    
    # Write to Delta Lake
    # WHY "overwrite"? This is a full refresh of dimension table.
    # In production, you'd use "merge" for incremental updates.
    print(f"  Writing Delta table: {table_path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(table_path)
    )
    
    print(f"  ✅ Beneficiaries loaded to Delta!")
    return df.count()


# ============================================================
# SECTION 2: Load Providers (Doctors/Hospitals)
# ============================================================
def load_providers(spark, raw_path, delta_path):
    """
    Load providers CSV into Delta Lake table.
    
    STORY: NPI (National Provider Identifier) is a unique 
           10-digit number for every US healthcare provider.
           Every hospital, doctor, nurse practitioner has one.
    """
    print("\n" + "=" * 60)
    print("LOADING: Providers (Doctors/Hospitals)")
    print("=" * 60)
    
    csv_path = os.path.join(raw_path, "providers.csv")
    table_path = os.path.join(delta_path, "dim_provider")
    
    print(f"  Reading: {csv_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(csv_path)
    )
    
    df = (
        df
        .withColumn("IS_FACILITY", F.col("IS_FACILITY").cast(IntegerType()))
    )
    
    print(f"  Records: {df.count()}")
    df.show(3, truncate=False)
    
    print(f"  Writing Delta table: {table_path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(table_path)
    )
    
    print(f"  ✅ Providers loaded to Delta!")
    return df.count()


# ============================================================
# SECTION 3: Load Claims (The BIG Table!)
# ============================================================
def load_claims(spark, raw_path, delta_path):
    """
    Load claims CSV into Delta Lake table WITH PARTITIONING.
    
    THIS IS THE MOST IMPORTANT TABLE because:
    1. It's the largest (10,000 rows, millions in real life)
    2. It's partitioned by SERVICE_MONTH
    3. It's the main table we'll OPTIMIZE, VACUUM, ANALYZE
    
    WHAT IS PARTITIONING?
    
    STORY: Imagine a filing cabinet with 24 drawers,
           one for each month (Jan 2023 → Dec 2024).
           
           Without partitioning:
             "Find all January claims" → search ALL drawers ❌
             
           With partitioning by month:
             "Find all January claims" → open ONLY the Jan drawer ✅
             Spark skips 23 other drawers = MUCH faster!
             
    This is called "partition pruning" — a key optimization.
    
    INTERVIEW: "I partitioned claims by service month because 
               most analytics queries filter by date range.
               This enables partition pruning, where Spark 
               only reads relevant partitions instead of 
               scanning the entire table."
    """
    print("\n" + "=" * 60)
    print("LOADING: Claims (Main Fact Table)")
    print("=" * 60)
    
    csv_path = os.path.join(raw_path, "claims.csv")
    table_path = os.path.join(delta_path, "fact_claim_line")
    
    print(f"  Reading: {csv_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(csv_path)
    )
    
    # Cast to proper types
    df = (
        df
        .withColumn("CLM_FROM_DT", F.to_date("CLM_FROM_DT", "yyyy-MM-dd"))
        .withColumn("CLM_THRU_DT", F.to_date("CLM_THRU_DT", "yyyy-MM-dd"))
        .withColumn("CLM_PMT_DT", F.to_date("CLM_PMT_DT", "yyyy-MM-dd"))
        .withColumn("CLM_ALLOWED_AMT", F.col("CLM_ALLOWED_AMT").cast(FloatType()))
        .withColumn("CLM_PMT_AMT", F.col("CLM_PMT_AMT").cast(FloatType()))
        .withColumn("CLM_BENE_PD_AMT", F.col("CLM_BENE_PD_AMT").cast(FloatType()))
    )
    
    print(f"  Total records: {df.count()}")
    print(f"  Schema:")
    df.printSchema()
    print(f"  Sample data:")
    df.show(3, truncate=False)
    
    # Show partition distribution
    # WHY: We want to see how data is spread across months
    #      Uneven distribution = some partitions too big, some too small
    print("\n  📊 Records per month (partition distribution):")
    (
        df.groupBy("SERVICE_MONTH")
        .count()
        .orderBy("SERVICE_MONTH")
        .show(30, truncate=False)
    )
    
    # ========================================================
    # Write to Delta Lake WITH PARTITIONING
    # ========================================================
    # KEY DECISION: We partition by SERVICE_MONTH
    # 
    # This creates a folder structure like:
    #   fact_claim_line/
    #   ├── SERVICE_MONTH=2023-01/
    #   │   ├── part-00000.parquet   (data file)
    #   │   └── part-00001.parquet   (data file)
    #   ├── SERVICE_MONTH=2023-02/
    #   │   ├── part-00000.parquet
    #   │   └── part-00001.parquet
    #   └── ... (24 month folders)
    #
    # INTERVIEW: "Partitioning creates physical data separation.
    #            When a query says WHERE SERVICE_MONTH = '2024-01',
    #            Spark reads ONLY that folder — skipping 23 others."
    
    print(f"\n  Writing partitioned Delta table: {table_path}")
    print(f"  Partition column: SERVICE_MONTH")
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("SERVICE_MONTH")    # This is the magic line!
        .save(table_path)
    )
    
    print(f"  ✅ Claims loaded to Delta with monthly partitions!")
    return df.count()


# ============================================================
# SECTION 4: Create Small Files (for OPTIMIZE demo!)
# ============================================================
def create_small_files(spark, delta_path):
    """
    PURPOSE: Deliberately create many small files in the claims table.
    
    WHY ON EARTH would we do this?
    
    STORY: Remember the hospital file room story? 
           We need to CREATE the messy room first,
           so our optimizer can CLEAN IT UP later!
           
           Real-world cause of small files:
           - Streaming data arriving every few seconds
           - Many small batch jobs writing throughout the day
           - Frequent UPDATE/DELETE operations
           
           We simulate this by appending tiny batches.
           
    After this function:
    - fact_claim_line will have MANY small files
    - Our optimizer will detect "43% small files!"
    - Then we'll run OPTIMIZE to fix it
    - The "before vs after" is your portfolio's WOW moment
    """
    print("\n" + "=" * 60)
    print("CREATING SMALL FILES (for optimization demo)")
    print("=" * 60)
    
    table_path = os.path.join(delta_path, "fact_claim_line")
    
    # Read the existing table
    existing_df = spark.read.format("delta").load(table_path)
    
    # Append 20 tiny batches (10 rows each)
    # Each append creates new small files — this is the "problem"
    print("  Appending 20 small batches (simulating streaming/micro-batches)...")
    
    for i in range(20):
        # Take a random sample of 10 rows
        tiny_batch = existing_df.sample(fraction=0.001).limit(10)
        
        if tiny_batch.count() > 0:
            (
                tiny_batch.write
                .format("delta")
                .mode("append")           # Append = adds new files
                .partitionBy("SERVICE_MONTH")
                .save(table_path)
            )
            
            if (i + 1) % 5 == 0:
                print(f"    Batch {i+1}/20 appended")
    
    print(f"  ✅ Created small files! Table now has many tiny files.")
    print(f"  This simulates real-world 'small file problem'")
    print(f"  that Predictive Optimization detects and fixes.")


# ============================================================
# SECTION 5: Verify Delta Tables
# ============================================================
def verify_tables(spark, delta_path):
    """
    Verify all Delta tables were created correctly.
    
    WHAT IS DELTA TABLE HISTORY?
    Every operation (write, append, delete) is logged.
    You can see WHO did WHAT and WHEN — like a bank statement.
    This is "time travel" — a key Delta Lake feature.
    """
    print("\n" + "=" * 60)
    print("VERIFICATION: Checking all Delta tables")
    print("=" * 60)
    
    tables = {
        "dim_beneficiary": os.path.join(delta_path, "dim_beneficiary"),
        "dim_provider": os.path.join(delta_path, "dim_provider"),
        "fact_claim_line": os.path.join(delta_path, "fact_claim_line"),
    }
    
    for table_name, path in tables.items():
        if os.path.exists(path):
            df = spark.read.format("delta").load(path)
            dt = DeltaTable.forPath(spark, path)
            
            print(f"\n  📋 {table_name}")
            print(f"     Rows: {df.count()}")
            print(f"     Columns: {len(df.columns)}")
            print(f"     Path: {path}")
            
            # Show Delta history (version log)
            print(f"     History (last 5 operations):")
            dt.history(5).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
        else:
            print(f"\n  ❌ {table_name} — NOT FOUND at {path}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("HEALTHCARE CLAIMS — CSV TO DELTA LAKE LOADER")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Paths
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_path, "data", "raw")
    delta_path = os.path.join(base_path, "data", "delta")
    
    print(f"\n  Raw data path:   {raw_path}")
    print(f"  Delta table path: {delta_path}")
    
    # Clean delta directory for fresh start
    if os.path.exists(delta_path):
        print(f"\n  Cleaning existing Delta tables...")
        for item in os.listdir(delta_path):
            item_path = os.path.join(delta_path, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
    os.makedirs(delta_path, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load all tables
        bene_count = load_beneficiaries(spark, raw_path, delta_path)
        prov_count = load_providers(spark, raw_path, delta_path)
        claims_count = load_claims(spark, raw_path, delta_path)
        
        # Create small files problem (for optimization demo)
        create_small_files(spark, delta_path)
        
        # Verify everything
        verify_tables(spark, delta_path)
        
        # Final summary
        print("\n" + "=" * 60)
        print("✅ ALL TABLES LOADED SUCCESSFULLY!")
        print("=" * 60)
        print(f"  dim_beneficiary:  {bene_count} patients")
        print(f"  dim_provider:     {prov_count} providers")
        print(f"  fact_claim_line:  {claims_count} claims + small files")
        print(f"\n  Delta tables at: {delta_path}")
        print(f"\n  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
    finally:
        spark.stop()
        print("\n  Spark session stopped.")


if __name__ == "__main__":
    main()