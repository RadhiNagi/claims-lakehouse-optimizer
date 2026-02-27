"""
============================================================
Spark Session Manager (Singleton Pattern)
============================================================
WHAT: Creates and manages ONE SparkSession for the whole app
WHY:  Creating a SparkSession is expensive (takes seconds).
      We create it ONCE and reuse it everywhere.

STORY: Think of SparkSession as a KITCHEN.
       You don't build a new kitchen every time someone 
       orders food! You build ONE kitchen and all chefs 
       share it. That's the "singleton pattern" — 
       only ONE instance exists.

INTERVIEW: "I implemented a singleton pattern for the 
       SparkSession to avoid the overhead of creating 
       multiple sessions. The session is initialized 
       lazily on first use and reused across all API 
       requests."
============================================================
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from app.config import SPARK_MASTER

# Module-level variable (the single kitchen)
_spark_session = None


def get_spark() -> SparkSession:
    """
    Get or create the SparkSession.
    
    First call: Creates the session (takes a few seconds)
    Every call after: Returns the same session (instant!)
    
    This is called "lazy initialization" — we only build 
    the kitchen when the first order comes in, not at startup.
    """
    global _spark_session
    
    if _spark_session is None or _spark_session._sc._jsc is None:
        print("  Creating new SparkSession...")
        
        builder = (
            SparkSession.builder
            .appName("ClaimsLakehouseOptimizer")
            .master(SPARK_MASTER)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        )
        
        _spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
        print(f"  SparkSession ready: {_spark_session.version}")
    
    return _spark_session


def stop_spark():
    """Stop the SparkSession (cleanup on shutdown)"""
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
        print("  SparkSession stopped.")