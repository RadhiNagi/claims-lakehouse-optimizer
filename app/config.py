"""
============================================================
Application Configuration
============================================================
"""

import os

# Database connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://optimizer:opt_pass@postgres:5432/optimizer_db"
)

# Spark configuration
# "local[*]" means: run Spark locally, use all available CPU cores
# In production, this would be "spark://cluster:7077" or a Databricks URL
SPARK_MODE = os.getenv("SPARK_MODE", "local")
SPARK_MASTER = "local[*]" if SPARK_MODE == "local" else os.getenv("SPARK_MASTER", "local[*]")

# App settings
APP_NAME = "Claims Lakehouse Predictive Optimizer"
APP_VERSION = "1.0.0"
DEBUG = os.getenv("DEBUG", "true").lower() == "true"

# Optimization thresholds
SMALL_FILE_THRESHOLD_PERCENT = 30
VACUUM_RETENTION_DAYS = 7
STATS_STALENESS_HOURS = 24

# Data paths (work both locally and in Docker!)
DATA_RAW_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
DATA_DELTA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "delta")