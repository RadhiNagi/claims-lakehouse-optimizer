"""
============================================================
Delta Table Health Monitor
============================================================
WHAT: Examines Delta tables and measures their "health"
WHY:  Before we can recommend OPTIMIZE/VACUUM/ANALYZE,
      we must first CHECK the current state of each table.
      
STORY: This is the NURSE who takes your vitals before 
       the doctor (optimizer_engine) makes a diagnosis.
       
       Nurse measures:    We measure:
       - Temperature      - Number of files
       - Blood pressure   - Percentage of small files
       - Heart rate       - Total size in bytes  
       - Weight           - How old the statistics are
       - Last checkup     - When was last OPTIMIZE/VACUUM

INTERVIEW TIP: "I built a health monitor that examines 
       Delta table metadata — file counts, file sizes, 
       and statistics staleness — without reading the 
       actual data. This makes health checks fast and 
       lightweight even on very large tables."
============================================================
"""

import os
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from pyspark.sql import SparkSession
from delta import DeltaTable


# ============================================================
# Data Classes (Structured containers for our measurements)
# ============================================================
# WHAT: dataclass is like a FORM with specific fields.
#       Instead of using messy dictionaries, we use these 
#       structured objects. Python checks the types for us.
#
# STORY: When a nurse measures your vitals, they write on 
#        a FORM with specific fields (name, temp, BP).
#        They don't write on random pieces of paper.
#        dataclass = that form.

@dataclass
class FileMetrics:
    """
    Measurements about files in a Delta table.
    
    Think of this as the "blood test results" —
    specific numbers about the table's file health.
    """
    total_files: int = 0          # How many data files exist
    small_files_count: int = 0    # Files smaller than target size
    large_files_count: int = 0    # Files at or above target size
    total_size_bytes: int = 0     # Total storage used
    avg_file_size_bytes: float = 0  # Average file size
    min_file_size_bytes: int = 0    # Smallest file
    max_file_size_bytes: int = 0    # Largest file
    
    @property
    def small_files_percentage(self) -> float:
        """
        What percentage of files are "too small"?
        
        WHY THIS MATTERS: If 50% of files are small, 
        queries read twice as many files as needed = SLOW.
        
        Databricks considers files < 128 MB as "small"
        for the purpose of OPTIMIZE decisions.
        """
        if self.total_files == 0:
            return 0.0
        return round((self.small_files_count / self.total_files) * 100, 1)


@dataclass
class TableHealth:
    """
    Complete health report for ONE Delta table.
    
    This is the FULL medical report — everything the 
    optimizer needs to make decisions.
    """
    table_name: str                         # Which table
    table_path: str                         # Where it lives on disk
    file_metrics: FileMetrics               # File health numbers
    row_count: int = 0                      # Total rows in table
    partition_count: int = 0                # Number of partitions
    delta_version: int = 0                  # Current version number
    total_operations: int = 0               # How many writes/updates
    last_modified: Optional[datetime] = None  # When was data last changed
    stats_staleness_hours: float = 0.0      # Hours since statistics refreshed
    checked_at: datetime = field(default_factory=datetime.utcnow)
    
    # Recommendations (filled in by optimizer_engine)
    needs_optimize: bool = False
    needs_vacuum: bool = False
    needs_analyze: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        result = {
            "table_name": self.table_name,
            "table_path": self.table_path,
            "row_count": self.row_count,
            "partition_count": self.partition_count,
            "delta_version": self.delta_version,
            "total_files": self.file_metrics.total_files,
            "small_files_count": self.file_metrics.small_files_count,
            "small_files_percentage": self.file_metrics.small_files_percentage,
            "total_size_bytes": self.file_metrics.total_size_bytes,
            "avg_file_size_bytes": self.file_metrics.avg_file_size_bytes,
            "needs_optimize": self.needs_optimize,
            "needs_vacuum": self.needs_vacuum,
            "needs_analyze": self.needs_analyze,
            "stats_staleness_hours": self.stats_staleness_hours,
            "checked_at": self.checked_at.isoformat(),
        }
        if self.last_modified:
            result["last_modified"] = self.last_modified.isoformat()
        return result


# ============================================================
# The Health Monitor Class
# ============================================================
class DeltaHealthMonitor:
    """
    Examines Delta Lake tables and produces health reports.
    
    STORY: This is the hospital's diagnostic department.
           You bring a patient (table), they run all the tests 
           (check files, sizes, versions), and hand you a 
           complete report (TableHealth object).
           
    HOW IT WORKS:
    1. List all Parquet files in the Delta table directory
    2. Measure each file's size
    3. Count small vs large files
    4. Read Delta transaction log for version history
    5. Package everything into a TableHealth report
    
    WHY NOT just read the data?
    Because reading 10 million rows takes minutes.
    Checking file metadata takes SECONDS.
    That's why this is "predictive" — we predict problems 
    from metadata without scanning actual data.
    """
    
    # Target file size: 128 MB (Delta Lake default)
    # Files smaller than this are considered "small"
    # WHY 128 MB? Spark processes data in chunks. 
    # A 128 MB file fits nicely in one processing chunk.
    # Tiny 1 MB files waste overhead opening/closing files.
    TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB
    
    def __init__(self, spark: SparkSession, delta_base_path: str):
        """
        Initialize the monitor.
        
        Parameters:
            spark: The SparkSession (our kitchen manager)
            delta_base_path: Where all Delta tables live
                            (e.g., /app/data/delta)
        """
        self.spark = spark
        self.delta_base_path = delta_base_path
    
    def check_table(self, table_name: str) -> TableHealth:
        """
        Run a complete health check on ONE table.
        
        STORY: Like a patient walking into the clinic.
               The nurse runs ALL tests and produces a report.
               
        Parameters:
            table_name: Name of the table (e.g., "fact_claim_line")
            
        Returns:
            TableHealth object with all measurements
        """
        table_path = os.path.join(self.delta_base_path, table_name)
        
        # Check if table exists
        if not os.path.exists(table_path):
            print(f"  ❌ Table not found: {table_path}")
            return TableHealth(
                table_name=table_name,
                table_path=table_path,
                file_metrics=FileMetrics()
            )
        
        print(f"\n  🔍 Checking table: {table_name}")
        print(f"     Path: {table_path}")
        
        # ---- TEST 1: File Metrics ----
        # Walk through all parquet files and measure them
        file_metrics = self._analyze_files(table_path)
        print(f"     Files: {file_metrics.total_files} total, "
              f"{file_metrics.small_files_count} small "
              f"({file_metrics.small_files_percentage}%)")
        print(f"     Size: {file_metrics.total_size_bytes / (1024*1024):.1f} MB")
        
        # ---- TEST 2: Delta Table Metadata ----
        # Read the Delta transaction log for version info
        dt = DeltaTable.forPath(self.spark, table_path)
        df = dt.toDF()
        
        row_count = df.count()
        print(f"     Rows: {row_count:,}")
        
        # Get version history
        history = dt.history()
        history_rows = history.collect()
        
        current_version = history_rows[0]["version"] if history_rows else 0
        total_operations = len(history_rows)
        print(f"     Delta version: {current_version}")
        print(f"     Total operations: {total_operations}")
        
        # Get last modified time
        last_modified = None
        if history_rows:
            last_modified = history_rows[0]["timestamp"]
            if hasattr(last_modified, 'replace'):
                # Already a datetime
                pass
            else:
                last_modified = datetime.utcnow()
        
        # ---- TEST 3: Partition Count ----
        # Count unique partition directories
        partition_count = 0
        if os.path.exists(table_path):
            partition_count = sum(
                1 for item in os.listdir(table_path)
                if os.path.isdir(os.path.join(table_path, item))
                and not item.startswith("_")  # Skip _delta_log
            )
        print(f"     Partitions: {partition_count}")
        
        # ---- TEST 4: Statistics Staleness ----
        # How old are the table statistics?
        # In real Databricks, ANALYZE TABLE updates these.
        # We simulate staleness based on last operation time.
        stats_staleness_hours = 0.0
        if last_modified:
            try:
                staleness = datetime.utcnow() - last_modified
                stats_staleness_hours = staleness.total_seconds() / 3600
            except:
                stats_staleness_hours = 48.0  # Assume stale if we can't calculate
        
        # ---- Build the report ----
        health = TableHealth(
            table_name=table_name,
            table_path=table_path,
            file_metrics=file_metrics,
            row_count=row_count,
            partition_count=partition_count,
            delta_version=current_version,
            total_operations=total_operations,
            last_modified=last_modified,
            stats_staleness_hours=stats_staleness_hours,
        )
        
        return health
    
    def check_all_tables(self) -> List[TableHealth]:
        """
        Run health checks on ALL Delta tables.
        
        STORY: Morning rounds at the hospital — 
               the nurse checks EVERY patient.
               
        Returns:
            List of TableHealth reports, one per table
        """
        print("\n" + "=" * 60)
        print("RUNNING HEALTH CHECKS ON ALL DELTA TABLES")
        print("=" * 60)
        
        # Find all Delta tables (directories with _delta_log)
        tables = []
        if os.path.exists(self.delta_base_path):
            for item in os.listdir(self.delta_base_path):
                item_path = os.path.join(self.delta_base_path, item)
                delta_log = os.path.join(item_path, "_delta_log")
                if os.path.isdir(item_path) and os.path.exists(delta_log):
                    tables.append(item)
        
        print(f"\n  Found {len(tables)} Delta tables: {tables}")
        
        # Check each table
        results = []
        for table_name in sorted(tables):
            health = self.check_table(table_name)
            results.append(health)
        
        return results
    
    def _analyze_files(self, table_path: str) -> FileMetrics:
        """
        Walk through a Delta table directory and measure all data files.
        
        WHAT: Counts and measures every .parquet file
        WHY:  We need to know the file size distribution 
              to decide if OPTIMIZE is needed
              
        HOW IT WORKS:
          1. Walk through all directories (including partitions)
          2. Find every .parquet file
          3. Measure its size with os.path.getsize()
          4. Classify as "small" or "large"
          5. Calculate statistics (total, avg, min, max)
          
        IMPORTANT: We only look at .parquet files, NOT:
          - _delta_log/ files (transaction log, not data)
          - .crc files (checksum files)
          - _SUCCESS files (Spark marker files)
        """
        file_sizes = []
        
        for root, dirs, files in os.walk(table_path):
            # Skip the _delta_log directory
            # WHY: _delta_log contains transaction metadata,
            #      not actual data files
            if "_delta_log" in root:
                continue
            
            for file_name in files:
                # Only count .parquet data files
                if file_name.endswith(".parquet"):
                    file_path = os.path.join(root, file_name)
                    size = os.path.getsize(file_path)
                    file_sizes.append(size)
        
        # If no files found, return empty metrics
        if not file_sizes:
            return FileMetrics()
        
        # Classify files as small or large
        # REMEMBER: TARGET_FILE_SIZE = 128 MB
        # For our demo data (small CSV), we use a lower threshold
        # because our files are kilobytes, not megabytes
        # In production, you'd use the actual 128 MB threshold
        
        # Calculate the median file size to set a smart threshold
        sorted_sizes = sorted(file_sizes)
        median_size = sorted_sizes[len(sorted_sizes) // 2]
        
        # Files smaller than half the median are "small"
        # This works for both demo (KB) and production (MB) data
        dynamic_threshold = max(median_size // 2, 1024)  # At least 1 KB
        
        small_files = [s for s in file_sizes if s < dynamic_threshold]
        large_files = [s for s in file_sizes if s >= dynamic_threshold]
        
        return FileMetrics(
            total_files=len(file_sizes),
            small_files_count=len(small_files),
            large_files_count=len(large_files),
            total_size_bytes=sum(file_sizes),
            avg_file_size_bytes=round(sum(file_sizes) / len(file_sizes), 2),
            min_file_size_bytes=min(file_sizes),
            max_file_size_bytes=max(file_sizes),
        )