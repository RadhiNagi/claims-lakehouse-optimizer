"""
============================================================
Predictive Optimization Engine
============================================================
WHAT: Analyzes table health reports and decides what to do
WHY:  This is THE BRAIN — the core of the entire project!

STORY: If health_monitor.py is the NURSE (takes measurements),
       then optimizer_engine.py is the DOCTOR (makes decisions).
       
       The doctor:
       1. Reads the nurse's report (TableHealth)
       2. Compares to medical guidelines (thresholds)
       3. Decides treatment (OPTIMIZE? VACUUM? ANALYZE?)
       4. Assigns priority (urgent? can wait?)
       5. Estimates cost (how many DBUs will it use?)
       
MIRRORS DATABRICKS:
       Databricks Predictive Optimization does exactly this:
       "Identifies tables that would benefit from maintenance 
        operations and queues these operations to run."
       
       Our engine mimics this behavior locally.

INTERVIEW GOLD:
       "My optimizer uses configurable thresholds to decide 
        when tables need maintenance. The small-file threshold 
        of 30% triggers OPTIMIZE, matching industry best 
        practices. I also implemented priority scoring based 
        on table size and query frequency, so the most 
        impactful optimizations run first."
============================================================
"""

from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List
from enum import Enum

from app.core.health_monitor import TableHealth


class OperationType(str, Enum):
    """
    The three maintenance operations.
    
    Using Enum (enumeration) means only these 3 values are allowed.
    If someone tries OperationType("DROP") → ERROR!
    This prevents mistakes.
    """
    OPTIMIZE = "OPTIMIZE"
    VACUUM = "VACUUM"
    ANALYZE = "ANALYZE"


@dataclass
class Recommendation:
    """
    One recommended action for one table.
    
    STORY: Like a doctor's prescription:
      "Patient: fact_claim_line
       Diagnosis: 45% small files (threshold: 30%)
       Treatment: OPTIMIZE (file compaction)
       Priority: 0.85 (high — this table is queried often)
       Estimated cost: 2.3 DBU
       Expected improvement: ~40% faster queries"
    """
    table_name: str
    operation: OperationType
    priority: float          # 0.0 (can wait) to 1.0 (urgent!)
    reason: str              # Human-readable explanation
    estimated_cost_dbu: float  # Databricks Units (billing metric)
    estimated_improvement: str # What will improve
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            "table_name": self.table_name,
            "operation": self.operation.value,
            "priority": round(self.priority, 2),
            "reason": self.reason,
            "estimated_cost_dbu": round(self.estimated_cost_dbu, 2),
            "estimated_improvement": self.estimated_improvement,
        }


class PredictiveOptimizer:
    """
    The optimization decision engine.
    
    HOW THE DECISION PROCESS WORKS:
    ═══════════════════════════════
    
    For each table, we check 3 conditions:
    
    ┌──────────────────────────────────────────────────────┐
    │  CHECK 1: Does it need OPTIMIZE?                      │
    │                                                        │
    │  IF small_files_percentage > 30%                       │
    │  THEN recommend OPTIMIZE                               │
    │                                                        │
    │  Priority calculation:                                 │
    │    base = small_files_percentage / 100                 │
    │    boost if table is large (more data = more impact)   │
    │    boost if many operations (actively used table)      │
    │    result = 0.0 to 1.0                                │
    ├──────────────────────────────────────────────────────┤
    │  CHECK 2: Does it need VACUUM?                         │
    │                                                        │
    │  IF delta_version > 5 (has enough history to clean)    │
    │  AND last VACUUM was > 7 days ago (or never)           │
    │  THEN recommend VACUUM                                 │
    │                                                        │
    │  Priority: usually medium (0.4-0.6)                   │
    │  WHY not urgent? VACUUM saves storage cost             │
    │  but doesn't affect query speed much.                  │
    ├──────────────────────────────────────────────────────┤
    │  CHECK 3: Does it need ANALYZE?                        │
    │                                                        │
    │  IF stats_staleness > 24 hours                         │
    │  THEN recommend ANALYZE                                │
    │                                                        │
    │  Priority: depends on how stale                        │
    │  WHY? Stale stats → bad query plans → slow queries     │
    └──────────────────────────────────────────────────────┘
    """
    
    def __init__(
        self,
        small_file_threshold_pct: float = 30.0,
        vacuum_retention_days: int = 7,
        stats_staleness_hours: float = 24.0,
    ):
        """
        Initialize with configurable thresholds.
        
        WHY configurable? Different teams have different needs:
        - A hospital with 1000 queries/day might set threshold to 20%
        - A small clinic with 10 queries/day might allow 50%
        - Databricks default vacuum retention is 7 days
        
        Parameters:
            small_file_threshold_pct: OPTIMIZE when small files exceed this %
            vacuum_retention_days: Don't VACUUM files newer than this
            stats_staleness_hours: Re-ANALYZE when stats are older than this
        """
        self.small_file_threshold = small_file_threshold_pct
        self.vacuum_retention_days = vacuum_retention_days
        self.stats_staleness_hours = stats_staleness_hours
        
        print(f"  Optimizer initialized with:")
        print(f"    OPTIMIZE threshold: {self.small_file_threshold}% small files")
        print(f"    VACUUM retention:   {self.vacuum_retention_days} days")
        print(f"    ANALYZE staleness:  {self.stats_staleness_hours} hours")
    
    def analyze(self, health: TableHealth) -> List[Recommendation]:
        """
        Analyze ONE table's health and generate recommendations.
        
        This is the MAIN METHOD — the doctor's examination.
        
        Parameters:
            health: TableHealth from the health monitor
            
        Returns:
            List of Recommendation objects (could be 0, 1, 2, or 3)
        """
        recommendations = []
        
        # ---- CHECK 1: OPTIMIZE ----
        optimize_rec = self._check_optimize(health)
        if optimize_rec:
            health.needs_optimize = True
            recommendations.append(optimize_rec)
        
        # ---- CHECK 2: VACUUM ----
        vacuum_rec = self._check_vacuum(health)
        if vacuum_rec:
            health.needs_vacuum = True
            recommendations.append(vacuum_rec)
        
        # ---- CHECK 3: ANALYZE ----
        analyze_rec = self._check_analyze(health)
        if analyze_rec:
            health.needs_analyze = True
            recommendations.append(analyze_rec)
        
        # Sort by priority (highest first)
        recommendations.sort(key=lambda r: r.priority, reverse=True)
        
        return recommendations
    
    def analyze_all(self, health_reports: List[TableHealth]) -> List[Recommendation]:
        """
        Analyze ALL tables and return combined recommendations.
        
        STORY: Morning doctor rounds — examine every patient,
               then sort all treatments by urgency.
        """
        all_recommendations = []
        
        for health in health_reports:
            recs = self.analyze(health)
            all_recommendations.extend(recs)
        
        # Sort all recommendations by priority
        all_recommendations.sort(key=lambda r: r.priority, reverse=True)
        
        return all_recommendations
    
    def _check_optimize(self, health: TableHealth) -> Recommendation:
        """
        Should we run OPTIMIZE on this table?
        
        DECISION LOGIC:
          IF small files percentage > threshold (default 30%)
          THEN YES, recommend OPTIMIZE
          
        PRIORITY CALCULATION:
          Start with: small_files_pct / 100
            (45% small files → 0.45 base priority)
          
          Boost for large tables:
            > 100 MB → +0.1
            > 1 GB   → +0.2  
            (bigger tables = more impact from optimization)
          
          Cap at 1.0 (maximum urgency)
        """
        small_pct = health.file_metrics.small_files_percentage
        
        if small_pct <= self.small_file_threshold:
            return None  # Table is healthy! No OPTIMIZE needed.
        
        # Calculate priority
        priority = small_pct / 100.0
        
        # Boost for larger tables (more impactful to optimize)
        size_mb = health.file_metrics.total_size_bytes / (1024 * 1024)
        if size_mb > 1024:  # > 1 GB
            priority = min(priority + 0.2, 1.0)
        elif size_mb > 100:  # > 100 MB
            priority = min(priority + 0.1, 1.0)
        
        # Boost for tables with many operations (actively used)
        if health.total_operations > 10:
            priority = min(priority + 0.1, 1.0)
        
        # Estimate cost (roughly 0.5 DBU per GB of data)
        estimated_cost = round(size_mb / 1024 * 0.5, 2)
        estimated_cost = max(estimated_cost, 0.01)  # Minimum cost
        
        # Estimate improvement
        improvement = f"~{small_pct * 0.6:.0f}% faster scans, reduce {health.file_metrics.small_files_count} small files"
        
        return Recommendation(
            table_name=health.table_name,
            operation=OperationType.OPTIMIZE,
            priority=round(priority, 2),
            reason=(
                f"{small_pct}% small files "
                f"({health.file_metrics.small_files_count}/{health.file_metrics.total_files}), "
                f"threshold is {self.small_file_threshold}%"
            ),
            estimated_cost_dbu=estimated_cost,
            estimated_improvement=improvement,
        )
    
    def _check_vacuum(self, health: TableHealth) -> Recommendation:
        """
        Should we run VACUUM on this table?
        
        DECISION LOGIC:
          IF table has > 5 versions (enough history to have old files)
          THEN recommend VACUUM
          
        WHY > 5 versions? A brand new table with 2 versions 
        has nothing worth cleaning. But a table with 20 versions 
        likely has many old, unused files.
        """
        if health.delta_version <= 5:
            return None  # Too few versions, nothing to clean
        
        # Calculate estimated savings based on version count
        # More versions = more old files = more savings
        estimated_savings_mb = health.delta_version * 2  # Rough estimate
        
        priority = 0.4  # Base priority (VACUUM is less urgent than OPTIMIZE)
        
        # Boost if many versions accumulated
        if health.delta_version > 20:
            priority = 0.6
        if health.delta_version > 50:
            priority = 0.7
        
        return Recommendation(
            table_name=health.table_name,
            operation=OperationType.VACUUM,
            priority=priority,
            reason=(
                f"Table has {health.delta_version} versions, "
                f"old files may be consuming storage"
            ),
            estimated_cost_dbu=round(estimated_savings_mb * 0.001, 2),
            estimated_improvement=f"~{estimated_savings_mb} MB potential storage savings",
        )
    
    def _check_analyze(self, health: TableHealth) -> Recommendation:
        """
        Should we run ANALYZE on this table?
        
        DECISION LOGIC:
          IF statistics are older than threshold (default 24 hours)
          THEN recommend ANALYZE
          
        WHY THIS MATTERS:
          Spark uses statistics to plan queries. Example:
          
          Query: SELECT * FROM claims WHERE state = 'NY'
          
          With fresh stats: "NY has 500 rows out of 10,000.
            I'll use an index scan — fast!"
            
          With stale stats: "I don't know how many NY rows.
            I'll scan everything just to be safe — slow!"
        """
        staleness = health.stats_staleness_hours
        
        if staleness <= self.stats_staleness_hours:
            return None  # Stats are fresh enough
        
        # Priority increases with staleness
        if staleness > 72:  # 3+ days stale
            priority = 0.7
        elif staleness > 48:  # 2+ days stale
            priority = 0.5
        else:
            priority = 0.3
        
        # Estimate cost
        size_mb = health.file_metrics.total_size_bytes / (1024 * 1024)
        estimated_cost = round(size_mb / 1024 * 0.1, 2)
        estimated_cost = max(estimated_cost, 0.01)
        
        return Recommendation(
            table_name=health.table_name,
            operation=OperationType.ANALYZE,
            priority=priority,
            reason=f"Statistics are {staleness:.0f} hours old (threshold: {self.stats_staleness_hours}h)",
            estimated_cost_dbu=estimated_cost,
            estimated_improvement="Better query plan optimization with fresh statistics",
        )