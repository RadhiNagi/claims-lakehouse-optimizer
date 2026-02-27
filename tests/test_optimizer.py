"""
============================================================
Tests for the Predictive Optimizer
============================================================
WHAT: Automated tests that verify the optimizer logic
WHY:  Proves correctness, prevents bugs, impresses interviewers

HOW TO RUN: docker compose exec api python -m pytest tests/ -v

INTERVIEW: "I wrote unit tests covering the optimizer's 
       threshold logic, priority scoring, and edge cases.
       Tests run in the CI/CD pipeline on every commit."
============================================================
"""

import pytest
from app.core.health_monitor import FileMetrics, TableHealth
from app.core.optimizer_engine import PredictiveOptimizer, OperationType


# ============================================================
# Test FileMetrics
# ============================================================
class TestFileMetrics:
    """Tests for the FileMetrics data class"""
    
    def test_small_files_percentage_normal(self):
        """50 small files out of 100 = 50%"""
        metrics = FileMetrics(
            total_files=100,
            small_files_count=50,
            large_files_count=50,
            total_size_bytes=1000000,
        )
        assert metrics.small_files_percentage == 50.0
    
    def test_small_files_percentage_zero_files(self):
        """Empty table should return 0%"""
        metrics = FileMetrics(total_files=0, small_files_count=0)
        assert metrics.small_files_percentage == 0.0
    
    def test_small_files_percentage_all_small(self):
        """All files small = 100%"""
        metrics = FileMetrics(total_files=10, small_files_count=10)
        assert metrics.small_files_percentage == 100.0
    
    def test_small_files_percentage_none_small(self):
        """No small files = 0%"""
        metrics = FileMetrics(total_files=10, small_files_count=0)
        assert metrics.small_files_percentage == 0.0


# ============================================================
# Test PredictiveOptimizer
# ============================================================
class TestPredictiveOptimizer:
    """Tests for the optimization decision engine"""
    
    def setup_method(self):
        """Create optimizer with default thresholds before each test"""
        self.optimizer = PredictiveOptimizer(
            small_file_threshold_pct=30.0,
            vacuum_retention_days=7,
            stats_staleness_hours=24.0,
        )
    
    def _make_health(
        self,
        table_name="test_table",
        total_files=100,
        small_files=50,
        total_size=1000000,
        delta_version=1,
        total_operations=1,
        staleness_hours=0,
    ) -> TableHealth:
        """Helper to create a TableHealth object for testing"""
        return TableHealth(
            table_name=table_name,
            table_path=f"/test/{table_name}",
            file_metrics=FileMetrics(
                total_files=total_files,
                small_files_count=small_files,
                large_files_count=total_files - small_files,
                total_size_bytes=total_size,
                avg_file_size_bytes=total_size / max(total_files, 1),
            ),
            delta_version=delta_version,
            total_operations=total_operations,
            stats_staleness_hours=staleness_hours,
        )
    
    # ---- OPTIMIZE Tests ----
    
    def test_optimize_needed_above_threshold(self):
        """Tables with >30% small files should get OPTIMIZE recommendation"""
        health = self._make_health(small_files=50, total_files=100)  # 50%
        recs = self.optimizer.analyze(health)
        
        optimize_recs = [r for r in recs if r.operation == OperationType.OPTIMIZE]
        assert len(optimize_recs) == 1
        assert health.needs_optimize is True
    
    def test_optimize_not_needed_below_threshold(self):
        """Tables with <30% small files should NOT get OPTIMIZE"""
        health = self._make_health(small_files=10, total_files=100)  # 10%
        recs = self.optimizer.analyze(health)
        
        optimize_recs = [r for r in recs if r.operation == OperationType.OPTIMIZE]
        assert len(optimize_recs) == 0
        assert health.needs_optimize is False
    
    def test_optimize_exactly_at_threshold(self):
        """Tables at exactly 30% should NOT trigger (must exceed)"""
        health = self._make_health(small_files=30, total_files=100)  # 30%
        recs = self.optimizer.analyze(health)
        
        optimize_recs = [r for r in recs if r.operation == OperationType.OPTIMIZE]
        assert len(optimize_recs) == 0
    
    def test_optimize_priority_increases_with_severity(self):
        """Higher small file % = higher priority"""
        health_bad = self._make_health(small_files=80, total_files=100)  # 80%
        health_ok = self._make_health(small_files=35, total_files=100)  # 35%
        
        recs_bad = self.optimizer.analyze(health_bad)
        recs_ok = self.optimizer.analyze(health_ok)
        
        opt_bad = [r for r in recs_bad if r.operation == OperationType.OPTIMIZE][0]
        opt_ok = [r for r in recs_ok if r.operation == OperationType.OPTIMIZE][0]
        
        assert opt_bad.priority > opt_ok.priority
    
    # ---- VACUUM Tests ----
    
    def test_vacuum_needed_many_versions(self):
        """Tables with >5 versions should get VACUUM recommendation"""
        health = self._make_health(delta_version=20, small_files=0)
        recs = self.optimizer.analyze(health)
        
        vacuum_recs = [r for r in recs if r.operation == OperationType.VACUUM]
        assert len(vacuum_recs) == 1
    
    def test_vacuum_not_needed_few_versions(self):
        """Tables with <=5 versions should NOT get VACUUM"""
        health = self._make_health(delta_version=3, small_files=0)
        recs = self.optimizer.analyze(health)
        
        vacuum_recs = [r for r in recs if r.operation == OperationType.VACUUM]
        assert len(vacuum_recs) == 0
    
    # ---- ANALYZE Tests ----
    
    def test_analyze_needed_stale_stats(self):
        """Tables with stats >24h old should get ANALYZE recommendation"""
        health = self._make_health(staleness_hours=48, small_files=0)
        recs = self.optimizer.analyze(health)
        
        analyze_recs = [r for r in recs if r.operation == OperationType.ANALYZE]
        assert len(analyze_recs) == 1
    
    def test_analyze_not_needed_fresh_stats(self):
        """Tables with fresh stats should NOT get ANALYZE"""
        health = self._make_health(staleness_hours=12, small_files=0)
        recs = self.optimizer.analyze(health)
        
        analyze_recs = [r for r in recs if r.operation == OperationType.ANALYZE]
        assert len(analyze_recs) == 0
    
    # ---- Combined Tests ----
    
    def test_healthy_table_no_recommendations(self):
        """A perfectly healthy table gets zero recommendations"""
        health = self._make_health(
            small_files=5,        # Only 5% small files
            total_files=100,
            delta_version=2,       # Few versions
            staleness_hours=1,     # Fresh stats
        )
        recs = self.optimizer.analyze(health)
        assert len(recs) == 0
        assert health.needs_optimize is False
        assert health.needs_vacuum is False
        assert health.needs_analyze is False
    
    def test_unhealthy_table_multiple_recommendations(self):
        """A sick table can get multiple recommendations"""
        health = self._make_health(
            small_files=60,        # 60% small files → OPTIMIZE
            total_files=100,
            delta_version=25,      # Many versions → VACUUM
            staleness_hours=72,    # 3 days stale → ANALYZE
        )
        recs = self.optimizer.analyze(health)
        
        operations = {r.operation for r in recs}
        assert OperationType.OPTIMIZE in operations
        assert OperationType.VACUUM in operations
        assert OperationType.ANALYZE in operations
        assert len(recs) == 3
    
    def test_recommendations_sorted_by_priority(self):
        """Recommendations should be sorted highest priority first"""
        health = self._make_health(
            small_files=60,
            total_files=100,
            delta_version=25,
            staleness_hours=72,
        )
        recs = self.optimizer.analyze(health)
        
        priorities = [r.priority for r in recs]
        assert priorities == sorted(priorities, reverse=True)
    
    # ---- Custom Threshold Tests ----
    
    def test_custom_threshold(self):
        """Optimizer should respect custom thresholds"""
        strict_optimizer = PredictiveOptimizer(
            small_file_threshold_pct=10.0  # Very strict!
        )
        health = self._make_health(small_files=15, total_files=100)  # 15%
        
        recs = strict_optimizer.analyze(health)
        optimize_recs = [r for r in recs if r.operation == OperationType.OPTIMIZE]
        assert len(optimize_recs) == 1  # 15% > 10% threshold


# ============================================================
# Test API Endpoints
# ============================================================
class TestAPI:
    """Basic API tests"""
    
    def test_recommendation_has_required_fields(self):
        """Every recommendation must have all required fields"""
        optimizer = PredictiveOptimizer()
        health = TableHealth(
            table_name="test",
            table_path="/test",
            file_metrics=FileMetrics(total_files=100, small_files_count=50),
            delta_version=20,
            stats_staleness_hours=48,
        )
        recs = optimizer.analyze(health)
        
        for rec in recs:
            d = rec.to_dict()
            assert "table_name" in d
            assert "operation" in d
            assert "priority" in d
            assert "reason" in d
            assert "estimated_cost_dbu" in d
            assert "estimated_improvement" in d
            assert 0 <= d["priority"] <= 1
