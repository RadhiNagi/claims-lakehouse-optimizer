"""
============================================================
Pydantic Schemas (Data Validation)
============================================================
WHAT: Defines the SHAPE of data going in/out of the API
WHY:  Prevents bad data from entering the system!

STORY: Imagine a hospital reception desk. Before a patient 
       enters, the receptionist checks:
       - Do they have a name? ✓
       - Is their date of birth a real date? ✓
       - Is their phone number 10 digits? ✓
       
       Pydantic does the same for API data:
       - Is table_name a string? ✓
       - Is small_files_percentage a number between 0-100? ✓
       - Is operation_type one of OPTIMIZE/VACUUM/ANALYZE? ✓
       
       If ANY check fails, the API returns an error 
       BEFORE bad data reaches the database.
============================================================
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from enum import Enum


class OperationType(str, Enum):
    """
    The three operations Databricks Predictive Optimization runs.
    Using an Enum means the API only accepts these exact values.
    Anything else → automatic error!
    """
    OPTIMIZE = "OPTIMIZE"
    VACUUM = "VACUUM"
    ANALYZE = "ANALYZE"


class TableHealthResponse(BaseModel):
    """
    WHAT: The health report for ONE Delta table
    WHEN: Returned when someone asks "how healthy is this table?"
    
    STORY: Like a patient's health checkup report:
           - Blood pressure (total_files)
           - Cholesterol (small_files_percentage)  
           - Weight (total_size_bytes)
    """
    table_name: str
    total_files: int = Field(ge=0, description="Total number of data files")
    small_files_count: int = Field(ge=0, description="Files smaller than 128MB")
    small_files_percentage: float = Field(ge=0, le=100, description="% of files that are small")
    total_size_bytes: int = Field(ge=0, description="Total table size in bytes")
    needs_optimize: bool = Field(description="Does this table need OPTIMIZE?")
    needs_vacuum: bool = Field(description="Does this table need VACUUM?")
    needs_analyze: bool = Field(description="Does this table need ANALYZE?")
    last_checked: datetime


class OperationHistoryResponse(BaseModel):
    """
    WHAT: Record of one optimization operation
    WHEN: Shown in the dashboard operations history table
    
    STORY: Like a hospital procedure log:
           "At 10:00am, Dr. Smith performed surgery on Patient X,
            it took 45 minutes and cost $5000"
    """
    id: int
    table_name: str
    operation_type: OperationType
    started_at: datetime
    completed_at: Optional[datetime] = None
    files_affected: int = 0
    bytes_saved: int = 0
    duration_seconds: float = 0
    status: str = "completed"


class RecommendationResponse(BaseModel):
    """
    WHAT: The optimizer's recommendation for a table
    WHEN: The "brain" decides a table needs maintenance
    
    STORY: Like a doctor's recommendation:
           "Patient has high cholesterol (small files at 45%).
            I recommend treatment (OPTIMIZE). 
            Priority: HIGH. Estimated cost: $50."
    """
    table_name: str
    operation: OperationType
    priority: float = Field(ge=0, le=1, description="0=low, 1=urgent")
    reason: str
    estimated_cost_dbu: float
    estimated_improvement: str


class HealthSummaryResponse(BaseModel):
    """
    WHAT: Overall health summary across ALL tables
    WHEN: Dashboard home page top-level metrics
    """
    total_tables: int
    healthy_tables: int
    tables_needing_attention: int
    total_operations_today: int
    total_bytes_saved: int
    total_cost_dbu: float