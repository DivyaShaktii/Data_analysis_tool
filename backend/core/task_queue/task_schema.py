# backend/core/task_queue/task_schema.py
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Enum for possible task statuses"""
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskType(str, Enum):
    """Enum for common analytical task types"""
    GENERAL_ANALYSIS = "general_analysis"
    DATA_VISUALIZATION = "data_visualization"
    DATA_SUMMARY = "data_summary"
    PREDICTIVE_MODEL = "predictive_model"
    CORRELATION_ANALYSIS = "correlation_analysis"
    COMPARATIVE_ANALYSIS = "comparative_analysis"
    TIME_SERIES_ANALYSIS = "time_series_analysis"
    FEATURE_ANALYSIS = "feature_analysis"
    OUTLIER_DETECTION = "outlier_detection"
    CLUSTERING = "clustering"
    CLASSIFICATION = "classification"
    REGRESSION = "regression"


class Task(BaseModel):
    """
    Schema for analytics tasks to be processed by the system.
    """
    id: str
    user_id: str
    session_id: str
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    
    status: TaskStatus = TaskStatus.QUEUED
    task_type: str
    description: str = ""
    
    # Task-specific parameters
    parameters: Dict[str, Any] = {}
    
    # Results stored when task is completed
    results: Optional[Dict[str, Any]] = None
    
    # Error information if task failed
    error: Optional[str] = None
    
    # Task metadata
    priority: int = 1  # Higher number = higher priority
    estimated_duration: Optional[int] = None  # in seconds
    dependencies: List[str] = []  # List of task IDs that must complete before this one
    
    # Agent assignment
    assigned_agent: Optional[str] = None
    
    def update_status(self, new_status: TaskStatus, error_message: Optional[str] = None) -> None:
        """Update the task status and related timestamps"""
        self.status = new_status
        self.updated_at = datetime.now().isoformat()
        
        if new_status == TaskStatus.IN_PROGRESS and not self.started_at:
            self.started_at = datetime.now().isoformat()
        
        elif new_status == TaskStatus.COMPLETED:
            self.completed_at = datetime.now().isoformat()
        
        elif new_status == TaskStatus.FAILED and error_message:
            self.error = error_message
    
    def add_results(self, results: Dict[str, Any]) -> None:
        """Add results to the task and mark as completed"""
        self.results = results
        self.update_status(TaskStatus.COMPLETED)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary for storage"""
        return self.dict()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """Create a Task instance from dictionary data"""
        return cls(**data)