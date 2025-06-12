from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class ExperimentCreate(BaseModel):
    """Schema for creating a new experiment."""
    stats_source: str
    query: str
    iterations: int


class ExperimentResponse(BaseModel):
    """Schema for experiment response."""
    id: int
    stats_source: str
    query: str
    iterations: int
    avg_time: Optional[float]
    stddev_time: Optional[float]
    created_at: datetime
    
    class Config:
        from_attributes = True


class TrialResponse(BaseModel):
    """Schema for trial response."""
    id: int
    experiment_id: int
    run_index: int
    execution_time: float
    cost_estimate: Optional[float]
    
    class Config:
        from_attributes = True


class ExperimentWithTrials(BaseModel):
    """Schema for experiment with all its trials."""
    id: int
    stats_source: str
    query: str
    iterations: int
    avg_time: Optional[float]
    stddev_time: Optional[float]
    created_at: datetime
    trials: List[TrialResponse]
    
    class Config:
        from_attributes = True 