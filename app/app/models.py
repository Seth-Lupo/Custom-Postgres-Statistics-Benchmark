from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import datetime


class Experiment(SQLModel, table=True):
    """Experiment model storing aggregate results."""
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=200)
    stats_source: str = Field(max_length=100)
    config_name: Optional[str] = Field(default=None, max_length=100)
    config_yaml: Optional[str] = Field(default=None)
    query: str
    iterations: int
    stats_reset_strategy: str = Field(default="once", max_length=50)  # "once" or "per_trial"
    transaction_handling: str = Field(default="rollback", max_length=50)  # "rollback" or "persist"
    avg_time: Optional[float] = Field(default=None)
    stddev_time: Optional[float] = Field(default=None)
    exit_status: Optional[str] = Field(default="PENDING", max_length=50)  # SUCCESS, FAILED, PENDING
    experiment_logs: Optional[str] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_executed: bool = Field(default=False)
    
    # Relationship to trials
    trials: List["Trial"] = Relationship(back_populates="experiment")


class Trial(SQLModel, table=True):
    """Individual trial results."""
    id: Optional[int] = Field(default=None, primary_key=True)
    experiment_id: int = Field(foreign_key="experiment.id")
    run_index: int
    execution_time: float
    cost_estimate: Optional[float] = Field(default=None)
    pg_stats_snapshot: Optional[str] = Field(default=None)  # JSON string of pg_stats data
    pg_statistic_snapshot: Optional[str] = Field(default=None)  # JSON string of pg_statistic data
    
    # Relationship to experiment
    experiment: Optional[Experiment] = Relationship(back_populates="trials") 