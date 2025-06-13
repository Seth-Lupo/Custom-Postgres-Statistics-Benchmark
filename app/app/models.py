from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import datetime


class Experiment(SQLModel, table=True):
    """Experiment model storing aggregate results."""
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=200)
    stats_source: str = Field(max_length=100)
    query: str
    iterations: int
    avg_time: Optional[float] = Field(default=None)
    stddev_time: Optional[float] = Field(default=None)
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
    
    # Relationship to experiment
    experiment: Optional[Experiment] = Relationship(back_populates="trials") 