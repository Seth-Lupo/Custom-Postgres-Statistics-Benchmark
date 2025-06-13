from sqlalchemy import text
from ..base import StatsSource, StatsSourceConfig
from sqlmodel import Session

class DirectPgStatsSource(StatsSource):
    """Statistics source that uses PostgreSQL's built-in statistics."""
    
    def __init__(self, config: StatsSourceConfig = None):
        super().__init__(config)
        self.logger.info(f"Initialized {self.name()} with configuration: {self.config.name}")
    
    def apply_statistics(self, session: Session) -> None:
        """Run ANALYZE to ensure built-in statistics are up-to-date."""
        self.logger.info(f"Applying {self.name()} - using PostgreSQL's built-in statistics")
        self.logger.debug("No custom statistics modifications needed - using direct PostgreSQL analysis")
        super().apply_statistics(session)
        self.logger.info(f"{self.name()} application completed")
        
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Built-in PostgreSQL Statistics" 