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
        self.logger.info("Applying statistics to PostgreSQL. THIS IS A TEST LOG.")
        self.logger.info(f"TEST DATA: {self.config.get_data('message')}")

        super().apply_statistics(session)
        
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Built-in PostgreSQL Statistics" 