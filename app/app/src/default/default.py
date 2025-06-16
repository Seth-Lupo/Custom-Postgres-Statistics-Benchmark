from sqlalchemy import text
from ..base import StatsSource, StatsSourceConfig, StatsSourceSettings, StatsSourceLegacyConfig
from sqlmodel import Session

class DefaultStatsSource(StatsSource):
    """Statistics source that uses PostgreSQL's built-in statistics."""
    
    def __init__(self, settings: StatsSourceSettings = None, config: StatsSourceConfig = None, legacy_config: StatsSourceLegacyConfig = None):
        super().__init__(settings=settings, config=config, legacy_config=legacy_config)
        self.logger.info(f"Initialized {self.name()} with settings: {self.settings.name}, config: {self.config.name}")
    
    def apply_statistics(self, session: Session) -> None:
        """Run ANALYZE to ensure built-in statistics are up-to-date."""
        self.logger.info("Applying statistics to PostgreSQL. THIS IS A TEST LOG.")
        self.logger.info(f"TEST DATA: {self.config.get_data('message')}")

        super().apply_statistics(session)
        
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Built-in PostgreSQL Statistics" 