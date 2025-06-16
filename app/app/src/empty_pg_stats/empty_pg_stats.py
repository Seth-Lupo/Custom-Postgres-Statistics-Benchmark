from ..base import StatsSource, StatsSourceConfig, StatsSourceSettings, StatsSourceLegacyConfig
from sqlmodel import Session

class EmptyPgStatsStatsSource(StatsSource):
    """
    A statistics source that only clears all PostgreSQL statistics and caches,
    without applying any new ones.
    """

    def __init__(self, settings: StatsSourceSettings = None, config: StatsSourceConfig = None, legacy_config: StatsSourceLegacyConfig = None):
        super().__init__(settings=settings, config=config, legacy_config=legacy_config)
        self.logger.info(f"Initialized {self.name()} with settings: {self.settings.name}, config: {self.config.name}")

    def apply_statistics(self, session: Session) -> None:
        """
        Clears all PostgreSQL statistics, counters, and caches.
        Does not perform an ANALYZE or apply any new statistics.
        """
        self.logger.info(f"Starting statistics clearing for {self.name()}")
        
        # Clear all caches and reset counters
        self.clear_caches(session)
        
        self.logger.info(f"Statistics clearing for {self.name()} completed successfully")

    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Empty PG Stats" 