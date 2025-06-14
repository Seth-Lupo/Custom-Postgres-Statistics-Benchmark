from ..base import StatsSource, StatsSourceConfig
from sqlmodel import Session

class EmptyPgStatsStatsSource(StatsSource):
    """
    A statistics source that only clears all PostgreSQL statistics and caches,
    without applying any new ones.
    """

    def __init__(self, config: StatsSourceConfig = None):
        super().__init__(config)
        self.logger.info(f"Initialized {self.name()} with configuration: {self.config.name}")

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