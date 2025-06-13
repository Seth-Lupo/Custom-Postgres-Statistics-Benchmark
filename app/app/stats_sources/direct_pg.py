from sqlalchemy import text
from .base import StatsSource
from sqlmodel import Session

class DirectPgStatsSource(StatsSource):
    """Statistics source that uses PostgreSQL's built-in statistics."""
    
    def apply_statistics(self, session: Session) -> None:
        """Run ANALYZE to ensure built-in statistics are up-to-date."""
        super().apply_statistics(session)
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Built-in PostgreSQL Statistics" 