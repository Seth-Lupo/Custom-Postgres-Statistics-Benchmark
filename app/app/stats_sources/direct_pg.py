from sqlalchemy.orm import Session
from sqlalchemy import text
from .base import StatsSource


class DirectPgStatsSource(StatsSource):
    """Statistics source that uses PostgreSQL's built-in statistics."""
    
    def apply_statistics(self, session: Session) -> None:
        """No-op: uses built-in PostgreSQL statistics."""
        # Ensure we have fresh statistics by running ANALYZE
        analyze_stmt = text("ANALYZE;")
        session.execute(analyze_stmt)
        session.commit()
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Built-in PostgreSQL Statistics" 