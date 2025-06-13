from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from .base import StatsSource


class DirectPgStatsSource(StatsSource):
    """Statistics source that uses PostgreSQL's built-in statistics."""
    
    async def apply_statistics(self, session: AsyncSession) -> None:
        """No-op: uses built-in PostgreSQL statistics."""
        # Ensure we have fresh statistics by running ANALYZE
        analyze_stmt = text("ANALYZE;")
        await session.execute(analyze_stmt)
        await session.commit()
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Built-in PostgreSQL Statistics" 