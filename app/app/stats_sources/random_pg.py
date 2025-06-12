from sqlalchemy.orm import Session
from sqlalchemy import text
from .base import StatsSource
import random


class RandomPgStatsSource(StatsSource):
    """Statistics source that applies random statistics values."""
    
    def apply_statistics(self, session: Session) -> None:
        """Apply random statistics to all columns in pg_stats."""
        # Get all tables and columns from pg_stats
        result = session.execute(text("""
            SELECT DISTINCT schemaname, tablename, attname
            FROM pg_stats
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        """))
        
        for row in result:
            schema, table, column = row
            # Generate random statistics value between 1 and 10000
            random_stats = random.randint(1, 10000)
            
            try:
                # Apply random statistics to each column
                session.execute(text(f"""
                    ALTER TABLE {schema}.{table} 
                    ALTER COLUMN {column} SET STATISTICS {random_stats}
                """))
            except Exception as e:
                # Log error but continue with other columns
                print(f"Error setting statistics for {schema}.{table}.{column}: {e}")
                continue
        
        # Run ANALYZE to update statistics
        session.execute(text("ANALYZE;"))
        session.commit()
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Random PostgreSQL Statistics" 