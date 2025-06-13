from sqlalchemy import text
from ..base import StatsSource, StatsSourceConfig
from ...logging_config import stats_logger
import random
from sqlmodel import Session

class RandomPgStatsSource(StatsSource):
    """Statistics source that applies random statistics values."""
    
    def __init__(self, config: StatsSourceConfig = None):
        super().__init__(config)
    
    def apply_statistics(self, session: Session) -> None:
        """Apply random statistics to all columns in pg_stats."""
        # Get configuration values
        min_stats = self.config.get_setting('min_stats_value', 1)
        max_stats = self.config.get_setting('max_stats_value', 10000)
        skip_system_schemas = self.config.get_setting('skip_system_schemas', True)
        excluded_schemas = self.config.get_setting('excluded_schemas', ['information_schema', 'pg_catalog'])
        
        # Build the WHERE clause for schema filtering
        where_clause = ""
        if skip_system_schemas and excluded_schemas:
            excluded_list = "', '".join(excluded_schemas)
            where_clause = f"WHERE schemaname NOT IN ('{excluded_list}')"
        
        # Get all tables and columns from pg_stats
        query = f"""
            SELECT DISTINCT schemaname, tablename, attname
            FROM pg_stats
            {where_clause}
        """
        
        result = session.execute(text(query))
        
        for row in result.fetchall():
            schema, table, column = row
            # Generate random statistics value within configured range
            random_stats = random.randint(min_stats, max_stats)
            
            try:
                # Apply random statistics to each column
                session.execute(text(f"""
                    ALTER TABLE {schema}.{table} 
                    ALTER COLUMN {column} SET STATISTICS {random_stats}
                """))
            except Exception as e:
                # Log error but continue with other columns
                stats_logger.warning(f"Error setting statistics for {schema}.{table}.{column}: {e}")
                continue
        
        # Run ANALYZE to update statistics
        super().apply_statistics(session)
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Random PostgreSQL Statistics" 