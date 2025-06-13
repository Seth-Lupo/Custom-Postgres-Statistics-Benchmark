from sqlalchemy import text
from ..base import StatsSource, StatsSourceConfig
import random
from sqlmodel import Session

class RandomPgStatsSource(StatsSource):
    """Statistics source that applies random statistics values."""
    
    def __init__(self, config: StatsSourceConfig = None):
        super().__init__(config)
        self.logger.info(f"Initialized {self.name()} with configuration: {self.config.name}")
        
        # Log configuration details
        min_stats = self.config.get_setting('min_stats_value', 1)
        max_stats = self.config.get_setting('max_stats_value', 10000)
        skip_system_schemas = self.config.get_setting('skip_system_schemas', True)
        excluded_schemas = self.config.get_setting('excluded_schemas', ['information_schema', 'pg_catalog'])
        
        self.logger.info(f"Random statistics range: {min_stats} - {max_stats}")
        self.logger.info(f"Skip system schemas: {skip_system_schemas}")
        if excluded_schemas:
            self.logger.info(f"Excluded schemas: {', '.join(excluded_schemas)}")
    
    def apply_statistics(self, session: Session) -> None:
        """Apply random statistics to all columns in pg_stats."""
        self.logger.info(f"Starting {self.name()} application")
        
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
            self.logger.debug(f"Using schema filter: {where_clause}")
        
        # Get all tables and columns from pg_stats
        query = f"""
            SELECT DISTINCT schemaname, tablename, attname
            FROM pg_stats
            {where_clause}
        """
        
        self.logger.debug("Querying pg_stats for table and column information")
        result = session.execute(text(query))
        
        rows = result.fetchall()
        total_columns = len(rows)
        self.logger.info(f"Found {total_columns} columns to apply random statistics to")
        
        success_count = 0
        error_count = 0
        
        for i, row in enumerate(rows):
            schema, table, column = row
            # Generate random statistics value within configured range
            random_stats = random.randint(min_stats, max_stats)
            
            try:
                # Apply random statistics to each column
                session.execute(text(f"""
                    ALTER TABLE {schema}.{table} 
                    ALTER COLUMN {column} SET STATISTICS {random_stats}
                """))
                success_count += 1
                
                if (i + 1) % 10 == 0 or i == total_columns - 1:
                    self.logger.debug(f"Progress: {i + 1}/{total_columns} columns processed")
                    
            except Exception as e:
                # Log error but continue with other columns
                self.logger.warning(f"Error setting statistics for {schema}.{table}.{column}: {e}")
                error_count += 1
                continue
        
        self.logger.info(f"Random statistics application summary: {success_count} successful, {error_count} errors")
        
        # Run ANALYZE to update statistics
        self.logger.info("Running ANALYZE to apply random statistics")
        super().apply_statistics(session)
        self.logger.info(f"{self.name()} application completed successfully")
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Random PostgreSQL Statistics" 