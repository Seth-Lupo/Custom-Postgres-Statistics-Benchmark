from abc import ABC, abstractmethod
from sqlalchemy import text
from ..logging_config import stats_logger
from sqlmodel import Session


class StatsSource(ABC):
    """Abstract base class for statistics sources."""
    
    def clear_caches(self, session: Session) -> None:
        """Clear PostgreSQL caches and buffers."""
        try:
            stats_logger.info("Clearing PostgreSQL caches and buffers")
            
            # First commit any pending transaction
            session.commit()
            
            # These operations need to be outside a transaction
            conn = session.connection().connection
            conn.set_session(autocommit=True)
            try:
                # DISCARD ALL must be run outside a transaction
                session.execute(text("DISCARD ALL"))
            finally:
                # Reset autocommit to false for subsequent operations
                conn.set_session(autocommit=False)
            
            # These can run inside a transaction
            session.execute(text("SET LOCAL statement_timeout = 0"))  # Prevent timeouts during cache clearing
            session.execute(text("SET LOCAL work_mem = '16MB'"))  # Minimize work memory
            session.execute(text("SET LOCAL maintenance_work_mem = '16MB'"))  # Minimize maintenance memory
            
            # Reset various PostgreSQL statistics counters
            session.execute(text("SELECT pg_stat_reset()"))
            session.execute(text("SELECT pg_stat_reset_shared('bgwriter')"))
            session.execute(text("SELECT pg_stat_reset_single_table_counters(0)"))
            
            # Attempt to clear statement statistics if extension is available
            try:
                session.execute(text("SELECT pg_stat_statements_reset()"))
            except Exception:
                stats_logger.debug("pg_stat_statements extension not available, skipping reset")
            
            session.commit()
            stats_logger.info("Successfully cleared caches and buffers")
        except Exception as e:
            stats_logger.error(f"Failed to clear caches: {str(e)}")
            session.rollback()
            raise
    
    def apply_statistics(self, session: Session) -> None:
        """Apply statistics to the database."""
        try:
            # First clear all caches
            self.clear_caches(session)
            
            stats_logger.info("Running ANALYZE to update statistics")
            # Properly use text() for the ANALYZE statement
            analyze_stmt = text("ANALYZE VERBOSE")  # Added VERBOSE for better logging
            session.execute(analyze_stmt)
            session.commit()
            stats_logger.info("ANALYZE completed successfully")
        except Exception as e:
            stats_logger.error(f"Failed to run ANALYZE: {str(e)}")
            session.rollback()
            raise
    
    @abstractmethod
    def name(self) -> str:
        """Return the name of this statistics source."""
        raise NotImplementedError() 