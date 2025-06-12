from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..logging_config import stats_logger


class StatsSource(ABC):
    """Abstract base class for statistics sources."""
    
    def apply_statistics(self, session: Session) -> None:
        """Apply statistics to the database."""
        try:
            stats_logger.info("Running ANALYZE to update statistics")
            # Properly use text() for the ANALYZE statement
            analyze_stmt = text("ANALYZE")
            session.execute(analyze_stmt)
            session.commit()
            stats_logger.info("ANALYZE completed successfully")
        except Exception as e:
            stats_logger.error(f"Failed to run ANALYZE: {str(e)}")
            raise
    
    @abstractmethod
    def name(self) -> str:
        """Return the name of this statistics source."""
        raise NotImplementedError() 