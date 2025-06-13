from abc import ABC, abstractmethod
from sqlalchemy import text
from ..logging_config import stats_logger, stats_source_logger
from sqlmodel import Session
import yaml
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any


class StatsSourceConfig:
    """Container for statistics source configuration."""
    
    def __init__(self, config_data: Dict[str, Any]):
        self.name = config_data.get('name', 'default')
        self.description = config_data.get('description', '')
        self.settings = config_data.get('settings', {})
        self.data = config_data.get('data', {})
    
    def get_setting(self, key: str, default=None):
        """Get a configuration setting with optional default."""
        return self.settings.get(key, default)
    
    def get_data(self, key: str, default=None):
        """Get a configuration data with optional default."""
        return self.data.get(key, default)


class StatsSource(ABC):
    """Abstract base class for statistics sources."""
    
    def __init__(self, config: StatsSourceConfig = None):
        self.config = config or self._load_default_config()
        # Use the specialized stats source logger for frontend integration
        self.logger = stats_source_logger
    
    def _load_default_config(self) -> StatsSourceConfig:
        """Load the default configuration for this stats source."""
        config_path = self._get_config_path('default.yaml')
        return self._load_config_from_file(config_path)
    
    def _get_config_dir(self) -> Path:
        """Get the configuration directory for this stats source."""
        # Get the directory name from the class name
        # e.g., DirectPgStatsSource -> direct_pg
        class_name = self.__class__.__name__
        if class_name.endswith('StatsSource'):
            class_name = class_name[:-11]  # Remove 'StatsSource' suffix
        
        # Convert CamelCase to snake_case
        import re
        snake_case = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', class_name)
        snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_case).lower()
        
        # Return the config directory path
        src_dir = Path(__file__).parent
        return src_dir / snake_case / 'config'
    
    def _get_config_path(self, config_name: str) -> Path:
        """Get the full path to a configuration file."""
        return self._get_config_dir() / config_name
    
    def _load_config_from_file(self, config_path: Path) -> StatsSourceConfig:
        """Load configuration from a YAML file."""
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            return StatsSourceConfig(config_data)
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {str(e)}")
            # Return a minimal default config
            return StatsSourceConfig({'name': 'default', 'settings': {}})
    
    def get_available_configs(self) -> List[Tuple[str, str]]:
        """Get list of available configurations as (filename, display_name) tuples."""
        config_dir = self._get_config_dir()
        configs = []
        
        if config_dir.exists():
            for config_file in config_dir.glob('*.yaml'):
                try:
                    config = self._load_config_from_file(config_file)
                    configs.append((config_file.stem, config.description or config.name))
                except Exception as e:
                    self.logger.warning(f"Failed to load config {config_file}: {str(e)}")
        
        return configs
    
    def load_config(self, config_name: str) -> StatsSourceConfig:
        """Load a specific configuration by name."""
        config_path = self._get_config_path(f"{config_name}.yaml")
        return self._load_config_from_file(config_path)
    
    def clear_caches(self, session: Session) -> None:
        """Clear PostgreSQL caches and buffers."""
        if not self.config.get_setting('clear_caches', True):
            self.logger.info("Cache clearing disabled by configuration")
            return
            
        try:
            self.logger.info("Clearing PostgreSQL caches and buffers")
            
            # First commit any pending transaction
            session.commit()
            
            # These operations need to be outside a transaction
            conn = session.connection().connection
            conn.set_session(autocommit=True)
            try:
                # DISCARD ALL must be run outside a transaction
                session.execute(text("DISCARD ALL"))
                self.logger.debug("Executed DISCARD ALL command")
            finally:
                # Reset autocommit to false for subsequent operations
                conn.set_session(autocommit=False)
            
            # Configure memory settings from config
            work_mem = self.config.get_setting('work_mem', '16MB')
            maintenance_work_mem = self.config.get_setting('maintenance_work_mem', '16MB')
            timeout = self.config.get_setting('analyze_timeout_seconds', 300)
            
            session.execute(text(f"SET LOCAL statement_timeout = {timeout * 1000}"))  # Convert to milliseconds
            session.execute(text(f"SET LOCAL work_mem = '{work_mem}'"))
            session.execute(text(f"SET LOCAL maintenance_work_mem = '{maintenance_work_mem}'"))
            self.logger.debug(f"Configured memory settings: work_mem={work_mem}, maintenance_work_mem={maintenance_work_mem}")
            
            # Reset various PostgreSQL statistics counters if enabled
            if self.config.get_setting('reset_counters', True):
                session.execute(text("SELECT pg_stat_reset()"))
                session.execute(text("SELECT pg_stat_reset_shared('bgwriter')"))
                session.execute(text("SELECT pg_stat_reset_single_table_counters(0)"))
                self.logger.debug("Reset PostgreSQL statistics counters")
            
            # Attempt to clear statement statistics if extension is available
            try:
                session.execute(text("SELECT pg_stat_statements_reset()"))
                self.logger.debug("Reset pg_stat_statements statistics")
            except Exception:
                self.logger.debug("pg_stat_statements extension not available, skipping reset")
            
            session.commit()
            self.logger.info("Successfully cleared caches and buffers")
        except Exception as e:
            self.logger.error(f"Failed to clear caches: {str(e)}")
            session.rollback()
            raise
    
    def apply_statistics(self, session: Session) -> None:
        """Apply statistics to the database."""
        try:
            self.logger.info(f"Starting statistics application for {self.name()}")
            
            # First clear all caches
            self.clear_caches(session)
            
            self.logger.info("Running ANALYZE to update statistics")
            
            # Use verbose mode if configured
            analyze_verbose = self.config.get_setting('analyze_verbose', True)
            analyze_stmt = text("ANALYZE VERBOSE" if analyze_verbose else "ANALYZE")
            
            session.execute(analyze_stmt)
            session.commit()
            self.logger.info("ANALYZE completed successfully")
            self.logger.info(f"Statistics application for {self.name()} completed successfully")
        except Exception as e:
            self.logger.error(f"Failed to run ANALYZE: {str(e)}")
            session.rollback()
            raise
    
    @abstractmethod
    def name(self) -> str:
        """Return the name of this statistics source."""
        raise NotImplementedError()
    
    def display_name(self) -> str:
        """Return the display name including configuration."""
        return f"{self.name()} ({self.config.name})" 