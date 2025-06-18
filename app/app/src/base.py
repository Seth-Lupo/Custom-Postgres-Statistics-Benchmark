from abc import ABC, abstractmethod
from sqlalchemy import text
from ..logging_config import stats_logger, stats_source_logger
from sqlmodel import Session
import yaml
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any


class StatsSourceSettings:
    """Container for statistics source runtime settings."""
    
    def __init__(self, settings_data: Dict[str, Any]):
        self.name = settings_data.get('name', 'default')
        self.description = settings_data.get('description', '')
        self.analyze_verbose = settings_data.get('analyze_verbose', True)
        self.analyze_timeout_seconds = settings_data.get('analyze_timeout_seconds', 300)
        self.clear_caches = settings_data.get('clear_caches', True)
        self.reset_counters = settings_data.get('reset_counters', True)
        self.work_mem = settings_data.get('work_mem', '16MB')
        self.maintenance_work_mem = settings_data.get('maintenance_work_mem', '16MB')
        self.stats_reset_strategy = settings_data.get('stats_reset_strategy', 'once')
        self.transaction_handling = settings_data.get('transaction_handling', 'rollback')


class StatsSourceConfig:
    """Container for statistics source configuration data."""
    
    def __init__(self, config_data: Dict[str, Any]):
        self.name = config_data.get('name', 'default')
        self.description = config_data.get('description', '')
        self.message = config_data.get('message', '')
        # Store all other data fields for extensibility
        self._data = {k: v for k, v in config_data.items() 
                     if k not in ['name', 'description', 'message']}
    
    def get_data(self, key: str, default=None):
        """Get configuration data with optional default."""
        if key == 'message':
            return self.message
        return self._data.get(key, default)


# Legacy class for backward compatibility during transition
class StatsSourceLegacyConfig:
    """Legacy container for statistics source configuration - for backward compatibility."""
    
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
    
    def __init__(self, settings: StatsSourceSettings = None, config: StatsSourceConfig = None, legacy_config: StatsSourceLegacyConfig = None):
        # Support both new and legacy initialization patterns
        if legacy_config:
            # Legacy mode - convert to new format
            self.settings = self._legacy_to_settings(legacy_config)
            self.config = self._legacy_to_config(legacy_config)
        else:
            self.settings = settings or self._load_default_settings()
            self.config = config or self._load_default_config()
        
        # Use the specialized stats source logger for frontend integration
        self.logger = stats_source_logger
    
    def _legacy_to_settings(self, legacy_config: StatsSourceLegacyConfig) -> StatsSourceSettings:
        """Convert legacy config to new settings format."""
        settings_data = {
            'name': legacy_config.name,
            'description': legacy_config.description,
            'analyze_verbose': legacy_config.get_setting('analyze_verbose', True),
            'analyze_timeout_seconds': legacy_config.get_setting('analyze_timeout_seconds', 300),
            'clear_caches': legacy_config.get_setting('clear_caches', True),
            'reset_counters': legacy_config.get_setting('reset_counters', True),
            'work_mem': legacy_config.get_setting('work_mem', '16MB'),
            'maintenance_work_mem': legacy_config.get_setting('maintenance_work_mem', '16MB'),
            'stats_reset_strategy': legacy_config.get_setting('stats_reset_strategy', 'once'),
            'transaction_handling': legacy_config.get_setting('transaction_handling', 'rollback'),
        }
        return StatsSourceSettings(settings_data)
    
    def _legacy_to_config(self, legacy_config: StatsSourceLegacyConfig) -> StatsSourceConfig:
        """Convert legacy config to new config format."""
        config_data = {
            'name': legacy_config.name,
            'description': legacy_config.description,
            'message': legacy_config.get_data('message', ''),
        }
        return StatsSourceConfig(config_data)
    
    def _load_default_settings(self) -> StatsSourceSettings:
        """Load the default settings."""
        settings_path = self._get_settings_path('default.yaml')
        return self._load_settings_from_file(settings_path)
    
    def _load_default_config(self) -> StatsSourceConfig:
        """Load the default configuration for this stats source."""
        config_path = self._get_config_path('default.yaml')
        return self._load_config_from_file(config_path)
    
    def _get_settings_dir(self) -> Path:
        """Get the settings directory."""
        src_dir = Path(__file__).parent
        return src_dir / 'settings'
    
    def _get_config_dir(self) -> Path:
        """Get the configuration directory for this stats source."""
        # Get the directory name from the class name
        # e.g., DefaultStatsSource -> default
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
    
    def _get_settings_path(self, settings_name: str) -> Path:
        """Get the full path to a settings file."""
        return self._get_settings_dir() / settings_name
    
    def _get_config_path(self, config_name: str) -> Path:
        """Get the full path to a configuration file."""
        return self._get_config_dir() / config_name
    
    def _load_settings_from_file(self, settings_path: Path) -> StatsSourceSettings:
        """Load settings from a YAML file."""
        try:
            with open(settings_path, 'r') as f:
                settings_data = yaml.safe_load(f)
            return StatsSourceSettings(settings_data)
        except Exception as e:
            self.logger.error(f"Failed to load settings from {settings_path}: {str(e)}")
            # Return a minimal default settings
            return StatsSourceSettings({'name': 'default'})
    
    def _load_config_from_file(self, config_path: Path) -> StatsSourceConfig:
        """Load configuration from a YAML file."""
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            return StatsSourceConfig(config_data)
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {str(e)}")
            # Return a minimal default config
            return StatsSourceConfig({'name': 'default'})
    
    def get_available_settings(self) -> List[Tuple[str, str]]:
        """Get list of available settings as (filename, display_name) tuples."""
        settings_dir = self._get_settings_dir()
        settings = []
        
        if settings_dir.exists():
            for settings_file in settings_dir.glob('*.yaml'):
                try:
                    settings_obj = self._load_settings_from_file(settings_file)
                    settings.append((settings_file.stem, settings_obj.description or settings_obj.name))
                except Exception as e:
                    self.logger.warning(f"Failed to load settings {settings_file}: {str(e)}")
        
        return settings
    
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
    
    def load_settings(self, settings_name: str) -> StatsSourceSettings:
        """Load specific settings by name."""
        settings_path = self._get_settings_path(f"{settings_name}.yaml")
        return self._load_settings_from_file(settings_path)
    
    def load_config(self, config_name: str) -> StatsSourceConfig:
        """Load a specific configuration by name."""
        config_path = self._get_config_path(f"{config_name}.yaml")
        return self._load_config_from_file(config_path)
    
    def get_settings_content(self, settings_name: str) -> str:
        """Get the raw content of a settings file."""
        settings_path = self._get_settings_path(f"{settings_name}.yaml")
        if not settings_path.exists():
            return ""
        try:
            with open(settings_path, 'r') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Failed to read settings file {settings_path}: {str(e)}")
            return ""
    
    def get_config_content(self, config_name: str) -> str:
        """Get the raw content of a configuration file."""
        config_path = self._get_config_path(f"{config_name}.yaml")
        if not config_path.exists():
            return ""
        try:
            with open(config_path, 'r') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Failed to read config file {config_path}: {str(e)}")
            return ""
    
    # Legacy methods for backward compatibility
    def _load_default_config_legacy(self) -> StatsSourceLegacyConfig:
        """Load the default configuration for this stats source (legacy method)."""
        config_path = self._get_config_path('default.yaml')
        return self._load_config_from_file_legacy(config_path)
    
    def _load_config_from_file_legacy(self, config_path: Path) -> StatsSourceLegacyConfig:
        """Load configuration from a YAML file (legacy method)."""
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            return StatsSourceLegacyConfig(config_data)
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {str(e)}")
            # Return a minimal default config
            return StatsSourceLegacyConfig({'name': 'default', 'settings': {}})
    
    def clear_caches(self, session: Session) -> None:
        """Clear PostgreSQL caches and buffers."""
        if not self.settings.clear_caches:
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
            
            # Configure memory settings from settings
            work_mem = self.settings.work_mem
            maintenance_work_mem = self.settings.maintenance_work_mem
            timeout = self.settings.analyze_timeout_seconds
            
            session.execute(text(f"SET LOCAL statement_timeout = {timeout * 1000}"))  # Convert to milliseconds
            session.execute(text(f"SET LOCAL work_mem = '{work_mem}'"))
            session.execute(text(f"SET LOCAL maintenance_work_mem = '{maintenance_work_mem}'"))
            self.logger.debug(f"Configured memory settings: work_mem={work_mem}, maintenance_work_mem={maintenance_work_mem}")
            
            # Reset various PostgreSQL statistics counters if enabled
            if self.settings.reset_counters:
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
            analyze_verbose = self.settings.analyze_verbose
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
        return f"{self.name()} (Settings: {self.settings.name}, Config: {self.config.name})" 