import logging
import logging.handlers
import os
from datetime import datetime
from typing import Callable, Optional
import threading

# Ensure logs directory exists
os.makedirs("app/logs", exist_ok=True)

# Create formatters
CONSOLE_FORMAT = '%(levelname)s - %(message)s'
FILE_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
STATS_SOURCE_FORMAT = '[%(asctime)s] %(levelname)s: %(message)s'

class StatsSourceStreamHandler(logging.Handler):
    """Custom logging handler for stats sources that can stream to frontend."""
    
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self._stream_callback = None
        self._experiment_logs = []
        self._lock = threading.Lock()
        
    def set_stream_callback(self, callback: Optional[Callable[[str, str], None]]):
        """Set the callback function to stream logs to frontend.
        
        Args:
            callback: Function that takes (log_level, message) and streams to frontend
        """
        with self._lock:
            self._stream_callback = callback
    
    def emit(self, record):
        """Emit a log record to the stream callback and experiment logs."""
        try:
            msg = self.format(record)
            
            with self._lock:
                # Store in experiment logs
                self._experiment_logs.append(msg)
                
                # Stream to frontend if callback is set
                if self._stream_callback:
                    self._stream_callback(record.levelname, record.getMessage())
                    
        except Exception:
            self.handleError(record)
    
    def get_experiment_logs(self) -> list:
        """Get all experiment logs captured so far."""
        with self._lock:
            return self._experiment_logs.copy()
    
    def clear_experiment_logs(self):
        """Clear all captured experiment logs."""
        with self._lock:
            self._experiment_logs.clear()

def setup_logger(name):
    """Set up a logger with both file and console handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
    logger.addHandler(console_handler)
    
    # File handler - daily rotating
    today = datetime.now().strftime('%Y-%m-%d')
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=f"app/logs/experiment_{today}.log",
        when='midnight',
        interval=1,
        backupCount=30,  # Keep 30 days of logs
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    logger.addHandler(file_handler)
    
    return logger

def setup_stats_source_logger(name="stats_source"):
    """Set up a specialized logger for stats sources with streaming capability."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
    logger.addHandler(console_handler)
    
    # File handler - daily rotating
    today = datetime.now().strftime('%Y-%m-%d')
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=f"app/logs/stats_source_{today}.log",
        when='midnight',
        interval=1,
        backupCount=30,  # Keep 30 days of logs
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    logger.addHandler(file_handler)
    
    # Add the custom stream handler for frontend integration
    stream_handler = StatsSourceStreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(STATS_SOURCE_FORMAT))
    logger.addHandler(stream_handler)
    
    # Store reference to stream handler for easy access
    logger.stream_handler = stream_handler
    
    return logger

# Create loggers for different components
experiment_logger = setup_logger('experiment')
stats_logger = setup_logger('stats')
query_logger = setup_logger('query')
web_logger = setup_logger('web')

# Create specialized stats source logger
stats_source_logger = setup_stats_source_logger('stats_source') 