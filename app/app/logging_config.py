import logging
import logging.handlers
import os
from datetime import datetime

# Ensure logs directory exists
os.makedirs("app/logs", exist_ok=True)

# Create formatters
CONSOLE_FORMAT = '%(levelname)s - %(message)s'
FILE_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

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

# Create loggers for different components
experiment_logger = setup_logger('experiment')
stats_logger = setup_logger('stats')
query_logger = setup_logger('query')
web_logger = setup_logger('web') 