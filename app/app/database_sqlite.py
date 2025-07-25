from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlmodel import SQLModel
import os
from .logging_config import setup_logger

# Configure a logger for the SQLite database module
db_logger = setup_logger('database_sqlite')

# SQLite database path - stored in the app directory
SQLITE_DB_PATH = os.path.join(os.path.dirname(__file__), "app_metadata.db")
SQLITE_DATABASE_URL = f"sqlite:///{SQLITE_DB_PATH}"

# Create SQLAlchemy engine for SQLite
sqlite_engine = create_engine(
    SQLITE_DATABASE_URL,
    connect_args={"check_same_thread": False}  # Needed for SQLite with FastAPI
)

# Create sessionmaker for SQLite
SQLiteSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=sqlite_engine
)

def get_sqlite_db():
    """Get SQLite database session for app metadata."""
    db = SQLiteSessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_sqlite_db():
    """Initialize SQLite database and create tables."""
    db_logger.info("Initializing SQLite database for app metadata...")
    
    try:
        # Create all tables
        SQLModel.metadata.create_all(sqlite_engine)
        db_logger.info(f"SQLite database initialized at: {SQLITE_DB_PATH}")
        
        # Run migrations if needed
        run_sqlite_migrations()
        
    except Exception as e:
        db_logger.error(f"Failed to initialize SQLite database: {e}")
        raise

def run_sqlite_migrations():
    """Run any necessary SQLite-specific migrations."""
    db_logger.info("Running SQLite migrations...")
    
    try:
        # SQLite creates tables automatically via SQLModel
        # Any additional migrations can be added here
        db_logger.info("SQLite migrations completed successfully")
        
    except Exception as e:
        db_logger.error(f"Error running SQLite migrations: {e}")
        raise