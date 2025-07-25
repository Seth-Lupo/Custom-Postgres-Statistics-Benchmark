from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlmodel import SQLModel
import os
import time
import subprocess
from .logging_config import setup_logger

# Configure a logger for the database module
db_logger = setup_logger('database')

# Database URLs - PostgreSQL is now only used for experiment execution
ADMIN_DATABASE_URL = os.getenv("ADMIN_DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres/postgres")

# Create SQLAlchemy engine for admin operations (creating/dropping experiment databases)
admin_engine = create_engine(ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")

def wait_for_postgres():
    """Wait for PostgreSQL to be ready for experiment execution."""
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            with admin_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("PostgreSQL connection established for experiment execution!")
            return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"PostgreSQL not ready yet (attempt {i + 1}/{max_retries}). Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print(f"Failed to connect to PostgreSQL after {max_retries} attempts: {e}")
                raise

# Initialize PostgreSQL connection for experiment execution
def init_db():
    """Initialize PostgreSQL for experiment execution only."""
    wait_for_postgres()

def get_db_session(db_name: str):
    """Get a session for a specific database."""
    db_url = f"postgresql+psycopg2://postgres:postgres@postgres:5432/{db_name}"
    temp_engine = create_engine(db_url)
    TempSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=temp_engine
    )
    db = TempSessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_database(db_name: str):
    """Create a new database."""
    db_logger.info(f"Creating database: {db_name}")
    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE {db_name}"))
        db_logger.info(f"Database '{db_name}' created successfully.")
    except Exception as e:
        db_logger.error(f"Failed to create database '{db_name}': {e}")
        # If db exists, log it and continue
        if "already exists" in str(e):
            db_logger.warning(f"Database '{db_name}' already exists.")
            return
        raise

def drop_database(db_name: str):
    """Drop a database."""
    db_logger.info(f"Dropping database: {db_name}")
    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE {db_name} WITH (FORCE)"))
        db_logger.info(f"Database '{db_name}' dropped successfully.")
    except Exception as e:
        db_logger.error(f"Failed to drop database '{db_name}': {e}")
        raise

def load_dump(db_name: str, dump_path: str):
    """Load a SQL dump into the specified database."""
    db_logger.info(f"Loading dump '{dump_path}' into database '{db_name}'")
    try:
        # Construct the psql command
        command = [
            "psql",
            "-h", "postgres",
            "-U", "postgres",
            "-d", db_name,
            "-f", dump_path
        ]

        # Set the password for psql
        env = os.environ.copy()
        env["PGPASSWORD"] = "postgres"

        # Execute the command
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=env,
            check=True  # Raise an exception if the command fails
        )
        
        db_logger.info(f"Successfully loaded dump '{dump_path}' into '{db_name}'.")
        db_logger.debug(f"psql stdout: {result.stdout}")

    except subprocess.CalledProcessError as e:
        error_message = (
            f"Failed to load dump '{dump_path}' into database '{db_name}'.\n"
            f"Stderr: {e.stderr}\n"
            f"Stdout: {e.stdout}"
        )
        db_logger.error(error_message)
        raise RuntimeError(error_message) from e
    except Exception as e:
        db_logger.error(f"An unexpected error occurred while loading dump: {e}")
        raise 