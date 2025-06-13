from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
import os
import time
import subprocess
from .logging_config import setup_logger

# Configure a logger for the database module
db_logger = setup_logger('database')

# Async database URLs
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@postgres:5432/experiment")
ADMIN_DATABASE_URL = os.getenv("ADMIN_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@postgres/postgres")

# Sync database URLs (needed for psycopg2 operations)
SYNC_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/experiment")
SYNC_ADMIN_DATABASE_URL = os.getenv("ADMIN_DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres/postgres")

# Create SQLAlchemy engines
engine = create_async_engine(DATABASE_URL)
admin_engine = create_async_engine(ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")

# Create sync engines for operations that require it
sync_engine = create_engine(SYNC_DATABASE_URL)
sync_admin_engine = create_engine(SYNC_ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")

# Create async sessionmakers
AsyncSessionLocal = sessionmaker(
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    bind=engine
)

async def wait_for_db():
    """Wait for database to be ready."""
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            # Try to connect to the database using sync connection for initial check
            with sync_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database connection established!")
            return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"Database not ready yet (attempt {i + 1}/{max_retries}). Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print(f"Failed to connect to database after {max_retries} attempts: {e}")
                raise

async def get_db():
    """Get database session."""
    async with AsyncSessionLocal() as db:
        try:
            yield db
        finally:
            await db.close()

# Initialize database connection
async def init_db():
    await wait_for_db()

async def create_db_and_tables():
    """Create database tables."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

async def get_session():
    """Dependency to get database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

async def get_db_session(db_name: str):
    """Get a session for a specific database."""
    db_url = f"postgresql+asyncpg://postgres:postgres@postgres:5432/{db_name}"
    temp_engine = create_async_engine(db_url)
    AsyncTempSessionLocal = sessionmaker(
        class_=AsyncSession,
        autocommit=False,
        autoflush=False,
        bind=temp_engine
    )
    async with AsyncTempSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

async def create_database(db_name: str):
    """Create a new database."""
    db_logger.info(f"Creating database: {db_name}")
    try:
        # Use sync connection for database creation
        with sync_admin_engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE {db_name}"))
        db_logger.info(f"Database '{db_name}' created successfully.")
    except Exception as e:
        db_logger.error(f"Failed to create database '{db_name}': {e}")
        # If db exists, log it and continue
        if "already exists" in str(e):
            db_logger.warning(f"Database '{db_name}' already exists.")
            return
        raise

async def drop_database(db_name: str):
    """Drop a database."""
    db_logger.info(f"Dropping database: {db_name}")
    try:
        # Use sync connection for database dropping
        with sync_admin_engine.connect() as connection:
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