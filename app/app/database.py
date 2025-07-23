from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlmodel import SQLModel
import os
import time
import subprocess
from .logging_config import setup_logger

# Configure a logger for the database module
db_logger = setup_logger('database')

# Database URLs
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/experiment")
ADMIN_DATABASE_URL = os.getenv("ADMIN_DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres/postgres")

# Create SQLAlchemy engines
engine = create_engine(DATABASE_URL)
admin_engine = create_engine(ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")

# Create sessionmakers
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

def wait_for_db():
    """Wait for database to be ready."""
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            with engine.connect() as conn:
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

def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize database connection
def init_db():
    wait_for_db()

def create_db_and_tables():
    """Create database tables."""
    with engine.begin() as conn:
        SQLModel.metadata.create_all(conn)
        
    # Run any pending migrations
    run_migrations()

def run_migrations():
    """Run database migrations."""
    db_logger.info("Running database migrations...")
    
    try:
        with engine.begin() as conn:
            # Check if document table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'document'
                );
            """))
            
            document_table_exists = result.scalar()
            
            if not document_table_exists:
                db_logger.info("Creating document table...")
                
                # Create document table manually to ensure all columns are present
                conn.execute(text("""
                    CREATE TABLE document (
                        id SERIAL PRIMARY KEY,
                        experiment_id INTEGER NOT NULL REFERENCES experiment(id) ON DELETE CASCADE,
                        name VARCHAR(200) NOT NULL,
                        filename VARCHAR(200) NOT NULL,
                        content_type VARCHAR(100) NOT NULL,
                        document_type VARCHAR(50) NOT NULL,
                        content TEXT NOT NULL,
                        size_bytes INTEGER DEFAULT 0,
                        source VARCHAR(200),
                        extra_metadata TEXT,
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                    );
                """))
                
                # Create indexes for better performance
                conn.execute(text("CREATE INDEX idx_document_experiment_id ON document(experiment_id);"))
                conn.execute(text("CREATE INDEX idx_document_type ON document(document_type);"))
                conn.execute(text("CREATE INDEX idx_document_created_at ON document(created_at);"))
                
                db_logger.info("Document table created successfully with indexes")
            else:
                db_logger.info("Document table already exists, skipping creation")
                
        db_logger.info("Migrations completed successfully")
        
    except Exception as e:
        db_logger.error(f"Error running migrations: {e}")
        raise

def get_session():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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