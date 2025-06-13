"""
Migration script to add configuration fields to the experiments table.
Run this script to update your existing database with the new config fields.
"""
import os
import sys
from sqlalchemy import create_engine, text

def get_database_url():
    """Get database URL from environment or use default."""
    return os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/llm_pg_stats')

def add_config_fields():
    """Add config_name and config_yaml fields to experiments table."""
    engine = create_engine(get_database_url())
    
    with engine.connect() as conn:
        # Check if fields already exist
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'experiment' 
            AND column_name IN ('config_name', 'config_yaml', 'exit_status', 'experiment_logs')
        """))
        existing_columns = [row[0] for row in result.fetchall()]
        
        if 'config_name' not in existing_columns:
            print("Adding config_name field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN config_name VARCHAR(100)"))
            conn.commit()
            print("✓ config_name field added")
        else:
            print("config_name field already exists")
        
        if 'config_yaml' not in existing_columns:
            print("Adding config_yaml field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN config_yaml TEXT"))
            conn.commit()
            print("✓ config_yaml field added")
        else:
            print("config_yaml field already exists")
        
        if 'exit_status' not in existing_columns:
            print("Adding exit_status field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN exit_status VARCHAR(50) DEFAULT 'PENDING'"))
            conn.commit()
            print("✓ exit_status field added")
        else:
            print("exit_status field already exists")
        
        if 'experiment_logs' not in existing_columns:
            print("Adding experiment_logs field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN experiment_logs TEXT"))
            conn.commit()
            print("✓ experiment_logs field added")
        else:
            print("experiment_logs field already exists")
    
    print("Migration completed successfully!")

if __name__ == "__main__":
    print("Running migration to add configuration fields...")
    try:
        add_config_fields()
    except Exception as e:
        print(f"Migration failed: {e}")
        sys.exit(1) 