"""
Migration script to add configuration tracking fields to the experiments table.
Run this script to update your existing database with the new config tracking fields.
"""
import os
import sys
from sqlalchemy import create_engine, text

def get_database_url():
    """Get database URL from environment or use default."""
    return os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/llm_pg_stats')

def add_config_tracking_fields():
    """Add config tracking fields to experiments table."""
    engine = create_engine(get_database_url())
    
    with engine.connect() as conn:
        # Check if config tracking fields already exist
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'experiment' 
            AND column_name IN ('original_config_yaml', 'config_modified', 'config_modified_at')
        """))
        existing_columns = [row[0] for row in result.fetchall()]
        
        print("=== Adding configuration tracking fields to experiment table ===")
        
        if 'original_config_yaml' not in existing_columns:
            print("Adding original_config_yaml field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN original_config_yaml TEXT"))
            conn.commit()
            print("✓ original_config_yaml field added")
        else:
            print("original_config_yaml field already exists")
        
        if 'config_modified' not in existing_columns:
            print("Adding config_modified field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN config_modified BOOLEAN DEFAULT FALSE"))
            conn.commit()
            print("✓ config_modified field added")
        else:
            print("config_modified field already exists")
        
        if 'config_modified_at' not in existing_columns:
            print("Adding config_modified_at field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN config_modified_at TIMESTAMP"))
            conn.commit()
            print("✓ config_modified_at field added")
        else:
            print("config_modified_at field already exists")
    
    print("\nConfiguration tracking migration completed successfully!")

if __name__ == "__main__":
    print("Starting configuration tracking database migration...")
    try:
        add_config_tracking_fields()
        print("✓ Migration completed successfully!")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1) 