"""
Migration script to add configuration fields to the experiments table and statistics snapshot fields to the trials table.
Run this script to update your existing database with the new config fields.
"""
import os
import sys
from sqlalchemy import create_engine, text

def get_database_url():
    """Get database URL from environment or use default."""
    return os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/llm_pg_stats')

def add_config_fields():
    """Add config fields to experiments table and statistics snapshot fields to trials table."""
    engine = create_engine(get_database_url())
    
    with engine.connect() as conn:
        # Check if experiment fields already exist
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'experiment' 
            AND column_name IN ('config_name', 'config_yaml', 'exit_status', 'experiment_logs', 'stats_reset_strategy', 'transaction_handling')
        """))
        existing_experiment_columns = [row[0] for row in result.fetchall()]
        
        # Check if trial fields already exist
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'trial' 
            AND column_name IN ('pg_stats_snapshot', 'pg_statistic_snapshot')
        """))
        existing_trial_columns = [row[0] for row in result.fetchall()]
        
        # Add experiment table fields
        print("=== Updating experiment table ===")
        
        if 'config_name' not in existing_experiment_columns:
            print("Adding config_name field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN config_name VARCHAR(100)"))
            conn.commit()
            print("✓ config_name field added")
        else:
            print("config_name field already exists")
        
        if 'config_yaml' not in existing_experiment_columns:
            print("Adding config_yaml field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN config_yaml TEXT"))
            conn.commit()
            print("✓ config_yaml field added")
        else:
            print("config_yaml field already exists")
        
        if 'exit_status' not in existing_experiment_columns:
            print("Adding exit_status field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN exit_status VARCHAR(50) DEFAULT 'PENDING'"))
            conn.commit()
            print("✓ exit_status field added")
        else:
            print("exit_status field already exists")
        
        if 'experiment_logs' not in existing_experiment_columns:
            print("Adding experiment_logs field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN experiment_logs TEXT"))
            conn.commit()
            print("✓ experiment_logs field added")
        else:
            print("experiment_logs field already exists")
        
        if 'stats_reset_strategy' not in existing_experiment_columns:
            print("Adding stats_reset_strategy field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN stats_reset_strategy VARCHAR(50) DEFAULT 'once'"))
            conn.commit()
            print("✓ stats_reset_strategy field added")
        else:
            print("stats_reset_strategy field already exists")
        
        if 'transaction_handling' not in existing_experiment_columns:
            print("Adding transaction_handling field...")
            conn.execute(text("ALTER TABLE experiment ADD COLUMN transaction_handling VARCHAR(50) DEFAULT 'rollback'"))
            conn.commit()
            print("✓ transaction_handling field added")
        else:
            print("transaction_handling field already exists")
        
        # Add trial table fields
        print("\n=== Updating trial table ===")
        
        if 'pg_stats_snapshot' not in existing_trial_columns:
            print("Adding pg_stats_snapshot field...")
            conn.execute(text("ALTER TABLE trial ADD COLUMN pg_stats_snapshot TEXT"))
            conn.commit()
            print("✓ pg_stats_snapshot field added")
        else:
            print("pg_stats_snapshot field already exists")
        
        if 'pg_statistic_snapshot' not in existing_trial_columns:
            print("Adding pg_statistic_snapshot field...")
            conn.execute(text("ALTER TABLE trial ADD COLUMN pg_statistic_snapshot TEXT"))
            conn.commit()
            print("✓ pg_statistic_snapshot field added")
        else:
            print("pg_statistic_snapshot field already exists")
    
    print("\nMigration completed successfully!")

if __name__ == "__main__":
    print("Starting database migration...")
    try:
        add_config_fields()
        print("✓ Migration completed successfully!")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1) 