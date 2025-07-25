#!/usr/bin/env python3
"""
Test script to verify SQLite migration for app metadata.

This script tests:
1. SQLite database creation and connection
2. Creating experiment records in SQLite
3. PostgreSQL is still used for experiment execution
"""

from app.database_sqlite import init_sqlite_db, get_sqlite_db, SQLITE_DB_PATH
from app.database import init_db, create_database, drop_database
from app.models import Experiment, Trial
from datetime import datetime

def test_sqlite_metadata():
    """Test SQLite for app metadata storage."""
    print("Testing SQLite metadata storage...")
    
    # Initialize SQLite database
    init_sqlite_db()
    print(f"✓ SQLite database initialized at: {SQLITE_DB_PATH}")
    
    # Get SQLite session
    db_gen = get_sqlite_db()
    db = next(db_gen)
    
    try:
        # Create a test experiment
        test_experiment = Experiment(
            name="Test SQLite Migration",
            stats_source="default",
            config_name="default",
            config_yaml="test: true",
            settings_name="default",
            settings_yaml="test: true",
            query="SELECT 1",
            iterations=1,
            stats_reset_strategy="once",
            transaction_handling="rollback",
            exit_status="PENDING",
            created_at=datetime.utcnow()
        )
        
        db.add(test_experiment)
        db.commit()
        db.refresh(test_experiment)
        
        print(f"✓ Created experiment record with ID: {test_experiment.id}")
        
        # Query the experiment back
        retrieved = db.get(Experiment, test_experiment.id)
        assert retrieved is not None
        assert retrieved.name == "Test SQLite Migration"
        print("✓ Successfully retrieved experiment from SQLite")
        
        # Create a test trial
        test_trial = Trial(
            experiment_id=test_experiment.id,
            run_index=0,
            execution_time=1.234,
            cost_estimate=0.5
        )
        
        db.add(test_trial)
        db.commit()
        print("✓ Created trial record in SQLite")
        
        # Clean up
        db.delete(test_trial)
        db.delete(test_experiment)
        db.commit()
        print("✓ Cleaned up test records")
        
    finally:
        # Close session
        try:
            next(db_gen)
        except StopIteration:
            pass

def test_postgres_experiment_execution():
    """Test PostgreSQL is still used for experiment execution."""
    print("\nTesting PostgreSQL for experiment execution...")
    
    # Initialize PostgreSQL
    init_db()
    print("✓ PostgreSQL connection established")
    
    # Test creating an experiment database
    test_db_name = "test_experiment_db_12345"
    
    try:
        create_database(test_db_name)
        print(f"✓ Created experiment database: {test_db_name}")
        
        drop_database(test_db_name)
        print(f"✓ Dropped experiment database: {test_db_name}")
        
    except Exception as e:
        print(f"✗ PostgreSQL test failed: {e}")
        # Try to clean up
        try:
            drop_database(test_db_name)
        except:
            pass
        raise

def main():
    """Run all tests."""
    print("=" * 60)
    print("SQLite Migration Test Suite")
    print("=" * 60)
    
    try:
        test_sqlite_metadata()
        test_postgres_experiment_execution()
        
        print("\n" + "=" * 60)
        print("✓ All tests passed! Migration successful.")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ Test failed: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()