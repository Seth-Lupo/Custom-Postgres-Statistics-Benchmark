#!/usr/bin/env python3
"""
Test script for the modular Schneider AI implementation.

This tests the refactored modular architecture:
1. AI Response Handler - processes AI responses to pg_stats DataFrame
2. PG Stats Processor - validates and cleans the DataFrame 
3. Stats Translator - translates to pg_statistic format
4. PostgreSQL Inserter - inserts into the database
"""

import pandas as pd
import sys
import os

# Add the app to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def test_modules():
    """Test each module independently with mock data."""
    print("=" * 60)
    print("Modular Schneider AI Test Suite")
    print("=" * 60)
    
    # Test 1: AI Response Handler
    print("\n1. Testing AI Response Handler...")
    try:
        from app.src.schneider_ai.ai_response_handler import AIResponseHandler
        import logging
        
        logger = logging.getLogger('test')
        logger.setLevel(logging.DEBUG)
        
        config = {
            'provider': 'llmproxy',
            'model': 'test-model',
            'temperature': 0.3,
            'system_prompt': 'Test prompt',
            'estimation_prompt': 'Test estimation prompt: {col_names}, {size}, {sample_data}',
            'max_retries': 1
        }
        
        handler = AIResponseHandler(config, logger)
        print("✓ AI Response Handler initialized successfully")
        
        # Test CSV parsing
        mock_csv = "attname;null_frac;n_distinct;correlation\ntable1.col1;0.1;10;0.5\ntable1.col2;0.2;20;-0.3"
        df = handler._parse_response_to_dataframe(mock_csv)
        
        if not df.empty and len(df) == 2:
            print(f"✓ CSV parsing works: {len(df)} rows parsed")
            print(f"  Columns: {df.columns.tolist()}")
        else:
            print("✗ CSV parsing failed")
        
    except Exception as e:
        print(f"✗ AI Response Handler test failed: {e}")
    
    # Test 2: PG Stats Processor
    print("\n2. Testing PG Stats Processor...")
    try:
        from app.src.schneider_ai.pg_stats_processor import PGStatsProcessor
        
        # Mock schema info
        schema_info = {
            'tables': {
                'table1': {
                    'columns': [
                        {'name': 'col1', 'data_type': 'integer'},
                        {'name': 'col2', 'data_type': 'varchar'}
                    ],
                    'row_count': 1000
                }
            }
        }
        
        processor = PGStatsProcessor(schema_info, logger)
        print("✓ PG Stats Processor initialized successfully")
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'attname': ['table1.col1', 'table1.col2'],
            'null_frac': [0.1, 0.2],
            'n_distinct': [10, -0.5],
            'correlation': [0.5, None]
        })
        
        processed_df = processor.process_pg_stats(test_df)
        
        if not processed_df.empty:
            print(f"✓ Processing works: {len(processed_df)} rows processed")
            summary = processor.get_statistics_summary(processed_df)
            print(f"  Summary: {summary['total_rows']} rows for {len(summary['tables'])} tables")
        else:
            print("✗ Processing failed - empty result")
        
    except Exception as e:
        print(f"✗ PG Stats Processor test failed: {e}")
    
    # Test 3: Stats Translator (requires database connection - skip for now)
    print("\n3. Testing Stats Translator...")
    print("  → Skipped (requires database connection)")
    
    # Test 4: PostgreSQL Inserter (requires database connection - skip for now)
    print("\n4. Testing PostgreSQL Inserter...")
    print("  → Skipped (requires database connection)")
    
    # Test 5: Main class import
    print("\n5. Testing Main Class Import...")
    try:
        from app.src.schneider_ai.schneider_ai import SchneiderAIStatsSource
        from app.src.base import StatsSourceConfig, StatsSourceSettings
        
        # Create mock config
        config_data = {
            'provider': 'llmproxy',
            'model': 'test-model'
        }
        config = StatsSourceConfig(config_data)
        
        source = SchneiderAIStatsSource(config=config)
        print(f"✓ Main class imported and initialized: {source.name()}")
        
    except Exception as e:
        print(f"✗ Main class test failed: {e}")
    
    print("\n" + "=" * 60)
    print("Module structure verification complete!")
    print("=" * 60)
    
    # Show module structure
    print("\nModular Architecture Overview:")
    print("├── ai_response_handler.py")
    print("│   ├── AIResponseHandler class")
    print("│   ├── Input: schema_info dict")
    print("│   └── Output: pg_stats DataFrame")
    print("├── pg_stats_processor.py")
    print("│   ├── PGStatsProcessor class")  
    print("│   ├── Input: pg_stats DataFrame")
    print("│   └── Output: processed pg_stats DataFrame")
    print("├── stats_translator.py")
    print("│   ├── StatsTranslator class")
    print("│   ├── Input: pg_stats DataFrame") 
    print("│   └── Output: pg_statistic DataFrame")
    print("├── postgres_inserter.py")
    print("│   ├── PostgresInserter class")
    print("│   ├── Input: pg_statistic DataFrame")
    print("│   └── Output: insertion results")
    print("└── schneider_ai.py")
    print("    ├── SchneiderAIStatsSource class")
    print("    └── Orchestrates the entire pipeline")

if __name__ == "__main__":
    test_modules()