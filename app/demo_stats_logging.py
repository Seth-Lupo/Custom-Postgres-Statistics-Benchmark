#!/usr/bin/env python3
"""
Demonstration script for the enhanced stats source logging infrastructure.
This script shows how stats source logs are captured and can be streamed to the frontend.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.logging_config import stats_source_logger
from app.stats_sources.random_pg import RandomPgStatsSource
from app.stats_sources.direct_pg import DirectPgStatsSource
from app.stats_sources.base import StatsSourceConfig

def demo_frontend_callback(log_level: str, message: str):
    """Simulate frontend callback that receives streamed logs."""
    print(f"[FRONTEND STREAM] {log_level}: {message}")

def main():
    print("=== Stats Source Logging Infrastructure Demo ===\n")
    
    # Set up the stream callback to simulate frontend integration
    print("1. Setting up frontend stream callback...")
    stats_source_logger.stream_handler.set_stream_callback(demo_frontend_callback)
    stats_source_logger.stream_handler.clear_experiment_logs()
    
    print("2. Demonstrating DirectPgStatsSource logging...\n")
    
    # Create a DirectPgStatsSource instance (this will log initialization)
    direct_stats = DirectPgStatsSource()
    
    print("\n3. Demonstrating RandomPgStatsSource logging...\n")
    
    # Create custom configuration for demonstration
    custom_config_data = {
        'name': 'demo_config',
        'description': 'Demo configuration for logging showcase',
        'settings': {
            'min_stats_value': 50,
            'max_stats_value': 500,
            'analyze_verbose': True,
            'clear_caches': True,
            'skip_system_schemas': True,
            'excluded_schemas': ['information_schema', 'pg_catalog']
        }
    }
    
    custom_config = StatsSourceConfig(custom_config_data)
    random_stats = RandomPgStatsSource(custom_config)
    
    print("\n4. Simulating stats source operations (without actual database)...\n")
    
    # Simulate some log messages that would occur during stats application
    stats_source_logger.info("Beginning statistics application process")
    stats_source_logger.info("Preparing database environment for statistics updates")
    stats_source_logger.debug("Configuration validated successfully")
    stats_source_logger.info("Found 25 tables with 150 columns to process")
    stats_source_logger.debug("Processing table: users (5 columns)")
    stats_source_logger.debug("Processing table: orders (8 columns)")
    stats_source_logger.debug("Processing table: products (12 columns)")
    stats_source_logger.info("Statistics application progress: 50% complete")
    stats_source_logger.warning("Encountered minor issue with table 'legacy_data' - skipping")
    stats_source_logger.info("Statistics application progress: 100% complete")
    stats_source_logger.info("Running ANALYZE to finalize statistics")
    stats_source_logger.info("Statistics application completed successfully")
    
    print("\n5. Retrieving captured experiment logs...\n")
    
    # Get all captured logs
    captured_logs = stats_source_logger.stream_handler.get_experiment_logs()
    
    print("All captured logs (as would be saved to experiment record):")
    print("-" * 60)
    for log in captured_logs:
        print(log)
    
    print(f"\nTotal logs captured: {len(captured_logs)}")
    
    print("\n6. Cleaning up logging infrastructure...\n")
    
    # Clean up (as done in experiment runner)
    stats_source_logger.stream_handler.set_stream_callback(None)
    stats_source_logger.stream_handler.clear_experiment_logs()
    
    print("Demo completed! The logging infrastructure is now ready for integration.")
    print("\nKey features demonstrated:")
    print("• Stats source logs are automatically captured")
    print("• Logs are streamed to frontend in real-time via callback")
    print("• Logs are stored for inclusion in experiment records")
    print("• Different log levels (INFO, DEBUG, WARNING) are supported")
    print("• Configuration details are automatically logged")
    print("• Progress information is provided for long-running operations")

if __name__ == "__main__":
    main() 