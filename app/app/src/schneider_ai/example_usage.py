#!/usr/bin/env python3
"""
Example usage of SchneiderAIStatsSource

This script demonstrates how to use the Schneider AI statistics estimator
with different configurations.
"""

import sys
from pathlib import Path
from sqlmodel import Session, create_engine

# Add the parent directory to path to import the module
sys.path.append(str(Path(__file__).parent.parent))

from schneider_ai import SchneiderAIStatsSource
from base import StatsSourceSettings, StatsSourceConfig


def example_usage():
    """Example demonstrating how to use SchneiderAIStatsSource."""
    
    print("SchneiderAI Stats Source Example")
    print("=" * 40)
    
    try:
        # Example 1: Using default configuration
        print("\n1. Loading with default configuration:")
        stats_source = SchneiderAIStatsSource()
        print(f"   Source Name: {stats_source.name()}")
        print(f"   Display Name: {stats_source.display_name()}")
        print(f"   API Endpoint: {stats_source.api_endpoint}")
        print(f"   Model: {stats_source.model}")
        print(f"   Temperature: {stats_source.temperature}")
        
        # Example 2: Using fast configuration
        print("\n2. Loading with fast configuration:")
        stats_source_fast = SchneiderAIStatsSource()
        stats_source_fast.config = stats_source_fast.load_config('fast')
        # Reinitialize with new config
        stats_source_fast.__init__(config=stats_source_fast.config)
        print(f"   Source Name: {stats_source_fast.name()}")
        print(f"   Display Name: {stats_source_fast.display_name()}")
        print(f"   Iterations: {stats_source_fast.num_iterations}")
        print(f"   Temperature: {stats_source_fast.temperature}")
        
        # Example 3: Show available configurations
        print("\n3. Available configurations:")
        configs = stats_source.get_available_configs()
        for config_name, description in configs:
            print(f"   - {config_name}: {description}")
        
        # Example 4: Show configuration details
        print("\n4. Default configuration content:")
        config_content = stats_source.get_config_content('default')
        print("   " + config_content.replace('\n', '\n   '))
        
        print("\n5. Target columns mapping:")
        for stat_name, col_idx in stats_source.target_columns.items():
            print(f"   - {stat_name}: column {col_idx}")
        
        print("\n✅ SchneiderAI module loaded successfully!")
        print("\nTo use with a database:")
        print("1. Ensure the AI API is running on the configured endpoint")
        print("2. Create a database session")
        print("3. Call stats_source.apply_statistics(session)")
        
    except ImportError as e:
        print(f"❌ Failed to import reference module: {e}")
        print("Make sure the ai-method-reference/proxy/reference.py is available")
    except Exception as e:
        print(f"❌ Error: {e}")


def show_config_structure():
    """Show the expected configuration structure."""
    print("\nConfiguration Structure:")
    print("=" * 30)
    print("""
The schneider-ai module uses the following configuration structure:

📁 app/app/src/schneider_ai/
├── 📄 __init__.py                 # Module exports
├── 📄 schneider_ai.py            # Main implementation
├── 📄 example_usage.py           # This example file
└── 📁 config/
    ├── 📄 default.yaml           # Default configuration
    └── 📄 fast.yaml              # Fast/testing configuration

Key configuration sections:
- API Configuration: endpoint, key, model settings
- RAG Settings: retrieval-augmented generation options
- Estimation Settings: iterations, epsilon values
- PostgreSQL Statistics: target column mappings
- Prompts: system and estimation prompt templates
- Validation: accuracy thresholds and retry limits
""")


if __name__ == "__main__":
    example_usage()
    show_config_structure() 