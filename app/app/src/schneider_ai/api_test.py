#!/usr/bin/env python3
"""
Simple API test to isolate the Schneider AI API issues
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from reference import generate, model_info

def test_api():
    print("Testing Schneider AI API...")
    print("=" * 50)
    
    # Test 1: Model info
    print("\n1. Testing model_info()...")
    model_info_response = model_info()
    print(f"Model info response: {model_info_response}")
    
    # Test 2: Very simple request with 4o-mini
    print("\n2. Testing simple generation with 4o-mini...")
    simple_response = generate(
        model="4o-mini",
        system="You are a helpful assistant.",
        query="Say hello",
        temperature=0.3
    )
    print(f"Simple response: {simple_response}")
    
    # Test 3: CSV request (similar to our use case but much shorter)
    print("\n3. Testing CSV generation with 4o-mini...")
    csv_response = generate(
        model="4o-mini",
        system="You output semicolon-separated CSV data.",
        query="Create a CSV with columns: name;age;city for 2 sample people",
        temperature=0.3
    )
    print(f"CSV response: {csv_response}")
    
    # Test 4: Try with Claude Haiku
    print("\n4. Testing with Claude Haiku...")
    claude_response = generate(
        model="us.anthropic.claude-3-haiku-20240307-v1:0",
        system="You are a helpful assistant.",
        query="Say hello",
        temperature=0.3
    )
    print(f"Claude response: {claude_response}")
    
    # Test 5: CSV with Claude Haiku
    print("\n5. Testing CSV generation with Claude Haiku...")
    claude_csv_response = generate(
        model="us.anthropic.claude-3-haiku-20240307-v1:0",
        system="You output semicolon-separated CSV data with no other text.",
        query="Create a CSV with columns: name;age;city for 2 sample people",
        temperature=0.3
    )
    print(f"Claude CSV response: {claude_csv_response}")
    
    # Test 6: PostgreSQL statistics simulation with Claude
    print("\n6. Testing PostgreSQL statistics simulation with Claude...")
    pg_stats_response = generate(
        model="us.anthropic.claude-3-haiku-20240307-v1:0",
        system="You are a database statistics estimator. Output only semicolon-separated CSV data.",
        query="Estimate PostgreSQL pg_stats for a table 'users' with columns: id (integer), name (varchar), email (varchar). Output format: table_name;column_name;null_frac;n_distinct;correlation",
        temperature=0.3
    )
    print(f"PG Stats response: {pg_stats_response}")

if __name__ == "__main__":
    test_api() 