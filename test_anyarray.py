#!/usr/bin/env python3
"""Test script to verify anyarray updates in Schneider AI method."""

import requests
import time
import json

# Base URL
BASE_URL = "http://localhost:8000"

def test_schneider_ai():
    """Run a test experiment with Schneider AI to check anyarray updates."""
    
    print("🧪 Testing Schneider AI anyarray updates...")
    
    # First check if we have uploaded files
    response = requests.get(f"{BASE_URL}/")
    if response.status_code != 200:
        print("❌ Failed to connect to server")
        return
    
    print("✅ Connected to server")
    
    # Get the experiment form
    response = requests.get(f"{BASE_URL}/experiment")
    if response.status_code != 200:
        print("❌ Failed to get experiment page")
        return
    
    print("✅ Experiment page accessible")
    
    # Check uploaded files
    response = requests.get(f"{BASE_URL}/api/upload/list")
    if response.status_code == 200:
        files = response.json()
        print(f"📁 Available files: {files}")
    
    # Create experiment data
    experiment_data = {
        "experiment_name": f"anyarray_test_{int(time.time())}",
        "dump_file": "stats.sql",
        "query_file": "stats.sql",
        "iterations": "1",
        "stats_source": "schneider_ai",
        "settings_name": "default",
        "config_name": "proxy"
    }
    
    print(f"📤 Submitting experiment: {experiment_data['experiment_name']}")
    
    # Submit experiment
    response = requests.post(
        f"{BASE_URL}/experiment",
        data=experiment_data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        }
    )
    
    if response.status_code == 200:
        print("✅ Experiment started successfully!")
        print("📊 Check the logs for anyarray update attempts")
        
        # Extract experiment ID from response if possible
        # Monitor logs for a bit
        print("\n🔍 Monitoring experiment progress...")
        time.sleep(5)
        
    else:
        print(f"❌ Failed to start experiment: {response.status_code}")
        print(response.text[:500])

if __name__ == "__main__":
    test_schneider_ai()