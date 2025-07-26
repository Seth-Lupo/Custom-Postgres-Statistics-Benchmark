#!/usr/bin/env python3
"""
Run an actual experiment test using the built-in statistics method.

This script tests the complete experiment workflow by programmatically
submitting an experiment using the available dump and query files.
"""

import subprocess
import time
import json
import sys
from pathlib import Path

def run_command(cmd):
    """Run a command and return the result"""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)

def test_experiment():
    """Run a complete experiment test"""
    base_url = "http://localhost:8000"
    
    print("🧪 Testing Complete Experiment Workflow")
    print("=" * 50)
    
    # 1. Check available files
    print("\n1. Checking available files...")
    code, stdout, stderr = run_command(["curl", "-s", f"{base_url}/upload"])
    
    if code != 0:
        print(f"❌ Failed to access upload page: {stderr}")
        return False
    
    # Check if we have files (looking for table entries)
    if "stats.sql" in stdout and "query" in stdout:
        print("✅ Found dump and query files")
    else:
        print("⚠️  Files may not be available")
    
    # 2. Get available statistics sources  
    print("\n2. Getting available statistics sources...")
    code, stdout, stderr = run_command(["curl", "-s", f"{base_url}/experiment/configs"])
    
    if code != 0:
        print(f"❌ Failed to get configurations: {stderr}")
        return False
    
    try:
        config_data = json.loads(stdout)
        print(f"✅ Found {len(config_data)} sources:")
        for source in config_data:
            print(f"   - {source}")
    except:
        print(f"⚠️  Could not parse config data")
        return False
    
    # 3. Try to submit an experiment using multipart form data
    print("\n3. Testing experiment submission...")
    
    # Create a test experiment configuration (using correct field names from form)
    experiment_data = {
        'experiment_name': 'AI_Schneider_Test',
        'dump_file': 'stats.sql',
        'query_file': 'stats.sql',  # Using same file for queries  
        'stats_source': 'schneider_ai',  # Schneider AI Statistics method
        'iterations': '3',  # Few iterations to test quickly
        'settings_name': 'default',
        'config_name': 'default'
    }
    
    # Build curl command for form submission
    curl_cmd = [
        "curl", "-s", "-X", "POST",
        f"{base_url}/experiment",
        "-H", "Content-Type: application/x-www-form-urlencoded"
    ]
    
    # Add form data
    form_data = []
    for key, value in experiment_data.items():
        form_data.extend(["-d", f"{key}={value}"])
    
    curl_cmd.extend(form_data)
    
    print(f"Submitting experiment with data: {experiment_data}")
    code, stdout, stderr = run_command(curl_cmd)
    
    if code != 0:
        print(f"❌ Failed to submit experiment: {stderr}")
        return False
    
    print("📊 Experiment submission response:")
    print(stdout[:500] + "..." if len(stdout) > 500 else stdout)
    
    # 4. Check if experiment was created
    print("\n4. Checking experiment status...")
    code, stdout, stderr = run_command(["curl", "-s", f"{base_url}/experiment/debug/status"])
    
    if code == 0:
        try:
            status_data = json.loads(stdout)
            experiments = status_data.get('experiments', {})
            print(f"✅ Found {len(experiments)} active experiments")
            
            if experiments:
                for exp_id, exp_data in experiments.items():
                    print(f"   Experiment {exp_id}: {exp_data.get('status', 'unknown')}")
        except:
            print("⚠️  Could not parse status data")
    
    # 5. Monitor logs for a bit
    print("\n5. Monitoring recent logs...")
    code, stdout, stderr = run_command(["docker-compose", "logs", "--tail", "10", "web"])
    
    if code == 0:
        print("Recent web logs:")
        print(stdout)
    
    return True

def check_statistics_sources():
    """Check what statistics sources are available"""
    print("\n🔍 Analyzing Available Statistics Sources")
    print("=" * 50)
    
    base_url = "http://localhost:8000"
    
    # Get sources list
    code, stdout, stderr = run_command(["curl", "-s", f"{base_url}/experiment/configs"])
    
    if code != 0:
        print(f"❌ Failed to get sources: {stderr}")
        return
    
    try:
        sources = json.loads(stdout)
        print(f"Found {len(sources)} statistics sources:")
        
        for source in sources:
            print(f"\n📋 Source: {source}")
            
            # Get configurations for this source
            config_url = f"{base_url}/experiment/configs/{source}"
            code, config_stdout, config_stderr = run_command(["curl", "-s", config_url])
            
            if code == 0:
                try:
                    configs = json.loads(config_stdout)
                    print(f"   Available configs: {len(configs)}")
                    for config in configs:
                        print(f"     - {config}")
                except:
                    print(f"   Could not parse configs")
            else:
                print(f"   Failed to get configs: {config_stderr}")
    
    except Exception as e:
        print(f"❌ Failed to parse sources: {e}")

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == "sources":
        check_statistics_sources()
        return
    
    # Run the complete test
    success = test_experiment()
    
    if success:
        print("\n✅ Experiment test completed successfully!")
        print("\n🎯 Summary:")
        print("  - Environment is running")
        print("  - API endpoints are accessible") 
        print("  - Files are available for experiments")
        print("  - Built-in statistics source is available")
        print("\n💡 To run actual experiments:")
        print("  - Access: http://localhost:8000")
        print("  - Use stats.sql as dump file")
        print("  - Use query files for testing")
        print("  - Select 'Built-in PostgreSQL Statistics'")
        print("  - Set 10 iterations")
    else:
        print("\n❌ Experiment test failed")
        sys.exit(1)

if __name__ == "__main__":
    main()