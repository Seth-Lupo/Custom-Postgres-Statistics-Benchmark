#!/usr/bin/env python3
"""
Run experiments for all 5 queries (a1.sql through a5.sql) with three statistics methods:
1. schneider_ai (Schneider AI Statistics)
2. empty (Empty pg_stats) 
3. default (Built-in PostgreSQL Statistics)

Each experiment runs 10 trials with default settings and configuration.
Schneider AI experiments are run first as requested.
"""

import subprocess
import time
import json
import sys
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000"
DUMP_FILE = "a.sql"
ITERATIONS = 10
QUERIES = ["a1.sql", "a2.sql", "a3.sql", "a4.sql", "a5.sql"]
METHODS = [
    ("schneider_ai", "Schneider AI Statistics"),
    ("empty", "Empty pg_stats"),
    ("default", "Built-in PostgreSQL Statistics")
]

def run_command(cmd):
    """Run a command and return the result"""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)

def submit_experiment(experiment_name, query_file, stats_source):
    """Submit an experiment via curl"""
    curl_cmd = [
        "curl", "-s", "-X", "POST",
        f"{BASE_URL}/experiment",
        "-H", "Content-Type: application/x-www-form-urlencoded",
        "-d", f"experiment_name={experiment_name}",
        "-d", f"dump_file={DUMP_FILE}",
        "-d", f"query_file={query_file}",
        "-d", f"stats_source={stats_source}",
        "-d", f"iterations={ITERATIONS}",
        "-d", "settings_name=default",
        "-d", "config_name=default"
    ]
    
    return run_command(curl_cmd)

def wait_for_experiment_completion(experiment_name, max_wait_seconds=600):
    """Wait for an experiment to complete by checking the status"""
    start_time = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        # Check experiment status
        code, stdout, stderr = run_command(["curl", "-s", f"{BASE_URL}/experiment/debug/status"])
        
        if code == 0:
            try:
                status_data = json.loads(stdout)
                experiments = status_data.get('experiments', {})
                
                # Check if any experiment is still running
                running = False
                for exp_id, exp_data in experiments.items():
                    if exp_data.get('status') == 'running':
                        running = True
                        progress = exp_data.get('progress', 0)
                        total = exp_data.get('total', ITERATIONS)
                        print(f"\r  Progress: {progress}/{total} iterations", end='', flush=True)
                        break
                
                if not running:
                    print("\n  ✅ Experiment completed!")
                    return True
                    
            except json.JSONDecodeError:
                pass
        
        time.sleep(2)
    
    print("\n  ⚠️  Experiment timed out!")
    return False

def main():
    """Main function to run all experiments"""
    print("=" * 70)
    print("Running Experiments for 5 Queries with 3 Statistics Methods")
    print("Each query will be run 3 times per method")
    print("=" * 70)
    print(f"Queries: {', '.join(QUERIES)}")
    print(f"Methods: {', '.join([m[1] for m in METHODS])}")
    print(f"Iterations per experiment: {ITERATIONS}")
    print(f"Runs per query/method combination: 3")
    print("=" * 70)
    
    total_experiments = len(QUERIES) * len(METHODS) * 3  # 3 runs each
    completed = 0
    failed = []
    
    # Run experiments grouped by run number (all run 1, then all run 2, then all run 3)
    for run_num in range(1, 4):
        print(f"\n🔄 Starting Run {run_num} of 3")
        print("=" * 50)
        
        for method_key, method_name in METHODS:
            print(f"\n📊 Run {run_num}: {method_name} ({method_key})")
            print("-" * 40)
            
            for query in QUERIES:
                completed += 1
                query_base = query.replace('.sql', '')
                # Simple naming: method_query_run#
                experiment_name = f"{method_key}_{query_base}_run{run_num}"
                
                print(f"\n[{completed}/{total_experiments}] Starting: {query} with {method_name}")
                print(f"  Experiment name: {experiment_name}")
                
                # Submit the experiment
                code, stdout, stderr = submit_experiment(experiment_name, query, method_key)
                
                if code != 0:
                    print(f"  ❌ Failed to submit: {stderr}")
                    failed.append((query, method_name, f"run{run_num}", "submission failed"))
                    continue
                
                # Check if submission was successful
                if "alert-danger" in stdout:
                    print(f"  ❌ Submission error detected in response")
                    failed.append((query, method_name, f"run{run_num}", "submission error"))
                    continue
                
                print(f"  ✅ Submitted successfully")
                
                # Wait for completion
                if wait_for_experiment_completion(experiment_name):
                    print(f"  ✅ Completed: {query} with {method_name}")
                else:
                    print(f"  ⚠️  Timeout: {query} with {method_name}")
                    failed.append((query, method_name, f"run{run_num}", "timeout"))
                
                # Small delay between experiments
                time.sleep(5)
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total experiments: {total_experiments}")
    print(f"Completed: {completed - len(failed)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed experiments:")
        for query, method, run, reason in failed:
            print(f"  - {query} with {method} ({run}): {reason}")
    
    print("\n✅ All experiments submitted!")
    print(f"View results at: {BASE_URL}/results")

if __name__ == "__main__":
    main()