#!/usr/bin/env python3
"""Debug database queries"""

import sqlite3
import pandas as pd
from pathlib import Path

# Database path
DB_PATH = Path("../../app/app/app_metadata.db")

# Connect to database
conn = sqlite3.connect(DB_PATH)

# Check experiments
print("=== Experiments ===")
exp_query = """
SELECT id, name, stats_source, avg_time, stddev_time
FROM experiment 
WHERE avg_time IS NOT NULL
LIMIT 10
"""
df_exp = pd.read_sql_query(exp_query, conn)
print(df_exp)

# Check trials
print("\n=== Trials ===")
trial_query = """
SELECT t.*, e.name as exp_name
FROM trial t
JOIN experiment e ON t.experiment_id = e.id
LIMIT 10
"""
df_trial = pd.read_sql_query(trial_query, conn)
print(df_trial)

# Check query parsing
print("\n=== Query Name Parsing ===")
parse_query = """
SELECT 
    name,
    CASE 
        WHEN name LIKE '%_a1_%' OR name LIKE '%_a1' THEN 'a1'
        WHEN name LIKE '%_a2_%' OR name LIKE '%_a2' THEN 'a2'
        WHEN name LIKE '%_a3_%' OR name LIKE '%_a3' THEN 'a3'
        WHEN name LIKE '%_a4_%' OR name LIKE '%_a4' THEN 'a4'
        WHEN name LIKE '%_a5_%' OR name LIKE '%_a5' THEN 'a5'
        ELSE 'unknown'
    END as query_name
FROM experiment
WHERE avg_time IS NOT NULL
LIMIT 20
"""
df_parse = pd.read_sql_query(parse_query, conn)
print(df_parse)

conn.close()