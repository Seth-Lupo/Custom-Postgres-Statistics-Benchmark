#!/usr/bin/env python3
import sqlite3
import pandas as pd
from pathlib import Path

# Database path
DB_PATH = Path("../../app/app/app_metadata.db")

# Connect to database
conn = sqlite3.connect(DB_PATH)

# Test the query
query = """
SELECT 
    CASE 
        WHEN e.stats_source LIKE '%Built-in%' THEN 'PostgreSQL\\nBuilt-in'
        WHEN e.stats_source LIKE '%Empty%' THEN 'Empty\\nStatistics'
        WHEN e.stats_source LIKE '%Schneider%' THEN 'LLM-based\\n(Schneider AI)'
        ELSE 'Unknown'
    END as method,
    CASE 
        WHEN e.name LIKE '%_a1_%' OR e.name LIKE '%_a1' THEN 'a1'
        WHEN e.name LIKE '%_a2_%' OR e.name LIKE '%_a2' THEN 'a2'
        WHEN e.name LIKE '%_a3_%' OR e.name LIKE '%_a3' THEN 'a3'
        WHEN e.name LIKE '%_a4_%' OR e.name LIKE '%_a4' THEN 'a4'
        WHEN e.name LIKE '%_a5_%' OR e.name LIKE '%_a5' THEN 'a5'
        ELSE 'unknown'
    END as query_name,
    AVG(e.avg_time) as avg_execution_time,
    AVG(e.stddev_time) as avg_stddev
FROM experiment e
WHERE e.avg_time IS NOT NULL
GROUP BY method, query_name
ORDER BY query_name, method
"""

df = pd.read_sql_query(query, conn)
print("Query results:")
print(df)
print(f"\nNumber of rows: {len(df)}")
print(f"Unique methods: {df['method'].unique()}")
print(f"Unique queries: {df['query_name'].unique()}")

conn.close()