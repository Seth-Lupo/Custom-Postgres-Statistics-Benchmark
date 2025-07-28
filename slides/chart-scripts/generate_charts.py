#!/usr/bin/env python3
"""
Generate charts for the presentation from SQLite experiment data.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Set style
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except:
    # Fallback to available style
    plt.style.use('ggplot')
sns.set_palette("husl")

# Database path
DB_PATH = Path("../../app/app/app_metadata.db")
OUTPUT_DIR = Path("../images")
OUTPUT_DIR.mkdir(exist_ok=True)

# Connect to database
conn = sqlite3.connect(DB_PATH)

# Query 1: Get execution times by method and query, keeping Schneider runs separate
query1 = """
SELECT 
    CASE 
        WHEN e.stats_source LIKE '%Built-in%' THEN 'PostgreSQL Built-in'
        WHEN e.stats_source LIKE '%Empty%' THEN 'Empty Statistics'
        WHEN e.stats_source LIKE '%Schneider%' AND e.name LIKE '%run1%' THEN 'Schneider AI Run 1'
        WHEN e.stats_source LIKE '%Schneider%' AND e.name LIKE '%run2%' THEN 'Schneider AI Run 2'
        WHEN e.stats_source LIKE '%Schneider%' AND e.name LIKE '%run3%' THEN 'Schneider AI Run 3'
        WHEN e.stats_source LIKE '%Schneider%' THEN 'Schneider AI'
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
    e.avg_time as avg_execution_time,
    e.stddev_time as avg_stddev
FROM experiment e
WHERE e.avg_time IS NOT NULL
ORDER BY query_name, 
    CASE 
        WHEN method LIKE 'Schneider%' THEN 1
        WHEN method LIKE 'Empty%' THEN 2
        WHEN method LIKE 'PostgreSQL%' THEN 3
    END,
    method
"""

df_avg = pd.read_sql_query(query1, conn)

# Chart 1: Average execution time by method and query (LOG SCALE)
if not df_avg.empty:
    fig, ax = plt.subplots(figsize=(12, 7))
    queries = df_avg['query_name'].unique()
    
    # Group methods - Schneider runs together
    all_methods = df_avg['method'].unique()
    
    # Define colors - same color for all Schneider runs
    color_map = {
        'Schneider AI Run 1': '#2ecc71',
        'Schneider AI Run 2': '#2ecc71',
        'Schneider AI Run 3': '#2ecc71',
        'Empty Statistics': '#e74c3c',
        'PostgreSQL Built-in': '#3498db'
    }
    
    # Calculate bar positions
    x = np.arange(len(queries))
    total_bars = len(all_methods)
    width = 0.8 / total_bars  # Adjust width based on number of bars
    
    # Plot each method
    for i, method in enumerate(all_methods):
        data = df_avg[df_avg['method'] == method]
        values = []
        errors = []
        for q in queries:
            row = data[data['query_name'] == q]
            if not row.empty:
                values.append(row['avg_execution_time'].values[0])
                errors.append(row['avg_stddev'].values[0] if row['avg_stddev'].values[0] else 0)
            else:
                values.append(0.001)  # Small value for log scale
                errors.append(0)
        
        if values:  # Only plot if we have data
            # Determine color and label
            color = color_map.get(method, 'gray')
            # Simplify label for legend
            if 'Schneider' in method:
                label = method.replace('Schneider AI ', 'LLM ')
            else:
                label = method
                
            ax.bar(x + i*width - 0.4, values, width, 
                   label=label, 
                   yerr=errors, 
                   capsize=3,
                   color=color,
                   alpha=0.8,
                   edgecolor='black',
                   linewidth=0.5)

    ax.set_xlabel('Query', fontsize=12)
    ax.set_ylabel('Execution Time (seconds) - Log₂ Scale', fontsize=12)
    ax.set_title('Query Execution Time: All Runs Shown (Logarithmic Scale)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.set_yscale('log', base=2)  # Set logarithmic scale base 2
    
    # Custom legend
    ax.legend(ncol=2, loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3, which='both', axis='y')
    
    # Add note about log scale and runs
    ax.text(0.02, 0.02, 'Note: Log₂ scale. LLM method shows 3 separate runs (same color)', 
            transform=ax.transAxes, 
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=9)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'execution_time_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
else:
    print("No data for average execution times")

# Query 2: Get first trial times (cold cache)
query2 = """
SELECT 
    CASE 
        WHEN e.stats_source LIKE '%Built-in%' THEN 'PostgreSQL Built-in'
        WHEN e.stats_source LIKE '%Empty%' THEN 'Empty Statistics'
        WHEN e.stats_source LIKE '%Schneider%' THEN 'LLM-based (Schneider AI)'
    END as method,
    CASE 
        WHEN e.name LIKE '%_a1_%' OR e.name LIKE '%_a1' THEN 'a1'
        WHEN e.name LIKE '%_a2_%' OR e.name LIKE '%_a2' THEN 'a2'
        WHEN e.name LIKE '%_a3_%' OR e.name LIKE '%_a3' THEN 'a3'
        WHEN e.name LIKE '%_a4_%' OR e.name LIKE '%_a4' THEN 'a4'
        WHEN e.name LIKE '%_a5_%' OR e.name LIKE '%_a5' THEN 'a5'
        ELSE 'unknown'
    END as query_name,
    t.execution_time as first_trial_time
FROM experiment e
JOIN trial t ON e.id = t.experiment_id
WHERE t.run_index = 1
ORDER BY query_name, method
"""

df_first = pd.read_sql_query(query2, conn)

# Chart 2: First trial execution times (cold cache)
if not df_first.empty:
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Pivot for better plotting
        pivot_first = df_first.pivot(index='query_name', columns='method', values='first_trial_time')
        
        if not pivot_first.empty:
            pivot_first.plot(kind='bar', ax=ax, width=0.8)
            
            ax.set_xlabel('Query', fontsize=12)
            ax.set_ylabel('First Trial Execution Time (seconds)', fontsize=12)
            ax.set_title('First Trial Performance (Cold Cache)', fontsize=14, fontweight='bold')
            ax.legend(title='Method', bbox_to_anchor=(1.05, 1), loc='upper left')
            ax.grid(True, alpha=0.3)
            plt.xticks(rotation=0)
            
            plt.tight_layout()
            plt.savefig(OUTPUT_DIR / 'first_trial_comparison.png', dpi=300, bbox_inches='tight')
            plt.close()
    except Exception as e:
        print(f"Error creating first trial chart: {e}")
else:
    print("No data for first trial times")

# Query 3: Overhead analysis - comparing to empty baseline
query3 = """
WITH method_times AS (
    SELECT 
        CASE 
        WHEN e.name LIKE '%_a1_%' OR e.name LIKE '%_a1' THEN 'a1'
        WHEN e.name LIKE '%_a2_%' OR e.name LIKE '%_a2' THEN 'a2'
        WHEN e.name LIKE '%_a3_%' OR e.name LIKE '%_a3' THEN 'a3'
        WHEN e.name LIKE '%_a4_%' OR e.name LIKE '%_a4' THEN 'a4'
        WHEN e.name LIKE '%_a5_%' OR e.name LIKE '%_a5' THEN 'a5'
        ELSE 'unknown'
    END as query_name,
        CASE 
            WHEN e.stats_source LIKE '%Built-in%' THEN 'builtin'
            WHEN e.stats_source LIKE '%Empty%' THEN 'empty'
            WHEN e.stats_source LIKE '%Schneider%' THEN 'llm'
        END as method,
        AVG(e.avg_time) as avg_time
    FROM experiment e
    WHERE e.avg_time IS NOT NULL
    GROUP BY query_name, method
)
SELECT 
    query_name,
    MAX(CASE WHEN method = 'empty' THEN avg_time END) as empty_time,
    MAX(CASE WHEN method = 'builtin' THEN avg_time END) as builtin_time,
    MAX(CASE WHEN method = 'llm' THEN avg_time END) as llm_time
FROM method_times
GROUP BY query_name
"""

df_overhead = pd.read_sql_query(query3, conn)

# Calculate overhead percentages
if not df_overhead.empty and 'empty_time' in df_overhead.columns:
    # Handle division by zero and NaN values
    df_overhead['builtin_overhead'] = df_overhead.apply(
        lambda row: ((row['builtin_time'] - row['empty_time']) / row['empty_time'] * 100) 
        if pd.notna(row['empty_time']) and row['empty_time'] > 0 else 0, axis=1)
    df_overhead['llm_overhead'] = df_overhead.apply(
        lambda row: ((row['llm_time'] - row['empty_time']) / row['empty_time'] * 100) 
        if pd.notna(row['empty_time']) and row['empty_time'] > 0 else 0, axis=1)

    # Chart 3: Overhead comparison
    if len(df_overhead) > 0:
        fig, ax = plt.subplots(figsize=(10, 6))

        x = np.arange(len(df_overhead))
        width = 0.35

        bars1 = ax.bar(x - width/2, df_overhead['builtin_overhead'], width, label='PostgreSQL Built-in', color='skyblue')
        bars2 = ax.bar(x + width/2, df_overhead['llm_overhead'], width, label='LLM-based', color='lightcoral')

        ax.set_xlabel('Query', fontsize=12)
        ax.set_ylabel('Overhead vs Empty Statistics (%)', fontsize=12)
        ax.set_title('Statistics Method Overhead Analysis', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(df_overhead['query_name'])
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if pd.notna(height):
                    ax.annotate(f'{height:.1f}%',
                               xy=(bar.get_x() + bar.get_width() / 2, height),
                               xytext=(0, 3),
                               textcoords="offset points",
                               ha='center', va='bottom')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / 'overhead_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
else:
    print("No data for overhead analysis")

# Query 4: Trial-by-trial performance for Schneider AI
query4 = """
SELECT 
    CASE 
        WHEN e.name LIKE '%_a1_%' OR e.name LIKE '%_a1' THEN 'a1'
        WHEN e.name LIKE '%_a2_%' OR e.name LIKE '%_a2' THEN 'a2'
        WHEN e.name LIKE '%_a3_%' OR e.name LIKE '%_a3' THEN 'a3'
        WHEN e.name LIKE '%_a4_%' OR e.name LIKE '%_a4' THEN 'a4'
        WHEN e.name LIKE '%_a5_%' OR e.name LIKE '%_a5' THEN 'a5'
        ELSE 'unknown'
    END as query_name,
    t.run_index,
    t.execution_time
FROM experiment e
JOIN trial t ON e.id = t.experiment_id
WHERE e.stats_source LIKE '%Schneider%'
ORDER BY query_name, t.run_index
"""

df_trials = pd.read_sql_query(query4, conn)

# Chart 4: Trial progression for LLM method
if not df_trials.empty:
    fig, ax = plt.subplots(figsize=(10, 6))

    queries = df_trials['query_name'].unique()
    if len(queries) > 0:
        for query in queries:
            data = df_trials[df_trials['query_name'] == query]
            if not data.empty:
                ax.plot(data['run_index'], data['execution_time'], marker='o', label=f'Query {query}', linewidth=2)

        ax.set_xlabel('Trial Number', fontsize=12)
        ax.set_ylabel('Execution Time (seconds)', fontsize=12)
        ax.set_title('LLM Method Performance Across Trials', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / 'trial_progression.png', dpi=300, bbox_inches='tight')
        plt.close()
else:
    print("No data for trial progression")

# Close connection
conn.close()

print("Charts generated successfully!")
print(f"Output directory: {OUTPUT_DIR}")
print("Generated files:")
for f in OUTPUT_DIR.glob("*.png"):
    print(f"  - {f.name}")