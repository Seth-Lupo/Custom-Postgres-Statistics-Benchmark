#!/usr/bin/env python3
"""
Generate charts based on first trial only (cold cache performance).
Also generate detailed trial-by-trial analysis for query a1.
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
    plt.style.use('ggplot')
sns.set_palette("husl")

# Database path
DB_PATH = Path("../../app/app/app_metadata.db")
OUTPUT_DIR = Path("../images")
OUTPUT_DIR.mkdir(exist_ok=True)

# Connect to database
conn = sqlite3.connect(DB_PATH)

# Query 1: Get FIRST trial execution times by method and query - keep Schneider runs separate
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
    t.execution_time as first_trial_time
FROM experiment e
JOIN trial t ON e.id = t.experiment_id
WHERE t.run_index = 1
ORDER BY query_name, 
    CASE 
        WHEN method LIKE 'Schneider%' THEN 1
        WHEN method LIKE 'Empty%' THEN 2
        WHEN method LIKE 'PostgreSQL%' THEN 3
    END,
    method
"""

df_first = pd.read_sql_query(query1, conn)

# Chart 1: First trial execution times comparison (LOG SCALE)
if not df_first.empty:
    fig, ax = plt.subplots(figsize=(12, 7))
    
    queries = df_first['query_name'].unique()
    all_methods = df_first['method'].unique()
    
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
    width = 0.8 / total_bars
    
    # Plot each method
    for i, method in enumerate(all_methods):
        data = df_first[df_first['method'] == method]
        values = []
        for q in queries:
            row = data[data['query_name'] == q]
            if not row.empty:
                values.append(row['first_trial_time'].values[0])
            else:
                values.append(0.001)  # Small value for log scale
        
        if values:
            color = color_map.get(method, 'gray')
            # Simplify label for legend
            if 'Schneider' in method:
                label = method.replace('Schneider AI ', 'LLM ')
            else:
                label = method
                
            bars = ax.bar(x + i*width - 0.4, values, width, 
                          label=label,
                          color=color,
                          alpha=0.8,
                          edgecolor='black',
                          linewidth=0.5)
            
            # Add value labels
            for bar, val in zip(bars, values):
                if val > 0.001:
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.05,
                            f'{val:.3f}',
                            ha='center', va='bottom', fontsize=7, rotation=45)
    
    ax.set_xlabel('Query', fontsize=12)
    ax.set_ylabel('First Trial Execution Time (seconds) - Log₂ Scale', fontsize=12)
    ax.set_title('First Trial Performance (Cold Cache): All Runs Shown', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.set_yscale('log', base=2)
    ax.legend(ncol=2, loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3, which='both', axis='y')
    
    # Add note about log scale
    ax.text(0.02, 0.02, 'Note: Log₂ scale. LLM method shows 3 separate runs (same color)', 
            transform=ax.transAxes, 
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'first_trial_comparison_fixed.png', dpi=300, bbox_inches='tight')
    plt.close()

# Query 2: Get all trials for query a1 only
query2 = """
SELECT 
    e.name,
    CASE 
        WHEN e.stats_source LIKE '%Built-in%' THEN 'PostgreSQL Built-in'
        WHEN e.stats_source LIKE '%Empty%' THEN 'Empty Statistics'
        WHEN e.stats_source LIKE '%Schneider%' THEN 'LLM-based (Schneider AI)'
        ELSE 'Unknown'
    END as method,
    t.run_index as trial_number,
    t.execution_time
FROM experiment e
JOIN trial t ON e.id = t.experiment_id
WHERE (e.name LIKE '%_a1_%' OR e.name LIKE '%_a1')
ORDER BY method, t.run_index
"""

df_a1_trials = pd.read_sql_query(query2, conn)

# Chart 2: Query a1 trial-by-trial comparison
if not df_a1_trials.empty:
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Prepare data for grouped bar chart
    methods = df_a1_trials['method'].unique()
    trials = sorted(df_a1_trials['trial_number'].unique())
    
    # Set up bar positions
    x = np.arange(len(trials))
    width = 0.25
    
    # Plot bars for each method
    for i, method in enumerate(methods):
        data = df_a1_trials[df_a1_trials['method'] == method]
        values = []
        for trial in trials:
            trial_data = data[data['trial_number'] == trial]
            if not trial_data.empty:
                values.append(trial_data['execution_time'].values[0])
            else:
                values.append(0)
        
        ax.bar(x + i*width, values, width, label=method, alpha=0.8)
    
    ax.set_xlabel('Trial Number', fontsize=12)
    ax.set_ylabel('Execution Time (seconds)', fontsize=12)
    ax.set_title('Query a1: Trial-by-Trial Performance Comparison', fontsize=14, fontweight='bold')
    ax.set_xticks(x + width)
    ax.set_xticklabels(trials)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add annotation about cache effects
    ax.text(0.02, 0.98, 'Note: Trial 1 shows cold cache performance', 
            transform=ax.transAxes, 
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=10)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'a1_trial_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

# Query 3: Get statistics about first vs subsequent trials
query3 = """
WITH trial_stats AS (
    SELECT 
        CASE 
            WHEN e.stats_source LIKE '%Built-in%' THEN 'PostgreSQL Built-in'
            WHEN e.stats_source LIKE '%Empty%' THEN 'Empty Statistics'
            WHEN e.stats_source LIKE '%Schneider%' THEN 'LLM-based (Schneider AI)'
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
        AVG(CASE WHEN t.run_index = 1 THEN t.execution_time END) as first_trial_avg,
        AVG(CASE WHEN t.run_index > 1 THEN t.execution_time END) as subsequent_trials_avg
    FROM experiment e
    JOIN trial t ON e.id = t.experiment_id
    GROUP BY method, query_name
)
SELECT 
    method,
    query_name,
    first_trial_avg,
    subsequent_trials_avg,
    (first_trial_avg - subsequent_trials_avg) / first_trial_avg * 100 as cache_improvement_pct
FROM trial_stats
WHERE first_trial_avg IS NOT NULL AND subsequent_trials_avg IS NOT NULL
ORDER BY query_name, method
"""

df_cache_stats = pd.read_sql_query(query3, conn)

# Chart 3: Cache improvement visualization
if not df_cache_stats.empty:
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Pivot for plotting
    pivot_cache = df_cache_stats.pivot(index='query_name', columns='method', values='cache_improvement_pct')
    
    if not pivot_cache.empty:
        pivot_cache.plot(kind='bar', ax=ax, width=0.8)
        
        ax.set_xlabel('Query', fontsize=12)
        ax.set_ylabel('Cache Improvement (%)', fontsize=12)
        ax.set_title('Performance Improvement from Cold to Warm Cache', fontsize=14, fontweight='bold')
        ax.legend(title='Method', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        plt.xticks(rotation=0)
        
        # Add percentage labels
        for container in ax.containers:
            ax.bar_label(container, fmt='%.1f%%', padding=3, rotation=90, fontsize=8)
        
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / 'cache_improvement_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()

# Query 4: Box plot for query a1 showing variance
query4 = """
SELECT 
    CASE 
        WHEN e.stats_source LIKE '%Built-in%' THEN 'PostgreSQL\\nBuilt-in'
        WHEN e.stats_source LIKE '%Empty%' THEN 'Empty\\nStatistics'
        WHEN e.stats_source LIKE '%Schneider%' THEN 'LLM-based\\n(Schneider AI)'
        ELSE 'Unknown'
    END as method,
    t.execution_time
FROM experiment e
JOIN trial t ON e.id = t.experiment_id
WHERE (e.name LIKE '%_a1_%' OR e.name LIKE '%_a1')
"""

df_a1_box = pd.read_sql_query(query4, conn)

# Chart 4: Box plot for query a1
if not df_a1_box.empty:
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Prepare data for cleaner visualization
    methods = df_a1_box['method'].unique()
    data_by_method = [df_a1_box[df_a1_box['method'] == method]['execution_time'].values 
                      for method in methods]
    
    # Create box plot with better styling
    bp = ax.boxplot(data_by_method, 
                    labels=[m.replace('\\n', '\n') for m in methods],
                    patch_artist=True,
                    widths=0.6,
                    showmeans=True,
                    meanprops=dict(marker='D', markerfacecolor='red', markersize=8))
    
    # Color the boxes
    colors = ['lightblue', 'lightgreen', 'lightyellow']
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    # Styling
    ax.set_xlabel('Method', fontsize=14, fontweight='bold')
    ax.set_ylabel('Execution Time (seconds)', fontsize=14, fontweight='bold')
    ax.set_title('Query a1: Execution Time Distribution Across All Trials', fontsize=16, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add statistical annotations
    for i, (method, data) in enumerate(zip(methods, data_by_method)):
        # Add median value
        median = np.median(data)
        ax.text(i+1, median, f'{median:.4f}', 
                horizontalalignment='center',
                verticalalignment='bottom',
                fontweight='bold',
                fontsize=10)
        
        # Add count of trials
        ax.text(i+1, ax.get_ylim()[0] + 0.001, f'n={len(data)}',
                horizontalalignment='center',
                fontsize=9,
                style='italic')
    
    # Add legend
    ax.text(0.02, 0.98, 'Red diamond = mean, Line = median', 
            transform=ax.transAxes, 
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
            fontsize=10)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'a1_execution_time_boxplot.png', dpi=300, bbox_inches='tight')
    plt.close()

# Chart 5: Alternative clearer visualization - bar chart with error bars
if not df_a1_box.empty:
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Calculate statistics for each method
    methods = []
    means = []
    stds = []
    medians = []
    
    for method in df_a1_box['method'].unique():
        data = df_a1_box[df_a1_box['method'] == method]['execution_time'].values
        methods.append(method.replace('\\n', '\n'))
        means.append(np.mean(data))
        stds.append(np.std(data))
        medians.append(np.median(data))
    
    # Create bar chart with error bars
    x = np.arange(len(methods))
    width = 0.6
    
    bars = ax.bar(x, means, width, yerr=stds, capsize=10, 
                   color=['#3498db', '#2ecc71', '#f39c12'], 
                   alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Add median markers
    for i, median in enumerate(medians):
        ax.plot(i, median, 'r_', markersize=30, markeredgewidth=3, label='Median' if i == 0 else '')
    
    # Styling
    ax.set_xlabel('Method', fontsize=14, fontweight='bold')
    ax.set_ylabel('Execution Time (seconds)', fontsize=14, fontweight='bold')
    ax.set_title('Query a1: Mean Execution Time with Standard Deviation', fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(methods)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, (bar, mean, std) in enumerate(zip(bars, means, stds)):
        # Mean value on bar
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + std + 0.001,
                f'{mean:.4f}s',
                ha='center', va='bottom', fontweight='bold', fontsize=11)
        # Std dev below
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()/2,
                f'±{std:.4f}',
                ha='center', va='center', fontsize=10, color='white', fontweight='bold')
    
    ax.legend(loc='upper right')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'a1_execution_time_barplot.png', dpi=300, bbox_inches='tight')
    plt.close()

# Close connection
conn.close()

print("First trial and a1 analysis charts generated successfully!")
print(f"Output directory: {OUTPUT_DIR}")
print("Generated files:")
for f in OUTPUT_DIR.glob("*.png"):
    print(f"  - {f.name}")