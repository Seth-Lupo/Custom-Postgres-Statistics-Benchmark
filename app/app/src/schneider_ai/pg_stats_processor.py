"""
PG Stats Processor Module

This module processes and validates pg_stats data from AI responses.

Input: pandas DataFrame with pg_stats columns
Output: processed and validated pandas DataFrame with pg_stats columns
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional
import re

class PGStatsProcessor:
    """Processes and validates pg_stats data."""
    
    def __init__(self, schema_info: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the processor with schema information.
        
        Args:
            schema_info: Database schema information for validation
            logger: Logger instance
        """
        self.schema_info = schema_info
        self.logger = logger
        
        # Build lookup tables for efficient validation
        self._build_schema_lookups()
    
    def _build_schema_lookups(self):
        """Build lookup tables for schema validation."""
        self.valid_columns = {}  # {table_name: {column_name: column_info}}
        
        for table_name, table_data in self.schema_info.get('tables', {}).items():
            self.valid_columns[table_name] = {}
            for col in table_data.get('columns', []):
                self.valid_columns[table_name][col['name']] = col
    
    def process_pg_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and validate pg_stats DataFrame.
        
        Args:
            df: Raw DataFrame from AI response
            
        Returns:
            Processed and validated DataFrame
        """
        if df.empty:
            self.logger.warning("Empty DataFrame provided for processing")
            return df
        
        self.logger.info(f"Processing pg_stats DataFrame with {len(df)} rows")
        
        # 1. Parse and validate attname column
        df = self._process_attname_column(df)
        
        # 2. Validate and clean numeric columns
        df = self._process_numeric_columns(df)
        
        # 3. Process array columns
        df = self._process_array_columns(df)
        
        # 4. Validate against schema
        df = self._validate_against_schema(df)
        
        # 5. Remove invalid rows
        initial_count = len(df)
        df = df.dropna(subset=['table_name', 'column_name'])
        removed_count = initial_count - len(df)
        
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} invalid rows")
        
        self.logger.info(f"Processing complete: {len(df)} valid rows")
        return df
    
    def _process_attname_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse attname into table_name and column_name."""
        def parse_attname(attname):
            if pd.isna(attname):
                return None, None
            
            attname = str(attname).strip()
            if '.' in attname:
                parts = attname.split('.', 1)
                return parts[0], parts[1]
            else:
                # If no table specified, we'll need to infer it
                return None, attname
        
        # Parse attname
        df[['table_name', 'column_name']] = df['attname'].apply(
            lambda x: pd.Series(parse_attname(x))
        )
        
        # Try to infer missing table names
        df = self._infer_table_names(df)
        
        return df
    
    def _infer_table_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Try to infer table names for columns without explicit table."""
        for idx, row in df.iterrows():
            if pd.isna(row['table_name']) and not pd.isna(row['column_name']):
                column_name = row['column_name']
                
                # Look for this column name in our schema
                matching_tables = []
                for table_name, columns in self.valid_columns.items():
                    if column_name in columns:
                        matching_tables.append(table_name)
                
                if len(matching_tables) == 1:
                    # Unique match found
                    df.at[idx, 'table_name'] = matching_tables[0]
                    self.logger.debug(f"Inferred table '{matching_tables[0]}' for column '{column_name}'")
                elif len(matching_tables) > 1:
                    # Multiple matches - use the first one but log warning
                    df.at[idx, 'table_name'] = matching_tables[0]
                    self.logger.warning(f"Multiple tables found for column '{column_name}': {matching_tables}. Using '{matching_tables[0]}'")
        
        return df
    
    def _process_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and validate numeric columns."""
        # null_frac: must be between 0 and 1
        if 'null_frac' in df.columns:
            df['null_frac'] = pd.to_numeric(df['null_frac'], errors='coerce')
            df['null_frac'] = df['null_frac'].clip(lower=0.0, upper=1.0)
        
        # avg_width: must be positive integer
        if 'avg_width' in df.columns:
            df['avg_width'] = pd.to_numeric(df['avg_width'], errors='coerce')
            df['avg_width'] = df['avg_width'].fillna(4).astype('Int64')  # Default to 4
            df['avg_width'] = df['avg_width'].clip(lower=1)
        
        # n_distinct: can be positive (absolute count) or negative (ratio)
        if 'n_distinct' in df.columns:
            df['n_distinct'] = pd.to_numeric(df['n_distinct'], errors='coerce')
            
            # Validate n_distinct against row counts
            for idx, row in df.iterrows():
                if not pd.isna(row.get('n_distinct')) and not pd.isna(row.get('table_name')):
                    table_info = self.schema_info.get('tables', {}).get(row['table_name'])
                    if table_info:
                        row_count = table_info.get('row_count', 1)
                        n_distinct = row['n_distinct']
                        
                        if n_distinct > 0:
                            # Positive: cap at row count
                            df.at[idx, 'n_distinct'] = min(n_distinct, row_count)
                        else:
                            # Negative: ensure between -1 and 0
                            df.at[idx, 'n_distinct'] = max(-1.0, min(0.0, n_distinct))
        
        # correlation: must be between -1 and 1
        if 'correlation' in df.columns:
            df['correlation'] = pd.to_numeric(df['correlation'], errors='coerce')
            df['correlation'] = df['correlation'].clip(lower=-1.0, upper=1.0)
        
        return df
    
    def _process_array_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process PostgreSQL array columns."""
        array_columns = ['most_common_vals', 'most_common_freqs', 'histogram_bounds']
        
        for col in array_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_pg_array)
        
        # Validate most_common_vals and most_common_freqs have same length
        if 'most_common_vals' in df.columns and 'most_common_freqs' in df.columns:
            for idx, row in df.iterrows():
                vals = row.get('most_common_vals')
                freqs = row.get('most_common_freqs')
                
                if vals is not None and freqs is not None:
                    if isinstance(vals, list) and isinstance(freqs, list):
                        if len(vals) != len(freqs):
                            # Truncate to minimum length
                            min_len = min(len(vals), len(freqs))
                            df.at[idx, 'most_common_vals'] = vals[:min_len]
                            df.at[idx, 'most_common_freqs'] = freqs[:min_len]
                            self.logger.warning(f"Adjusted array lengths for row {idx}")
                
                # Ensure frequencies sum to <= 1.0
                if isinstance(freqs, list) and len(freqs) > 0:
                    freq_sum = sum(float(f) for f in freqs if f is not None)
                    if freq_sum > 1.0:
                        # Normalize frequencies
                        df.at[idx, 'most_common_freqs'] = [f/freq_sum for f in freqs]
        
        return df
    
    def _parse_pg_array(self, value):
        """Parse PostgreSQL array format to Python list."""
        if pd.isna(value) or value in ['NULL', 'null', None]:
            return None
        
        value = str(value).strip()
        
        # Already a list
        if isinstance(value, list):
            return value
        
        # PostgreSQL array format: {val1,val2,val3}
        if value.startswith('{') and value.endswith('}'):
            content = value[1:-1]
            if not content:
                return []
            
            # Simple split for numeric arrays
            if ',' in content and '"' not in content:
                try:
                    return [float(x.strip()) for x in content.split(',')]
                except ValueError:
                    # Fall back to string array
                    return [x.strip() for x in content.split(',')]
            
            # Handle quoted strings
            elements = []
            current = ''
            in_quotes = False
            
            for char in content:
                if char == '"' and (not current or current[-1] != '\\'):
                    in_quotes = not in_quotes
                elif char == ',' and not in_quotes:
                    elements.append(current.strip('"'))
                    current = ''
                    continue
                current += char
            
            if current:
                elements.append(current.strip('"'))
            
            return elements
        
        return None
    
    def _validate_against_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate rows against actual schema."""
        valid_mask = pd.Series([True] * len(df))
        
        for idx, row in df.iterrows():
            table_name = row.get('table_name')
            column_name = row.get('column_name')
            
            if pd.isna(table_name) or pd.isna(column_name):
                valid_mask[idx] = False
                continue
            
            # Check if table exists
            if table_name not in self.valid_columns:
                self.logger.warning(f"Invalid table name: {table_name}")
                valid_mask[idx] = False
                continue
            
            # Check if column exists in table
            if column_name not in self.valid_columns[table_name]:
                self.logger.warning(f"Invalid column: {table_name}.{column_name}")
                valid_mask[idx] = False
                continue
            
            # Additional data type validation
            column_info = self.valid_columns[table_name][column_name]
            data_type = column_info.get('data_type', '').lower()
            
            # Validate correlation only for sortable types
            if not pd.isna(row.get('correlation')):
                sortable_types = ['integer', 'bigint', 'smallint', 'numeric', 'decimal', 
                                'real', 'double precision', 'date', 'timestamp', 'time']
                if not any(t in data_type for t in sortable_types):
                    # Non-sortable type shouldn't have correlation
                    df.at[idx, 'correlation'] = None
        
        # Apply validation mask
        df['is_valid'] = valid_mask
        invalid_count = (~valid_mask).sum()
        
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} invalid rows")
        
        return df[valid_mask].drop(columns=['is_valid'])
    
    def get_statistics_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get summary statistics about the processed data."""
        if df.empty:
            return {'total_rows': 0, 'tables': {}}
        
        summary = {
            'total_rows': len(df),
            'tables': {}
        }
        
        # Group by table
        for table_name, table_df in df.groupby('table_name'):
            table_summary = {
                'column_count': len(table_df),
                'columns_with_null_frac': (~table_df['null_frac'].isna()).sum() if 'null_frac' in df.columns else 0,
                'columns_with_n_distinct': (~table_df['n_distinct'].isna()).sum() if 'n_distinct' in df.columns else 0,
                'columns_with_correlation': (~table_df['correlation'].isna()).sum() if 'correlation' in df.columns else 0,
                'columns_with_histogram': table_df['histogram_bounds'].apply(lambda x: x is not None and len(x) > 0).sum() if 'histogram_bounds' in df.columns else 0,
            }
            summary['tables'][table_name] = table_summary
        
        return summary