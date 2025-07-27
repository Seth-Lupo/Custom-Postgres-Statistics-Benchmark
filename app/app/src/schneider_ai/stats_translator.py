"""
Stats Translator Module - FIXED VERSION

This module properly translates pg_stats format to pg_statistic format using the stakind system.

PostgreSQL's pg_statistic uses stakind values to organize complex statistics:
- stakind = 1: Most Common Values (stavalues1 = values, stanumbers1 = frequencies)  
- stakind = 2: Histogram bounds (stavalues2 = bounds)
- stakind = 3: Correlation (stanumbers3 = correlation value)
- stakind = 4: Most common elements for arrays
- stakind = 5: Distinct element count histogram

Input: pandas DataFrame with pg_stats columns
Output: Complete pg_statistic rows ready for insertion/update
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import text
from sqlmodel import Session

# PostgreSQL stakind constants
STATISTIC_KIND_MCV = 1          # Most common values
STATISTIC_KIND_HISTOGRAM = 2    # Histogram bounds  
STATISTIC_KIND_CORRELATION = 3  # Correlation
STATISTIC_KIND_MCELEM = 4       # Most common elements (arrays)
STATISTIC_KIND_DECHIST = 5      # Distinct element histogram

class StatsTranslator:
    """Translates pg_stats to pg_statistic format using proper stakind system."""
    
    def __init__(self, session: Session, logger: logging.Logger):
        """
        Initialize the translator.
        
        Args:
            session: Database session for OID lookups
            logger: Logger instance
        """
        self.session = session
        self.logger = logger
        
        # Cache for OID lookups
        self.oid_cache = {}
        self.attnum_cache = {}
    
    def translate_to_pg_statistic(self, pg_stats_df: pd.DataFrame) -> pd.DataFrame:
        """
        Translate pg_stats DataFrame to complete pg_statistic rows.
        
        Args:
            pg_stats_df: DataFrame with pg_stats columns
            
        Returns:
            DataFrame with complete pg_statistic rows ready for insertion
        """
        if pg_stats_df.empty:
            self.logger.warning("Empty DataFrame provided for translation")
            return pd.DataFrame()
        
        self.logger.info(f"Translating {len(pg_stats_df)} pg_stats rows to pg_statistic format")
        
        # Prepare list to collect complete pg_statistic rows
        pg_statistic_rows = []
        
        # Process each row
        for idx, row in pg_stats_df.iterrows():
            table_name = row.get('table_name')
            column_name = row.get('column_name')
            
            if pd.isna(table_name) or pd.isna(column_name):
                continue
            
            # Get OIDs
            table_oid = self._get_table_oid(table_name)
            if table_oid is None:
                self.logger.warning(f"Could not find OID for table {table_name}")
                continue
            
            attnum = self._get_column_attnum(table_oid, column_name)
            if attnum is None:
                self.logger.warning(f"Could not find attnum for {table_name}.{column_name}")
                continue
            
            # Create a complete pg_statistic row
            pg_stat_row = self._create_pg_statistic_row(table_oid, attnum, row, table_name, column_name)
            if pg_stat_row:
                pg_statistic_rows.append(pg_stat_row)
        
        # Convert to DataFrame
        result_df = pd.DataFrame(pg_statistic_rows)
        
        if not result_df.empty:
            self.logger.info(f"Translated to {len(result_df)} complete pg_statistic rows")
        else:
            self.logger.warning("No statistics were translated")
        
        return result_df
    
    def _create_pg_statistic_row(self, table_oid: int, attnum: int, stats_row: pd.Series, 
                                table_name: str, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Create a complete pg_statistic row from pg_stats data.
        
        This properly organizes statistics using the stakind system.
        """
        # Start with base row structure
        n_distinct_value = stats_row.get('n_distinct', 0.0)
        if n_distinct_value != 0.0:
            self.logger.info(f"📊 n_distinct for {table_name}.{column_name}: {n_distinct_value}")
        
        pg_stat_row = {
            'starelid': table_oid,
            'staattnum': attnum,
            'stainherit': False,
            
            # Simple statistics (always present)
            'stanullfrac': float(stats_row.get('null_frac', 0.0)),
            'stawidth': int(stats_row.get('avg_width', 4)),
            'stadistinct': float(n_distinct_value),
            
            # Complex statistics arrays (up to 5 slots)
            'stakind1': 0, 'stakind2': 0, 'stakind3': 0, 'stakind4': 0, 'stakind5': 0,
            'staop1': 0, 'staop2': 0, 'staop3': 0, 'staop4': 0, 'staop5': 0,
            'stacoll1': 0, 'stacoll2': 0, 'stacoll3': 0, 'stacoll4': 0, 'stacoll5': 0,
            'stanumbers1': None, 'stanumbers2': None, 'stanumbers3': None, 'stanumbers4': None, 'stanumbers5': None,
            'stavalues1': None, 'stavalues2': None, 'stavalues3': None, 'stavalues4': None, 'stavalues5': None,
            
            # Metadata for tracking
            'table_name': table_name,
            'column_name': column_name
        }
        
        # Track which slots we've used
        next_slot = 1
        
        # 1. Most Common Values + Frequencies (if both present)
        mcv_vals = stats_row.get('most_common_vals')
        mcv_freqs = stats_row.get('most_common_freqs')
        
        if (mcv_vals is not None and mcv_freqs is not None and 
            isinstance(mcv_vals, list) and isinstance(mcv_freqs, list) and 
            len(mcv_vals) > 0 and len(mcv_freqs) > 0):
            
            if next_slot <= 5:
                self.logger.debug(f"Adding MCV statistics to slot {next_slot} for {table_name}.{column_name}")
                pg_stat_row[f'stakind{next_slot}'] = STATISTIC_KIND_MCV
                pg_stat_row[f'stanumbers{next_slot}'] = self._convert_to_pg_array(mcv_freqs, 'float4[]')
                pg_stat_row[f'stavalues{next_slot}'] = self._convert_to_pg_array(mcv_vals, 'anyarray')
                next_slot += 1
        
        # 2. Histogram bounds (if present)
        histogram = stats_row.get('histogram_bounds')
        if (histogram is not None and isinstance(histogram, list) and len(histogram) > 0):
            
            if next_slot <= 5:
                self.logger.debug(f"Adding histogram statistics to slot {next_slot} for {table_name}.{column_name}")
                pg_stat_row[f'stakind{next_slot}'] = STATISTIC_KIND_HISTOGRAM
                pg_stat_row[f'stavalues{next_slot}'] = self._convert_to_pg_array(histogram, 'anyarray')
                next_slot += 1
        
        # 3. Correlation (if present and makes sense for this data type)
        correlation = stats_row.get('correlation')
        if not pd.isna(correlation):
            
            if next_slot <= 5:
                self.logger.debug(f"Adding correlation statistics to slot {next_slot} for {table_name}.{column_name}")
                pg_stat_row[f'stakind{next_slot}'] = STATISTIC_KIND_CORRELATION
                pg_stat_row[f'stanumbers{next_slot}'] = self._convert_to_pg_array([float(correlation)], 'float4[]')
                next_slot += 1
        
        # If we added any complex statistics, return the row
        if next_slot > 1:
            return pg_stat_row
        
        # If we only have simple statistics, still return the row
        if (not pd.isna(stats_row.get('null_frac')) or 
            not pd.isna(stats_row.get('n_distinct')) or
            not pd.isna(stats_row.get('avg_width'))):
            return pg_stat_row
        
        # No statistics to add
        self.logger.debug(f"No statistics to add for {table_name}.{column_name}")
        return None
    
    def _convert_to_pg_array(self, values: List[Any], pg_type: str) -> Any:
        """
        Convert Python list to PostgreSQL array format.
        
        Args:
            values: List of values to convert
            pg_type: PostgreSQL array type ('float4[]', 'anyarray', etc.)
            
        Returns:
            Python list for SQLAlchemy to handle properly
        """
        if not values:
            return None
        
        try:
            if pg_type == 'float4[]':
                # Numeric array - return as list of floats
                return [float(v) for v in values]
            
            elif pg_type == 'anyarray':
                # Generic array - return as list, let SQLAlchemy handle the casting
                converted_values = []
                for v in values:
                    if v is None:
                        converted_values.append(None)
                    elif isinstance(v, str):
                        converted_values.append(v)
                    else:
                        converted_values.append(str(v))
                
                return converted_values
            
            else:
                # Default case - return as list of strings
                return [str(v) for v in values]
                
        except Exception as e:
            self.logger.error(f"Failed to convert array {values} to {pg_type}: {str(e)}")
            return None
    
    def _get_table_oid(self, table_name: str) -> Optional[int]:
        """Get OID for a table, with caching."""
        if table_name in self.oid_cache:
            return self.oid_cache[table_name]
        
        try:
            query = """
            SELECT c.oid 
            FROM pg_class c 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE c.relname = :table_name AND n.nspname = 'public'
            """
            
            result = self.session.execute(text(query), {"table_name": table_name})
            row = result.fetchone()
            
            if row:
                oid = row[0]
                self.oid_cache[table_name] = oid
                return oid
            else:
                self.oid_cache[table_name] = None
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting OID for table {table_name}: {str(e)}")
            return None
    
    def _get_column_attnum(self, table_oid: int, column_name: str) -> Optional[int]:
        """Get attribute number for a column, with caching."""
        cache_key = f"{table_oid}:{column_name}"
        
        if cache_key in self.attnum_cache:
            return self.attnum_cache[cache_key]
        
        try:
            query = """
            SELECT attnum 
            FROM pg_attribute 
            WHERE attrelid = :table_oid 
            AND attname = :column_name
            AND attnum > 0
            AND NOT attisdropped
            """
            
            result = self.session.execute(
                text(query), 
                {"table_oid": table_oid, "column_name": column_name}
            )
            row = result.fetchone()
            
            if row:
                attnum = row[0]
                self.attnum_cache[cache_key] = attnum
                return attnum
            else:
                self.attnum_cache[cache_key] = None
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting attnum for column {column_name}: {str(e)}")
            return None
    
    def get_column_operator_info(self, table_oid: int, attnum: int) -> Dict[str, int]:
        """
        Get operator and collation info for a column.
        This is needed for proper stakind organization.
        """
        try:
            query = """
            SELECT t.typname, t.oid as type_oid,
                   (SELECT oid FROM pg_operator WHERE oprname = '<' AND oprleft = t.oid AND oprright = t.oid LIMIT 1) as lt_op,
                   (SELECT oid FROM pg_operator WHERE oprname = '=' AND oprleft = t.oid AND oprright = t.oid LIMIT 1) as eq_op
            FROM pg_attribute a
            JOIN pg_type t ON a.atttypid = t.oid
            WHERE a.attrelid = :table_oid AND a.attnum = :attnum
            """
            
            result = self.session.execute(
                text(query),
                {"table_oid": table_oid, "attnum": attnum}
            )
            row = result.fetchone()
            
            if row:
                return {
                    'type_name': row[0],
                    'type_oid': row[1],
                    'lt_operator': row[2] or 0,
                    'eq_operator': row[3] or 0,
                    'collation': 0  # Default for now
                }
            else:
                return {'lt_operator': 0, 'eq_operator': 0, 'collation': 0}
                
        except Exception as e:
            self.logger.error(f"Error getting operator info: {str(e)}")
            return {'lt_operator': 0, 'eq_operator': 0, 'collation': 0}