"""
Stats Translator Module

This module translates pg_stats format (human-readable) to pg_statistic format (system catalog).

Input: pandas DataFrame with pg_stats columns
Output: pandas DataFrame with pg_statistic columns and values
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import text
from sqlmodel import Session

class StatsTranslator:
    """Translates pg_stats to pg_statistic format."""
    
    def __init__(self, session: Session, target_columns: Dict[str, int], logger: logging.Logger):
        """
        Initialize the translator.
        
        Args:
            session: Database session for OID lookups
            target_columns: Mapping of stat names to pg_statistic column indices
            logger: Logger instance
        """
        self.session = session
        self.target_columns = target_columns
        self.logger = logger
        
        # Cache for OID lookups
        self.oid_cache = {}
        self.attnum_cache = {}
    
    def translate_to_pg_statistic(self, pg_stats_df: pd.DataFrame) -> pd.DataFrame:
        """
        Translate pg_stats DataFrame to pg_statistic format.
        
        Args:
            pg_stats_df: DataFrame with pg_stats columns
            
        Returns:
            DataFrame with pg_statistic format:
            - starelid: table OID
            - staattnum: column attribute number
            - stainherit: inheritance flag (always False)
            - stat_column: column index in pg_statistic
            - stat_value: value to insert
        """
        if pg_stats_df.empty:
            self.logger.warning("Empty DataFrame provided for translation")
            return pd.DataFrame()
        
        self.logger.info(f"Translating {len(pg_stats_df)} pg_stats rows to pg_statistic format")
        
        # Prepare list to collect all statistic entries
        statistic_entries = []
        
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
            
            # Translate each statistic type
            # 1. null_frac -> stanullfrac (column 3)
            if not pd.isna(row.get('null_frac')) and 'stanullfrac' in self.target_columns:
                statistic_entries.append({
                    'starelid': table_oid,
                    'staattnum': attnum,
                    'stainherit': False,
                    'stat_column': self.target_columns['stanullfrac'],
                    'stat_value': float(row['null_frac']),
                    'stat_type': 'stanullfrac',
                    'table_name': table_name,
                    'column_name': column_name
                })
            
            # 2. n_distinct -> stadistinct (column 5)
            if not pd.isna(row.get('n_distinct')) and 'stadistinct' in self.target_columns:
                statistic_entries.append({
                    'starelid': table_oid,
                    'staattnum': attnum,
                    'stainherit': False,
                    'stat_column': self.target_columns['stadistinct'],
                    'stat_value': float(row['n_distinct']),
                    'stat_type': 'stadistinct',
                    'table_name': table_name,
                    'column_name': column_name
                })
            
            # 3. correlation -> stanumbers1 (column 16)
            # Note: stanumbers1 is an array, so we need special handling
            if not pd.isna(row.get('correlation')) and 'stanumbers1' in self.target_columns:
                # Convert single correlation value to PostgreSQL array format
                correlation_value = float(row['correlation'])
                pg_array = f'{{{correlation_value}}}'  # PostgreSQL array literal
                
                statistic_entries.append({
                    'starelid': table_oid,
                    'staattnum': attnum,
                    'stainherit': False,
                    'stat_column': self.target_columns['stanumbers1'],
                    'stat_value': pg_array,
                    'stat_type': 'stanumbers1',
                    'table_name': table_name,
                    'column_name': column_name
                })
            
            # 4. most_common_freqs -> stanumbers1 (if correlation not present)
            elif 'most_common_freqs' in row and row.get('most_common_freqs') is not None and 'stanumbers1' in self.target_columns:
                freqs = row['most_common_freqs']
                if isinstance(freqs, list) and len(freqs) > 0:
                    # Convert list to PostgreSQL array format
                    pg_array = '{' + ','.join(str(f) for f in freqs) + '}'
                    
                    statistic_entries.append({
                        'starelid': table_oid,
                        'staattnum': attnum,
                        'stainherit': False,
                        'stat_column': self.target_columns['stanumbers1'],
                        'stat_value': pg_array,
                        'stat_type': 'stanumbers1',
                        'table_name': table_name,
                        'column_name': column_name
                    })
            
            # Additional statistics can be added here as needed
            # For now, we're focusing on the three main statistics from the config
        
        # Convert to DataFrame
        result_df = pd.DataFrame(statistic_entries)
        
        if not result_df.empty:
            self.logger.info(f"Translated to {len(result_df)} pg_statistic entries")
            
            # Log summary by statistic type
            stat_counts = result_df['stat_type'].value_counts()
            self.logger.debug(f"Statistics breakdown: {stat_counts.to_dict()}")
        else:
            self.logger.warning("No statistics were translated")
        
        return result_df
    
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
    
    def get_existing_statistics(self, table_oid: int, attnum: int) -> Dict[str, Any]:
        """Get existing statistics for a column."""
        try:
            query = """
            SELECT stanullfrac, stawidth, stadistinct, 
                   stakind1, stakind2, stakind3, stakind4, stakind5,
                   stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5
            FROM pg_statistic 
            WHERE starelid = :table_oid 
            AND staattnum = :attnum 
            AND stainherit = false
            """
            
            result = self.session.execute(
                text(query),
                {"table_oid": table_oid, "attnum": attnum}
            )
            row = result.fetchone()
            
            if row:
                return {
                    'stanullfrac': row[0],
                    'stawidth': row[1],
                    'stadistinct': row[2],
                    'stakind1': row[3],
                    'stakind2': row[4],
                    'stakind3': row[5],
                    'stakind4': row[6],
                    'stakind5': row[7],
                    'stanumbers1': row[8],
                    'stanumbers2': row[9],
                    'stanumbers3': row[10],
                    'stanumbers4': row[11],
                    'stanumbers5': row[12],
                    'exists': True
                }
            else:
                return {'exists': False}
                
        except Exception as e:
            self.logger.error(f"Error getting existing statistics: {str(e)}")
            return {'exists': False}
    
    def prepare_insert_values(self, stat_entry: Dict[str, Any], 
                            existing_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare values for inserting a new pg_statistic row.
        Uses existing values where available, defaults otherwise.
        """
        # Start with defaults
        values = {
            'starelid': stat_entry['starelid'],
            'staattnum': stat_entry['staattnum'],
            'stainherit': False,
            'stanullfrac': existing_stats.get('stanullfrac', 0.0),
            'stawidth': existing_stats.get('stawidth', 4),
            'stadistinct': existing_stats.get('stadistinct', 0.0),
            'stakind1': existing_stats.get('stakind1', 0),
            'stakind2': existing_stats.get('stakind2', 0),
            'stakind3': existing_stats.get('stakind3', 0),
            'stakind4': existing_stats.get('stakind4', 0),
            'stakind5': existing_stats.get('stakind5', 0),
            'staop1': 0,
            'staop2': 0,
            'staop3': 0,
            'staop4': 0,
            'staop5': 0,
            'stacoll1': 0,
            'stacoll2': 0,
            'stacoll3': 0,
            'stacoll4': 0,
            'stacoll5': 0,
            'stanumbers1': existing_stats.get('stanumbers1'),
            'stanumbers2': existing_stats.get('stanumbers2'),
            'stanumbers3': existing_stats.get('stanumbers3'),
            'stanumbers4': existing_stats.get('stanumbers4'),
            'stanumbers5': existing_stats.get('stanumbers5'),
            'stavalues1': None,
            'stavalues2': None,
            'stavalues3': None,
            'stavalues4': None,
            'stavalues5': None
        }
        
        # Override with the specific statistic value
        stat_column = stat_entry['stat_column']
        stat_value = stat_entry['stat_value']
        
        if stat_column == 3:  # stanullfrac
            values['stanullfrac'] = stat_value
        elif stat_column == 5:  # stadistinct
            values['stadistinct'] = stat_value
        elif stat_column == 16:  # stanumbers1
            values['stanumbers1'] = stat_value
        
        return values