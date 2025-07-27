"""
PostgreSQL Inserter Module - AGGRESSIVE ANYARRAY APPROACH

This module uses more aggressive techniques to update anyarray columns in pg_statistic.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Union
from sqlalchemy import text
from sqlmodel import Session

class PostgresInserterAggressive:
    """Aggressive approach to updating pg_statistic including anyarray columns."""
    
    def __init__(self, session: Session, logger: logging.Logger, advanced_logging: bool = True):
        self.session = session
        self.logger = logger
        self.advanced_logging = advanced_logging
        
    def update_statistics_with_anyarray(self, pg_statistic_df: pd.DataFrame) -> Dict[str, int]:
        """Update statistics using aggressive anyarray handling techniques."""
        counts = {'updated': 0, 'failed': 0}
        
        self.logger.info("🔥 AGGRESSIVE: Attempting anyarray updates with multiple techniques")
        
        for idx, row in pg_statistic_df.iterrows():
            table_oid = row['starelid']
            attnum = row['staattnum']
            table_name = row.get('table_name', 'unknown')
            column_name = row.get('column_name', 'unknown')
            
            if self.advanced_logging:
                self.logger.info(f"🎯 Processing {table_name}.{column_name}")
            
            # Try multiple approaches in order of likelihood to succeed
            success = False
            
            # Approach 1: Use array_in with explicit type OID
            if not success:
                success = self._try_array_in_approach(row)
                if success:
                    self.logger.info(f"✅ array_in approach succeeded for {table_name}.{column_name}")
                    counts['updated'] += 1
                    continue
            
            # Approach 2: Use type-specific cast
            if not success:
                success = self._try_type_specific_cast(row)
                if success:
                    self.logger.info(f"✅ Type-specific cast succeeded for {table_name}.{column_name}")
                    counts['updated'] += 1
                    continue
            
            # Approach 3: Use temporary table approach
            if not success:
                success = self._try_temp_table_approach(row)
                if success:
                    self.logger.info(f"✅ Temp table approach succeeded for {table_name}.{column_name}")
                    counts['updated'] += 1
                    continue
            
            # Approach 4: Direct text representation
            if not success:
                success = self._try_text_representation(row)
                if success:
                    self.logger.info(f"✅ Text representation succeeded for {table_name}.{column_name}")
                    counts['updated'] += 1
                    continue
            
            if not success:
                self.logger.warning(f"❌ All approaches failed for {table_name}.{column_name}")
                counts['failed'] += 1
        
        try:
            self.session.commit()
            self.logger.info(f"📊 Aggressive update results: {counts['updated']} succeeded, {counts['failed']} failed")
        except Exception as e:
            self.logger.error(f"Failed to commit: {str(e)}")
            self.session.rollback()
            
        return counts
    
    def _try_array_in_approach(self, row: pd.Series) -> bool:
        """Try using array_in function with explicit type OID."""
        try:
            table_oid = row['starelid']
            attnum = row['staattnum']
            
            # Get the actual column type OID
            type_query = """
            SELECT a.atttypid, t.typname, t.typelem
            FROM pg_attribute a
            JOIN pg_type t ON a.atttypid = t.oid
            WHERE a.attrelid = :table_oid AND a.attnum = :attnum
            """
            result = self.session.execute(text(type_query), {
                "table_oid": table_oid,
                "attnum": attnum
            })
            type_info = result.fetchone()
            
            if not type_info:
                return False
                
            type_oid = type_info[0]
            type_name = type_info[1]
            elem_oid = type_info[2]
            
            # Build update for stavalues using array_in
            update_parts = []
            
            # Update basic statistics first
            update_parts.extend([
                f"stanullfrac = {float(row.get('stanullfrac', 0.0))}",
                f"stawidth = {int(row.get('stawidth', 4))}",
                f"stadistinct = {float(row.get('stadistinct', 0.0))}"
            ])
            
            # Update stakind values
            for i in range(1, 6):
                update_parts.append(f"stakind{i} = {int(row.get(f'stakind{i}', 0))}")
            
            # Try to update stavalues with array_in
            for i in range(1, 6):
                stavalues = row.get(f'stavalues{i}')
                if self._is_valid_array(stavalues):
                    # Convert to PostgreSQL array text format
                    array_text = self._to_pg_array_text(stavalues)
                    # Use array_in with the element type OID
                    update_parts.append(f"stavalues{i} = array_in('{array_text}', {elem_oid}, -1)")
                else:
                    update_parts.append(f"stavalues{i} = NULL")
            
            update_query = f"""
            UPDATE pg_statistic SET
                {', '.join(update_parts)}
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            """
            
            result = self.session.execute(text(update_query))
            return result.rowcount > 0
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.debug(f"array_in approach failed: {str(e)}")
            return False
    
    def _try_type_specific_cast(self, row: pd.Series) -> bool:
        """Try casting to specific array type based on column type."""
        try:
            table_oid = row['starelid']
            attnum = row['staattnum']
            
            # Get column type
            type_query = """
            SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
            FROM pg_attribute a
            WHERE a.attrelid = :table_oid AND a.attnum = :attnum
            """
            result = self.session.execute(text(type_query), {
                "table_oid": table_oid,
                "attnum": attnum
            })
            col_type = result.scalar()
            
            if not col_type:
                return False
            
            # Determine array type
            array_type = self._get_array_type_for_column(col_type)
            
            # Build update
            update_parts = []
            
            # Basic stats
            update_parts.extend([
                f"stanullfrac = {float(row.get('stanullfrac', 0.0))}",
                f"stawidth = {int(row.get('stawidth', 4))}",
                f"stadistinct = {float(row.get('stadistinct', 0.0))}"
            ])
            
            # Stakind
            for i in range(1, 6):
                update_parts.append(f"stakind{i} = {int(row.get(f'stakind{i}', 0))}")
            
            # Stavalues with specific cast
            for i in range(1, 6):
                stavalues = row.get(f'stavalues{i}')
                if self._is_valid_array(stavalues):
                    array_literal = self._make_typed_array_literal(stavalues, col_type)
                    # Try double cast: specific type -> anyarray
                    update_parts.append(f"stavalues{i} = {array_literal}::{array_type}")
                else:
                    update_parts.append(f"stavalues{i} = NULL")
            
            update_query = f"""
            UPDATE pg_statistic SET
                {', '.join(update_parts)}
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            """
            
            result = self.session.execute(text(update_query))
            return result.rowcount > 0
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.debug(f"Type-specific cast failed: {str(e)}")
            return False
    
    def _try_temp_table_approach(self, row: pd.Series) -> bool:
        """Try using a temporary table to stage the update."""
        try:
            table_oid = row['starelid']
            attnum = row['staattnum']
            
            # Create a temp table with the exact structure
            temp_table = f"temp_stats_{table_oid}_{attnum}"
            
            # Drop if exists
            self.session.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            
            # Create temp table
            create_query = f"""
            CREATE TEMP TABLE {temp_table} AS 
            SELECT * FROM pg_statistic 
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            LIMIT 0
            """
            self.session.execute(text(create_query))
            
            # Insert into temp table with proper types
            # This is more flexible than direct update
            insert_values = {
                'starelid': table_oid,
                'staattnum': attnum,
                'stainherit': False,
                'stanullfrac': float(row.get('stanullfrac', 0.0)),
                'stawidth': int(row.get('stawidth', 4)),
                'stadistinct': float(row.get('stadistinct', 0.0))
            }
            
            # Add stakind values
            for i in range(1, 6):
                for prefix in ['stakind', 'staop', 'stacoll']:
                    insert_values[f'{prefix}{i}'] = int(row.get(f'{prefix}{i}', 0))
            
            # Build insert with arrays
            columns = list(insert_values.keys())
            values = list(insert_values.values())
            
            # Add array columns
            for i in range(1, 6):
                # stanumbers
                stanumbers = row.get(f'stanumbers{i}')
                columns.append(f'stanumbers{i}')
                if self._is_valid_array(stanumbers):
                    values.append(f"ARRAY{stanumbers}::float4[]")
                else:
                    values.append("NULL")
                
                # stavalues - try with cast
                stavalues = row.get(f'stavalues{i}')
                columns.append(f'stavalues{i}')
                if self._is_valid_array(stavalues):
                    values.append(f"ARRAY{stavalues}")
                else:
                    values.append("NULL")
            
            # Execute insert
            insert_query = f"""
            INSERT INTO {temp_table} ({', '.join(columns)})
            VALUES ({', '.join(str(v) if not isinstance(v, str) else v for v in values)})
            """
            
            self.session.execute(text(insert_query))
            
            # Now update from temp table
            update_query = f"""
            UPDATE pg_statistic p
            SET stanullfrac = t.stanullfrac,
                stawidth = t.stawidth,
                stadistinct = t.stadistinct
            FROM {temp_table} t
            WHERE p.starelid = t.starelid 
            AND p.staattnum = t.staattnum 
            AND p.stainherit = t.stainherit
            """
            
            result = self.session.execute(text(update_query))
            
            # Clean up
            self.session.execute(text(f"DROP TABLE {temp_table}"))
            
            return result.rowcount > 0
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.debug(f"Temp table approach failed: {str(e)}")
            # Clean up on failure
            try:
                self.session.execute(text(f"DROP TABLE IF EXISTS temp_stats_{table_oid}_{attnum}"))
            except:
                pass
            return False
    
    def _try_text_representation(self, row: pd.Series) -> bool:
        """Try using text representation and conversion."""
        try:
            table_oid = row['starelid']
            attnum = row['staattnum']
            
            # Update only non-array fields as fallback
            update_parts = []
            
            # Basic statistics
            update_parts.extend([
                f"stanullfrac = {float(row.get('stanullfrac', 0.0))}",
                f"stawidth = {int(row.get('stawidth', 4))}",
                f"stadistinct = {float(row.get('stadistinct', 0.0))}"
            ])
            
            # All integer fields
            for i in range(1, 6):
                for prefix in ['stakind', 'staop', 'stacoll']:
                    update_parts.append(f"{prefix}{i} = {int(row.get(f'{prefix}{i}', 0))}")
            
            # stanumbers arrays
            for i in range(1, 6):
                stanumbers = row.get(f'stanumbers{i}')
                if self._is_valid_array(stanumbers):
                    array_literal = self._make_pg_array_literal(stanumbers, 'float')
                    update_parts.append(f"stanumbers{i} = {array_literal}::float4[]")
                else:
                    update_parts.append(f"stanumbers{i} = NULL")
            
            update_query = f"""
            UPDATE pg_statistic SET
                {', '.join(update_parts)}
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            """
            
            result = self.session.execute(text(update_query))
            return result.rowcount > 0
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.debug(f"Text representation failed: {str(e)}")
            return False
    
    def _is_valid_array(self, value) -> bool:
        """Check if value is a valid array."""
        if value is None:
            return False
        if isinstance(value, str) and value.upper() == 'NULL':
            return False
        if isinstance(value, (list, tuple, np.ndarray)):
            if len(value) == 1 and isinstance(value[0], str) and value[0].upper() == 'NULL':
                return False
            return len(value) > 0
        return False
    
    def _to_pg_array_text(self, values: List[Any]) -> str:
        """Convert values to PostgreSQL array text format."""
        parts = []
        for v in values:
            if v is None:
                parts.append('NULL')
            else:
                # Escape special characters
                s = str(v).replace('\\', '\\\\').replace('"', '\\"')
                parts.append(f'"{s}"')
        return '{' + ','.join(parts) + '}'
    
    def _make_pg_array_literal(self, values: List[Any], array_type: str) -> str:
        """Create PostgreSQL array literal."""
        parts = []
        for v in values:
            if v is None:
                parts.append('NULL')
            elif array_type == 'float':
                parts.append(str(float(v)))
            else:
                escaped = str(v).replace("'", "''")
                parts.append(f"'{escaped}'")
        return "ARRAY[" + ",".join(parts) + "]"
    
    def _make_typed_array_literal(self, values: List[Any], col_type: str) -> str:
        """Create array literal based on column type."""
        if 'int' in col_type.lower():
            parts = [str(int(float(v))) if v is not None else 'NULL' for v in values]
        elif 'float' in col_type.lower() or 'numeric' in col_type.lower():
            parts = [str(float(v)) if v is not None else 'NULL' for v in values]
        else:
            parts = [f"'{str(v).replace('\'', '\'\'')}'" if v is not None else 'NULL' for v in values]
        return "ARRAY[" + ",".join(parts) + "]"
    
    def _get_array_type_for_column(self, col_type: str) -> str:
        """Get the appropriate array type for a column type."""
        col_type_lower = col_type.lower()
        
        if 'bigint' in col_type_lower:
            return 'bigint[]'
        elif 'integer' in col_type_lower or 'int' in col_type_lower:
            return 'integer[]'
        elif 'smallint' in col_type_lower:
            return 'smallint[]'
        elif 'numeric' in col_type_lower or 'decimal' in col_type_lower:
            return 'numeric[]'
        elif 'real' in col_type_lower:
            return 'real[]'
        elif 'double' in col_type_lower:
            return 'double precision[]'
        elif 'text' in col_type_lower:
            return 'text[]'
        elif 'varchar' in col_type_lower or 'character varying' in col_type_lower:
            return 'text[]'
        elif 'timestamp' in col_type_lower:
            return 'timestamp[]'
        elif 'date' in col_type_lower:
            return 'date[]'
        else:
            return 'text[]'  # Default fallback