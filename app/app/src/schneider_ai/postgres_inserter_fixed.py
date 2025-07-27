"""
PostgreSQL Inserter Module - ENHANCED TYPE-SAFE VERSION

This module handles updating pg_statistic rows in PostgreSQL.

IMPORTANT LIMITATION: Due to PostgreSQL's anyarray pseudo-type, we cannot directly
update stavalues columns with typed arrays. This module updates:
- Basic statistics: null fraction, average width, distinct count
- Stakind values (statistics types)  
- Stanumbers arrays (frequency/correlation data)

The stavalues arrays (most common values, histogram bounds) must be populated
by PostgreSQL's ANALYZE command.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Union
from sqlalchemy import text
from sqlmodel import Session

class PostgresInserterFixed:
    """Handles inserting complete pg_statistic rows into PostgreSQL with proper type conversion."""
    
    def __init__(self, session: Session, logger: logging.Logger, advanced_logging: bool = True):
        """
        Initialize the inserter.
        
        Args:
            session: Database session
            logger: Logger instance
            advanced_logging: Enable detailed logging for debugging
        """
        self.session = session
        self.logger = logger
        self.advanced_logging = advanced_logging
        
        # Cache for column type information
        self.column_type_cache = {}
    
    def insert_statistics(self, pg_statistic_df: pd.DataFrame) -> Dict[str, int]:
        """
        Insert complete pg_statistic rows into PostgreSQL.
        
        Args:
            pg_statistic_df: DataFrame with complete pg_statistic rows
            
        Returns:
            Dictionary with counts of successful inserts/updates and failures
        """
        if pg_statistic_df.empty:
            self.logger.warning("Empty DataFrame provided for insertion")
            return {'updated': 0, 'inserted': 0, 'failed': 0}
        
        self.logger.info(f"🔧 ENHANCED: Updating {len(pg_statistic_df)} pg_statistic rows")
        self.logger.info("📌 NOTE: Updating basic statistics only (null fraction, width, distinct count, stakind, stanumbers)")
        self.logger.info("📌 stavalues arrays cannot be updated due to PostgreSQL anyarray limitations")
        
        counts = {
            'updated': 0,
            'inserted': 0,
            'failed': 0
        }
        
        # Process each complete row
        for idx, row in pg_statistic_df.iterrows():
            try:
                success = self._insert_or_update_complete_row(row)
                if success == 'updated':
                    counts['updated'] += 1
                elif success == 'inserted':
                    counts['inserted'] += 1
                else:
                    counts['failed'] += 1
            except Exception as e:
                self.logger.error(f"Failed to process row {idx}: {str(e)}")
                counts['failed'] += 1
        
        # Commit all changes
        try:
            self.session.commit()
            self.logger.info(f"✅ Statistics insertion complete: {counts['updated']} updated, "
                           f"{counts['inserted']} inserted, {counts['failed']} failed")
        except Exception as e:
            self.logger.error(f"Failed to commit statistics: {str(e)}")
            self.session.rollback()
            raise
        
        return counts
    
    def _get_column_type(self, table_oid: int, attnum: int) -> Optional[str]:
        """Get the actual data type of a column for proper type casting."""
        cache_key = f"{table_oid}_{attnum}"
        if cache_key in self.column_type_cache:
            return self.column_type_cache[cache_key]
        
        try:
            query = """
            SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
            FROM pg_attribute a
            WHERE a.attrelid = :table_oid
            AND a.attnum = :attnum
            AND NOT a.attisdropped
            """
            
            result = self.session.execute(text(query), {
                "table_oid": table_oid,
                "attnum": attnum
            })
            row = result.fetchone()
            
            if row:
                data_type = row[0]
                self.column_type_cache[cache_key] = data_type
                if self.advanced_logging:
                    self.logger.info(f"🔍 Column type for OID {table_oid}, attnum {attnum}: {data_type}")
                return data_type
            
        except Exception as e:
            self.logger.error(f"Failed to get column type: {str(e)}")
        
        return None
    
    def _insert_or_update_complete_row(self, stat_row: pd.Series) -> str:
        """
        Insert or update a complete pg_statistic row.
        
        Strategy: Since INSERT with anyarray is problematic, we ensure
        the row exists first by running ANALYZE on the table, then UPDATE.
        
        Returns:
            'updated', 'inserted', or 'failed'
        """
        table_oid = stat_row['starelid']
        attnum = stat_row['staattnum']
        table_name = stat_row.get('table_name', 'unknown')
        column_name = stat_row.get('column_name', 'unknown')
        
        if self.advanced_logging:
            self.logger.info(f"🔍 ENHANCED: Processing row for {table_name}.{column_name}")
            self.logger.info(f"🔍 OID={table_oid}, attnum={attnum}")
        
        # Get column type for proper array casting
        column_type = self._get_column_type(table_oid, attnum)
        if self.advanced_logging and column_type:
            self.logger.info(f"🔍 Column type: {column_type}")
        
        # Since ANALYZE has already been run globally, just check if row exists
        check_stats_query = """
        SELECT COUNT(*) 
        FROM pg_statistic 
        WHERE starelid = :table_oid 
        AND staattnum = :attnum 
        AND stainherit = false
        """
        result = self.session.execute(text(check_stats_query), {
            "table_oid": table_oid,
            "attnum": attnum
        })
        stats_count = result.scalar()
        
        if stats_count == 0:
            if self.advanced_logging:
                self.logger.warning(f"⚠️ No statistics row exists for {table_name}.{column_name} (OID={table_oid}, attnum={attnum})")
            return 'failed'
        
        # Now try to update the existing row
        if self._update_complete_row(stat_row, column_type):
            return 'updated'
        
        # If update still fails, it means the row doesn't exist or there's another issue
        # Skip INSERT since it will fail with anyarray type mismatch
        if self.advanced_logging:
            self.logger.warning(f"⚠️ Could not update statistics for {table_name}.{column_name}")
        
        return 'failed'
    
    def _update_complete_row(self, stat_row: pd.Series, column_type: Optional[str]) -> bool:
        """Update existing pg_statistic row with complete data using proper type casting."""
        try:
            table_oid = stat_row['starelid']
            attnum = stat_row['staattnum']
            
            # Build complete UPDATE query with all values embedded
            # This bypasses SQLAlchemy parameter binding for array types
            update_parts = []
            
            # Simple statistics - embed directly
            update_parts.append(f"stainherit = {self._format_simple_value(stat_row, 'stainherit')}")
            update_parts.append(f"stanullfrac = {self._format_simple_value(stat_row, 'stanullfrac')}")
            update_parts.append(f"stawidth = {self._format_simple_value(stat_row, 'stawidth')}")
            update_parts.append(f"stadistinct = {self._format_simple_value(stat_row, 'stadistinct')}")
            
            # Integer arrays - embed directly
            for prefix in ['stakind', 'staop', 'stacoll']:
                for i in range(1, 6):
                    field = f"{prefix}{i}"
                    update_parts.append(f"{field} = {int(stat_row.get(field, 0))}")
            
            # Handle array values with proper type casting
            for i in range(1, 6):
                # stanumbers (always float4[])
                stanumbers_field = f'stanumbers{i}'
                stanumbers_value = stat_row.get(stanumbers_field)
                if self._is_valid_array(stanumbers_value):
                    # For stanumbers, we can use direct array literal since it's always float4[]
                    array_literal = self._make_pg_array_literal(stanumbers_value, 'float')
                    update_parts.append(f"{stanumbers_field} = {array_literal}::float4[]")
                    # No need to add to params - it's embedded in the query
                else:
                    update_parts.append(f"{stanumbers_field} = NULL")
                
                # stavalues - Keep NULL for now, will try to update separately
                stavalues_field = f'stavalues{i}'
                update_parts.append(f"{stavalues_field} = NULL")
            
            # Build complete query with WHERE clause
            update_query = f"""
            UPDATE pg_statistic SET
                {', '.join(update_parts)}
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            """
            
            if self.advanced_logging:
                self.logger.info(f"🔍 Executing direct SQL UPDATE")
                self.logger.info(f"🔍 Update parts: {update_parts[:3]}...") # Show first few
            
            # Execute as raw SQL without parameters
            result = self.session.execute(text(update_query))
            
            if result.rowcount > 0:
                if self.advanced_logging:
                    self.logger.info(f"✅ Updated {result.rowcount} rows (basic stats)")
                
                # Now try to update stavalues fields separately
                anyarray_success = 0
                for i in range(1, 6):
                    stavalues_field = f'stavalues{i}'
                    stavalues_value = stat_row.get(stavalues_field)
                    if self._is_valid_array(stavalues_value) and column_type:
                        if self._try_update_anyarray_field(
                            table_oid, attnum, stavalues_field, stavalues_value, column_type, i
                        ):
                            anyarray_success += 1
                            if self.advanced_logging:
                                self.logger.info(f"✅ Updated {stavalues_field} anyarray")
                
                if anyarray_success > 0:
                    self.logger.info(f"🎉 Successfully updated {anyarray_success} anyarray fields!")
                
                return True
            else:
                if self.advanced_logging:
                    self.logger.info(f"🔍 No rows updated")
                return False
                
        except Exception as e:
            if self.advanced_logging:
                self.logger.error(f"❌ Update failed: {str(e)}")
            self.logger.error(f"Failed to update pg_statistic row: {str(e)}")
            self.session.rollback()
            return False
    
    def _insert_complete_row(self, stat_row: pd.Series, column_type: Optional[str]) -> bool:
        """Insert new complete pg_statistic row with proper type casting."""
        try:
            # Build complete INSERT query with all values embedded
            # This bypasses SQLAlchemy parameter binding for array types
            field_names = []
            values = []
            
            # Required fields - embed directly in SQL
            field_names.append('starelid')
            values.append(str(int(stat_row['starelid'])))
            
            field_names.append('staattnum')
            values.append(str(int(stat_row['staattnum'])))
            
            field_names.append('stainherit')
            values.append(self._format_simple_value(stat_row, 'stainherit'))
            
            field_names.append('stanullfrac')
            values.append(self._format_simple_value(stat_row, 'stanullfrac'))
            
            field_names.append('stawidth')
            values.append(self._format_simple_value(stat_row, 'stawidth'))
            
            field_names.append('stadistinct')
            values.append(self._format_simple_value(stat_row, 'stadistinct'))
            
            # Integer arrays - embed directly
            for prefix in ['stakind', 'staop', 'stacoll']:
                for i in range(1, 6):
                    field = f"{prefix}{i}"
                    field_names.append(field)
                    values.append(str(int(stat_row.get(field, 0))))
            
            # Array values with casting
            for i in range(1, 6):
                # stanumbers
                stanumbers_field = f'stanumbers{i}'
                field_names.append(stanumbers_field)
                stanumbers_value = stat_row.get(stanumbers_field)
                if self._is_valid_array(stanumbers_value):
                    array_literal = self._make_pg_array_literal(stanumbers_value, 'float')
                    values.append(f"{array_literal}::float4[]")
                else:
                    values.append("NULL")
                
                # stavalues - just use typed array without anyarray cast
                stavalues_field = f'stavalues{i}'
                field_names.append(stavalues_field)
                stavalues_value = stat_row.get(stavalues_field)
                if self._is_valid_array(stavalues_value):
                    cast_type = self._get_array_cast_type(column_type, i, stat_row.get(f'stakind{i}', 0))
                    array_literal = self._make_pg_array_literal(stavalues_value, 'any')
                    # Just cast to specific type - PostgreSQL will handle anyarray storage
                    values.append(f"{array_literal}::{cast_type}")
                    
                    if self.advanced_logging:
                        self.logger.info(f"🔍 INSERT {stavalues_field} using direct cast to {cast_type}")
                else:
                    values.append("NULL")
            
            # Build complete query with all values embedded
            insert_query = f"""
            INSERT INTO pg_statistic ({', '.join(field_names)})
            VALUES ({', '.join(values)})
            """
            
            if self.advanced_logging:
                self.logger.info(f"🔍 Executing direct SQL INSERT")
                # Log stakind values
                stakind_values = []
                for i in range(1, 6):
                    val = int(stat_row.get(f'stakind{i}', 0))
                    if val > 0:
                        stakind_values.append(f'stakind{i}={val}')
                if stakind_values:
                    self.logger.info(f"🔍 Active stakind values: {', '.join(stakind_values)}")
            
            # Execute as raw SQL without parameters
            self.session.execute(text(insert_query))
            
            if self.advanced_logging:
                self.logger.info(f"✅ Insert successful")
            
            return True
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.error(f"❌ Insert failed: {str(e)}")
                self.logger.error(f"🔍 Query: {insert_query[:500]}...")
                # Log the specific field that might be causing issues
                if "anyarray" in str(e):
                    self.logger.error(f"🔍 Type mismatch detected. Column type: {column_type}")
            self.logger.error(f"Failed to insert pg_statistic row: {str(e)}")
            self.session.rollback()
            return False
    
    def _format_simple_value(self, stat_row: pd.Series, field: str) -> str:
        """Format simple value for direct SQL embedding."""
        value = self._prepare_simple_value(stat_row, field)
        if value is None:
            return 'NULL'
        elif isinstance(value, bool):
            return 'true' if value else 'false'
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # String values need quotes and escaping
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
    
    def _prepare_simple_value(self, stat_row: pd.Series, field: str) -> Any:
        """Prepare simple (non-array) values for SQL."""
        if field == 'starelid' or field == 'staattnum':
            return int(stat_row[field])
        elif field == 'stainherit':
            return bool(stat_row.get(field, False))
        elif field == 'stanullfrac':
            return float(stat_row.get(field, 0.0))
        elif field == 'stawidth':
            return int(stat_row.get(field, 4))
        elif field == 'stadistinct':
            return float(stat_row.get(field, 0.0))
        else:
            return stat_row.get(field)
    
    def _is_valid_array(self, value) -> bool:
        """Check if a value is a valid non-empty array."""
        if value is None:
            return False
        # Check for string "NULL" which AI sometimes returns
        if isinstance(value, str) and value.upper() == 'NULL':
            return False
        if isinstance(value, (list, tuple, np.ndarray)):
            # Check if it's a single-element array containing "NULL"
            if len(value) == 1 and isinstance(value[0], str) and value[0].upper() == 'NULL':
                return False
            return len(value) > 0
        return False
    
    def _prepare_float_array(self, values) -> List[float]:
        """Convert values to float array for stanumbers columns."""
        if not self._is_valid_array(values):
            return None
        
        result = []
        for v in values:
            try:
                result.append(float(v))
            except (ValueError, TypeError):
                result.append(0.0)
        
        return result
    
    def _prepare_float_array_string(self, values) -> str:
        """Convert values to comma-separated string for ARRAY constructor."""
        if not self._is_valid_array(values):
            return None
        
        float_values = []
        for v in values:
            try:
                float_values.append(str(float(v)))
            except (ValueError, TypeError):
                float_values.append('0.0')
        
        return ','.join(float_values)
    
    def _prepare_stavalues_array(self, values, column_type: Optional[str]) -> List[Any]:
        """
        Prepare stavalues array with proper type conversion.
        SQLAlchemy needs properly typed values to avoid text[] inference.
        """
        if not self._is_valid_array(values):
            return None
        
        # Determine column base type
        base_type = column_type.lower() if column_type else ''
        if '(' in base_type:
            base_type = base_type.split('(')[0]
        
        result = []
        for v in values:
            if v is None or (isinstance(v, str) and v.upper() == 'NULL'):
                result.append(None)
            elif base_type in ['integer', 'bigint', 'smallint']:
                # Convert to integer
                try:
                    result.append(int(float(str(v))))
                except (ValueError, TypeError):
                    result.append(str(v))
            elif base_type in ['numeric', 'decimal', 'real', 'double precision', 'float']:
                # Convert to float
                try:
                    result.append(float(str(v)))
                except (ValueError, TypeError):
                    result.append(str(v))
            elif base_type in ['boolean']:
                # Convert to boolean
                try:
                    val_str = str(v).lower()
                    if val_str in ['true', 't', 'yes', 'y', '1']:
                        result.append(True)
                    elif val_str in ['false', 'f', 'no', 'n', '0']:
                        result.append(False)
                    else:
                        result.append(str(v))
                except:
                    result.append(str(v))
            else:
                # Keep as string for text types and others
                result.append(str(v))
        
        return result
    
    def _prepare_stavalues_array_string(self, values, column_type: Optional[str]) -> str:
        """
        Prepare stavalues as comma-separated string for string_to_array function.
        This is used for numeric types where we can use string_to_array.
        """
        if not self._is_valid_array(values):
            return None
        
        # Determine if we need integer conversion
        base_type = column_type.lower() if column_type else ''
        if '(' in base_type:
            base_type = base_type.split('(')[0]
        
        is_integer_type = base_type in ['integer', 'bigint', 'smallint']
        
        # For numeric types, format appropriately
        result_parts = []
        for v in values:
            if v is None:
                result_parts.append('')  # Empty string for NULL in string_to_array
            else:
                if is_integer_type:
                    # Convert float strings to integers
                    try:
                        result_parts.append(str(int(float(v))))
                    except (ValueError, TypeError):
                        result_parts.append(str(v))
                else:
                    result_parts.append(str(v))
        
        return ','.join(result_parts)
    
    def _needs_quotes_for_type(self, column_type: Optional[str]) -> bool:
        """Determine if values need quotes based on column type."""
        if not column_type:
            return True  # Default to quoted for safety
        
        base_type = column_type.lower()
        if '(' in base_type:
            base_type = base_type.split('(')[0]
        
        # Numeric types don't need quotes
        numeric_types = [
            'integer', 'bigint', 'smallint', 'numeric', 'decimal',
            'real', 'double precision', 'float', 'boolean'
        ]
        
        return base_type not in numeric_types
    
    def _prepare_array_literal(self, values, column_type: Optional[str]) -> str:
        """
        Prepare a PostgreSQL array literal like '{val1,val2,val3}'.
        """
        if not self._is_valid_array(values):
            return None
        
        # Build array literal
        parts = []
        for v in values:
            if v is None:
                parts.append('NULL')
            else:
                # Escape special characters
                escaped = str(v).replace('\\', '\\\\').replace('"', '\\"')
                # For text values, wrap in quotes
                if self._needs_quotes_for_type(column_type):
                    escaped = escaped.replace("'", "''")
                    parts.append(f'"{escaped}"')
                else:
                    parts.append(escaped)
        
        return '{' + ','.join(parts) + '}'
    
    def _get_array_cast_type(self, column_type: Optional[str], slot_num: int, stakind: int) -> str:
        """
        Determine the proper cast type for stavalues based on column type and stakind.
        
        Returns the PostgreSQL array type to cast to (e.g., 'integer[]', 'text[]', etc.)
        """
        if not column_type:
            # Default fallback - let PostgreSQL figure it out
            return "anyarray"
        
        # Extract base type from column type
        base_type = column_type.lower()
        
        # Remove any precision/scale info
        if '(' in base_type:
            base_type = base_type.split('(')[0]
        
        # Map to array types
        type_mapping = {
            'integer': 'integer[]',
            'bigint': 'bigint[]',
            'smallint': 'smallint[]',
            'numeric': 'numeric[]',
            'decimal': 'decimal[]',
            'real': 'real[]',
            'double precision': 'double precision[]',
            'float': 'float[]',
            'text': 'text[]',
            'varchar': 'text[]',
            'character varying': 'text[]',
            'char': 'text[]',
            'character': 'text[]',
            'timestamp': 'timestamp[]',
            'timestamp with time zone': 'timestamp with time zone[]',
            'timestamp without time zone': 'timestamp without time zone[]',
            'date': 'date[]',
            'time': 'time[]',
            'boolean': 'boolean[]',
            'uuid': 'uuid[]',
            'json': 'json[]',
            'jsonb': 'jsonb[]'
        }
        
        # Try direct mapping first
        if base_type in type_mapping:
            array_type = type_mapping[base_type]
            if self.advanced_logging:
                self.logger.info(f"🔍 Mapped {base_type} to {array_type}")
            return array_type
        
        # For unknown types, try adding [] to the base type
        array_type = f"{base_type}[]"
        if self.advanced_logging:
            self.logger.info(f"🔍 Using default array type: {array_type}")
        
        return array_type
    
    def clear_statistics_for_tables(self, table_names: List[str]) -> int:
        """Clear existing statistics for specified tables."""
        total_deleted = 0
        
        for table_name in table_names:
            try:
                # Get table OID
                oid_query = """
                SELECT c.oid 
                FROM pg_class c 
                JOIN pg_namespace n ON c.relnamespace = n.oid 
                WHERE c.relname = :table_name AND n.nspname = 'public'
                """
                
                result = self.session.execute(text(oid_query), {"table_name": table_name})
                row = result.fetchone()
                
                if not row:
                    self.logger.warning(f"Table {table_name} not found")
                    continue
                
                table_oid = row[0]
                
                # Delete statistics
                delete_query = """
                DELETE FROM pg_statistic 
                WHERE starelid = :table_oid AND stainherit = false
                """
                
                result = self.session.execute(text(delete_query), {"table_oid": table_oid})
                deleted = result.rowcount
                total_deleted += deleted
                
                self.logger.debug(f"Deleted {deleted} statistics for table {table_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to clear statistics for {table_name}: {str(e)}")
        
        if total_deleted > 0:
            self.logger.info(f"Cleared {total_deleted} statistics entries")
        
        return total_deleted
    
    def verify_statistics(self, pg_statistic_df: pd.DataFrame) -> Dict[str, Any]:
        """Verify that statistics were properly inserted."""
        if pg_statistic_df.empty:
            return {'verified': 0, 'missing': 0, 'total': 0}
        
        verified = 0
        missing = 0
        
        for idx, row in pg_statistic_df.iterrows():
            table_oid = row['starelid']
            attnum = row['staattnum']
            
            # Query to check if statistic exists
            check_query = """
            SELECT stakind1, stakind2, stakind3, stakind4, stakind5,
                   stanullfrac, stadistinct
            FROM pg_statistic
            WHERE starelid = :table_oid
            AND staattnum = :attnum
            AND stainherit = false
            """
            
            result = self.session.execute(
                text(check_query),
                {"table_oid": table_oid, "attnum": attnum}
            )
            stat_row = result.fetchone()
            
            if stat_row:
                verified += 1
                if self.advanced_logging:
                    self.logger.info(f"✅ Verified statistics for OID {table_oid}, attnum {attnum}")
            else:
                missing += 1
                if self.advanced_logging:
                    self.logger.warning(f"❌ Missing statistics for OID {table_oid}, attnum {attnum}")
        
        success_rate = verified / len(pg_statistic_df) if len(pg_statistic_df) > 0 else 0
        
        self.logger.info(f"📊 Verification complete: {verified}/{len(pg_statistic_df)} statistics confirmed "
                        f"(success rate: {success_rate:.1%})")
        
        return {
            'verified': verified,
            'missing': missing,
            'total': len(pg_statistic_df),
            'success_rate': success_rate
        }
    
    def _execute_raw_insert(self, query: str, params: Dict[str, Any]):
        """Execute raw INSERT with proper array handling."""
        # Build the full query with array literals embedded
        final_query = self._build_query_with_arrays(query, params)
        
        if self.advanced_logging:
            self.logger.info(f"🔍 Executing raw INSERT query")
            # Log first 500 chars of query for debugging
            self.logger.info(f"🔍 Query preview: {final_query[:500]}...")
        
        # Execute using session's execute to maintain transaction consistency
        self.session.execute(text(final_query))
    
    def _execute_raw_update(self, query: str, params: Dict[str, Any]):
        """Execute raw UPDATE with proper array handling."""
        # Build the full query with array literals embedded
        final_query = self._build_query_with_arrays(query, params)
        
        if self.advanced_logging:
            self.logger.info(f"🔍 Executing raw UPDATE query")
            # Log first 500 chars of query for debugging
            self.logger.info(f"🔍 Query preview: {final_query[:500]}...")
        
        # Execute using session's execute to maintain transaction consistency
        return self.session.execute(text(final_query))
    
    def _make_pg_array_literal(self, values: List[Any], array_type: str, column_type: str = None) -> str:
        """Create a PostgreSQL array literal string."""
        if not values:
            return 'NULL'
        
        parts = []
        for v in values:
            if v is None:
                parts.append('NULL')
            elif array_type == 'float':
                parts.append(str(float(v)))
            elif array_type == 'any':
                # For anyarray, check if numeric or text based on column type
                if column_type and ('int' in column_type.lower()):
                    # Integer type - convert float strings to int
                    try:
                        parts.append(str(int(float(v))))
                    except:
                        escaped = str(v).replace("'", "''")
                        parts.append(f"'{escaped}'")
                else:
                    try:
                        # Try to convert to number
                        float(v)
                        parts.append(str(v))
                    except:
                        # Text value - escape quotes
                        escaped = str(v).replace("'", "''")
                        parts.append(f"'{escaped}'")
            else:
                # Default text handling
                escaped = str(v).replace("'", "''")
                parts.append(f"'{escaped}'")
        
        return "ARRAY[" + ",".join(parts) + "]"
    
    def _build_query_with_arrays(self, query: str, params: Dict[str, Any]) -> str:
        """Build complete query with all parameters replaced, including arrays."""
        final_query = query
        
        # First, handle array parameters
        for key, value in params.items():
            param_pattern = f':{key}'
            
            if key.startswith('stanumbers') or key.startswith('stavalues'):
                if value is None:
                    final_query = final_query.replace(param_pattern, 'NULL')
                elif isinstance(value, list):
                    if key.startswith('stanumbers'):
                        array_literal = self._make_pg_array_literal(value, 'float')
                    else:
                        array_literal = self._make_pg_array_literal(value, 'any')
                    final_query = final_query.replace(param_pattern, array_literal)
            else:
                # Handle non-array parameters
                if value is None:
                    final_query = final_query.replace(param_pattern, 'NULL')
                elif isinstance(value, bool):
                    final_query = final_query.replace(param_pattern, 'true' if value else 'false')
                elif isinstance(value, (int, float)):
                    final_query = final_query.replace(param_pattern, str(value))
                else:
                    # String values need quotes
                    escaped = str(value).replace("'", "''")
                    final_query = final_query.replace(param_pattern, f"'{escaped}'")
        
        return final_query
    
    def create_empty_statistics_for_table(self, table_name: str) -> int:
        """Create empty pg_statistic rows for all columns in a table without analyzing real data."""
        try:
            # Get table OID and column information
            table_info_query = """
            SELECT c.oid as table_oid, a.attnum, a.attname, a.atttypid
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE c.relname = :table_name 
            AND n.nspname = 'public'
            AND a.attnum > 0
            AND NOT a.attisdropped
            ORDER BY a.attnum
            """
            
            result = self.session.execute(text(table_info_query), {"table_name": table_name})
            columns = result.fetchall()
            
            if not columns:
                self.logger.warning(f"No columns found for table {table_name}")
                return 0
            
            rows_created = 0
            # Insert minimal empty statistics for each column
            for col in columns:
                try:
                    table_oid, attnum, attname, atttypid = col
                    
                    # Check if statistics row already exists
                    check_query = """
                    SELECT COUNT(*) FROM pg_statistic 
                    WHERE starelid = :table_oid AND staattnum = :attnum AND stainherit = false
                    """
                    result = self.session.execute(text(check_query), {
                        "table_oid": table_oid,
                        "attnum": attnum
                    })
                    exists = result.scalar() > 0
                    
                    if exists:
                        if self.advanced_logging:
                            self.logger.debug(f"Statistics row already exists for {table_name}.{attname}")
                        continue
                    
                    # Create minimal empty statistics row (no ON CONFLICT for system catalogs)
                    insert_query = """
                    INSERT INTO pg_statistic (
                        starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct,
                        stakind1, stakind2, stakind3, stakind4, stakind5,
                        staop1, staop2, staop3, staop4, staop5,
                        stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
                        stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
                        stavalues1, stavalues2, stavalues3, stavalues4, stavalues5
                    ) VALUES (
                        :table_oid, :attnum, false, 0.0, 4, 0.0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0,
                        NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL
                    )
                    """
                    
                    self.session.execute(text(insert_query), {
                        "table_oid": table_oid,
                        "attnum": attnum
                    })
                    rows_created += 1
                    
                    if self.advanced_logging:
                        self.logger.debug(f"Created empty statistics for {table_name}.{attname}")
                        
                except Exception as col_error:
                    self.logger.warning(f"Failed to create empty statistics for {table_name}.{attname}: {str(col_error)}")
                    # Continue with other columns instead of failing completely
                    continue
            
            if self.advanced_logging:
                self.logger.info(f"✅ Created {rows_created} empty statistics rows for {table_name}")
            
            return rows_created
                    
        except Exception as e:
            self.logger.error(f"Failed to create empty statistics for {table_name}: {str(e)}")
            # Don't raise - return 0 so the process can continue
            return 0
    
    def disable_autovacuum_for_tables(self, table_names: List[str]) -> None:
        """Disable autovacuum for specific tables to prevent automatic ANALYZE."""
        if not table_names:
            return
        
        successful_disables = 0
        for table_name in table_names:
            try:
                # Disable autovacuum for this table (autovacuum_analyze_enabled doesn't exist)
                disable_query = f"""
                ALTER TABLE "{table_name}" SET (
                    autovacuum_enabled = false
                )
                """
                self.session.execute(text(disable_query))
                successful_disables += 1
                
                if self.advanced_logging:
                    self.logger.info(f"🚫 Disabled autovacuum for {table_name}")
                    
            except Exception as e:
                self.logger.warning(f"Failed to disable autovacuum for {table_name}: {str(e)}")
                # Don't let one failure stop the others - commit what we have so far
                try:
                    self.session.commit()
                except:
                    self.session.rollback()
        
        try:
            self.session.commit()
            self.logger.info(f"🚫 Autovacuum disabled for {successful_disables}/{len(table_names)} tables")
        except Exception as e:
            self.logger.error(f"Failed to commit autovacuum changes: {str(e)}")
            self.session.rollback()
    
    def re_enable_autovacuum_for_tables(self, table_names: List[str]) -> None:
        """Re-enable autovacuum for specific tables."""
        if not table_names:
            return
            
        for table_name in table_names:
            try:
                # Re-enable autovacuum
                enable_query = f"""
                ALTER TABLE "{table_name}" RESET (
                    autovacuum_enabled
                )
                """
                self.session.execute(text(enable_query))
                
                if self.advanced_logging:
                    self.logger.info(f"✅ Re-enabled autovacuum for {table_name}")
                    
            except Exception as e:
                self.logger.warning(f"Failed to re-enable autovacuum for {table_name}: {str(e)}")
        
        self.session.commit()
        self.logger.info(f"✅ Autovacuum re-enabled for {len(table_names)} tables")
    
    def _try_update_anyarray_field(self, table_oid: int, attnum: int, field_name: str, 
                                   values: List[Any], column_type: str, slot_num: int) -> bool:
        """Try to update a single anyarray field using various techniques."""
        if self.advanced_logging:
            self.logger.info(f"🎯 Attempting anyarray update for {field_name} with values: {values[:3]}...")
        
        try:
            # Get type OID information
            type_query = """
            SELECT t.oid, t.typelem, t.typname
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
                if self.advanced_logging:
                    self.logger.warning(f"⚠️ No type info found for OID={table_oid}, attnum={attnum}")
                return False
            
            type_oid, elem_oid, type_name = type_info
            
            # If elem_oid is None or 0, try to get array element type differently
            if not elem_oid or elem_oid == 0:
                # For base types, we need to find the array type's element
                array_type_query = """
                SELECT t.oid, t.typelem 
                FROM pg_type t 
                WHERE t.typname = :array_type_name
                """
                array_type_name = type_name + '[]' if not type_name.endswith('[]') else type_name
                result = self.session.execute(text(array_type_query), {"array_type_name": array_type_name})
                array_info = result.fetchone()
                if array_info and array_info[1]:
                    elem_oid = array_info[1]
                else:
                    # Fallback: assume it's the base type OID
                    elem_oid = type_oid
            
            # Method 1: Try array_in with element type OID
            if self.advanced_logging:
                self.logger.info(f"🔧 Method 1: Trying array_in with elem_oid={elem_oid}")
            
            array_text = self._to_pg_array_text(values, elem_oid)
            
            update_query = f"""
            UPDATE pg_statistic 
            SET {field_name} = array_in('{array_text}', {elem_oid}, -1)
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            """
            
            try:
                result = self.session.execute(text(update_query))
                if result.rowcount > 0:
                    if self.advanced_logging:
                        self.logger.info(f"✅ Method 1 SUCCESS: array_in worked!")
                    return True
            except Exception as e:
                if self.advanced_logging:
                    self.logger.warning(f"❌ Method 1 failed: {str(e)}")
            
            # Method 2: Try with explicit cast through text
            if self.advanced_logging:
                self.logger.info(f"🔧 Method 2: Trying explicit cast to array type")
            
            array_type = self._get_array_cast_type(column_type, slot_num, 0)
            array_literal = self._make_pg_array_literal(values, 'any', column_type)
            
            update_query = f"""
            UPDATE pg_statistic 
            SET {field_name} = {array_literal}::{array_type}
            WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
            """
            
            try:
                result = self.session.execute(text(update_query))
                if result.rowcount > 0:
                    if self.advanced_logging:
                        self.logger.info(f"✅ Method 2 SUCCESS: Cast to {array_type} worked!")
                    return True
            except Exception as e:
                if self.advanced_logging:
                    self.logger.warning(f"❌ Method 2 failed: {str(e)}")
            
            # Method 3: Try string_to_array for simple types
            if 'int' in column_type.lower() or 'numeric' in column_type.lower():
                if self.advanced_logging:
                    self.logger.info(f"🔧 Method 3: Trying string_to_array for numeric type")
                
                values_str = ','.join(str(v) for v in values if v is not None)
                
                update_query = f"""
                UPDATE pg_statistic 
                SET {field_name} = string_to_array('{values_str}', ',')::{array_type}
                WHERE starelid = {table_oid} AND staattnum = {attnum} AND stainherit = false
                """
                
                try:
                    result = self.session.execute(text(update_query))
                    if result.rowcount > 0:
                        if self.advanced_logging:
                            self.logger.info(f"✅ Method 3 SUCCESS: string_to_array worked!")
                        return True
                except Exception as e:
                    if self.advanced_logging:
                        self.logger.warning(f"❌ Method 3 failed: {str(e)}")
            
            return False
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.error(f"Failed to update anyarray field {field_name}: {str(e)}")
            # Try to recover from transaction error
            try:
                self.session.rollback()
                self.session.begin()
            except:
                pass
            return False
    
    def _to_pg_array_text(self, values: List[Any], elem_type_oid: int = None) -> str:
        """Convert values to PostgreSQL array text format for array_in."""
        parts = []
        for v in values:
            if v is None:
                parts.append('NULL')
            else:
                # Convert based on element type OID
                if elem_type_oid in [20, 21, 23]:  # bigint, int2, int4
                    # Convert float strings to integers
                    try:
                        int_val = int(float(str(v)))
                        parts.append(str(int_val))
                    except:
                        parts.append(f'"{str(v)}"')
                else:
                    # Escape backslashes and quotes for text types
                    s = str(v).replace('\\', '\\\\').replace('"', '\\"')
                    parts.append(f'"{s}"')
        return '{' + ','.join(parts) + '}'