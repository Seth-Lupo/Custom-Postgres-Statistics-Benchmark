"""
PostgreSQL Inserter Module

This module handles inserting pg_statistic data into PostgreSQL.

Input: pandas DataFrame with pg_statistic format
"""

import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlmodel import Session

class PostgresInserter:
    """Handles inserting statistics into PostgreSQL pg_statistic table."""
    
    def __init__(self, session: Session, logger: logging.Logger, advanced_logging: bool = False):
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
    
    def insert_statistics(self, pg_statistic_df: pd.DataFrame) -> Dict[str, int]:
        """
        Insert statistics into pg_statistic table.
        
        Args:
            pg_statistic_df: DataFrame with pg_statistic format
            
        Returns:
            Dictionary with counts of successful inserts/updates and failures
        """
        if pg_statistic_df.empty:
            self.logger.warning("Empty DataFrame provided for insertion")
            return {'updated': 0, 'inserted': 0, 'failed': 0}
        
        self.logger.info(f"Inserting {len(pg_statistic_df)} statistics into pg_statistic")
        
        counts = {
            'updated': 0,
            'inserted': 0,
            'failed': 0
        }
        
        # Process each statistic entry
        for idx, row in pg_statistic_df.iterrows():
            try:
                success = self._insert_or_update_statistic(row)
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
            self.logger.info(f"Statistics insertion complete: {counts['updated']} updated, "
                           f"{counts['inserted']} inserted, {counts['failed']} failed")
        except Exception as e:
            self.logger.error(f"Failed to commit statistics: {str(e)}")
            self.session.rollback()
            raise
        
        return counts
    
    def _insert_or_update_statistic(self, stat_row: pd.Series) -> str:
        """
        Insert or update a single statistic entry.
        
        Returns:
            'updated', 'inserted', or 'failed'
        """
        table_oid = stat_row['starelid']
        attnum = stat_row['staattnum']
        stat_column = stat_row['stat_column']
        stat_value = stat_row['stat_value']
        stat_type = stat_row.get('stat_type', 'unknown')
        
        if self.advanced_logging:
            self.logger.info(f"🔍 ADVANCED_LOG: Processing {stat_type} for "
                           f"{stat_row.get('table_name', 'unknown')}.{stat_row.get('column_name', 'unknown')}")
            self.logger.info(f"🔍 ADVANCED_LOG: OID={table_oid}, attnum={attnum}, "
                           f"column={stat_column}, value={stat_value}")
        
        # Try to update first
        if self._update_statistic(table_oid, attnum, stat_column, stat_value):
            return 'updated'
        
        # If no rows updated, insert new row
        if self._insert_statistic(table_oid, attnum, stat_column, stat_value):
            return 'inserted'
        
        return 'failed'
    
    def _update_statistic(self, table_oid: int, attnum: int, 
                         stat_column: int, stat_value: Any) -> bool:
        """Update existing statistic."""
        try:
            # Build column name from index
            column_map = {
                3: 'stanullfrac',
                5: 'stadistinct',
                16: 'stanumbers1'
            }
            
            if stat_column not in column_map:
                self.logger.warning(f"Unsupported column index: {stat_column}")
                return False
            
            column_name = column_map[stat_column]
            
            # Build update query
            update_query = f"""
            UPDATE pg_statistic 
            SET {column_name} = :value 
            WHERE starelid = :table_oid 
            AND staattnum = :attnum 
            AND stainherit = false
            """
            
            if self.advanced_logging:
                self.logger.info(f"🔍 ADVANCED_LOG: UPDATE query: {update_query}")
                self.logger.info(f"🔍 ADVANCED_LOG: Parameters: value={stat_value}, "
                               f"table_oid={table_oid}, attnum={attnum}")
            
            result = self.session.execute(
                text(update_query),
                {"value": stat_value, "table_oid": table_oid, "attnum": attnum}
            )
            
            if result.rowcount > 0:
                if self.advanced_logging:
                    self.logger.info(f"🔍 ADVANCED_LOG: ✅ Updated {result.rowcount} rows")
                return True
            else:
                if self.advanced_logging:
                    self.logger.info(f"🔍 ADVANCED_LOG: No rows updated")
                return False
                
        except Exception as e:
            if self.advanced_logging:
                self.logger.error(f"🔍 ADVANCED_LOG: ❌ Update failed: {str(e)}")
            self.logger.error(f"Failed to update statistic: {str(e)}")
            return False
    
    def _insert_statistic(self, table_oid: int, attnum: int,
                         stat_column: int, stat_value: Any) -> bool:
        """Insert new statistic row."""
        try:
            # Prepare default values
            values = {
                'table_oid': table_oid,
                'attnum': attnum,
                'stanullfrac': 0.0,
                'stawidth': 4,
                'stadistinct': 0.0,
                'stanumbers1': None
            }
            
            # Set the specific statistic value
            if stat_column == 3:
                values['stanullfrac'] = stat_value
            elif stat_column == 5:
                values['stadistinct'] = stat_value
            elif stat_column == 16:
                values['stanumbers1'] = stat_value
            
            # Insert query
            insert_query = """
            INSERT INTO pg_statistic (
                starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct,
                stakind1, stakind2, stakind3, stakind4, stakind5,
                staop1, staop2, staop3, staop4, staop5,
                stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
                stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
                stavalues1, stavalues2, stavalues3, stavalues4, stavalues5
            ) VALUES (
                :table_oid, :attnum, false, :stanullfrac, :stawidth, :stadistinct,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
                :stanumbers1, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL
            )
            """
            
            if self.advanced_logging:
                self.logger.info(f"🔍 ADVANCED_LOG: INSERT query executed")
                self.logger.info(f"🔍 ADVANCED_LOG: Parameters: {values}")
            
            self.session.execute(text(insert_query), values)
            
            if self.advanced_logging:
                self.logger.info(f"🔍 ADVANCED_LOG: ✅ Insert successful")
            
            return True
            
        except Exception as e:
            if self.advanced_logging:
                self.logger.error(f"🔍 ADVANCED_LOG: ❌ Insert failed: {str(e)}")
            self.logger.error(f"Failed to insert statistic: {str(e)}")
            return False
    
    def clear_statistics_for_tables(self, table_names: List[str]) -> int:
        """
        Clear existing statistics for specified tables.
        
        Args:
            table_names: List of table names to clear
            
        Returns:
            Number of rows deleted
        """
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
        """
        Verify that statistics were properly inserted.
        
        Returns:
            Dictionary with verification results
        """
        if pg_statistic_df.empty:
            return {'verified': 0, 'missing': 0, 'total': 0}
        
        verified = 0
        missing = 0
        
        for idx, row in pg_statistic_df.iterrows():
            table_oid = row['starelid']
            attnum = row['staattnum']
            stat_column = row['stat_column']
            expected_value = row['stat_value']
            
            # Query to check if statistic exists
            check_query = """
            SELECT stanullfrac, stadistinct, stanumbers1
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
                # Check specific column value
                if stat_column == 3:  # stanullfrac
                    actual_value = stat_row[0]
                elif stat_column == 5:  # stadistinct
                    actual_value = stat_row[1]
                elif stat_column == 16:  # stanumbers1
                    actual_value = stat_row[2]
                else:
                    actual_value = None
                
                if actual_value is not None:
                    verified += 1
                else:
                    missing += 1
            else:
                missing += 1
        
        return {
            'verified': verified,
            'missing': missing,
            'total': len(pg_statistic_df),
            'success_rate': verified / len(pg_statistic_df) if len(pg_statistic_df) > 0 else 0
        }