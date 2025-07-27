import json
import time
import logging
from typing import Dict, List, Tuple, Any, Optional
from sqlalchemy import text
from sqlmodel import Session
import pandas as pd
from pathlib import Path

from ..base import StatsSource, StatsSourceConfig, StatsSourceSettings
from .ai_response_handler import AIResponseHandler
from .pg_stats_processor import PGStatsProcessor
from .stats_translator import StatsTranslator
from .postgres_inserter_fixed import PostgresInserterFixed

# Advanced logging flag for debugging
ADVANCED_LOGGING = True


class SchneiderAIStatsSource(StatsSource):
    """Statistics source that uses AI to estimate PostgreSQL statistics via modular pipeline."""
    
    def __init__(self, settings: StatsSourceSettings = None, config: StatsSourceConfig = None):
        super().__init__(settings=settings, config=config)
        self.logger.debug(f"Initialized {self.name()} with settings: {self.settings.name}, config: {self.config.name}")
        
        # Extract configuration for modules
        self.module_config = {
            'provider': self.config.get_data('provider', 'llmproxy'),
            'model': self.config.get_data('model', 'us.anthropic.claude-3-haiku-20240307-v1:0'),
            'temperature': self.config.get_data('temperature', 0.3),
            'session_id': self.config.get_data('session_id', 'schneider_stats_session'),
            'rag_usage': self.config.get_data('rag_usage', False),
            'rag_threshold': self.config.get_data('rag_threshold', 0.5),
            'rag_k': self.config.get_data('rag_k', 0),
            'max_retries': self.config.get_data('max_retries', 3),
            'system_prompt': self.config.get_data('system_prompt', 
                'You make predictions about pg_stats tables for postgres databases. '
                'You will always make a guess and never guess randomly. '
                'You will always output a semicolon, never comma, separated csv with no other information but the csv. '
                'Please do not guess NULL for the list columns unless very necessary, '
                'please always generate a pg_stats table and never the raw data.'),
            'estimation_prompt': self.config.get_data('estimation_prompt',
                'I have a postgres sql database that I want you to estimate the pg_stats for. '
                'PLEASE MAKE SURE THAT THE CSVS ARE SEMICOLON SEPARATED AND NOT COMMA SEPARATED. '
                'The column names and descriptions for pg_stats are: '
                'attname name (references pg_attribute.attname): Name of column described by this row, '
                'null_frac float4: Fraction of column entries that are null '
                'avg_width int4 Average width in bytes of columns entries '
                'n_distinct float4 If greater than zero, the estimated number of distinct values in the column. '
                'If less than zero, the negative of the number of distinct values divided by the number of rows. '
                '(The negated form is used when ANALYZE believes that the number of distinct values is likely to increase as the table grows; '
                'the positive form is used when the column seems to have a fixed number of possible values.) '
                'For example, -1 indicates a unique column in which the number of distinct values is the same as the number of rows. '
                'most_common_vals anyarray A list of the most common values in the column. '
                '(Null if no values seem to be more common than any others.) '
                'most_common_freqs float4[] A list of the frequencies of the most common values, '
                'i.e., number of occurrences of each divided by total number of rows. (Null when most_common_vals is.) '
                'histogram_bounds anyarray A list of values that divide the columns values into groups of approximately equal population. '
                'The values in most_common_vals, if present, are omitted from this histogram calculation. '
                '(This column is null if the column data type does not have a < operator or if the most_common_vals list accounts for the entire population.) '
                'correlation float4 Statistical correlation between physical row ordering and logical ordering of the column values. '
                'This ranges from -1 to +1. When the value is near -1 or +1, an index scan on the column will be estimated to be cheaper than when it is near zero, '
                'due to reduction of random access to the disk. (This column is null if the column data type does not have a < operator.) '
                'The column names in the database are {col_names}. The total size of the database is {size}. '
                'Please do not use elipses in your histogram predictions and make guesses whenever possible based on patterns in this style of database, '
                'do not guess randomly. This dataset {sample_data}. Record your answer in csv format. '
                'DO NOT COPY THIS AND ALWAYS GENERATE PG_STATS.')
        }
        
        # Note: Column mappings are now handled automatically by the stakind system
        # in the fixed StatsTranslator
        
        # Initialize modules (will be created per execution with proper session)
        self.ai_handler = None
        self.stats_processor = None
        self.translator = None
        self.inserter = None
    
    def apply_statistics(self, session: Session) -> None:
        """Apply AI-generated statistics to the database using modular pipeline."""
        start_time = time.time()
        try:
            self.logger.info(f"Starting AI statistics application for {self.name()}")
            
            # Initialize modules with session
            self._initialize_modules(session)
            
            # Clear caches
            self.clear_caches(session)
            
            # Step 1: Get database schema information
            schema_info = self.get_database_schema_info(session)
            if not schema_info:
                self.logger.warning("No schema information available, falling back to standard ANALYZE")
                super().apply_statistics(session)
                return
            
            # Step 2: Get AI estimates (returns pg_stats DataFrame)
            self.logger.info("Step 1/4: Getting AI estimates")
            pg_stats_df = self.ai_handler.get_ai_estimates(schema_info)
            
            if pg_stats_df.empty:
                self.logger.warning("No AI estimates received, falling back to standard ANALYZE")
                super().apply_statistics(session)
                return
            
            # Save AI interaction
            if hasattr(self, 'experiment_id') and self.experiment_id:
                self._save_ai_interaction(pg_stats_df)
            
            # Step 3: Process and validate pg_stats data
            self.logger.info("Step 2/4: Processing pg_stats data")
            processed_df = self.stats_processor.process_pg_stats(pg_stats_df)
            
            if processed_df.empty:
                self.logger.warning("No valid statistics after processing, falling back to standard ANALYZE")
                super().apply_statistics(session)
                return
            
            # Log processing summary
            summary = self.stats_processor.get_statistics_summary(processed_df)
            self.logger.info(f"Processed statistics: {summary['total_rows']} rows for "
                           f"{len(summary['tables'])} tables")
            
            # Step 4: Translate to pg_statistic format
            self.logger.info("Step 3/4: Translating to pg_statistic format")
            pg_statistic_df = self.translator.translate_to_pg_statistic(processed_df)
            
            if pg_statistic_df.empty:
                self.logger.warning("No statistics translated, falling back to standard ANALYZE")
                super().apply_statistics(session)
                return
            
            # Step 5: Disable autovacuum to prevent real data contamination
            self.logger.info("Step 4/7: Disabling autovacuum to prevent real data contamination")
            unique_tables = list(pg_statistic_df['table_name'].unique())
            try:
                self.inserter.disable_autovacuum_for_tables(unique_tables)
            except Exception as e:
                self.logger.warning(f"Failed to disable autovacuum: {str(e)}")
                # Continue anyway - not critical for the core functionality
            
            # Step 6: Clean existing statistics for target tables
            self.logger.info("Step 5/7: Cleaning existing pg_statistic data for target tables")
            cleaned_count = self.inserter.clear_statistics_for_tables(unique_tables)
            if cleaned_count > 0:
                self.logger.info(f"Cleaned {cleaned_count} existing statistics entries")
            session.commit()
            
            # Step 7: Create empty statistics rows without analyzing real data
            self.logger.info("Step 6/7: Creating empty pg_statistic rows (completely bypassing real data)")
            self.logger.info(f"Creating empty statistics for {len(unique_tables)} tables")
            total_rows_created = 0
            for table_name in unique_tables:
                rows_created = self.inserter.create_empty_statistics_for_table(table_name)
                if rows_created > 0:
                    self.logger.info(f"Created {rows_created} empty statistics rows for {table_name}")
                    total_rows_created += rows_created
                else:
                    self.logger.warning(f"No empty statistics rows created for {table_name}")
            
            try:
                session.commit()
                self.logger.info(f"✅ Successfully created {total_rows_created} empty statistics rows total")
            except Exception as e:
                self.logger.error(f"Failed to commit empty statistics creation: {str(e)}")
                session.rollback()
            
            # Step 8: Insert/Update AI statistics into PostgreSQL
            self.logger.info("Step 7/7: Applying AI statistics to PostgreSQL")
            insert_counts = self.inserter.insert_statistics(pg_statistic_df)
            
            total_success = insert_counts['updated'] + insert_counts['inserted']
            if total_success > 0:
                self.logger.info(f"Successfully applied {total_success} AI statistics")
                
                # Verify insertion if in debug mode
                if ADVANCED_LOGGING:
                    verification = self.inserter.verify_statistics(pg_statistic_df)
                    self.logger.info(f"Verification: {verification['verified']}/{verification['total']} "
                                   f"statistics confirmed (success rate: {verification['success_rate']:.2%})")
            else:
                self.logger.warning("Failed to apply AI statistics, falling back to standard ANALYZE")
                super().apply_statistics(session)
            
            # Re-enable autovacuum for the tables (optional - could leave disabled for experiments)
            self.logger.info("Re-enabling autovacuum for target tables")
            self.inserter.re_enable_autovacuum_for_tables(unique_tables)
            
            total_time = time.time() - start_time
            self.logger.info(f"🎉 AI statistics pipeline completed in {total_time:.2f} seconds")
            self.logger.info("💡 All statistics are now AI-generated with NO real data contamination!")
            
        except Exception as e:
            self.logger.error(f"Failed to apply AI statistics: {str(e)}")
            self.logger.debug("Exception details:", exc_info=True)
            session.rollback()
            # Fall back to standard statistics
            self.logger.info("Falling back to standard PostgreSQL statistics")
            super().apply_statistics(session)
    
    def _initialize_modules(self, session: Session):
        """Initialize pipeline modules with current session."""
        # AI Response Handler
        self.ai_handler = AIResponseHandler(self.module_config, self.logger)
        
        # Get schema info for processor initialization
        schema_info = self.get_database_schema_info(session)
        
        # PG Stats Processor
        self.stats_processor = PGStatsProcessor(schema_info, self.logger)
        
        # Stats Translator (fixed version that handles stakind system properly)
        self.translator = StatsTranslator(session, self.logger)
        
        # PostgreSQL Inserter - Using enhanced type-safe version
        self.inserter = PostgresInserterFixed(session, self.logger, ADVANCED_LOGGING)
    
    def get_database_schema_info(self, session: Session) -> Dict[str, Any]:
        """Get comprehensive database schema information for AI estimation."""
        try:
            self.logger.debug("Starting database schema analysis...")
            
            # Get table and column information
            schema_query = '''
            SELECT 
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.ordinal_position
            FROM information_schema.columns c
            JOIN information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
            WHERE c.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name, c.ordinal_position
            '''
            
            result = session.execute(text(schema_query))
            schema_rows = result.fetchall()
            
            if not schema_rows:
                self.logger.warning("No tables found in public schema")
                return {}
            
            # Get table statistics
            table_stats_query = '''
            SELECT 
                c.relname as tablename,
                COALESCE(c.reltuples::bigint, 0) as n_live_tup,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                pg_total_relation_size(c.oid) as total_size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' 
            AND c.relkind = 'r'
            '''
            
            stats_result = session.execute(text(table_stats_query))
            table_stats = {row[0]: {
                'row_count': row[1],
                'size_pretty': row[2],
                'size_bytes': row[3]
            } for row in stats_result.fetchall()}
            
            # Get database size
            db_size_query = "SELECT pg_size_pretty(pg_database_size(current_database()))"
            db_size_result = session.execute(text(db_size_query))
            db_size = db_size_result.scalar() or 'unknown'
            
            # Organize schema information
            tables = {}
            for row in schema_rows:
                table_name = row[0]
                if table_name not in tables:
                    stats = table_stats.get(table_name, {})
                    tables[table_name] = {
                        'columns': [],
                        'row_count': stats.get('row_count', 0),
                        'table_size': stats.get('size_pretty', 'unknown'),
                        'table_size_bytes': stats.get('size_bytes', 0)
                    }
                
                column_info = {
                    'name': row[1],
                    'data_type': row[2],
                    'nullable': row[3] == 'YES',
                    'default_value': row[4],
                    'max_length': row[5],
                    'numeric_precision': row[6],
                    'numeric_scale': row[7],
                    'position': row[8]
                }
                
                # Add sample data for this column
                sample_data = self.get_sample_data_for_column(session, table_name, row[1])
                if sample_data:
                    column_info['sample_values'] = sample_data
                    column_info['sample_stats'] = self._analyze_sample_data(sample_data, row[2])
                
                tables[table_name]['columns'].append(column_info)
            
            return {
                'tables': tables,
                'database_size': db_size,
                'total_tables': len(tables),
                'total_columns': sum(len(t['columns']) for t in tables.values())
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get database schema info: {str(e)}", exc_info=True)
            return {}
    
    def get_sample_data_for_column(self, session: Session, table_name: str, column_name: str, limit: int = 10) -> List[Any]:
        """Get sample data for a specific column to help AI estimation."""
        try:
            sample_query = f'''
            SELECT DISTINCT "{column_name}" 
            FROM "{table_name}" 
            WHERE "{column_name}" IS NOT NULL 
            ORDER BY "{column_name}" 
            LIMIT :limit
            '''
            
            result = session.execute(text(sample_query), {'limit': limit})
            return [row[0] for row in result.fetchall()]
            
        except Exception as e:
            self.logger.debug(f"Failed to get sample data for {table_name}.{column_name}: {str(e)}")
            return []
    
    def _analyze_sample_data(self, sample_data: List[Any], data_type: str) -> Dict[str, Any]:
        """Analyze sample data to provide insights for AI estimation."""
        if not sample_data:
            return {}
        
        analysis = {
            'sample_count': len(sample_data),
            'unique_count': len(set(str(v) for v in sample_data if v is not None)),
            'has_nulls': None in sample_data
        }
        
        # Type-specific analysis
        if data_type.lower() in ['integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision']:
            try:
                numeric_values = [float(v) for v in sample_data if v is not None]
                if numeric_values:
                    analysis.update({
                        'min_value': min(numeric_values),
                        'max_value': max(numeric_values),
                        'avg_value': sum(numeric_values) / len(numeric_values)
                    })
            except:
                pass
        
        return analysis
    
    def _save_ai_interaction(self, pg_stats_df: pd.DataFrame):
        """Save AI interaction data as document."""
        try:
            if hasattr(self, 'experiment_id') and self.experiment_id:
                from ...routers.document_routes import save_api_response_as_document
                
                # Save the processed pg_stats data
                stats_content = pg_stats_df.to_csv(index=False, sep=';')
                save_api_response_as_document(
                    experiment_id=self.experiment_id,
                    response_content=stats_content,
                    response_type="ai_pg_stats",
                    source_description="AI Generated pg_stats"
                )
        except Exception as e:
            self.logger.error(f"Failed to save AI interaction: {str(e)}")
    
    def _create_empty_statistics_rows(self, session: Session, table_name: str):
        """Create empty pg_statistic rows without analyzing real table data."""
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
            
            result = session.execute(text(table_info_query), {"table_name": table_name})
            columns = result.fetchall()
            
            if not columns:
                self.logger.warning(f"No columns found for table {table_name}")
                return
            
            # Insert minimal empty statistics for each column
            for col in columns:
                table_oid, attnum, attname, atttypid = col
                
                # Create minimal empty statistics row
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
                ON CONFLICT (starelid, staattnum, stainherit) DO NOTHING
                """
                
                session.execute(text(insert_query), {
                    "table_oid": table_oid,
                    "attnum": attnum
                })
                
                if ADVANCED_LOGGING:
                    self.logger.debug(f"Created empty statistics for {table_name}.{attname}")
                    
        except Exception as e:
            self.logger.error(f"Failed to create empty statistics for {table_name}: {str(e)}")
            raise
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Schneider AI Statistics Estimator (Fixed)"
    
    def get_pg_statistic_rows(self, session: Session) -> List[Tuple]:
        """Gets all the rows of pg_statistic that belong to the public namespace."""
        try:
            query = '''
            SELECT * FROM pg_statistic s WHERE s.starelid IN
            (SELECT c.oid as starelid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='public')
            '''
            
            result = session.execute(text(query))
            return result.fetchall()
        except Exception as e:
            self.logger.error(f"Failed to retrieve pg_statistic rows: {str(e)}")
            raise