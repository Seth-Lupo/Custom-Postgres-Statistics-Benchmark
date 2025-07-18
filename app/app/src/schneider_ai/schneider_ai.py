import json
import sys
import os
import csv
from io import StringIO
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from sqlalchemy import text
from sqlmodel import Session
import numpy as np
import time
import requests
from pprint import pprint
from ..base import StatsSource, StatsSourceConfig, StatsSourceSettings


class SchneiderAIStatsSource(StatsSource):
    """Statistics source that uses AI to estimate PostgreSQL statistics via API."""
    
    def __init__(self, settings: StatsSourceSettings = None, config: StatsSourceConfig = None):
        super().__init__(settings=settings, config=config)
        self.logger.info(f"Initialized {self.name()} with settings: {self.settings.name}, config: {self.config.name}")
        
        # Initialize API configuration from config
        self.api_endpoint = self.config.get_data('api_endpoint', 'https://a061igc186.execute-api.us-east-1.amazonaws.com/dev')
        self.api_key = self.config.get_data('api_key', 'blocked')
        self.model = self.config.get_data('model', 'us.anthropic.claude-3-haiku-20240307-v1:0')
        self.temperature = self.config.get_data('temperature', 0.3)
        self.session_id = self.config.get_data('session_id', 'schneider_stats_session')
        
        # RAG settings
        self.rag_usage = self.config.get_data('rag_usage', False)
        self.rag_threshold = self.config.get_data('rag_threshold', 0.5)
        self.rag_k = self.config.get_data('rag_k', 0)
        
        # Estimation settings
        self.num_iterations = self.config.get_data('num_iterations', 10)
        self.epsilon = self.config.get_data('epsilon', 0.1)
        
        # Column mappings - exact match from generator files
        self.target_columns = self.config.get_data('target_columns', {
            'stanullfrac': 3,    # null_frac
            'stadistinct': 5,    # n_distinct  
            'stanumbers1': 21    # statistical numbers array
        })
        
        # Prompts - matching generator approach
        self.system_prompt = self.config.get_data('system_prompt', 
            'You make predictions about pg_stats tables for postgres databases. '
            'You will always make a guess and never guess randomly. '
            'You will always output a semicolon, never comma, separated csv with no other information but the csv. '
            'Please do not guess NULL for the list columns unless very necessary, '
            'please always generate a pg_stats table and never the raw data.')
        
        self.estimation_prompt = self.config.get_data('estimation_prompt',
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
        
        # Validation settings
        self.accuracy_threshold = self.config.get_data('accuracy_threshold', 80.0)
        self.max_retries = self.config.get_data('max_retries', 3)
        
        # Test API connection
        self._test_api_connection()
    
    def get_pg_statistic_rows(self, session: Session) -> List[Tuple]:
        """
        Gets all the rows of pg_statistic that belong to the public namespace.
        Matches getStatRows() function from AI_Estimate.py
        """
        try:
            # Exact query from generator/AI_Estimate.py
            copy_public_namespace_stat_query = '''
            SELECT * FROM pg_statistic s WHERE s.starelid IN
            (SELECT c.oid as starelid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='public')
            '''
            
            result = session.execute(text(copy_public_namespace_stat_query))
            rows = result.fetchall()
            
            self.logger.info(f"Retrieved {len(rows)} pg_statistic rows from public namespace")
            return rows
        except Exception as e:
            self.logger.error(f"Failed to retrieve pg_statistic rows: {str(e)}")
            raise
    
    def _test_api_connection(self):
        """Test the API connection to ensure it's working before making estimation calls."""
        try:
            self.logger.info("🧪 Testing API connection...")
            
            # Try a simple health check or model info request
            headers = {
                 'x-api-key': self.api_key,
                 'request_type': 'call'
            }
            
            # Make a simple test request
            test_payload = {
                "model": self.model,
                "system": "You are a helpful assistant.",
                "query": "Hello, this is a test. Please respond with 'Connection successful'.",
                "temperature": 0.1,
                "lastk":0,
                "session_id": "test_connection"
            }
            
            self.logger.info(f"Testing connection to: {self.api_endpoint}")
            self.logger.info(f"Using API key: {self.api_key[:20]}...{self.api_key[-10:]}")
            
            response = requests.post(
                self.api_endpoint,
                json=test_payload,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                self.logger.info(f"✅ API connection successful! Status: {response.status_code}")
                try:
                    response_data = response.json()
                    self.logger.info(f"Test response: {response_data}")
                except:
                    self.logger.info(f"Test response (raw): {response.text}")
            else:
                self.logger.warning(f"⚠️ API connection test returned status {response.status_code}: {response.reason}")
                self.logger.warning(f"Response: {response.text}")
                
        except requests.RequestException as e:
            self.logger.error(f"❌ API connection test failed: {str(e)}")
            self.logger.error(f"Connection details: endpoint={self.api_endpoint}, "
                            f"api_key_length={len(self.api_key)}")
            self.logger.error(f"Exception details:", exc_info=True)
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during API connection test: {str(e)}")
            self.logger.error(f"Exception details:", exc_info=True)
    
    def get_database_schema_info(self, session: Session) -> Dict[str, Any]:
        """Get comprehensive database schema information for AI estimation."""
        try:
            self.logger.info("Starting comprehensive database schema analysis...")
            
            # Get table and column information with better PostgreSQL-specific queries
            self.logger.debug("Executing schema information query...")
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
            self.logger.info(f"Retrieved {len(schema_rows)} column definitions from information_schema")
            
            # Get table comments
            self.logger.debug("Retrieving table comments...")
            table_comments_query = '''
            SELECT 
                c.relname as table_name,
                COALESCE(d.description, '') as table_comment
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
            WHERE n.nspname = 'public' 
            AND c.relkind = 'r'
            '''
            
            table_comments_result = session.execute(text(table_comments_query))
            table_comments = {row[0]: row[1] for row in table_comments_result.fetchall()}
            self.logger.debug(f"Retrieved comments for {len(table_comments)} tables")
            
            # Get column comments
            self.logger.debug("Retrieving column comments...")
            column_comments_query = '''
            SELECT 
                c.relname as table_name,
                a.attname as column_name,
                COALESCE(d.description, '') as column_comment
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = c.oid
            LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
            WHERE n.nspname = 'public'
            AND c.relkind = 'r'
            AND a.attnum > 0
            AND NOT a.attisdropped
            '''
            
            column_comments_result = session.execute(text(column_comments_query))
            column_comments = {}
            for row in column_comments_result.fetchall():
                table_name, column_name, comment = row
                if table_name not in column_comments:
                    column_comments[table_name] = {}
                column_comments[table_name][column_name] = comment
            
            total_column_comments = sum(len(cols) for cols in column_comments.values())
            self.logger.debug(f"Retrieved {total_column_comments} column comments")
            
            if not schema_rows:
                self.logger.warning("No tables found in public schema")
                return {}
            
            # Get actual row counts for tables using PostgreSQL system catalogs
            # Use a more compatible approach that works across PostgreSQL versions
            table_stats_query = '''
            SELECT 
                n.nspname as schemaname,
                c.relname as tablename,
                COALESCE(s.n_tup_ins, 0) as n_tup_ins,
                COALESCE(s.n_tup_upd, 0) as n_tup_upd,
                COALESCE(s.n_tup_del, 0) as n_tup_del,
                COALESCE(s.n_live_tup, 0) as n_live_tup,
                COALESCE(s.n_dead_tup, 0) as n_dead_tup
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_stat_user_tables s ON s.schemaname = n.nspname AND s.relname = c.relname
            WHERE n.nspname = 'public' 
            AND c.relkind = 'r'
            '''
            
            try:
                self.logger.debug("Executing table statistics query...")
                stats_result = session.execute(text(table_stats_query))
                table_stats = {row[1]: dict(zip(['schema', 'table', 'inserts', 'updates', 'deletes', 'live_tuples', 'dead_tuples'], row)) 
                              for row in stats_result.fetchall()}
                self.logger.info(f"Retrieved statistics for {len(table_stats)} tables")
                
                # Log detailed table stats
                for table_name, stats in table_stats.items():
                    self.logger.debug(f"Table {table_name}: {stats['live_tuples']} live tuples, "
                                    f"{stats['inserts']} inserts, {stats['updates']} updates, {stats['deletes']} deletes")
                
            except Exception as e:
                self.logger.warning(f"Could not get table statistics: {str(e)}, using fallback approach")
                # Fallback to basic table info without statistics
                basic_table_query = '''
                SELECT 
                    n.nspname as schemaname,
                    c.relname as tablename,
                    0 as n_tup_ins,
                    0 as n_tup_upd,
                    0 as n_tup_del,
                    COALESCE(c.reltuples::bigint, 0) as n_live_tup,
                    0 as n_dead_tup
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public' 
                AND c.relkind = 'r'
                '''
                stats_result = session.execute(text(basic_table_query))
                table_stats = {row[1]: dict(zip(['schema', 'table', 'inserts', 'updates', 'deletes', 'live_tuples', 'dead_tuples'], row)) 
                              for row in stats_result.fetchall()}
                self.logger.info(f"Retrieved fallback statistics for {len(table_stats)} tables")
            
            # Get table sizes using a more compatible approach
            self.logger.debug("Retrieving table sizes...")
            size_query = '''
            SELECT 
                c.relname as tablename,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                pg_total_relation_size(c.oid) as total_size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' 
            AND c.relkind = 'r'
            '''
            
            size_result = session.execute(text(size_query))
            table_sizes = {row[0]: {'size_pretty': row[1], 'size_bytes': row[2]} 
                          for row in size_result.fetchall()}
            
            self.logger.info(f"Retrieved size information for {len(table_sizes)} tables")
            for table_name, size_info in table_sizes.items():
                self.logger.debug(f"Table {table_name}: {size_info['size_pretty']} ({size_info['size_bytes']} bytes)")
            
            # Get database size
            self.logger.debug("Retrieving database size...")
            db_size_query = "SELECT pg_size_pretty(pg_database_size(current_database())), pg_database_size(current_database())"
            db_size_result = session.execute(text(db_size_query))
            db_size_row = db_size_result.fetchone()
            db_size = db_size_row[0] if db_size_row else 'unknown'
            db_size_bytes = db_size_row[1] if db_size_row else 0
            self.logger.info(f"Database size: {db_size} ({db_size_bytes} bytes)")
            
            # Organize schema information with enhanced data
            self.logger.info("Organizing schema information and collecting sample data...")
            tables = {}
            for row in schema_rows:
                table_name = row[0]
                if table_name not in tables:
                    # Get table statistics
                    stats = table_stats.get(table_name, {})
                    sizes = table_sizes.get(table_name, {})
                    table_comment = table_comments.get(table_name, '')
                    
                    tables[table_name] = {
                        'columns': [],
                        'row_count': stats.get('live_tuples', 0),
                        'table_size': sizes.get('size_pretty', 'unknown'),
                        'table_size_bytes': sizes.get('size_bytes', 0),
                        'statistics': stats,
                        'comment': table_comment if table_comment else None
                    }
                    
                    self.logger.debug(f"Initialized table {table_name}: {stats.get('live_tuples', 0)} rows, "
                                    f"size {sizes.get('size_pretty', 'unknown')}")
                
                # Enhanced column information
                column_comment = column_comments.get(table_name, {}).get(row[1], '')
                column_info = {
                    'name': row[1],
                    'data_type': row[2],
                    'nullable': row[3] == 'YES',
                    'default_value': row[4],
                    'max_length': row[5],
                    'numeric_precision': row[6],
                    'numeric_scale': row[7],
                    'position': row[8],
                    'comment': column_comment if column_comment else None
                }
                
                # Add sample data for this column
                sample_data = self.get_sample_data_for_column(session, table_name, row[1])
                if sample_data:
                    column_info['sample_values'] = sample_data
                    column_info['sample_stats'] = self._analyze_sample_data(sample_data, row[2])
                    self.logger.debug(f"Column {table_name}.{row[1]}: {len(sample_data)} sample values, "
                                    f"data type {row[2]}")
                else:
                    self.logger.debug(f"Column {table_name}.{row[1]}: no sample data available")
                
                tables[table_name]['columns'].append(column_info)
            
            schema_summary = {
                'tables': tables,
                'database_size': db_size,
                'database_size_bytes': db_size_bytes,
                'total_tables': len(tables),
                'total_columns': sum(len(t['columns']) for t in tables.values()),
                'summary': self._generate_database_summary(tables)
            }
            
            self.logger.info(f"Schema analysis complete: {schema_summary['total_tables']} tables, "
                           f"{schema_summary['total_columns']} columns, database size {db_size}")
            
            # Log table-by-table summary
            for table_name, table_info in tables.items():
                self.logger.info(f"Table {table_name}: {len(table_info['columns'])} columns, "
                               f"{table_info['row_count']} rows, {table_info['table_size']}")
            
            return schema_summary
        except Exception as e:
            self.logger.error(f"Failed to get database schema info: {str(e)}")
            self.logger.debug(f"Schema info error details", exc_info=True)
            return {}
    
    def get_sample_data(self, session: Session, table_name: str, limit: int = 5) -> List[Dict]:
        """Get sample data from a table for AI context."""
        try:
            sample_query = f"SELECT * FROM {table_name} LIMIT {limit}"
            result = session.execute(text(sample_query))
            rows = result.fetchall()
            columns = result.keys()
            
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            self.logger.warning(f"Failed to get sample data from {table_name}: {str(e)}")
            return []
    
    def estimate_statistics_with_ai(self, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """Use AI to estimate PostgreSQL statistics with retry logic."""
        self.logger.info(f"Starting AI estimation process with {self.max_retries} max retries")
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"AI estimation attempt {attempt + 1}/{self.max_retries}")
                
                # Create a detailed but concise summary for the AI
                self.logger.debug("Building tables summary for AI prompt...")
                tables_summary = {}
                total_sample_values = 0
                
                for table_name, table_data in schema_info.get('tables', {}).items():
                    columns_summary = []
                    for col in table_data.get('columns', []):
                        col_summary = {
                            'name': col['name'],
                            'type': col['data_type'],
                            'nullable': col['nullable']
                        }
                        
                        # Add column comment if available
                        if col.get('comment'):
                            col_summary['comment'] = col['comment']
                        
                        # Add sample statistics if available
                        if 'sample_stats' in col and col['sample_stats']:
                            col_summary['sample_stats'] = col['sample_stats']
                            if 'sample_values' in col:
                                total_sample_values += len(col.get('sample_values', []))
                        
                        columns_summary.append(col_summary)
                    
                    table_summary = {
                        'row_count': table_data.get('row_count', 0),
                        'columns': columns_summary,
                        'table_size': table_data.get('table_size', 'unknown')
                    }
                    
                    # Add table comment if available
                    if table_data.get('comment'):
                        table_summary['comment'] = table_data['comment']
                    
                    tables_summary[table_name] = table_summary
                
                # Format the prompt with structured data - matching generator format
                col_names_list = []
                for table_name, table_data in schema_info.get('tables', {}).items():
                    for col in table_data.get('columns', []):
                        col_names_list.append(f"{table_name}.{col['name']}")
                
                formatted_prompt = self.estimation_prompt.format(
                    col_names=', '.join(col_names_list),
                    size=schema_info.get('database_size', 'unknown'),
                    sample_data=json.dumps(tables_summary, indent=2, default=str)
                )
                
                self.logger.info(f"Prepared AI prompt: {len(col_names_list)} columns, "
                               f"{total_sample_values} total sample values")
                self.logger.info(f"AI prompt length: {len(formatted_prompt)} characters")
                self.logger.debug(f"Column names for AI: {', '.join(col_names_list[:10])}...")  # Show first 10
                
                # Log the full prompt for debugging - at INFO level so it's always visible
                self.logger.info(f"=== FULL AI PROMPT ===")
                self.logger.info(f"System prompt: {self.system_prompt}")
                self.logger.info(f"User prompt: {formatted_prompt}")
                self.logger.info(f"=== END AI PROMPT ===")
                
                self.logger.info(f"Requesting AI estimation via API (attempt {attempt + 1}/{self.max_retries})")
                self.logger.info(f"Using model: {self.model}, temperature: {self.temperature}, "
                               f"endpoint: {self.api_endpoint}")
                
                # Verify API configuration before API call
                self.logger.info(f"API endpoint: {self.api_endpoint}")
                self.logger.info(f"API key: {self.api_key[:20]}...{self.api_key[-10:]}")
                
                # Log the API request parameters
                api_params = {
                    'model': self.model,
                    'system': self.system_prompt[:100] + "..." if len(self.system_prompt) > 100 else self.system_prompt,
                    'query_length': len(formatted_prompt),
                    'temperature': self.temperature,
                    'session_id': self.session_id,
                    'rag_threshold': self.rag_threshold,
                    'rag_usage': self.rag_usage,
                    'rag_k': self.rag_k
                }
                self.logger.info(f"API request parameters: {api_params}")
                
                # Add detailed prompt analysis
                total_prompt_length = len(self.system_prompt) + len(formatted_prompt)
                self.logger.info(f"📏 Prompt analysis:")
                self.logger.info(f"   System prompt: {len(self.system_prompt)} chars")
                self.logger.info(f"   User prompt: {len(formatted_prompt)} chars")
                self.logger.info(f"   Total length: {total_prompt_length} chars")
                
                # Check for potential issues
                if total_prompt_length > 100000:  # 100k chars - likely too large
                    self.logger.warning(f"⚠️ VERY LARGE PROMPT: {total_prompt_length} chars - likely to exceed model context window")
                elif total_prompt_length > 50000:  # 50k chars - potentially problematic
                    self.logger.warning(f"⚠️ LARGE PROMPT: {total_prompt_length} chars - might cause issues with smaller models")
                elif total_prompt_length > 20000:  # 20k chars - noteworthy
                    self.logger.info(f"📋 MODERATE PROMPT: {total_prompt_length} chars - should be fine for most models")
                else:
                    self.logger.info(f"✅ COMPACT PROMPT: {total_prompt_length} chars - optimal size")
                
                # Check model name format and warn about known issues
                if self.model == "4o-mini":
                    self.logger.warning(f"⚠️ Model '4o-mini' has known issues - consider using 'us.anthropic.claude-3-haiku-20240307-v1:0'")
                elif "claude" in self.model.lower():
                    self.logger.info(f"✅ Using Claude model - generally reliable for CSV generation")
                elif self.model.startswith("gpt"):
                    self.logger.info(f"🤖 Using GPT model - verify CSV formatting")
                
                # Sample data size analysis
                try:
                    sample_data_str = formatted_prompt.split('sample_data}')[0].split('{sample_data')[1] if '{sample_data' in formatted_prompt else ""
                    if sample_data_str:
                        sample_data_size = len(sample_data_str)
                        self.logger.info(f"📊 Sample data size: {sample_data_size} chars")
                        if sample_data_size > 30000:
                            self.logger.warning(f"⚠️ Large sample data ({sample_data_size} chars) - consider reducing")
                except:
                    self.logger.debug("Could not analyze sample data size")
                
                # Call the AI API directly
                self.logger.info("Making direct API call to LLM proxy...")
                # Print API credentials before making the call
                self.logger.info(f"🔑 API ENDPOINT: {self.api_endpoint}")
                self.logger.info(f"🔑 API KEY: {self.api_key}")
                
                # Prepare the request payload
                payload = {
                    "model": self.model,
                    "system": self.system_prompt,
                    "query": formatted_prompt,
                    "temperature": self.temperature,
                    "session_id": self.session_id,
                    "rag_usage": self.rag_usage,
                    "rag_threshold": self.rag_threshold,
                    "rag_k": self.rag_k
                }
                
                headers = {
                    'x-api-key': self.api_key,
                    'request_type': 'call'
                }
                
                self.logger.info(f"📤 REQUEST PAYLOAD: {json.dumps(payload, indent=2)}")
                self.logger.info(f"📤 REQUEST HEADERS: {headers}")
                
                # Make HTTP request
                response = requests.post(
                    self.api_endpoint,
                    json=payload,
                    headers=headers,
                    timeout=300  # 5 minute timeout
                )
                
                # Print HTTP response details
                self.logger.info(f"📋 HTTP RESPONSE ANALYSIS:")
                self.logger.info(f"   Status Code: {response.status_code}")
                self.logger.info(f"   Status Text: {response.reason}")
                self.logger.info(f"   Headers: {dict(response.headers)}")
                self.logger.info(f"   Response Type: {type(response)}")
                
                # Print raw response content
                raw_content = response.text
                self.logger.info(f"   Raw Content Length: {len(raw_content)}")
                self.logger.info(f"   Raw Content: {raw_content}")
                
                # Check if the request was successful
                if response.status_code != 200:
                    self.logger.error(f"❌ HTTP request failed with status {response.status_code}: {response.reason}")
                    self.logger.error(f"Error response content: {raw_content}")
                    # Continue to the next retry
                    if attempt < self.max_retries - 1:
                        self.logger.info(f"HTTP request failed, retrying in 2 seconds...")
                        time.sleep(2)
                    continue
                
                self.logger.info("✅ HTTP request successful")
                
                # Try to parse JSON response
                try:
                    response_data = response.json()
                    self.logger.info(f"📋 PARSED JSON RESPONSE:")
                    self.logger.info(f"   Type: {type(response_data)}")
                    
                    # Print the entire response object
                    self.logger.info("🔍 COMPLETE RESPONSE OBJECT:")
                    self.logger.info(json.dumps(response_data, indent=2))
                   
                    
                    if isinstance(response_data, dict):
                        self.logger.info(f"   Dictionary keys: {list(response_data.keys())}")
                        for key, value in response_data.items():
                            value_str = str(value) if len(str(value)) <= 200 else str(value)[:200] + "..."
                            self.logger.info(f"   {key}: {type(value)} = {value_str}")
                    else:
                        self.logger.info(f"   Content: {response_data}")
                        
                    # Use the parsed response data for further processing
                    response = response_data
                    
                except json.JSONDecodeError as je:
                    self.logger.error(f"❌ Failed to parse JSON response: {je}")
                    self.logger.error(f"Raw response content: {raw_content}")
                    # Use the raw text as response
                    response = raw_content
                
                # Process the response
                ai_response = None
                
                # Extract AI response based on format
                if isinstance(response, dict) and 'response' in response:
                    # Standard JSON format with 'response' key
                    ai_response = response['response']
                    self.logger.info(f"Successfully received AI estimation response from JSON (length: {len(ai_response)} chars)")
                elif isinstance(response, str) and (';' in response or ',' in response):
                    # Plain text CSV response - this is valid, not an error
                    ai_response = response
                    self.logger.info(f"Successfully received AI estimation response as plain text CSV (length: {len(ai_response)} chars)")
                elif isinstance(response, dict):
                    # JSON response with different structure - try common keys
                    self.logger.warning(f"Response is JSON but missing 'response' key. Keys found: {list(response.keys())}")
                    for key in ['data', 'result', 'content', 'text', 'body', 'output']:
                        if key in response and response[key]:
                            ai_response = response[key]
                            self.logger.info(f"Found response in '{key}' field (length: {len(str(ai_response))} chars)")
                            break
                
                if ai_response:
                    # Log the full AI response for debugging - always at INFO level so it's visible
                    self.logger.info(f"=== FULL AI RESPONSE ===")
                    self.logger.info(ai_response)
                    self.logger.info(f"=== END AI RESPONSE ===")
                    
                    # Post-process the response (matches generator approach)
                    self.logger.debug("Post-processing AI response...")
                    processed_response = self._post_process_ai_response(ai_response)
                    
                    # Parse and validate the response
                    self.logger.debug("Parsing AI response...")
                    parsed_estimates = self._parse_ai_response(processed_response)
                    if parsed_estimates:
                        self.logger.info(f"Successfully parsed estimates for {len(parsed_estimates)} tables")
                        
                        # Log parsed data details
                        for table_name, table_estimates in parsed_estimates.items():
                            self.logger.debug(f"Table {table_name}: {len(table_estimates)} column estimates")
                            for col_name, col_stats in table_estimates.items():
                                self.logger.debug(f"  {col_name}: {list(col_stats.keys())}")
                        
                        self.logger.debug("Validating AI estimates...")
                        validated_estimates = self._validate_ai_estimates(parsed_estimates, schema_info)
                        if validated_estimates:
                            total_validated = sum(len(t) for t in validated_estimates.values())
                            self.logger.info(f"Successfully validated {total_validated} column estimates "
                                           f"across {len(validated_estimates)} tables")
                            return validated_estimates
                        else:
                            self.logger.error("❌ AI estimates failed validation - no valid estimates found")
                            self.logger.error("DEBUGGING: Parsed estimates that failed validation:")
                            self.logger.error(f"{parsed_estimates}")
                    else:
                        self.logger.error("❌ Failed to parse AI response - no estimates extracted")
                        self.logger.error("DEBUGGING: Post-processed response that failed to parse:")
                        self.logger.error(f"'{processed_response}'")
                        self.logger.error("DEBUGGING: Original AI response that failed to parse:")
                        self.logger.error(f"=== FAILED AI RESPONSE ===")
                        self.logger.error(ai_response)
                        self.logger.error(f"=== END FAILED AI RESPONSE ===")
                elif isinstance(response, str):
                    # Plain text that doesn't look like CSV - likely an error message
                    self.logger.error(f"🚨 AI API returned unexpected plain text (not CSV format): {response[:200]}...")
                    self.logger.error("DEBUGGING: Full response:")
                    self.logger.error(f"=== PLAIN TEXT RESPONSE ===")
                    self.logger.error(response)
                    self.logger.error(f"=== END PLAIN TEXT RESPONSE ===")
                else:
                    # Completely unexpected response type
                    self.logger.error(f"🚨 Unexpected AI response type: {type(response)}")
                    self.logger.error(f"Response: {str(response)[:200]}...")
                    self.logger.error(f"=== UNEXPECTED RESPONSE ===")
                    self.logger.error(f"{response}")
                    self.logger.error(f"=== END UNEXPECTED RESPONSE ===")
                
                # If we got here, this attempt failed - continue to next attempt
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying AI estimation in 2 seconds...")
                    time.sleep(2)
                    
            except requests.RequestException as req_error:
                self.logger.error(f"💥 HTTP REQUEST FAILED: {str(req_error)}")
                self.logger.error(f"Request exception type: {type(req_error).__name__}")
                self.logger.error(f"Exception details:", exc_info=True)
                
                # Continue to the next retry
                if attempt < self.max_retries - 1:
                    self.logger.info(f"HTTP request failed, retrying in 2 seconds...")
                    time.sleep(2)
                continue
                
            except Exception as e:
                self.logger.error(f"💥 UNEXPECTED ERROR: {str(e)}")
                self.logger.error(f"Exception type: {type(e).__name__}")
                self.logger.error(f"Exception details:", exc_info=True)
                
                # Continue to the next retry
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Unexpected error, retrying in 2 seconds...")
                    time.sleep(2)
                continue
        
        self.logger.error(f"Failed to get valid AI estimation after {self.max_retries} attempts")
        return {}
    
    def _parse_ai_response(self, ai_response: str) -> Dict[str, Any]:
        """
        Parse the AI response to extract statistical estimates.
        Expects semicolon-separated CSV format matching generator approach.
        """
        try:
            self.logger.debug(f"Determining AI response format for content: '{ai_response[:100]}...'")
            
            # Handle CSV response (expected format from generator)
            if not ai_response.strip().startswith('{'):
                self.logger.debug("Detected CSV format, parsing as CSV...")
                return self._parse_csv_response(ai_response)
            
            # Try to parse as JSON if format is JSON
            if ai_response.strip().startswith('{'):
                self.logger.debug("Detected JSON format, parsing as JSON...")
                try:
                    json_result = json.loads(ai_response)
                    self.logger.debug(f"Successfully parsed JSON with keys: {list(json_result.keys()) if isinstance(json_result, dict) else type(json_result)}")
                    return json_result
                except json.JSONDecodeError as je:
                    self.logger.warning(f"Failed to parse as JSON: {je}")
                    self.logger.debug("Falling back to JSON extraction...")
            
            # If not JSON, try to extract JSON from the response
            import re
            json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
            if json_match:
                self.logger.debug("Found JSON content within response, extracting...")
                try:
                    extracted_json = json.loads(json_match.group())
                    self.logger.debug(f"Successfully extracted JSON with keys: {list(extracted_json.keys()) if isinstance(extracted_json, dict) else type(extracted_json)}")
                    return extracted_json
                except json.JSONDecodeError as je:
                    self.logger.warning(f"Failed to parse extracted JSON: {je}")
            
            self.logger.warning("Could not detect JSON format, falling back to CSV parsing")
            return self._parse_csv_response(ai_response)
        except Exception as e:
            self.logger.error(f"💥 PARSING FAILED: {str(e)}")
            self.logger.error(f"DEBUGGING: Response that failed to parse (length: {len(ai_response)}):")
            self.logger.error(f"=== FAILED PARSING RESPONSE ===")
            self.logger.error(ai_response)
            self.logger.error(f"=== END FAILED PARSING RESPONSE ===")
            self.logger.error(f"Exception type: {type(e).__name__}")
            self.logger.error(f"Exception details:", exc_info=True)
            return {}
    
    def _post_process_ai_response(self, ai_response: str) -> str:
        """
        Post-process the AI response with validation checks.
        Matches post_process_csv() function from generator/estimationv4.py
        """
        try:
            self.logger.debug("Starting AI response post-processing...")
            content = ai_response.strip()
            original_length = len(content)
            
            # Check 1: Remove trailing commas (from generator)
            if content.endswith(','):
                content = content.rstrip(',')
                self.logger.info("Removed trailing comma from AI response")
            
            # Check 2: Ensure we have semicolon-separated content
            comma_count = content.count(',')
            semicolon_count = content.count(';')
            
            if ';' not in content and ',' in content:
                # Convert comma-separated to semicolon-separated
                content = content.replace(',', ';')
                self.logger.info(f"Converted comma-separated to semicolon-separated format "
                               f"({comma_count} commas converted)")
            
            # Check 3: Check for excessive NULL values (from generator)
            lines = content.split('\n')
            if len(lines) > 1:
                null_count = sum(line.count('NULL') + line.count('null') for line in lines)
                total_fields = sum(len(line.split(';')) for line in lines)
                
                self.logger.debug(f"Response analysis: {len(lines)} lines, {total_fields} total fields, "
                                f"{null_count} NULL values")
                
                if total_fields > 0:
                    null_percentage = (null_count / total_fields) * 100
                    if null_percentage > 70:
                        self.logger.warning(f"AI response has {null_percentage:.1f}% NULL values "
                                          f"({null_count}/{total_fields}) - may need regeneration")
                    else:
                        self.logger.debug(f"NULL percentage acceptable: {null_percentage:.1f}%")
            
            self.logger.debug(f"Post-processing complete: {original_length} → {len(content)} characters")
            return content
            
        except Exception as e:
            self.logger.error(f"Error post-processing AI response: {str(e)}")
            return ai_response
    
    def _parse_csv_response(self, csv_response: str) -> Dict[str, Any]:
        """Parse CSV response from AI (semicolon-separated format)."""
        try:
            self.logger.debug("Starting CSV response parsing...")
            self.logger.info(f"CSV content to parse (length: {len(csv_response)}):")
            self.logger.info(f"=== CSV CONTENT ===")
            self.logger.info(csv_response)
            self.logger.info(f"=== END CSV CONTENT ===")
            
            # Clean up the response
            csv_content = csv_response.strip()
            
            # Handle potential trailing commas from generator post-processing
            if csv_content.endswith(','):
                csv_content = csv_content.rstrip(',')
                self.logger.debug("Cleaned trailing comma from CSV content")
            
            # Check if we have any content at all
            if not csv_content:
                self.logger.error("❌ CSV PARSING FAILED: Empty content after cleanup")
                return {}
            
            # Check for semicolon delimiter
            if ';' not in csv_content:
                self.logger.error(f"❌ CSV PARSING FAILED: No semicolon delimiter found in content")
                self.logger.error(f"Content analysis: {csv_content.count(',')} commas, {csv_content.count(';')} semicolons")
                self.logger.error(f"First 200 chars: '{csv_content[:200]}'")
                return {}
            
            # Parse CSV with semicolon delimiter (matches generator format)
            try:
                csv_reader = csv.DictReader(StringIO(csv_content), delimiter=';')
                header = csv_reader.fieldnames
                self.logger.debug(f"CSV headers detected: {header}")
                
                if not header:
                    self.logger.error("❌ CSV PARSING FAILED: No headers detected")
                    return {}
                
                if 'attname' not in header:
                    self.logger.error(f"❌ CSV PARSING FAILED: Required 'attname' column not found in headers: {header}")
                    return {}
                
            except Exception as csv_error:
                self.logger.error(f"❌ CSV PARSING FAILED: Error creating CSV reader: {csv_error}")
                self.logger.error(f"Content that failed: '{csv_content}'")
                return {}
            
            estimates = {}
            row_count = 0
            
            for row in csv_reader:
                row_count += 1
                self.logger.debug(f"Processing CSV row {row_count}: {dict(row)}")
                if 'attname' in row:
                    attname = row['attname']
                    self.logger.debug(f"Processing attname: {attname}")
                    
                    # Extract table name from attname if it's in format table.column
                    if '.' in attname:
                        table_name, column_name = attname.split('.', 1)
                    else:
                        # Default to first table if no table specified
                        table_name = 'unknown'
                        column_name = attname
                    
                    self.logger.debug(f"Mapped to table: {table_name}, column: {column_name}")
                    
                    if table_name not in estimates:
                        estimates[table_name] = {}
                        self.logger.debug(f"Created new table entry: {table_name}")
                    
                    column_stats = {}
                    
                    # Extract the three key statistics that match generator targets
                    stats_found = []
                    
                    if 'null_frac' in row:
                        try:
                            null_frac_val = float(row['null_frac'])
                            column_stats['stanullfrac'] = null_frac_val
                            stats_found.append(f"null_frac={null_frac_val}")
                        except (ValueError, TypeError):
                            self.logger.debug(f"Invalid null_frac value: {row['null_frac']}")
                    
                    if 'n_distinct' in row:
                        try:
                            n_distinct_val = float(row['n_distinct'])
                            column_stats['stadistinct'] = n_distinct_val
                            stats_found.append(f"n_distinct={n_distinct_val}")
                        except (ValueError, TypeError):
                            self.logger.debug(f"Invalid n_distinct value: {row['n_distinct']}")
                    
                    if 'correlation' in row:
                        try:
                            # This would map to stanumbers1 in generator approach
                            correlation = float(row['correlation'])
                            column_stats['stanumbers1'] = [correlation]  # Store as array
                            stats_found.append(f"correlation={correlation}")
                        except (ValueError, TypeError):
                            self.logger.debug(f"Invalid correlation value: {row['correlation']}")
                    
                    # Handle direct stanumbers1 arrays from CSV (like {0.3, 0.2, 0.1})
                    if 'stanumbers1' in row:
                        try:
                            stanumbers_val = row['stanumbers1']
                            if stanumbers_val and stanumbers_val != 'NULL':
                                # Parse PostgreSQL array format
                                if stanumbers_val.startswith('{') and stanumbers_val.endswith('}'):
                                    # Remove braces and split
                                    array_content = stanumbers_val[1:-1]
                                    if array_content.strip():
                                        values = [float(v.strip()) for v in array_content.split(',')]
                                        column_stats['stanumbers1'] = values
                        except (ValueError, TypeError):
                            pass
                    
                    if column_stats:
                        estimates[table_name][column_name] = column_stats
                        self.logger.debug(f"Added statistics for {table_name}.{column_name}: {', '.join(stats_found)}")
                    else:
                        self.logger.debug(f"No valid statistics found for {attname}")
            
            if row_count == 0:
                self.logger.error("❌ CSV PARSING FAILED: No data rows found in CSV")
                self.logger.error(f"Content: '{csv_content}'")
                return {}
            
            self.logger.info(f"✅ CSV parsing complete: {row_count} rows processed, "
                           f"{len(estimates)} tables with estimates")
            
            if not estimates:
                self.logger.error("❌ CSV PARSING WARNING: No estimates extracted from CSV")
                self.logger.error(f"Processed {row_count} rows but got no valid estimates")
            
            return estimates
            
        except Exception as e:
            self.logger.error(f"💥 CSV PARSING FAILED: {str(e)}")
            self.logger.error(f"Exception type: {type(e).__name__}")
            self.logger.error(f"Content that caused failure (length: {len(csv_response)}):")
            self.logger.error(f"=== FAILED CSV CONTENT ===")
            self.logger.error(csv_response)
            self.logger.error(f"=== END FAILED CSV CONTENT ===")
            self.logger.error(f"Exception details:", exc_info=True)
            return {}
    
    def apply_ai_statistics_to_pg(self, session: Session, estimates: Dict[str, Any]) -> bool:
        """
        Apply AI-generated estimates to pg_statistic table.
        Matches the approach from generator/AI_Estimate.py
        """
        try:
            if not estimates:
                self.logger.warning("No estimates to apply")
                return False
            
            total_estimates = sum(len(table_estimates) for table_estimates in estimates.values())
            self.logger.info(f"Applying {total_estimates} AI estimates to pg_statistic table")
            
            # Get current pg_statistic rows (matches getStatRows approach)
            self.logger.debug("Retrieving current pg_statistic rows...")
            current_rows = self.get_pg_statistic_rows(session)
            self.logger.debug(f"Found {len(current_rows)} existing pg_statistic rows")
            
            applied_count = 0
            failed_count = 0
            for table_name, table_estimates in estimates.items():
                if not isinstance(table_estimates, dict):
                    self.logger.warning(f"Invalid table estimates format for {table_name}")
                    continue
                
                self.logger.debug(f"Processing {len(table_estimates)} estimates for table {table_name}")
                
                for column_name, column_stats in table_estimates.items():
                    if not isinstance(column_stats, dict):
                        self.logger.warning(f"Invalid column stats format for {table_name}.{column_name}")
                        continue
                    
                    # Apply estimates to the three target columns from generator
                    for stat_name, stat_value in column_stats.items():
                        if stat_name in self.target_columns:
                            col_idx = self.target_columns[stat_name]
                            
                            # Handle array values for stanumbers1 (column 21)
                            original_value = stat_value
                            if col_idx == 21 and isinstance(stat_value, list):
                                # Convert array to PostgreSQL array format
                                if stat_value:
                                    pg_array = '{' + ','.join(str(v) for v in stat_value) + '}'
                                    self.logger.debug(f"Converted array {stat_value} to PostgreSQL format: {pg_array}")
                                else:
                                    pg_array = None
                                stat_value = pg_array
                            
                            self.logger.debug(f"Applying {stat_name} (column {col_idx}) = {stat_value} "
                                            f"to {table_name}.{column_name}")
                            
                            success = self._update_pg_statistic_column(
                                session, table_name, column_name, col_idx, stat_value
                            )
                            if success:
                                applied_count += 1
                                self.logger.info(f"✓ Applied {stat_name}={original_value} to {table_name}.{column_name}")
                            else:
                                failed_count += 1
                                self.logger.warning(f"✗ Failed to apply {stat_name} to {table_name}.{column_name}")
                        else:
                            self.logger.debug(f"Skipping unsupported statistic: {stat_name}")
            
            self.logger.info(f"Statistics application complete: {applied_count} applied, {failed_count} failed")
            return applied_count > 0
            
        except Exception as e:
            self.logger.error(f"Failed to apply AI statistics: {str(e)}")
            return False
    
    def _update_pg_statistic_column(self, session: Session, table_name: str, 
                                  column_name: str, col_idx: int, value: Any) -> bool:
        """
        Update a specific column in pg_statistic table.
        Matches insert_cr_into_pg_statistic() approach from AI_Estimate.py
        """
        try:
            self.logger.debug(f"Updating pg_statistic column {col_idx} for {table_name}.{column_name} with value: {value}")
            
            # First, find the OID for the table
            oid_query = '''
            SELECT c.oid FROM pg_class c 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE c.relname = %s AND n.nspname = 'public'
            '''
            oid_result = session.execute(text(oid_query), (table_name,))
            oid_row = oid_result.fetchone()
            
            if not oid_row:
                self.logger.warning(f"Table {table_name} not found in pg_class")
                return False
            
            table_oid = oid_row[0]
            self.logger.debug(f"Found table OID: {table_oid} for {table_name}")
            
            # Get column attribute number
            attnum_query = '''
            SELECT attnum FROM pg_attribute 
            WHERE attrelid = %s AND attname = %s
            '''
            attnum_result = session.execute(text(attnum_query), (table_oid, column_name))
            attnum_row = attnum_result.fetchone()
            
            if not attnum_row:
                self.logger.warning(f"Column {column_name} not found in pg_attribute for table {table_name}")
                return False
            
            attnum = attnum_row[0]
            self.logger.debug(f"Found column attnum: {attnum} for {column_name}")
            
            # Update the specific statistic based on column index - exact approach from generator
            if col_idx == 3:  # stanullfrac (null_frac)
                update_query = '''
                UPDATE pg_statistic SET stanullfrac = %s 
                WHERE starelid = %s AND staattnum = %s AND stainherit = false
                '''
            elif col_idx == 5:  # stadistinct (n_distinct)
                update_query = '''
                UPDATE pg_statistic SET stadistinct = %s 
                WHERE starelid = %s AND staattnum = %s AND stainherit = false
                '''
            elif col_idx == 21:  # stanumbers1 (statistical numbers)
                update_query = '''
                UPDATE pg_statistic SET stanumbers1 = %s 
                WHERE starelid = %s AND staattnum = %s AND stainherit = false
                '''
            else:
                self.logger.warning(f"Unsupported column index: {col_idx}")
                return False
            
            self.logger.debug(f"Executing update query: {update_query} with params: {(value, table_oid, attnum)}")
            session.execute(text(update_query), (value, table_oid, attnum))
            
            self.logger.debug(f"Successfully updated pg_statistic for {table_name}.{column_name} column {col_idx} = {value}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update pg_statistic: {str(e)}")
            return False
    
    def apply_statistics(self, session: Session) -> None:
        """Apply AI-generated statistics to the database."""
        start_time = time.time()
        try:
            self.logger.info(f"🚀 Starting AI statistics application for {self.name()}")
            self.logger.info(f"Configuration: model={self.model}, temperature={self.temperature}, "
                           f"max_retries={self.max_retries}")
            
            # First clear all caches
            cache_start = time.time()
            self.clear_caches(session)
            self.logger.debug(f"Cache clearing took {time.time() - cache_start:.2f} seconds")
            
            # Get database schema information
            schema_start = time.time()
            self.logger.info("📊 Gathering database schema information")
            schema_info = self.get_database_schema_info(session)
            schema_time = time.time() - schema_start
            self.logger.info(f"Schema analysis took {schema_time:.2f} seconds")
            
            if not schema_info:
                self.logger.warning("❌ No schema information available, falling back to standard ANALYZE")
                super().apply_statistics(session)
                return
            
            # Get AI estimates
            ai_start = time.time()
            self.logger.info("🤖 Requesting AI statistics estimation")
            estimates = self.estimate_statistics_with_ai(schema_info)
            ai_time = time.time() - ai_start
            self.logger.info(f"AI estimation took {ai_time:.2f} seconds")
            
            if estimates:
                # Apply AI estimates to pg_statistic
                apply_start = time.time()
                self.logger.info("📝 Applying AI estimates to pg_statistic")
                success = self.apply_ai_statistics_to_pg(session, estimates)
                apply_time = time.time() - apply_start
                self.logger.info(f"Statistics application took {apply_time:.2f} seconds")
                
                if success:
                    session.commit()
                    total_time = time.time() - start_time
                    self.logger.info(f"✅ AI statistics applied successfully in {total_time:.2f} seconds total")
                else:
                    self.logger.warning("❌ Failed to apply AI statistics, falling back to ANALYZE")
                    session.rollback()
                    fallback_start = time.time()
                    super().apply_statistics(session)
                    fallback_time = time.time() - fallback_start
                    self.logger.info(f"Fallback ANALYZE took {fallback_time:.2f} seconds")
            else:
                self.logger.warning("❌ No AI estimates available, falling back to standard ANALYZE")
                fallback_start = time.time()
                super().apply_statistics(session)
                fallback_time = time.time() - fallback_start
                self.logger.info(f"Fallback ANALYZE took {fallback_time:.2f} seconds")
            
            total_time = time.time() - start_time
            self.logger.info(f"🏁 Statistics application for {self.name()} completed in {total_time:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"💥 Failed to apply AI statistics: {str(e)}")
            self.logger.debug("Exception details:", exc_info=True)
            session.rollback()
            # Fall back to standard statistics
            self.logger.info("🔄 Falling back to standard PostgreSQL statistics")
            fallback_start = time.time()
            super().apply_statistics(session)
            fallback_time = time.time() - fallback_start
            self.logger.info(f"Fallback ANALYZE took {fallback_time:.2f} seconds")
    
    def name(self) -> str:
        """Return the name of this statistics source."""
        return "Schneider AI Statistics Estimator"

    def get_sample_data_for_column(self, session: Session, table_name: str, column_name: str, limit: int = 10) -> List[Any]:
        """Get sample data for a specific column to help AI estimation."""
        try:
            # Use proper SQL escaping for identifiers
            sample_query = f'''
            SELECT DISTINCT "{column_name}" 
            FROM "{table_name}" 
            WHERE "{column_name}" IS NOT NULL 
            ORDER BY "{column_name}" 
            LIMIT :limit
            '''
            
            result = session.execute(text(sample_query), {'limit': limit})
            sample_values = [row[0] for row in result.fetchall()]
            
            if sample_values:
                self.logger.debug(f"Collected {len(sample_values)} sample values for {table_name}.{column_name}: "
                                f"{sample_values[:3]}..." if len(sample_values) > 3 else f"{sample_values}")
            
            return sample_values
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
                numeric_values = [float(v) for v in sample_data if v is not None and str(v).replace('.', '').replace('-', '').isdigit()]
                if numeric_values:
                    analysis.update({
                        'min_value': min(numeric_values),
                        'max_value': max(numeric_values),
                        'avg_value': sum(numeric_values) / len(numeric_values)
                    })
            except:
                pass
        elif data_type.lower() in ['character varying', 'varchar', 'text', 'char', 'character']:
            try:
                str_values = [str(v) for v in sample_data if v is not None]
                if str_values:
                    lengths = [len(s) for s in str_values]
                    analysis.update({
                        'min_length': min(lengths),
                        'max_length': max(lengths),
                        'avg_length': sum(lengths) / len(lengths)
                    })
            except:
                pass
        
        return analysis
    
    def _generate_database_summary(self, tables: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a summary of the database for AI context."""
        if not tables:
            return {}
        
        total_rows = sum(t.get('row_count', 0) for t in tables.values())
        total_columns = sum(len(t.get('columns', [])) for t in tables.values())
        
        # Analyze data types distribution
        data_types = {}
        for table in tables.values():
            for col in table.get('columns', []):
                dtype = col.get('data_type', 'unknown')
                data_types[dtype] = data_types.get(dtype, 0) + 1
        
        return {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'data_type_distribution': data_types,
            'largest_table': max(tables.items(), key=lambda x: x[1].get('row_count', 0))[0] if tables else None,
            'average_table_size': total_rows // len(tables) if tables else 0
        }

    def _validate_ai_estimates(self, estimates: Dict[str, Any], schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and sanitize AI-generated estimates.
        Matches validation approach from generator with proper bounds checking.
        """
        self.logger.info(f"Starting validation of estimates for {len(estimates)} tables")
        validated = {}
        total_estimates = 0
        total_validated = 0
        
        for table_name, table_estimates in estimates.items():
            if table_name not in schema_info.get('tables', {}):
                self.logger.warning(f"AI estimated for unknown table: {table_name}")
                continue
            
            table_info = schema_info['tables'][table_name]
            validated[table_name] = {}
            self.logger.debug(f"Validating {len(table_estimates)} column estimates for table {table_name}")
            
            for column_name, column_stats in table_estimates.items():
                total_estimates += 1
                self.logger.debug(f"Validating column {table_name}.{column_name} with stats: {column_stats}")
                
                # Find the column in schema
                column_info = None
                for col in table_info.get('columns', []):
                    if col['name'] == column_name:
                        column_info = col
                        break
                
                if not column_info:
                    self.logger.warning(f"AI estimated for unknown column: {table_name}.{column_name}")
                    continue
                
                validated_stats = {}
                validation_results = []
                
                if isinstance(column_stats, dict):
                    # Validate stanullfrac (null fraction) - must be between 0 and 1
                    if 'stanullfrac' in column_stats:
                        try:
                            original_val = float(column_stats['stanullfrac'])
                            clamped_val = max(0.0, min(1.0, original_val))  # Clamp to [0,1]
                            validated_stats['stanullfrac'] = clamped_val
                            validation_results.append(f"stanullfrac: {original_val} → {clamped_val}")
                        except (ValueError, TypeError):
                            self.logger.warning(f"Invalid stanullfrac for {table_name}.{column_name}: {column_stats['stanullfrac']}")
                    
                    # Validate stadistinct (distinct values) - from generator validation
                    if 'stadistinct' in column_stats:
                        try:
                            original_val = float(column_stats['stadistinct'])
                            row_count = table_info.get('row_count', 1)
                            
                            if original_val > 0:
                                # Positive value: ensure it doesn't exceed row count
                                clamped_val = min(original_val, row_count)
                                validated_stats['stadistinct'] = clamped_val
                                validation_results.append(f"stadistinct: {original_val} → {clamped_val} (max {row_count})")
                            else:
                                # Negative value (ratio): ensure it's between -1 and 0
                                clamped_val = max(-1.0, min(0.0, original_val))
                                validated_stats['stadistinct'] = clamped_val
                                validation_results.append(f"stadistinct: {original_val} → {clamped_val} (ratio)")
                        except (ValueError, TypeError):
                            self.logger.warning(f"Invalid stadistinct for {table_name}.{column_name}: {column_stats['stadistinct']}")
                    
                    # Validate stanumbers1 (statistical numbers array) - matches generator
                    if 'stanumbers1' in column_stats:
                        numbers = column_stats['stanumbers1']
                        if isinstance(numbers, (list, tuple)) and len(numbers) > 0:
                            # Ensure all values are numeric and reasonable
                            try:
                                validated_numbers = []
                                for n in numbers:
                                    if n is not None:
                                        original_val = float(n)
                                        # Clamp correlation values to [-1, 1] like generator does
                                        clamped_val = max(-1.0, min(1.0, original_val))
                                        validated_numbers.append(clamped_val)
                                
                                if validated_numbers:
                                    validated_stats['stanumbers1'] = validated_numbers
                                    validation_results.append(f"stanumbers1: {len(validated_numbers)} values clamped to [-1,1]")
                            except Exception as e:
                                self.logger.warning(f"Invalid stanumbers1 for {table_name}.{column_name}: {e}")
                
                if validated_stats:
                    validated[table_name][column_name] = validated_stats
                    total_validated += 1
                    self.logger.debug(f"Validated {table_name}.{column_name}: {', '.join(validation_results)}")
                else:
                    self.logger.debug(f"No valid statistics for {table_name}.{column_name}")
        
        self.logger.info(f"Validation complete: {total_validated}/{total_estimates} estimates validated")
        return validated 