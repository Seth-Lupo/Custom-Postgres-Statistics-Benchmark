"""
AI Response Handler Module

This module handles:
1. Making API calls to AI providers (LLMProxy, OpenAI)
2. Parsing AI responses (CSV/JSON)
3. Converting responses to pg_stats DataFrame format

Output: pandas DataFrame with pg_stats columns
"""

import json
import csv
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Dict, Any, Optional, Tuple
import os

# Import OpenAI for OpenAI provider support
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

class AIResponseHandler:
    """Handles AI API interactions and response parsing."""
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the AI response handler.
        
        Args:
            config: Configuration dictionary with API settings
            logger: Logger instance
        """
        self.logger = logger
        self.config = config
        
        # Provider configuration
        self.provider = config.get('provider', 'llmproxy')
        self.model = config.get('model', 'us.anthropic.claude-3-haiku-20240307-v1:0')
        self.temperature = config.get('temperature', 0.3)
        self.session_id = config.get('session_id', 'schneider_stats_session')
        
        # API configuration
        if self.provider == 'llmproxy':
            self.api_endpoint = os.getenv('LLMPROXY_API_ENDPOINT', 'https://a061igc186.execute-api.us-east-1.amazonaws.com/dev')
            self.api_key = os.getenv('LLMPROXY_API_KEY', 'blocked')
        elif self.provider == 'openai':
            self.api_endpoint = os.getenv('OPENAI_API_URL', 'https://api.openai.com/v1')
            self.api_key = os.getenv('OPENAI_API_KEY')
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY environment variable is required when using OpenAI provider")
            if not OPENAI_AVAILABLE:
                raise ImportError("OpenAI library is not installed. Please run: pip install openai")
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
        
        # RAG settings for LLMProxy
        self.rag_usage = config.get('rag_usage', False)
        self.rag_threshold = config.get('rag_threshold', 0.5)
        self.rag_k = config.get('rag_k', 0)
        
        # Prompts
        self.system_prompt = config.get('system_prompt')
        self.estimation_prompt = config.get('estimation_prompt')
        
        # Retry settings
        self.max_retries = config.get('max_retries', 3)
    
    def get_ai_estimates(self, schema_info: Dict[str, Any]) -> pd.DataFrame:
        """
        Get AI estimates for database statistics.
        
        Args:
            schema_info: Database schema information
            
        Returns:
            DataFrame with pg_stats columns or empty DataFrame on failure
        """
        self.logger.info("Starting AI estimation process")
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"AI estimation attempt {attempt + 1}/{self.max_retries}")
                
                # Format the prompt
                formatted_prompt = self._format_prompt(schema_info)
                
                # Make API call
                ai_response = self._call_ai_api(self.system_prompt, formatted_prompt)
                
                if ai_response:
                    # Parse response to DataFrame
                    df = self._parse_response_to_dataframe(ai_response)
                    
                    if not df.empty:
                        self.logger.info(f"Successfully parsed {len(df)} rows from AI response")
                        return df
                    else:
                        self.logger.warning("AI response parsed but resulted in empty DataFrame")
                else:
                    self.logger.warning("No valid AI response received")
                
                # Retry on failure
                if attempt < self.max_retries - 1:
                    self.logger.debug("Retrying AI estimation...")
                    
            except Exception as e:
                self.logger.error(f"Error in AI estimation attempt {attempt + 1}: {str(e)}")
                if attempt < self.max_retries - 1:
                    continue
        
        self.logger.error(f"Failed to get valid AI estimates after {self.max_retries} attempts")
        return pd.DataFrame()
    
    def _format_prompt(self, schema_info: Dict[str, Any]) -> str:
        """Format the estimation prompt with schema information."""
        # Build column names list
        col_names_list = []
        tables_summary = {}
        
        for table_name, table_data in schema_info.get('tables', {}).items():
            columns_summary = []
            
            for col in table_data.get('columns', []):
                col_names_list.append(f"{table_name}.{col['name']}")
                
                col_summary = {
                    'name': col['name'],
                    'type': col['data_type'],
                    'nullable': col['nullable']
                }
                
                if col.get('comment'):
                    col_summary['comment'] = col['comment']
                
                if 'sample_stats' in col and col['sample_stats']:
                    col_summary['sample_stats'] = col['sample_stats']
                
                columns_summary.append(col_summary)
            
            table_summary = {
                'row_count': table_data.get('row_count', 0),
                'columns': columns_summary,
                'table_size': table_data.get('table_size', 'unknown')
            }
            
            if table_data.get('comment'):
                table_summary['comment'] = table_data['comment']
            
            tables_summary[table_name] = table_summary
        
        # Format the prompt
        return self.estimation_prompt.format(
            col_names=', '.join(col_names_list),
            size=schema_info.get('database_size', 'unknown'),
            sample_data=json.dumps(tables_summary, indent=2, default=str)
        )
    
    def _call_ai_api(self, system_prompt: str, user_prompt: str) -> str:
        """Call the appropriate AI API based on provider."""
        if self.provider == 'llmproxy':
            return self._call_llmproxy_api(system_prompt, user_prompt)
        elif self.provider == 'openai':
            return self._call_openai_api(system_prompt, user_prompt)
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def _call_llmproxy_api(self, system_prompt: str, user_prompt: str) -> str:
        """Call the LLM proxy API."""
        payload = {
            "model": self.model,
            "system": system_prompt,
            "query": user_prompt,
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
        
        response = requests.post(
            self.api_endpoint,
            json=payload,
            headers=headers,
            timeout=300
        )
        
        if response.status_code != 200:
            raise requests.RequestException(f"HTTP request failed with status {response.status_code}: {response.reason}")
        
        try:
            response_data = response.json()
            if isinstance(response_data, dict) and 'result' in response_data:
                return response_data['result']
            else:
                return response.text
        except json.JSONDecodeError:
            return response.text
    
    def _call_openai_api(self, system_prompt: str, user_prompt: str) -> str:
        """Call the OpenAI API."""
        client = openai.OpenAI(
            api_key=self.api_key,
            base_url=self.api_endpoint
        )
        
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.temperature,
            timeout=300
        )
        
        return response.choices[0].message.content
    
    def _parse_response_to_dataframe(self, response: str) -> pd.DataFrame:
        """
        Parse AI response to pandas DataFrame with pg_stats columns.
        
        Expected columns:
        - attname: table.column name
        - null_frac: fraction of null values
        - avg_width: average width in bytes
        - n_distinct: number of distinct values
        - most_common_vals: array of most common values
        - most_common_freqs: array of frequencies
        - histogram_bounds: histogram bounds array
        - correlation: correlation coefficient
        """
        try:
            # Clean and prepare response
            content = response.strip()
            
            # Convert commas to semicolons if needed
            if ';' not in content and ',' in content:
                content = content.replace(',', ';')
                self.logger.debug("Converted comma-separated to semicolon-separated")
            
            # Parse as CSV
            df = pd.read_csv(StringIO(content), delimiter=';')
            
            # Validate required columns
            if 'attname' not in df.columns:
                self.logger.error(f"Missing required 'attname' column. Found columns: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Clean and standardize data types
            df = self._clean_dataframe(df)
            
            self.logger.info(f"Parsed DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to parse AI response to DataFrame: {str(e)}")
            return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize DataFrame columns."""
        # Convert numeric columns
        numeric_columns = ['null_frac', 'avg_width', 'n_distinct', 'correlation']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle array columns (keep as strings for now, will be processed later)
        array_columns = ['most_common_vals', 'most_common_freqs', 'histogram_bounds']
        for col in array_columns:
            if col in df.columns:
                # Replace NULL/null with None
                df[col] = df[col].replace(['NULL', 'null'], None)
        
        return df
    
    def save_interaction(self, prompt: str, response: str, save_func: Optional[callable] = None):
        """Save AI interaction if save function is provided."""
        if save_func:
            try:
                save_func(prompt, response)
            except Exception as e:
                self.logger.error(f"Failed to save AI interaction: {str(e)}")