from typing import List, Optional, Dict, Any
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp
import os
import sys
import logging
import numpy as np
from functools import lru_cache

logger = logging.getLogger(__name__)

class LogAnalysisService:
    def __init__(self):
        self.df = None
        self.spark = None

    async def initialize(self):
        """Initialize Spark and load data"""
        try:
            self.spark = self._init_spark()
            self.df = await self._load_and_process_logs()
            logger.info("Data loaded successfully")
        except Exception as e:
            logger.error(f"Failed to initialize service: {e}")
            raise

    def _init_spark(self) -> SparkSession:
        """Initialize Spark session"""
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        return SparkSession.builder \
            .appName("NASA_Logs_Analysis") \
            .config("spark.driver.memory", "4g") \
            .config("spark.ui.port", "4050") \
            .getOrCreate()

    async def _load_and_process_logs(self) -> pd.DataFrame:
        """Load and process log data"""
        try:
            base_df = self.spark.read.text("access_log_JulAug_95.txt")
            
            logs_df = base_df.select(
                regexp_extract('value', r'(^\S+\.[\S+\.]+\S+)\s', 1).alias('host'),
                regexp_extract('value', r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 1).alias('method'),
                regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 2).alias('endpoint'),
                regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 3).alias('protocol'),
                regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias('status'),
                regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')
            )

            pandas_df = logs_df.toPandas()
            pandas_df['timestamp'] = pd.to_datetime(pandas_df['timestamp'], format='%d/%b/%Y:%H:%M:%S %z')
            pandas_df['content_size'] = pandas_df['content_size'].fillna(0)
            
            return pandas_df

        except Exception as e:
            logger.error(f"Error loading logs: {e}")
            raise

    def _filter_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        methods: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Apply filters to the dataframe"""
        filtered_df = self.df.copy()
        
        if start_date:
            filtered_df = filtered_df[filtered_df['timestamp'] >= start_date]
        if end_date:
            filtered_df = filtered_df[filtered_df['timestamp'] <= end_date]
        if methods:
            filtered_df = filtered_df[filtered_df['method'].isin(methods)]
            
        return filtered_df

    async def get_status_distribution(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        methods: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get status code distribution with filters"""
        filtered_df = self._filter_data(start_date, end_date, methods)
        
        def get_status_category(status):
            if 200 <= status < 300: return '2xx (Success)'
            elif 300 <= status < 400: return '3xx (Redirection)'
            elif 400 <= status < 500: return '4xx (Client Error)'
            elif 500 <= status < 600: return '5xx (Server Error)'
            else: return 'Other'
            
        filtered_df['status_category'] = filtered_df['status'].apply(get_status_category)
        status_counts = filtered_df['status_category'].value_counts().reset_index()
        
        return status_counts.to_dict(orient='records')

    async def get_top_endpoints(
        self,
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        methods: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get most requested endpoints with filters"""
        filtered_df = self._filter_data(start_date, end_date, methods)
        endpoint_counts = filtered_df['endpoint'].value_counts().head(limit).reset_index()
        endpoint_counts.columns = ['endpoint', 'count']
        
        return endpoint_counts.to_dict(orient='records')

    async def get_method_distribution(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get HTTP method distribution"""
        filtered_df = self._filter_data(start_date, end_date)
        method_counts = filtered_df['method'].value_counts().reset_index()
        method_counts.columns = ['method', 'count']
        
        return method_counts.to_dict(orient='records')

    async def get_content_size_distribution(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        bin_count: int = 50
    ) -> Dict[str, Any]:
        """Get content size distribution statistics"""
        filtered_df = self._filter_data(start_date, end_date)
        
        # Convert to KB
        sizes_kb = filtered_df['content_size'] / 1024
        
        hist, bin_edges = np.histogram(sizes_kb, bins=bin_count)
        
        return {
            'histogram': hist.tolist(),
            'bin_edges': bin_edges.tolist(),
            'mean': sizes_kb.mean(),
            'median': sizes_kb.median(),
            'max': sizes_kb.max(),
            'min': sizes_kb.min()
        }

    async def get_host_frequency(
        self,
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get most frequent hosts"""
        filtered_df = self._filter_data(start_date, end_date)
        host_counts = filtered_df['host'].value_counts().head(limit).reset_index()
        host_counts.columns = ['host', 'count']
        
        return host_counts.to_dict(orient='records')

    async def get_traffic_pattern(
        self,
        pattern_type: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get traffic patterns (hourly or daily)"""
        filtered_df = self._filter_data(start_date, end_date)
        
        if pattern_type == "hourly":
            traffic = filtered_df['timestamp'].dt.hour.value_counts().sort_index()
            return {'hours': list(traffic.index), 'counts': list(traffic.values)}
        else:
            traffic = filtered_df['timestamp'].dt.day_name().value_counts()
            return {'days': list(traffic.index), 'counts': list(traffic.values)}

    async def get_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get overall statistics"""
        filtered_df = self._filter_data(start_date, end_date)
        
        return {
            'total_requests': len(filtered_df),
            'unique_hosts': filtered_df['host'].nunique(),
            'total_data_mb': filtered_df['content_size'].sum() / (1024 * 1024),
            'avg_response_size_kb': filtered_df['content_size'].mean() / 1024,
            'success_rate': (filtered_df['status'].between(200, 299).sum() / len(filtered_df)) * 100
        }