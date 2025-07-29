"""
Base Data Ingester

Provides common functionality and interface for all data ingestion operations.
Implements design patterns for scalable data processing and error handling.

This module demonstrates:
- Object-oriented design principles
- Abstract base class patterns
- Comprehensive error handling
- Logging and monitoring integration
- Configuration management
- Data validation frameworks
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import boto3
import pandas as pd
import geopandas as gpd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import structlog

from ..utils.config import Config
from ..utils.metrics import MetricsCollector
from ..monitoring.health_check import HealthChecker


class BaseDataIngester(ABC):
    """
    Abstract base class for all data ingestion operations.
    
    Provides common functionality including:
    - Configuration management
    - Logging and metrics collection
    - Error handling and retry logic
    - Data validation
    - Progress tracking
    - Resource management
    """
    
    def __init__(
        self,
        config: Config,
        spark_session: Optional[SparkSession] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        """
        Initialize the base data ingester.
        
        Args:
            config: Configuration object containing ingestion parameters
            spark_session: Optional Spark session for distributed processing
            metrics_collector: Optional metrics collector for monitoring
        """
        self.config = config
        self.spark = spark_session
        self.metrics = metrics_collector or MetricsCollector()
        self.health_checker = HealthChecker()
        
        # Set up structured logging
        self.logger = structlog.get_logger(
            ingester_type=self.__class__.__name__,
            config_env=config.environment
        )
        
        # Initialize AWS clients if needed
        self._init_aws_clients()
        
        # Track ingestion statistics
        self.stats = {
            'records_processed': 0,
            'records_failed': 0,
            'bytes_processed': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
    
    def _init_aws_clients(self) -> None:
        """Initialize AWS service clients based on configuration."""
        try:
            if self.config.aws.use_aws:
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.config.aws.region,
                    aws_access_key_id=self.config.aws.access_key_id,
                    aws_secret_access_key=self.config.aws.secret_access_key
                )
                
                self.athena_client = boto3.client(
                    'athena',
                    region_name=self.config.aws.region,
                    aws_access_key_id=self.config.aws.access_key_id,
                    aws_secret_access_key=self.config.aws.secret_access_key
                )
                
                self.logger.info("AWS clients initialized successfully")
        except Exception as e:
            self.logger.error("Failed to initialize AWS clients", error=str(e))
            raise
    
    @abstractmethod
    def extract(self, source: Union[str, Path, Dict]) -> Any:
        """
        Extract data from the specified source.
        
        Args:
            source: Data source specification (path, URL, or configuration)
            
        Returns:
            Extracted data in appropriate format
        """
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """
        Validate the extracted data for quality and completeness.
        
        Args:
            data: Data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """
        Apply initial transformations to the extracted data.
        
        Args:
            data: Raw extracted data
            
        Returns:
            Transformed data ready for further processing
        """
        pass
    
    def load(self, data: Any, destination: Union[str, Path]) -> bool:
        """
        Load transformed data to the specified destination.
        
        Args:
            data: Transformed data to load
            destination: Target destination (S3 path, database, etc.)
            
        Returns:
            True if load was successful, False otherwise
        """
        try:
            self.logger.info("Starting data load", destination=str(destination))
            
            # Determine destination type and route accordingly
            if str(destination).startswith('s3://'):
                return self._load_to_s3(data, destination)
            elif str(destination).startswith('postgresql://'):
                return self._load_to_postgres(data, destination)
            else:
                return self._load_to_file(data, destination)
                
        except Exception as e:
            self.logger.error("Data load failed", error=str(e), destination=str(destination))
            self.stats['errors'].append(f"Load error: {str(e)}")
            return False
    
    def _load_to_s3(self, data: Any, s3_path: str) -> bool:
        """Load data to S3 bucket."""
        try:
            # Parse S3 path
            bucket, key = s3_path.replace('s3://', '').split('/', 1)
            
            # Convert data to appropriate format for S3 storage
            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                # Save as Parquet for efficient storage and querying
                parquet_buffer = data.to_parquet()
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=f"{key}.parquet",
                    Body=parquet_buffer
                )
            else:
                # Handle other data types
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=str(data)
                )
            
            self.logger.info("Data loaded to S3 successfully", s3_path=s3_path)
            return True
            
        except Exception as e:
            self.logger.error("S3 load failed", error=str(e), s3_path=s3_path)
            return False
    
    def _load_to_postgres(self, data: Any, connection_string: str) -> bool:
        """Load data to PostgreSQL database."""
        try:
            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                # Use pandas/geopandas to_sql method
                data.to_sql(
                    name=self.config.database.table_name,
                    con=connection_string,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=self.config.database.chunk_size
                )
            
            self.logger.info("Data loaded to PostgreSQL successfully")
            return True
            
        except Exception as e:
            self.logger.error("PostgreSQL load failed", error=str(e))
            return False
    
    def _load_to_file(self, data: Any, file_path: Union[str, Path]) -> bool:
        """Load data to local file system."""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                # Determine format based on file extension
                if file_path.suffix.lower() == '.parquet':
                    data.to_parquet(file_path)
                elif file_path.suffix.lower() == '.csv':
                    data.to_csv(file_path, index=False)
                elif file_path.suffix.lower() in ['.geojson', '.json']:
                    if isinstance(data, gpd.GeoDataFrame):
                        data.to_file(file_path, driver='GeoJSON')
                    else:
                        data.to_json(file_path)
                else:
                    # Default to parquet for efficiency
                    data.to_parquet(file_path.with_suffix('.parquet'))
            
            self.logger.info("Data loaded to file successfully", file_path=str(file_path))
            return True
            
        except Exception as e:
            self.logger.error("File load failed", error=str(e), file_path=str(file_path))
            return False
    
    def ingest(
        self,
        source: Union[str, Path, Dict],
        destination: Union[str, Path],
        validate_data: bool = True
    ) -> Dict[str, Any]:
        """
        Complete ingestion workflow: extract, validate, transform, and load.
        
        Args:
            source: Data source specification
            destination: Target destination for processed data
            validate_data: Whether to perform data validation
            
        Returns:
            Dictionary containing ingestion results and statistics
        """
        self.stats['start_time'] = time.time()
        
        try:
            self.logger.info(
                "Starting data ingestion",
                source=str(source),
                destination=str(destination)
            )
            
            # Extract data
            self.logger.info("Extracting data from source")
            data = self.extract(source)
            
            if data is None:
                raise ValueError("Data extraction returned None")
            
            # Validate data if requested
            if validate_data:
                self.logger.info("Validating extracted data")
                if not self.validate(data):
                    raise ValueError("Data validation failed")
            
            # Transform data
            self.logger.info("Transforming data")
            transformed_data = self.transform(data)
            
            # Load data to destination
            self.logger.info("Loading data to destination")
            load_success = self.load(transformed_data, destination)
            
            if not load_success:
                raise RuntimeError("Data load operation failed")
            
            # Update statistics
            self._update_stats(transformed_data)
            
            self.logger.info(
                "Data ingestion completed successfully",
                records_processed=self.stats['records_processed'],
                duration_seconds=self.stats['end_time'] - self.stats['start_time']
            )
            
            # Collect metrics
            self.metrics.increment_counter('ingestion_success')
            self.metrics.record_histogram(
                'ingestion_duration',
                self.stats['end_time'] - self.stats['start_time']
            )
            
            return {
                'success': True,
                'stats': self.stats,
                'message': 'Ingestion completed successfully'
            }
            
        except Exception as e:
            self.stats['end_time'] = time.time()
            self.stats['errors'].append(str(e))
            
            self.logger.error(
                "Data ingestion failed",
                error=str(e),
                duration_seconds=self.stats['end_time'] - self.stats['start_time']
            )
            
            # Collect error metrics
            self.metrics.increment_counter('ingestion_failure')
            
            return {
                'success': False,
                'stats': self.stats,
                'message': f'Ingestion failed: {str(e)}'
            }
    
    def _update_stats(self, data: Any) -> None:
        """Update ingestion statistics based on processed data."""
        self.stats['end_time'] = time.time()
        
        if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
            self.stats['records_processed'] = len(data)
            # Estimate bytes processed (rough approximation)
            self.stats['bytes_processed'] = data.memory_usage(deep=True).sum()
        elif hasattr(data, '__len__'):
            self.stats['records_processed'] = len(data)
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the ingester."""
        return self.health_checker.check_ingester_health(self)
    
    def cleanup(self) -> None:
        """Clean up resources and connections."""
        try:
            if hasattr(self, 'spark') and self.spark:
                # Don't stop the Spark session as it might be shared
                pass
            
            self.logger.info("Ingester cleanup completed")
            
        except Exception as e:
            self.logger.error("Error during cleanup", error=str(e))
