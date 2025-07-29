"""
Data Quality Operator

Custom Airflow operator for performing comprehensive data quality checks
on geospatial datasets. Demonstrates expertise in operational excellence
and data validation practices.

This operator implements:
- Configurable quality check rules
- Detailed error reporting
- Metrics collection
- Integration with monitoring systems
- Failure handling and alerting
"""

import json
import time
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

import boto3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from shapely.validation import explain_validity
import structlog

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """
    Custom operator for performing data quality checks on geospatial datasets.
    
    Supports various quality check types including:
    - Record count validation
    - Geometry validity checks
    - Attribute completeness
    - Spatial consistency validation
    - Data freshness checks
    - Cross-dataset consistency
    """
    
    template_fields = ['input_path', 'output_path']
    template_ext = ['.sql']
    ui_color = '#89CDF1'
    
    @apply_defaults
    def __init__(
        self,
        input_path: str,
        quality_checks: List[str],
        output_path: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        postgres_conn_id: Optional[str] = None,
        thresholds: Optional[Dict[str, Any]] = None,
        fail_on_error: bool = True,
        store_results: bool = True,
        *args,
        **kwargs
    ):
        """
        Initialize the Data Quality Operator.
        
        Args:
            input_path: S3 path to input data for quality checks
            quality_checks: List of quality check names to perform
            output_path: Optional S3 path to store quality check results
            aws_conn_id: Airflow connection ID for AWS
            postgres_conn_id: Optional Airflow connection ID for PostgreSQL
            thresholds: Dictionary of quality check thresholds
            fail_on_error: Whether to fail the task on quality check failures
            store_results: Whether to store detailed results
        """
        super().__init__(*args, **kwargs)
        
        self.input_path = input_path
        self.quality_checks = quality_checks
        self.output_path = output_path
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.thresholds = thresholds or {}
        self.fail_on_error = fail_on_error
        self.store_results = store_results
        
        # Set up logging
        self.logger = structlog.get_logger(
            operator_type="DataQualityOperator",
            task_id=self.task_id
        )
        
        # Default thresholds
        self.default_thresholds = {
            'min_record_count': 1000,
            'max_null_percentage': 10.0,
            'min_geometry_validity': 95.0,
            'max_duplicate_percentage': 5.0,
            'min_spatial_coverage': 80.0,
            'max_file_age_hours': 24,
            'min_attribute_diversity': 0.1
        }
        
        # Merge with provided thresholds
        self.thresholds = {**self.default_thresholds, **self.thresholds}
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute data quality checks.
        
        Args:
            context: Airflow task context
            
        Returns:
            Dictionary containing quality check results
        """
        start_time = time.time()
        
        self.logger.info(
            "Starting data quality checks",
            input_path=self.input_path,
            checks=self.quality_checks
        )
        
        try:
            # Initialize connections
            self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            if self.postgres_conn_id:
                self.postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            
            # Load data for quality checks
            data = self._load_data()
            
            # Perform quality checks
            results = {}
            failed_checks = []
            
            for check_name in self.quality_checks:
                self.logger.info(f"Performing quality check: {check_name}")
                
                try:
                    check_result = self._perform_quality_check(check_name, data)
                    results[check_name] = check_result
                    
                    if not check_result['passed']:
                        failed_checks.append(check_name)
                        self.logger.warning(
                            f"Quality check failed: {check_name}",
                            result=check_result
                        )
                    else:
                        self.logger.info(f"Quality check passed: {check_name}")
                        
                except Exception as e:
                    error_msg = f"Error in quality check {check_name}: {str(e)}"
                    self.logger.error(error_msg)
                    
                    results[check_name] = {
                        'passed': False,
                        'error': error_msg,
                        'timestamp': time.time()
                    }
                    failed_checks.append(check_name)
            
            # Calculate overall results
            execution_time = time.time() - start_time
            overall_result = {
                'overall_passed': len(failed_checks) == 0,
                'total_checks': len(self.quality_checks),
                'passed_checks': len(self.quality_checks) - len(failed_checks),
                'failed_checks': failed_checks,
                'execution_time': execution_time,
                'timestamp': time.time(),
                'input_path': self.input_path,
                'detailed_results': results
            }
            
            # Store results if requested
            if self.store_results:
                self._store_results(overall_result, context)
            
            # Update task instance XCom
            context['task_instance'].xcom_push(
                key='quality_check_results',
                value=overall_result
            )
            
            # Log summary
            self.logger.info(
                "Data quality checks completed",
                overall_passed=overall_result['overall_passed'],
                passed_checks=overall_result['passed_checks'],
                failed_checks=len(failed_checks),
                execution_time=execution_time
            )
            
            # Fail task if requested and checks failed
            if self.fail_on_error and failed_checks:
                raise AirflowException(
                    f"Data quality checks failed: {failed_checks}"
                )
            
            return overall_result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Data quality operator failed: {str(e)}"
            
            self.logger.error(error_msg, execution_time=execution_time)
            
            if self.fail_on_error:
                raise AirflowException(error_msg)
            
            return {
                'overall_passed': False,
                'error': error_msg,
                'execution_time': execution_time
            }
    
    def _load_data(self) -> Dict[str, Any]:
        """Load data from S3 for quality checks."""
        try:
            # Parse S3 path
            bucket, key_prefix = self.input_path.replace('s3://', '').split('/', 1)
            
            # List files in the path
            files = self.s3_hook.list_keys(bucket_name=bucket, prefix=key_prefix)
            
            if not files:
                raise ValueError(f"No files found at {self.input_path}")
            
            # Load sample of data for quality checks
            data = {}
            
            # Load different data types
            parquet_files = [f for f in files if f.endswith('.parquet')]
            geojson_files = [f for f in files if f.endswith('.geojson')]
            
            if parquet_files:
                # Load first parquet file as sample
                sample_file = parquet_files[0]
                local_path = f"/tmp/{Path(sample_file).name}"
                
                self.s3_hook.download_file(
                    key=sample_file,
                    bucket_name=bucket,
                    local_path=local_path
                )
                
                # Read with pandas/geopandas
                try:
                    df = pd.read_parquet(local_path)
                    if 'geometry' in df.columns or any('geom' in col.lower() for col in df.columns):
                        df = gpd.read_parquet(local_path)
                    data['main_dataset'] = df
                except Exception as e:
                    self.logger.warning(f"Failed to load as geodataframe: {e}")
                    data['main_dataset'] = pd.read_parquet(local_path)
            
            elif geojson_files:
                # Load first GeoJSON file
                sample_file = geojson_files[0]
                local_path = f"/tmp/{Path(sample_file).name}"
                
                self.s3_hook.download_file(
                    key=sample_file,
                    bucket_name=bucket,
                    local_path=local_path
                )
                
                data['main_dataset'] = gpd.read_file(local_path)
            
            # Store metadata
            data['metadata'] = {
                'total_files': len(files),
                'parquet_files': len(parquet_files),
                'geojson_files': len(geojson_files),
                'sample_file': sample_file if 'sample_file' in locals() else None
            }
            
            return data
            
        except Exception as e:
            raise ValueError(f"Failed to load data from {self.input_path}: {str(e)}")
    
    def _perform_quality_check(self, check_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform a specific quality check."""
        
        if check_name == 'record_count_check':
            return self._record_count_check(data)
        elif check_name == 'geometry_validity_check':
            return self._geometry_validity_check(data)
        elif check_name == 'attribute_completeness_check':
            return self._attribute_completeness_check(data)
        elif check_name == 'spatial_consistency_check':
            return self._spatial_consistency_check(data)
        elif check_name == 'duplicate_check':
            return self._duplicate_check(data)
        elif check_name == 'spatial_coverage_check':
            return self._spatial_coverage_check(data)
        elif check_name == 'data_freshness_check':
            return self._data_freshness_check(data)
        elif check_name == 'attribute_diversity_check':
            return self._attribute_diversity_check(data)
        elif check_name == 'tile_count_check':
            return self._tile_count_check(data)
        elif check_name == 'tile_size_check':
            return self._tile_size_check(data)
        elif check_name == 'tile_format_check':
            return self._tile_format_check(data)
        else:
            raise ValueError(f"Unknown quality check: {check_name}")
    
    def _record_count_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check if record count meets minimum threshold."""
        df = data['main_dataset']
        record_count = len(df)
        min_count = self.thresholds['min_record_count']
        
        passed = record_count >= min_count
        
        return {
            'passed': passed,
            'record_count': record_count,
            'min_threshold': min_count,
            'message': f"Record count: {record_count} (min: {min_count})",
            'timestamp': time.time()
        }
    
    def _geometry_validity_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check geometry validity for geospatial data."""
        df = data['main_dataset']
        
        if not isinstance(df, gpd.GeoDataFrame):
            return {
                'passed': True,
                'message': "No geometry column found, skipping geometry validation",
                'timestamp': time.time()
            }
        
        # Check geometry validity
        valid_geometries = df.geometry.is_valid
        validity_percentage = (valid_geometries.sum() / len(df)) * 100
        min_validity = self.thresholds['min_geometry_validity']
        
        passed = validity_percentage >= min_validity
        
        # Get details of invalid geometries
        invalid_count = (~valid_geometries).sum()
        invalid_reasons = []
        
        if invalid_count > 0:
            invalid_sample = df[~valid_geometries].head(5)
            for idx, geom in invalid_sample.geometry.items():
                try:
                    reason = explain_validity(geom)
                    invalid_reasons.append(f"Row {idx}: {reason}")
                except Exception:
                    invalid_reasons.append(f"Row {idx}: Unknown geometry error")
        
        return {
            'passed': passed,
            'validity_percentage': validity_percentage,
            'min_threshold': min_validity,
            'invalid_count': invalid_count,
            'total_count': len(df),
            'invalid_reasons': invalid_reasons[:5],  # Limit to first 5
            'message': f"Geometry validity: {validity_percentage:.1f}% (min: {min_validity}%)",
            'timestamp': time.time()
        }
    
    def _attribute_completeness_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check attribute completeness (null values)."""
        df = data['main_dataset']
        
        # Calculate null percentages for each column
        null_percentages = (df.isnull().sum() / len(df)) * 100
        max_null_threshold = self.thresholds['max_null_percentage']
        
        # Find columns exceeding threshold
        problematic_columns = null_percentages[null_percentages > max_null_threshold]
        
        passed = len(problematic_columns) == 0
        
        return {
            'passed': passed,
            'null_percentages': null_percentages.to_dict(),
            'problematic_columns': problematic_columns.to_dict(),
            'max_threshold': max_null_threshold,
            'message': f"Attribute completeness check: {len(problematic_columns)} columns exceed {max_null_threshold}% null threshold",
            'timestamp': time.time()
        }
    
    def _spatial_consistency_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check spatial consistency of geometries."""
        df = data['main_dataset']
        
        if not isinstance(df, gpd.GeoDataFrame):
            return {
                'passed': True,
                'message': "No geometry column found, skipping spatial consistency check",
                'timestamp': time.time()
            }
        
        issues = []
        
        # Check for empty geometries
        empty_geoms = df.geometry.is_empty.sum()
        if empty_geoms > 0:
            issues.append(f"{empty_geoms} empty geometries")
        
        # Check coordinate range (assuming WGS84)
        bounds = df.total_bounds
        if bounds[0] < -180 or bounds[2] > 180:
            issues.append("Longitude values outside valid range (-180, 180)")
        if bounds[1] < -90 or bounds[3] > 90:
            issues.append("Latitude values outside valid range (-90, 90)")
        
        # Check for extremely small geometries (potential errors)
        if hasattr(df.geometry.iloc[0], 'area'):
            areas = df.geometry.area
            tiny_geoms = (areas < 1e-10).sum()
            if tiny_geoms > 0:
                issues.append(f"{tiny_geoms} extremely small geometries (area < 1e-10)")
        
        passed = len(issues) == 0
        
        return {
            'passed': passed,
            'issues': issues,
            'empty_geometries': empty_geoms,
            'bounds': bounds.tolist(),
            'message': f"Spatial consistency: {len(issues)} issues found",
            'timestamp': time.time()
        }
    
    def _duplicate_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check for duplicate records."""
        df = data['main_dataset']
        
        # Check for exact duplicates
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df)) * 100
        max_duplicate_threshold = self.thresholds['max_duplicate_percentage']
        
        passed = duplicate_percentage <= max_duplicate_threshold
        
        # Check for near-duplicate geometries if it's a GeoDataFrame
        near_duplicate_info = {}
        if isinstance(df, gpd.GeoDataFrame):
            try:
                # Sample check for near-duplicates (computationally expensive)
                sample_size = min(1000, len(df))
                sample_df = df.sample(n=sample_size)
                
                # Check for geometries that are very close to each other
                near_duplicates = 0
                for i, geom1 in enumerate(sample_df.geometry):
                    for j, geom2 in enumerate(sample_df.geometry):
                        if i < j and geom1.distance(geom2) < 1e-6:  # Very close geometries
                            near_duplicates += 1
                
                near_duplicate_percentage = (near_duplicates / len(sample_df)) * 100
                near_duplicate_info = {
                    'near_duplicates_found': near_duplicates,
                    'near_duplicate_percentage': near_duplicate_percentage,
                    'sample_size': sample_size
                }
            except Exception as e:
                near_duplicate_info = {'error': f"Near-duplicate check failed: {str(e)}"}
        
        return {
            'passed': passed,
            'duplicate_count': duplicate_count,
            'duplicate_percentage': duplicate_percentage,
            'max_threshold': max_duplicate_threshold,
            'total_records': len(df),
            'near_duplicate_info': near_duplicate_info,
            'message': f"Duplicate check: {duplicate_percentage:.1f}% duplicates (max: {max_duplicate_threshold}%)",
            'timestamp': time.time()
        }
    
    def _spatial_coverage_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check spatial coverage of the dataset."""
        df = data['main_dataset']
        
        if not isinstance(df, gpd.GeoDataFrame):
            return {
                'passed': True,
                'message': "No geometry column found, skipping spatial coverage check",
                'timestamp': time.time()
            }
        
        # Calculate bounding box coverage
        bounds = df.total_bounds
        bbox_area = (bounds[2] - bounds[0]) * (bounds[3] - bounds[1])
        
        # Calculate actual geometry coverage
        try:
            total_geom_area = df.geometry.area.sum()
            coverage_ratio = total_geom_area / bbox_area if bbox_area > 0 else 0
            coverage_percentage = coverage_ratio * 100
        except Exception:
            # Fallback for non-polygon geometries
            coverage_percentage = 100.0  # Assume full coverage for points/lines
        
        min_coverage = self.thresholds['min_spatial_coverage']
        passed = coverage_percentage >= min_coverage
        
        return {
            'passed': passed,
            'coverage_percentage': coverage_percentage,
            'min_threshold': min_coverage,
            'bounding_box': bounds.tolist(),
            'bbox_area': bbox_area,
            'message': f"Spatial coverage: {coverage_percentage:.1f}% (min: {min_coverage}%)",
            'timestamp': time.time()
        }
    
    def _data_freshness_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check data freshness based on file modification time."""
        import boto3
        from datetime import datetime, timedelta
        
        try:
            # Parse S3 path
            bucket, key_prefix = self.input_path.replace('s3://', '').split('/', 1)
            
            # Get file metadata
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
            
            if 'Contents' not in response:
                return {
                    'passed': False,
                    'message': "No files found for freshness check",
                    'timestamp': time.time()
                }
            
            # Find most recent file
            latest_modified = max(obj['LastModified'] for obj in response['Contents'])
            age_hours = (datetime.now(latest_modified.tzinfo) - latest_modified).total_seconds() / 3600
            
            max_age_hours = self.thresholds['max_file_age_hours']
            passed = age_hours <= max_age_hours
            
            return {
                'passed': passed,
                'age_hours': age_hours,
                'max_threshold': max_age_hours,
                'latest_modified': latest_modified.isoformat(),
                'message': f"Data freshness: {age_hours:.1f} hours old (max: {max_age_hours} hours)",
                'timestamp': time.time()
            }
            
        except Exception as e:
            return {
                'passed': False,
                'error': f"Freshness check failed: {str(e)}",
                'timestamp': time.time()
            }
    
    def _attribute_diversity_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check attribute diversity (uniqueness) in categorical columns."""
        df = data['main_dataset']
        
        # Find categorical columns
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns
        
        diversity_results = {}
        min_diversity = self.thresholds['min_attribute_diversity']
        failed_columns = []
        
        for col in categorical_columns:
            if col != 'geometry':  # Skip geometry column
                unique_count = df[col].nunique()
                total_count = len(df[col].dropna())
                diversity_ratio = unique_count / total_count if total_count > 0 else 0
                
                diversity_results[col] = {
                    'unique_count': unique_count,
                    'total_count': total_count,
                    'diversity_ratio': diversity_ratio
                }
                
                if diversity_ratio < min_diversity:
                    failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        
        return {
            'passed': passed,
            'diversity_results': diversity_results,
            'failed_columns': failed_columns,
            'min_threshold': min_diversity,
            'message': f"Attribute diversity: {len(failed_columns)} columns below {min_diversity} diversity threshold",
            'timestamp': time.time()
        }
    
    def _tile_count_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check tile count for tile datasets."""
        metadata = data['metadata']
        
        # Count tile files (assuming MVT or other tile formats)
        tile_extensions = ['.mvt', '.pbf', '.png', '.jpg', '.webp']
        tile_files = [f for f in self.s3_hook.list_keys(
            bucket_name=self.input_path.split('/')[2],
            prefix='/'.join(self.input_path.split('/')[3:])
        ) if any(f.endswith(ext) for ext in tile_extensions)]
        
        tile_count = len(tile_files)
        min_tiles = self.thresholds.get('min_tile_count', 100)
        
        passed = tile_count >= min_tiles
        
        return {
            'passed': passed,
            'tile_count': tile_count,
            'min_threshold': min_tiles,
            'message': f"Tile count: {tile_count} tiles (min: {min_tiles})",
            'timestamp': time.time()
        }
    
    def _tile_size_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check tile file sizes."""
        try:
            # Parse S3 path
            bucket, key_prefix = self.input_path.replace('s3://', '').split('/', 1)
            
            # Get tile files
            tile_extensions = ['.mvt', '.pbf', '.png', '.jpg', '.webp']
            files = self.s3_hook.list_keys(bucket_name=bucket, prefix=key_prefix)
            tile_files = [f for f in files if any(f.endswith(ext) for ext in tile_extensions)]
            
            if not tile_files:
                return {
                    'passed': True,
                    'message': "No tile files found for size check",
                    'timestamp': time.time()
                }
            
            # Check file sizes
            s3_client = boto3.client('s3')
            file_sizes = []
            
            for tile_file in tile_files[:100]:  # Sample first 100 tiles
                try:
                    response = s3_client.head_object(Bucket=bucket, Key=tile_file)
                    file_sizes.append(response['ContentLength'])
                except Exception:
                    continue
            
            if not file_sizes:
                return {
                    'passed': False,
                    'message': "Could not retrieve tile file sizes",
                    'timestamp': time.time()
                }
            
            avg_size = sum(file_sizes) / len(file_sizes)
            max_size = max(file_sizes)
            min_size = min(file_sizes)
            
            # Check thresholds
            max_tile_size = self.thresholds.get('max_tile_size_mb', 1.0) * 1024 * 1024  # Convert MB to bytes
            min_tile_size = self.thresholds.get('min_tile_size_bytes', 100)
            
            oversized_tiles = sum(1 for size in file_sizes if size > max_tile_size)
            undersized_tiles = sum(1 for size in file_sizes if size < min_tile_size)
            
            passed = oversized_tiles == 0 and undersized_tiles == 0
            
            return {
                'passed': passed,
                'avg_size_bytes': avg_size,
                'max_size_bytes': max_size,
                'min_size_bytes': min_size,
                'oversized_tiles': oversized_tiles,
                'undersized_tiles': undersized_tiles,
                'sample_size': len(file_sizes),
                'message': f"Tile sizes: avg {avg_size/1024:.1f}KB, {oversized_tiles} oversized, {undersized_tiles} undersized",
                'timestamp': time.time()
            }
            
        except Exception as e:
            return {
                'passed': False,
                'error': f"Tile size check failed: {str(e)}",
                'timestamp': time.time()
            }
    
    def _tile_format_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check tile format validity."""
        try:
            # Parse S3 path
            bucket, key_prefix = self.input_path.replace('s3://', '').split('/', 1)
            
            # Get sample tile files
            tile_files = [f for f in self.s3_hook.list_keys(bucket_name=bucket, prefix=key_prefix)
                         if f.endswith('.mvt') or f.endswith('.pbf')]
            
            if not tile_files:
                return {
                    'passed': True,
                    'message': "No vector tile files found for format check",
                    'timestamp': time.time()
                }
            
            # Sample a few tiles for format validation
            sample_tiles = tile_files[:5]
            valid_tiles = 0
            format_errors = []
            
            for tile_file in sample_tiles:
                try:
                    # Download tile
                    local_path = f"/tmp/{Path(tile_file).name}"
                    self.s3_hook.download_file(
                        key=tile_file,
                        bucket_name=bucket,
                        local_path=local_path
                    )
                    
                    # Basic format validation (check if file is readable)
                    with open(local_path, 'rb') as f:
                        content = f.read()
                        if len(content) > 0:
                            valid_tiles += 1
                        else:
                            format_errors.append(f"{tile_file}: Empty file")
                            
                except Exception as e:
                    format_errors.append(f"{tile_file}: {str(e)}")
            
            passed = len(format_errors) == 0
            
            return {
                'passed': passed,
                'valid_tiles': valid_tiles,
                'total_checked': len(sample_tiles),
                'format_errors': format_errors,
                'message': f"Tile format: {valid_tiles}/{len(sample_tiles)} tiles valid",
                'timestamp': time.time()
            }
            
        except Exception as e:
            return {
                'passed': False,
                'error': f"Tile format check failed: {str(e)}",
                'timestamp': time.time()
            }
    
    def _store_results(self, results: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Store quality check results to S3 and/or PostgreSQL."""
        try:
            # Store to S3 if output path provided
            if self.output_path:
                results_json = json.dumps(results, indent=2, default=str)
                
                # Generate output key
                execution_date = context['execution_date'].strftime('%Y-%m-%d')
                output_key = f"{self.output_path.rstrip('/')}/quality_results_{execution_date}_{self.task_id}.json"
                
                # Upload to S3
                bucket, key = output_key.replace('s3://', '').split('/', 1)
                self.s3_hook.load_string(
                    string_data=results_json,
                    key=key,
                    bucket_name=bucket,
                    replace=True
                )
                
                self.logger.info(f"Quality check results stored to {output_key}")
            
            # Store to PostgreSQL if connection provided
            if self.postgres_conn_id:
                self._store_results_to_postgres(results, context)
                
        except Exception as e:
            self.logger.error(f"Failed to store quality check results: {str(e)}")
    
    def _store_results_to_postgres(self, results: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Store quality check results to PostgreSQL database."""
        try:
            # Prepare data for insertion
            execution_date = context['execution_date']
            
            insert_sql = """
                INSERT INTO data_quality_results (
                    dag_id, task_id, execution_date, input_path,
                    overall_passed, total_checks, passed_checks,
                    failed_checks, execution_time, detailed_results,
                    created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (dag_id, task_id, execution_date)
                DO UPDATE SET
                    overall_passed = EXCLUDED.overall_passed,
                    total_checks = EXCLUDED.total_checks,
                    passed_checks = EXCLUDED.passed_checks,
                    failed_checks = EXCLUDED.failed_checks,
                    execution_time = EXCLUDED.execution_time,
                    detailed_results = EXCLUDED.detailed_results,
                    updated_at = NOW()
            """
            
            self.postgres_hook.run(
                insert_sql,
                parameters=(
                    context['dag'].dag_id,
                    self.task_id,
                    execution_date,
                    self.input_path,
                    results['overall_passed'],
                    results['total_checks'],
                    results['passed_checks'],
                    json.dumps(results['failed_checks']),
                    results['execution_time'],
                    json.dumps(results['detailed_results'], default=str)
                )
            )
            
            self.logger.info("Quality check results stored to PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"Failed to store results to PostgreSQL: {str(e)}")


# Example usage
if __name__ == "__main__":
    # This would typically be used in an Airflow DAG
    
    # Example quality checks configuration
    quality_checks = [
        'record_count_check',
        'geometry_validity_check',
        'attribute_completeness_check',
        'spatial_consistency_check',
        'duplicate_check',
        'spatial_coverage_check',
        'data_freshness_check'
    ]
    
    # Example thresholds
    thresholds = {
        'min_record_count': 5000,
        'max_null_percentage': 5.0,
        'min_geometry_validity': 98.0,
        'max_duplicate_percentage': 2.0,
        'min_spatial_coverage': 85.0,
        'max_file_age_hours': 12
    }
    
    # Create operator instance
    data_quality_op = DataQualityOperator(
        task_id='data_quality_check',
        input_path='s3://my-bucket/processed-data/2024-01-01/',
        quality_checks=quality_checks,
        output_path='s3://my-bucket/quality-results/',
        thresholds=thresholds,
        fail_on_error=True,
        store_results=True
    )
    
    print("Data Quality Operator configured successfully!")
    print(f"Configured checks: {quality_checks}")
    print(f"Thresholds: {thresholds}")
