"""
Spark Geospatial Data Processor

Implements distributed geospatial data processing using Apache Spark with
optimized configurations for large-scale basemap data operations.

This module demonstrates:
- Advanced Spark configuration and optimization
- Distributed geospatial operations
- Custom UDFs for spatial processing
- Memory management and performance tuning
- Fault tolerance and error recovery
- Integration with various data sources and formats
"""

import os
import time
from typing import Dict, List, Optional, Any, Union, Tuple
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from shapely.wkt import loads as wkt_loads
from shapely.wkb import loads as wkb_loads

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, regexp_extract, split, explode, udf, broadcast,
    collect_list, struct, array, lit, coalesce, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    ArrayType, BooleanType, TimestampType, BinaryType
)
from pyspark.sql.window import Window
from pyspark import SparkConf
import structlog

from ..utils.config import Config
from ..utils.spatial_utils import SpatialUtils
from ..utils.spark_utils import SparkOptimizer
from ..monitoring.metrics import MetricsCollector


class SparkGeoProcessor:
    """
    High-performance distributed geospatial data processor using Apache Spark.
    
    Provides optimized processing capabilities for large-scale basemap data
    including spatial operations, data conflation, quality assurance, and
    multi-format I/O operations.
    """
    
    def __init__(
        self,
        config: Config,
        app_name: str = "BasemapGeoProcessor",
        spark_session: Optional[SparkSession] = None
    ):
        """
        Initialize the Spark geospatial processor.
        
        Args:
            config: Configuration object containing Spark and processing settings
            app_name: Name for the Spark application
            spark_session: Optional existing Spark session to reuse
        """
        self.config = config
        self.app_name = app_name
        self.logger = structlog.get_logger(processor_type="SparkGeoProcessor")
        self.metrics = MetricsCollector()
        self.spatial_utils = SpatialUtils()
        
        # Initialize or reuse Spark session
        if spark_session:
            self.spark = spark_session
            self.logger.info("Using provided Spark session")
        else:
            self.spark = self._create_spark_session()
            self.logger.info("Created new Spark session", app_name=app_name)
        
        # Register custom UDFs
        self._register_spatial_udfs()
        
        # Initialize Spark optimizer
        self.optimizer = SparkOptimizer(self.spark, config)
        
        # Performance tracking
        self.processing_stats = {
            'jobs_completed': 0,
            'total_records_processed': 0,
            'total_processing_time': 0.0,
            'errors': []
        }
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for geospatial processing."""
        
        # Build Spark configuration
        conf = SparkConf()
        
        # Basic application settings
        conf.set("spark.app.name", self.app_name)
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Memory and performance optimization
        conf.set("spark.executor.memory", self.config.spark.executor_memory)
        conf.set("spark.executor.cores", str(self.config.spark.executor_cores))
        conf.set("spark.executor.instances", str(self.config.spark.executor_instances))
        conf.set("spark.driver.memory", self.config.spark.driver_memory)
        conf.set("spark.driver.maxResultSize", self.config.spark.driver_max_result_size)
        
        # Serialization optimization
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # Geospatial-specific optimizations
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        
        # AWS EMR specific settings (if running on EMR)
        if self.config.aws.use_emr:
            conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            conf.set("spark.sql.parquet.enableVectorizedReader", "true")
        
        # Create Spark session
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(self.config.spark.log_level)
        
        return spark
    
    def _register_spatial_udfs(self) -> None:
        """Register custom User Defined Functions for spatial operations."""
        
        @udf(returnType=StringType())
        def point_to_wkt(lon: float, lat: float) -> str:
            """Convert longitude/latitude to WKT Point."""
            if lon is None or lat is None:
                return None
            return f"POINT({lon} {lat})"
        
        @udf(returnType=DoubleType())
        def calculate_distance(wkt1: str, wkt2: str) -> float:
            """Calculate distance between two WKT geometries."""
            try:
                if not wkt1 or not wkt2:
                    return None
                geom1 = wkt_loads(wkt1)
                geom2 = wkt_loads(wkt2)
                return geom1.distance(geom2)
            except Exception:
                return None
        
        @udf(returnType=BooleanType())
        def geometries_intersect(wkt1: str, wkt2: str) -> bool:
            """Check if two WKT geometries intersect."""
            try:
                if not wkt1 or not wkt2:
                    return False
                geom1 = wkt_loads(wkt1)
                geom2 = wkt_loads(wkt2)
                return geom1.intersects(geom2)
            except Exception:
                return False
        
        @udf(returnType=DoubleType())
        def geometry_area(wkt: str) -> float:
            """Calculate area of WKT geometry."""
            try:
                if not wkt:
                    return None
                geom = wkt_loads(wkt)
                return geom.area
            except Exception:
                return None
        
        @udf(returnType=DoubleType())
        def geometry_length(wkt: str) -> float:
            """Calculate length of WKT geometry."""
            try:
                if not wkt:
                    return None
                geom = wkt_loads(wkt)
                return geom.length
            except Exception:
                return None
        
        @udf(returnType=StringType())
        def simplify_geometry(wkt: str, tolerance: float) -> str:
            """Simplify WKT geometry with given tolerance."""
            try:
                if not wkt:
                    return None
                geom = wkt_loads(wkt)
                simplified = geom.simplify(tolerance)
                return simplified.wkt
            except Exception:
                return wkt
        
        @udf(returnType=StringType())
        def buffer_geometry(wkt: str, distance: float) -> str:
            """Create buffer around WKT geometry."""
            try:
                if not wkt:
                    return None
                geom = wkt_loads(wkt)
                buffered = geom.buffer(distance)
                return buffered.wkt
            except Exception:
                return None
        
        # Register UDFs with Spark
        self.spark.udf.register("point_to_wkt", point_to_wkt)
        self.spark.udf.register("calculate_distance", calculate_distance)
        self.spark.udf.register("geometries_intersect", geometries_intersect)
        self.spark.udf.register("geometry_area", geometry_area)
        self.spark.udf.register("geometry_length", geometry_length)
        self.spark.udf.register("simplify_geometry", simplify_geometry)
        self.spark.udf.register("buffer_geometry", buffer_geometry)
        
        self.logger.info("Spatial UDFs registered successfully")
    
    def process_osm_data(
        self,
        input_path: str,
        output_path: str,
        feature_types: List[str] = None
    ) -> Dict[str, Any]:
        """
        Process OSM data with distributed Spark operations.
        
        Args:
            input_path: Path to input OSM data (Parquet format)
            output_path: Path for output processed data
            feature_types: List of feature types to process (roads, pois, buildings)
            
        Returns:
            Dictionary containing processing results and statistics
        """
        start_time = time.time()
        
        try:
            self.logger.info(
                "Starting OSM data processing",
                input_path=input_path,
                output_path=output_path,
                feature_types=feature_types
            )
            
            # Default feature types
            if feature_types is None:
                feature_types = ['roads', 'pois', 'buildings']
            
            results = {}
            
            # Process each feature type
            for feature_type in feature_types:
                self.logger.info(f"Processing {feature_type}")
                
                if feature_type == 'roads':
                    result = self._process_roads(input_path, f"{output_path}/roads")
                elif feature_type == 'pois':
                    result = self._process_pois(input_path, f"{output_path}/pois")
                elif feature_type == 'buildings':
                    result = self._process_buildings(input_path, f"{output_path}/buildings")
                else:
                    self.logger.warning(f"Unknown feature type: {feature_type}")
                    continue
                
                results[feature_type] = result
            
            # Calculate overall statistics
            processing_time = time.time() - start_time
            total_records = sum(r.get('record_count', 0) for r in results.values())
            
            self.processing_stats['jobs_completed'] += 1
            self.processing_stats['total_records_processed'] += total_records
            self.processing_stats['total_processing_time'] += processing_time
            
            self.logger.info(
                "OSM data processing completed",
                processing_time=processing_time,
                total_records=total_records,
                feature_types=list(results.keys())
            )
            
            # Collect metrics
            self.metrics.record_histogram('processing_duration', processing_time)
            self.metrics.increment_counter('processing_jobs_completed')
            
            return {
                'success': True,
                'processing_time': processing_time,
                'total_records': total_records,
                'results': results,
                'output_path': output_path
            }
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"OSM data processing failed: {str(e)}"
            
            self.processing_stats['errors'].append(error_msg)
            self.logger.error(error_msg, processing_time=processing_time)
            
            # Collect error metrics
            self.metrics.increment_counter('processing_jobs_failed')
            
            return {
                'success': False,
                'error': error_msg,
                'processing_time': processing_time
            }
    
    def _process_roads(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """Process road features with Spark."""
        
        # Read OSM ways data
        ways_df = self.spark.read.parquet(f"{input_path}/ways")
        
        # Filter for road features
        road_types = [
            'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
            'unclassified', 'residential', 'service', 'motorway_link',
            'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link'
        ]
        
        roads_df = ways_df.filter(
            col("tags.highway").isin(road_types)
        ).select(
            col("id").alias("osm_id"),
            col("tags.highway").alias("road_type"),
            col("tags.name").alias("name"),
            col("tags.maxspeed").alias("maxspeed"),
            col("tags.surface").alias("surface"),
            col("tags.lanes").alias("lanes"),
            col("geometry").alias("geometry_wkt"),
            col("timestamp")
        )
        
        # Add computed fields
        roads_df = roads_df.withColumn(
            "length_meters",
            udf(lambda wkt: self._calculate_length(wkt), DoubleType())(col("geometry_wkt"))
        ).withColumn(
            "road_class",
            when(col("road_type").isin(['motorway', 'trunk']), "highway")
            .when(col("road_type").isin(['primary', 'secondary']), "arterial")
            .when(col("road_type").isin(['tertiary', 'unclassified']), "collector")
            .otherwise("local")
        )
        
        # Optimize partitioning for spatial queries
        roads_df = self.optimizer.optimize_spatial_partitioning(
            roads_df, 
            geometry_column="geometry_wkt"
        )
        
        # Write processed roads
        roads_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        record_count = roads_df.count()
        
        self.logger.info(f"Processed {record_count} road features")
        
        return {
            'record_count': record_count,
            'output_path': output_path,
            'feature_type': 'roads'
        }
    
    def _process_pois(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """Process point of interest features with Spark."""
        
        # Read OSM nodes data
        nodes_df = self.spark.read.parquet(f"{input_path}/nodes")
        
        # Define POI categories
        poi_amenities = [
            'restaurant', 'cafe', 'bar', 'pub', 'fast_food', 'fuel',
            'hospital', 'pharmacy', 'bank', 'atm', 'post_office',
            'school', 'university', 'library', 'place_of_worship'
        ]
        
        poi_shops = [
            'supermarket', 'convenience', 'clothes', 'electronics',
            'bakery', 'butcher', 'car_repair', 'hairdresser'
        ]
        
        # Filter for POI features
        pois_df = nodes_df.filter(
            col("tags.amenity").isin(poi_amenities) |
            col("tags.shop").isin(poi_shops) |
            col("tags.tourism").isNotNull()
        ).select(
            col("id").alias("osm_id"),
            col("lat"),
            col("lon"),
            col("tags.name").alias("name"),
            col("tags.amenity").alias("amenity"),
            col("tags.shop").alias("shop"),
            col("tags.tourism").alias("tourism"),
            col("tags.phone").alias("phone"),
            col("tags.website").alias("website"),
            col("tags.opening_hours").alias("opening_hours"),
            col("timestamp")
        )
        
        # Create point geometry
        pois_df = pois_df.withColumn(
            "geometry_wkt",
            udf(lambda lon, lat: f"POINT({lon} {lat})" if lon and lat else None, StringType())(
                col("lon"), col("lat")
            )
        )
        
        # Categorize POIs
        pois_df = pois_df.withColumn(
            "poi_category",
            when(col("amenity").isNotNull(), col("amenity"))
            .when(col("shop").isNotNull(), col("shop"))
            .when(col("tourism").isNotNull(), col("tourism"))
            .otherwise("other")
        ).withColumn(
            "poi_type",
            when(col("amenity").isNotNull(), lit("amenity"))
            .when(col("shop").isNotNull(), lit("shop"))
            .when(col("tourism").isNotNull(), lit("tourism"))
            .otherwise("other")
        )
        
        # Add importance scoring
        pois_df = pois_df.withColumn(
            "importance_score",
            when(col("poi_category").isin(['hospital', 'school', 'university']), 10)
            .when(col("poi_category").isin(['restaurant', 'fuel', 'bank']), 8)
            .when(col("poi_category").isin(['supermarket', 'pharmacy']), 7)
            .otherwise(5)
        )
        
        # Filter out POIs without names for better quality
        pois_df = pois_df.filter(col("name").isNotNull() & (col("name") != ""))
        
        # Write processed POIs
        pois_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        record_count = pois_df.count()
        
        self.logger.info(f"Processed {record_count} POI features")
        
        return {
            'record_count': record_count,
            'output_path': output_path,
            'feature_type': 'pois'
        }
    
    def _process_buildings(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """Process building features with Spark."""
        
        # Read OSM ways data (buildings are typically ways/polygons)
        ways_df = self.spark.read.parquet(f"{input_path}/ways")
        
        # Filter for building features
        buildings_df = ways_df.filter(
            col("tags.building").isNotNull() & 
            (col("tags.building") != "no")
        ).select(
            col("id").alias("osm_id"),
            col("tags.building").alias("building_type"),
            col("tags.name").alias("name"),
            col("tags.addr:housenumber").alias("house_number"),
            col("tags.addr:street").alias("street"),
            col("tags.addr:city").alias("city"),
            col("tags.addr:postcode").alias("postcode"),
            col("tags.height").alias("height"),
            col("tags.levels").alias("levels"),
            col("geometry").alias("geometry_wkt"),
            col("timestamp")
        )
        
        # Add computed fields
        buildings_df = buildings_df.withColumn(
            "area_sqm",
            udf(lambda wkt: self._calculate_area(wkt), DoubleType())(col("geometry_wkt"))
        ).withColumn(
            "building_class",
            when(col("building_type").isin(['house', 'detached', 'residential']), "residential")
            .when(col("building_type").isin(['commercial', 'retail', 'office']), "commercial")
            .when(col("building_type").isin(['industrial', 'warehouse']), "industrial")
            .when(col("building_type").isin(['school', 'hospital', 'church']), "institutional")
            .otherwise("other")
        )
        
        # Convert height and levels to numeric
        buildings_df = buildings_df.withColumn(
            "height_meters",
            when(col("height").rlike(r"^\d+(\.\d+)?$"), col("height").cast(DoubleType()))
            .otherwise(None)
        ).withColumn(
            "num_levels",
            when(col("levels").rlike(r"^\d+$"), col("levels").cast(IntegerType()))
            .otherwise(None)
        )
        
        # Estimate height from levels if height is missing
        buildings_df = buildings_df.withColumn(
            "estimated_height",
            coalesce(
                col("height_meters"),
                when(col("num_levels").isNotNull(), col("num_levels") * 3.0)
                .otherwise(None)
            )
        )
        
        # Filter out very small buildings (likely mapping errors)
        buildings_df = buildings_df.filter(
            (col("area_sqm").isNull()) | (col("area_sqm") > 10.0)
        )
        
        # Write processed buildings
        buildings_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        record_count = buildings_df.count()
        
        self.logger.info(f"Processed {record_count} building features")
        
        return {
            'record_count': record_count,
            'output_path': output_path,
            'feature_type': 'buildings'
        }
    
    def spatial_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        left_geom_col: str = "geometry_wkt",
        right_geom_col: str = "geometry_wkt",
        join_type: str = "intersects",
        broadcast_right: bool = False
    ) -> DataFrame:
        """
        Perform spatial join between two DataFrames.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_geom_col: Geometry column name in left DataFrame
            right_geom_col: Geometry column name in right DataFrame
            join_type: Type of spatial join (intersects, within, contains)
            broadcast_right: Whether to broadcast the right DataFrame
            
        Returns:
            Joined DataFrame
        """
        
        self.logger.info(
            "Performing spatial join",
            join_type=join_type,
            broadcast_right=broadcast_right
        )
        
        # Optimize right DataFrame if broadcasting
        if broadcast_right:
            right_df = broadcast(right_df)
        
        # Create spatial join condition based on join type
        if join_type == "intersects":
            join_condition = udf(
                lambda left_geom, right_geom: self._geometries_intersect(left_geom, right_geom),
                BooleanType()
            )(col(f"left.{left_geom_col}"), col(f"right.{right_geom_col}"))
        elif join_type == "within":
            join_condition = udf(
                lambda left_geom, right_geom: self._geometry_within(left_geom, right_geom),
                BooleanType()
            )(col(f"left.{left_geom_col}"), col(f"right.{right_geom_col}"))
        elif join_type == "contains":
            join_condition = udf(
                lambda left_geom, right_geom: self._geometry_contains(left_geom, right_geom),
                BooleanType()
            )(col(f"left.{left_geom_col}"), col(f"right.{right_geom_col}"))
        else:
            raise ValueError(f"Unsupported join type: {join_type}")
        
        # Perform cross join and filter by spatial condition
        result_df = left_df.alias("left").crossJoin(right_df.alias("right")) \
            .filter(join_condition)
        
        return result_df
    
    def data_quality_check(
        self,
        df: DataFrame,
        geometry_column: str = "geometry_wkt"
    ) -> Dict[str, Any]:
        """
        Perform comprehensive data quality checks on geospatial DataFrame.
        
        Args:
            df: DataFrame to check
            geometry_column: Name of geometry column
            
        Returns:
            Dictionary containing quality metrics
        """
        
        self.logger.info("Performing data quality checks")
        
        total_records = df.count()
        
        # Check for null geometries
        null_geometries = df.filter(col(geometry_column).isNull()).count()
        
        # Check for empty geometries
        empty_geometries = df.filter(
            col(geometry_column).isNotNull() & 
            (col(geometry_column) == "")
        ).count()
        
        # Check for invalid geometries
        invalid_geometries = df.filter(
            udf(lambda wkt: not self._is_valid_geometry(wkt), BooleanType())(col(geometry_column))
        ).count()
        
        # Calculate quality metrics
        valid_geometries = total_records - null_geometries - empty_geometries - invalid_geometries
        geometry_completeness = (valid_geometries / total_records) * 100 if total_records > 0 else 0
        
        quality_metrics = {
            'total_records': total_records,
            'valid_geometries': valid_geometries,
            'null_geometries': null_geometries,
            'empty_geometries': empty_geometries,
            'invalid_geometries': invalid_geometries,
            'geometry_completeness_percent': geometry_completeness,
            'quality_score': geometry_completeness
        }
        
        self.logger.info("Data quality check completed", **quality_metrics)
        
        return quality_metrics
    
    def optimize_for_tile_generation(
        self,
        df: DataFrame,
        zoom_levels: List[int],
        geometry_column: str = "geometry_wkt"
    ) -> Dict[int, DataFrame]:
        """
        Optimize DataFrame for tile generation at different zoom levels.
        
        Args:
            df: Input DataFrame
            zoom_levels: List of zoom levels to optimize for
            geometry_column: Name of geometry column
            
        Returns:
            Dictionary mapping zoom levels to optimized DataFrames
        """
        
        self.logger.info("Optimizing data for tile generation", zoom_levels=zoom_levels)
        
        optimized_dfs = {}
        
        for zoom in zoom_levels:
            # Calculate simplification tolerance based on zoom level
            tolerance = self._calculate_simplification_tolerance(zoom)
            
            # Simplify geometries for this zoom level
            simplified_df = df.withColumn(
                f"geometry_z{zoom}",
                udf(lambda wkt: self._simplify_geometry(wkt, tolerance), StringType())(
                    col(geometry_column)
                )
            )
            
            # Filter features based on zoom level importance
            if zoom <= 8:  # Low zoom - only major features
                filtered_df = simplified_df.filter(
                    col("importance_score").isNotNull() & 
                    (col("importance_score") >= 8)
                )
            elif zoom <= 12:  # Medium zoom - important features
                filtered_df = simplified_df.filter(
                    col("importance_score").isNotNull() & 
                    (col("importance_score") >= 6)
                )
            else:  # High zoom - all features
                filtered_df = simplified_df
            
            optimized_dfs[zoom] = filtered_df
            
            self.logger.info(
                f"Optimized data for zoom {zoom}",
                original_count=df.count(),
                optimized_count=filtered_df.count(),
                tolerance=tolerance
            )
        
        return optimized_dfs
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return self.processing_stats.copy()
    
    def shutdown(self) -> None:
        """Shutdown Spark session and cleanup resources."""
        try:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")
        except Exception as e:
            self.logger.error(f"Error stopping Spark session: {e}")
    
    # Helper methods for geometry operations
    
    def _calculate_length(self, wkt: str) -> float:
        """Calculate geometry length."""
        try:
            if not wkt:
                return None
            geom = wkt_loads(wkt)
            return geom.length
        except Exception:
            return None
    
    def _calculate_area(self, wkt: str) -> float:
        """Calculate geometry area."""
        try:
            if not wkt:
                return None
            geom = wkt_loads(wkt)
            return geom.area
        except Exception:
            return None
    
    def _geometries_intersect(self, wkt1: str, wkt2: str) -> bool:
        """Check if geometries intersect."""
        try:
            if not wkt1 or not wkt2:
                return False
            geom1 = wkt_loads(wkt1)
            geom2 = wkt_loads(wkt2)
            return geom1.intersects(geom2)
        except Exception:
            return False
    
    def _geometry_within(self, wkt1: str, wkt2: str) -> bool:
        """Check if geometry1 is within geometry2."""
        try:
            if not wkt1 or not wkt2:
                return False
            geom1 = wkt_loads(wkt1)
            geom2 = wkt_loads(wkt2)
            return geom1.within(geom2)
        except Exception:
            return False
    
    def _geometry_contains(self, wkt1: str, wkt2: str) -> bool:
        """Check if geometry1 contains geometry2."""
        try:
            if not wkt1 or not wkt2:
                return False
            geom1 = wkt_loads(wkt1)
            geom2 = wkt_loads(wkt2)
            return geom1.contains(geom2)
        except Exception:
            return False
    
    def _is_valid_geometry(self, wkt: str) -> bool:
        """Check if geometry is valid."""
        try:
            if not wkt:
                return False
            geom = wkt_loads(wkt)
            return geom.is_valid
        except Exception:
            return False
    
    def _simplify_geometry(self, wkt: str, tolerance: float) -> str:
        """Simplify geometry with given tolerance."""
        try:
            if not wkt:
                return None
            geom = wkt_loads(wkt)
            simplified = geom.simplify(tolerance)
            return simplified.wkt
        except Exception:
            return wkt
    
    def _calculate_simplification_tolerance(self, zoom: int) -> float:
        """Calculate appropriate simplification tolerance for zoom level."""
        # Higher zoom = more detail = lower tolerance
        # This is a simplified calculation - in practice you'd use more sophisticated methods
        base_tolerance = 0.001  # Base tolerance in degrees
        return base_tolerance / (2 ** (zoom - 1)) if zoom > 1 else base_tolerance


# Example usage and testing
if __name__ == "__main__":
    # This would typically be imported from a config module
    class MockConfig:
        def __init__(self):
            self.spark = MockSparkConfig()
            self.aws = MockAWSConfig()
    
    class MockSparkConfig:
        def __init__(self):
            self.executor_memory = "4g"
            self.executor_cores = 2
            self.executor_instances = 4
            self.driver_memory = "2g"
            self.driver_max_result_size = "1g"
            self.log_level = "WARN"
    
    class MockAWSConfig:
        def __init__(self):
            self.use_emr = False
    
    # Initialize processor
    config = MockConfig()
    processor = SparkGeoProcessor(config)
    
    # Example processing workflow
    try:
        # Process OSM data
        result = processor.process_osm_data(
            input_path="s3://my-bucket/osm-data/",
            output_path="s3://my-bucket/processed-data/",
            feature_types=['roads', 'pois', 'buildings']
        )
        
        print("Processing completed:", result)
        
        # Get processing statistics
        stats = processor.get_processing_stats()
        print("Processing stats:", stats)
        
    finally:
        # Cleanup
        processor.shutdown()