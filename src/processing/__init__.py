"""
Data Processing Module

Implements distributed data processing workflows using Apache Spark for
large-scale geospatial data transformation, conflation, and enrichment.

This module demonstrates expertise in:
- Apache Spark distributed computing
- Geospatial data processing at scale
- Data quality and validation
- Performance optimization
- Memory management for big data
- Spatial algorithms and operations
"""

from .spark_processor import SparkGeoProcessor
from .conflation_engine import ConflationEngine
from .quality_assurance import DataQualityProcessor
from .spatial_operations import SpatialOperationsProcessor
from .incremental_processor import IncrementalProcessor

__all__ = [
    "SparkGeoProcessor",
    "ConflationEngine",
    "DataQualityProcessor", 
    "SpatialOperationsProcessor",
    "IncrementalProcessor"
]
