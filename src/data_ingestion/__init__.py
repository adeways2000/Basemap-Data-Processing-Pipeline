"""
Data Ingestion Module

Handles the intake and initial processing of various geospatial data sources
including OpenStreetMap data, points of interest, satellite imagery, elevation
models, and proprietary datasets.

This module demonstrates expertise in:
- Big data ingestion patterns
- Multiple data format handling (Shapefile, GeoJSON, OSM PBF, GeoTIFF)
- Data validation and quality assurance
- Scalable processing architectures
- Error handling and recovery mechanisms
"""

from .osm_ingestion import OSMDataIngester
from .poi_ingestion import POIDataIngester
from .satellite_ingestion import SatelliteDataIngester
from .elevation_ingestion import ElevationDataIngester
from .base_ingester import BaseDataIngester
from .data_validator import DataValidator

__all__ = [
    "OSMDataIngester",
    "POIDataIngester", 
    "SatelliteDataIngester",
    "ElevationDataIngester",
    "BaseDataIngester",
    "DataValidator"
]
