"""
OpenStreetMap Data Ingester

Handles ingestion and processing of OpenStreetMap (OSM) data in various formats
including PBF, XML, and derived datasets. Implements efficient processing of
large-scale geographic data with proper handling of OSM data structures.

This module demonstrates:
- Large-scale geospatial data processing
- OSM data format expertise
- Distributed processing with Spark
- Memory-efficient streaming processing
- Spatial data validation and quality assurance
- Tag-based feature extraction and classification
"""

import os
import gzip
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Union, Iterator
from pathlib import Path
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import linemerge, polygonize
import osmium
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, regexp_extract, split, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

from .base_ingester import BaseDataIngester
from ..utils.config import Config
from ..utils.spatial_utils import SpatialUtils
from ..utils.osm_utils import OSMTagProcessor, OSMGeometryBuilder


class OSMDataIngester(BaseDataIngester):
    """
    Specialized ingester for OpenStreetMap data processing.
    
    Supports multiple OSM data formats and provides efficient processing
    of nodes, ways, and relations with proper geometry construction and
    attribute extraction.
    """
    
    # OSM feature classification based on tags
    ROAD_TAGS = {
        'highway': [
            'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
            'unclassified', 'residential', 'service', 'motorway_link',
            'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link'
        ]
    }
    
    POI_TAGS = {
        'amenity': [
            'restaurant', 'cafe', 'bar', 'pub', 'fast_food', 'fuel',
            'hospital', 'pharmacy', 'bank', 'atm', 'post_office',
            'school', 'university', 'library', 'place_of_worship'
        ],
        'shop': [
            'supermarket', 'convenience', 'clothes', 'electronics',
            'bakery', 'butcher', 'car_repair', 'hairdresser'
        ],
        'tourism': [
            'hotel', 'motel', 'hostel', 'museum', 'attraction',
            'viewpoint', 'information'
        ]
    }
    
    BUILDING_TAGS = {
        'building': ['yes', 'house', 'apartments', 'commercial', 'industrial']
    }
    
    def __init__(
        self,
        config: Config,
        spark_session: Optional[SparkSession] = None,
        **kwargs
    ):
        """
        Initialize OSM data ingester.
        
        Args:
            config: Configuration object
            spark_session: Optional Spark session for distributed processing
            **kwargs: Additional arguments passed to base class
        """
        super().__init__(config, spark_session, **kwargs)
        
        self.tag_processor = OSMTagProcessor()
        self.geometry_builder = OSMGeometryBuilder()
        self.spatial_utils = SpatialUtils()
        
        # OSM-specific configuration
        self.osm_config = config.osm
        self.chunk_size = getattr(self.osm_config, 'chunk_size', 10000)
        self.memory_limit = getattr(self.osm_config, 'memory_limit_mb', 2048)
        
        self.logger.info("OSM Data Ingester initialized", chunk_size=self.chunk_size)
    
    def extract(self, source: Union[str, Path, Dict]) -> Dict[str, Any]:
        """
        Extract OSM data from various sources.
        
        Args:
            source: OSM data source (file path, URL, or configuration dict)
            
        Returns:
            Dictionary containing extracted OSM elements
        """
        try:
            if isinstance(source, dict):
                return self._extract_from_config(source)
            elif str(source).startswith('http'):
                return self._extract_from_url(source)
            else:
                return self._extract_from_file(source)
                
        except Exception as e:
            self.logger.error("OSM data extraction failed", error=str(e), source=str(source))
            raise
    
    def _extract_from_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Extract OSM data from local file."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"OSM file not found: {file_path}")
        
        self.logger.info("Extracting OSM data from file", file_path=str(file_path))
        
        # Determine file format and process accordingly
        if file_path.suffix.lower() == '.pbf':
            return self._extract_from_pbf(file_path)
        elif file_path.suffix.lower() in ['.osm', '.xml']:
            return self._extract_from_xml(file_path)
        elif file_path.suffix.lower() == '.gz':
            # Handle compressed files
            if file_path.stem.endswith('.osm') or file_path.stem.endswith('.xml'):
                return self._extract_from_compressed_xml(file_path)
            else:
                raise ValueError(f"Unsupported compressed file format: {file_path}")
        else:
            raise ValueError(f"Unsupported OSM file format: {file_path.suffix}")
    
    def _extract_from_pbf(self, file_path: Path) -> Dict[str, Any]:
        """Extract data from OSM PBF format using osmium."""
        nodes = []
        ways = []
        relations = []
        
        class OSMHandler(osmium.SimpleHandler):
            def __init__(self, ingester):
                osmium.SimpleHandler.__init__(self)
                self.ingester = ingester
                self.node_count = 0
                self.way_count = 0
                self.relation_count = 0
            
            def node(self, n):
                if self.node_count % 100000 == 0:
                    self.ingester.logger.debug(f"Processed {self.node_count} nodes")
                
                # Extract relevant nodes (with tags or referenced by ways)
                if n.tags:
                    nodes.append({
                        'id': n.id,
                        'lat': n.location.lat,
                        'lon': n.location.lon,
                        'tags': dict(n.tags),
                        'version': n.version,
                        'timestamp': n.timestamp
                    })
                
                self.node_count += 1
            
            def way(self, w):
                if self.way_count % 10000 == 0:
                    self.ingester.logger.debug(f"Processed {self.way_count} ways")
                
                ways.append({
                    'id': w.id,
                    'nodes': [n.ref for n in w.nodes],
                    'tags': dict(w.tags),
                    'version': w.version,
                    'timestamp': w.timestamp
                })
                
                self.way_count += 1
            
            def relation(self, r):
                if self.relation_count % 1000 == 0:
                    self.ingester.logger.debug(f"Processed {self.relation_count} relations")
                
                relations.append({
                    'id': r.id,
                    'members': [
                        {
                            'type': m.type,
                            'ref': m.ref,
                            'role': m.role
                        } for m in r.members
                    ],
                    'tags': dict(r.tags),
                    'version': r.version,
                    'timestamp': r.timestamp
                })
                
                self.relation_count += 1
        
        # Process the PBF file
        handler = OSMHandler(self)
        handler.apply_file(str(file_path))
        
        self.logger.info(
            "PBF extraction completed",
            nodes=len(nodes),
            ways=len(ways),
            relations=len(relations)
        )
        
        return {
            'nodes': nodes,
            'ways': ways,
            'relations': relations,
            'metadata': {
                'source_file': str(file_path),
                'format': 'pbf',
                'total_elements': len(nodes) + len(ways) + len(relations)
            }
        }
    
    def _extract_from_xml(self, file_path: Path) -> Dict[str, Any]:
        """Extract data from OSM XML format."""
        nodes = []
        ways = []
        relations = []
        
        self.logger.info("Parsing OSM XML file", file_path=str(file_path))
        
        # Use iterative parsing for memory efficiency
        context = ET.iterparse(str(file_path), events=('start', 'end'))
        context = iter(context)
        event, root = next(context)
        
        for event, elem in context:
            if event == 'end':
                if elem.tag == 'node':
                    node_data = self._parse_node_element(elem)
                    if node_data:
                        nodes.append(node_data)
                
                elif elem.tag == 'way':
                    way_data = self._parse_way_element(elem)
                    if way_data:
                        ways.append(way_data)
                
                elif elem.tag == 'relation':
                    relation_data = self._parse_relation_element(elem)
                    if relation_data:
                        relations.append(relation_data)
                
                # Clear the element to free memory
                elem.clear()
                root.clear()
        
        self.logger.info(
            "XML extraction completed",
            nodes=len(nodes),
            ways=len(ways),
            relations=len(relations)
        )
        
        return {
            'nodes': nodes,
            'ways': ways,
            'relations': relations,
            'metadata': {
                'source_file': str(file_path),
                'format': 'xml',
                'total_elements': len(nodes) + len(ways) + len(relations)
            }
        }
    
    def _parse_node_element(self, elem) -> Optional[Dict]:
        """Parse a node element from XML."""
        try:
            node_id = int(elem.get('id'))
            lat = float(elem.get('lat'))
            lon = float(elem.get('lon'))
            
            # Extract tags
            tags = {}
            for tag_elem in elem.findall('tag'):
                key = tag_elem.get('k')
                value = tag_elem.get('v')
                if key and value:
                    tags[key] = value
            
            # Only include nodes with tags (POIs, etc.)
            if tags:
                return {
                    'id': node_id,
                    'lat': lat,
                    'lon': lon,
                    'tags': tags,
                    'version': elem.get('version'),
                    'timestamp': elem.get('timestamp')
                }
            
            return None
            
        except (ValueError, TypeError) as e:
            self.logger.warning("Failed to parse node element", error=str(e))
            return None
    
    def _parse_way_element(self, elem) -> Optional[Dict]:
        """Parse a way element from XML."""
        try:
            way_id = int(elem.get('id'))
            
            # Extract node references
            nodes = []
            for nd_elem in elem.findall('nd'):
                ref = nd_elem.get('ref')
                if ref:
                    nodes.append(int(ref))
            
            # Extract tags
            tags = {}
            for tag_elem in elem.findall('tag'):
                key = tag_elem.get('k')
                value = tag_elem.get('v')
                if key and value:
                    tags[key] = value
            
            return {
                'id': way_id,
                'nodes': nodes,
                'tags': tags,
                'version': elem.get('version'),
                'timestamp': elem.get('timestamp')
            }
            
        except (ValueError, TypeError) as e:
            self.logger.warning("Failed to parse way element", error=str(e))
            return None
    
    def _parse_relation_element(self, elem) -> Optional[Dict]:
        """Parse a relation element from XML."""
        try:
            relation_id = int(elem.get('id'))
            
            # Extract members
            members = []
            for member_elem in elem.findall('member'):
                member_type = member_elem.get('type')
                ref = member_elem.get('ref')
                role = member_elem.get('role', '')
                
                if member_type and ref:
                    members.append({
                        'type': member_type,
                        'ref': int(ref),
                        'role': role
                    })
            
            # Extract tags
            tags = {}
            for tag_elem in elem.findall('tag'):
                key = tag_elem.get('k')
                value = tag_elem.get('v')
                if key and value:
                    tags[key] = value
            
            return {
                'id': relation_id,
                'members': members,
                'tags': tags,
                'version': elem.get('version'),
                'timestamp': elem.get('timestamp')
            }
            
        except (ValueError, TypeError) as e:
            self.logger.warning("Failed to parse relation element", error=str(e))
            return None
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate extracted OSM data for completeness and quality.
        
        Args:
            data: Extracted OSM data dictionary
            
        Returns:
            True if data passes validation, False otherwise
        """
        try:
            # Check required structure
            required_keys = ['nodes', 'ways', 'relations', 'metadata']
            for key in required_keys:
                if key not in data:
                    self.logger.error(f"Missing required key in OSM data: {key}")
                    return False
            
            # Validate data counts
            node_count = len(data['nodes'])
            way_count = len(data['ways'])
            relation_count = len(data['relations'])
            
            if node_count == 0 and way_count == 0:
                self.logger.error("No valid OSM elements found")
                return False
            
            # Validate sample of nodes
            if node_count > 0:
                sample_nodes = data['nodes'][:min(100, node_count)]
                for node in sample_nodes:
                    if not self._validate_node(node):
                        return False
            
            # Validate sample of ways
            if way_count > 0:
                sample_ways = data['ways'][:min(100, way_count)]
                for way in sample_ways:
                    if not self._validate_way(way):
                        return False
            
            self.logger.info(
                "OSM data validation passed",
                nodes=node_count,
                ways=way_count,
                relations=relation_count
            )
            
            return True
            
        except Exception as e:
            self.logger.error("OSM data validation failed", error=str(e))
            return False
    
    def _validate_node(self, node: Dict) -> bool:
        """Validate a single node."""
        required_fields = ['id', 'lat', 'lon']
        for field in required_fields:
            if field not in node:
                self.logger.error(f"Node missing required field: {field}")
                return False
        
        # Validate coordinates
        lat, lon = node['lat'], node['lon']
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            self.logger.error(f"Invalid coordinates: lat={lat}, lon={lon}")
            return False
        
        return True
    
    def _validate_way(self, way: Dict) -> bool:
        """Validate a single way."""
        required_fields = ['id', 'nodes']
        for field in required_fields:
            if field not in way:
                self.logger.error(f"Way missing required field: {field}")
                return False
        
        # Validate node references
        if len(way['nodes']) < 2:
            self.logger.error(f"Way {way['id']} has insufficient nodes")
            return False
        
        return True
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, gpd.GeoDataFrame]:
        """
        Transform OSM data into structured GeoDataFrames by feature type.
        
        Args:
            data: Raw OSM data dictionary
            
        Returns:
            Dictionary of GeoDataFrames organized by feature type
        """
        try:
            self.logger.info("Starting OSM data transformation")
            
            # Create node lookup for geometry construction
            node_lookup = {
                node['id']: (node['lon'], node['lat'])
                for node in data['nodes']
            }
            
            # Process different feature types
            result = {}
            
            # Process POI nodes
            poi_nodes = self._extract_poi_nodes(data['nodes'])
            if poi_nodes:
                result['pois'] = self._create_poi_geodataframe(poi_nodes)
            
            # Process roads from ways
            road_ways = self._extract_road_ways(data['ways'])
            if road_ways:
                result['roads'] = self._create_road_geodataframe(road_ways, node_lookup)
            
            # Process buildings from ways
            building_ways = self._extract_building_ways(data['ways'])
            if building_ways:
                result['buildings'] = self._create_building_geodataframe(building_ways, node_lookup)
            
            # Process administrative boundaries from relations
            admin_relations = self._extract_admin_relations(data['relations'])
            if admin_relations:
                result['admin_boundaries'] = self._create_admin_geodataframe(
                    admin_relations, data['ways'], node_lookup
                )
            
            self.logger.info(
                "OSM data transformation completed",
                feature_types=list(result.keys()),
                total_features=sum(len(gdf) for gdf in result.values())
            )
            
            return result
            
        except Exception as e:
            self.logger.error("OSM data transformation failed", error=str(e))
            raise
    
    def _extract_poi_nodes(self, nodes: List[Dict]) -> List[Dict]:
        """Extract point of interest nodes based on tags."""
        poi_nodes = []
        
        for node in nodes:
            tags = node.get('tags', {})
            
            # Check if node matches POI criteria
            for tag_key, tag_values in self.POI_TAGS.items():
                if tag_key in tags and tags[tag_key] in tag_values:
                    poi_node = node.copy()
                    poi_node['poi_type'] = f"{tag_key}:{tags[tag_key]}"
                    poi_node['name'] = tags.get('name', '')
                    poi_nodes.append(poi_node)
                    break
        
        return poi_nodes
    
    def _create_poi_geodataframe(self, poi_nodes: List[Dict]) -> gpd.GeoDataFrame:
        """Create GeoDataFrame for POI nodes."""
        geometries = [Point(node['lon'], node['lat']) for node in poi_nodes]
        
        gdf = gpd.GeoDataFrame(poi_nodes, geometry=geometries, crs='EPSG:4326')
        
        # Clean up and standardize columns
        gdf['osm_id'] = gdf['id']
        gdf = gdf.drop(columns=['id', 'lat', 'lon'])
        
        return gdf
    
    def _extract_road_ways(self, ways: List[Dict]) -> List[Dict]:
        """Extract road ways based on highway tags."""
        road_ways = []
        
        for way in ways:
            tags = way.get('tags', {})
            
            if 'highway' in tags and tags['highway'] in self.ROAD_TAGS['highway']:
                road_way = way.copy()
                road_way['road_type'] = tags['highway']
                road_way['name'] = tags.get('name', '')
                road_way['maxspeed'] = tags.get('maxspeed', '')
                road_way['surface'] = tags.get('surface', '')
                road_ways.append(road_way)
        
        return road_ways
    
    def _create_road_geodataframe(
        self,
        road_ways: List[Dict],
        node_lookup: Dict[int, tuple]
    ) -> gpd.GeoDataFrame:
        """Create GeoDataFrame for road ways."""
        geometries = []
        valid_ways = []
        
        for way in road_ways:
            try:
                # Get coordinates for way nodes
                coords = []
                for node_id in way['nodes']:
                    if node_id in node_lookup:
                        coords.append(node_lookup[node_id])
                
                if len(coords) >= 2:
                    geometry = LineString(coords)
                    geometries.append(geometry)
                    valid_ways.append(way)
                
            except Exception as e:
                self.logger.warning(f"Failed to create geometry for way {way['id']}: {e}")
        
        if not valid_ways:
            return gpd.GeoDataFrame()
        
        gdf = gpd.GeoDataFrame(valid_ways, geometry=geometries, crs='EPSG:4326')
        
        # Clean up columns
        gdf['osm_id'] = gdf['id']
        gdf = gdf.drop(columns=['id', 'nodes'])
        
        return gdf
