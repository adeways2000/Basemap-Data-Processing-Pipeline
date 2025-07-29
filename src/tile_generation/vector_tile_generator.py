"""
Vector Tile Generator

Implements high-performance vector tile generation using Mapbox Vector Tile (MVT)
format. Supports multi-layer tiles with optimized geometry processing and
attribute handling for basemap applications.

This module demonstrates:
- MVT format expertise
- Multi-layer tile composition
- Geometry simplification and optimization
- Attribute filtering and optimization
- Spatial indexing for efficient tile generation
- Memory-efficient processing of large datasets
- Integration with various data sources
"""

import os
import math
import time
import json
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path
import concurrent.futures
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon, box
from shapely.ops import transform
from shapely.affinity import scale, translate
import pyproj
from pyproj import Transformer
import mapbox_vector_tile
import structlog

from ..utils.config import Config
from ..utils.spatial_utils import SpatialUtils
from ..utils.geometry_utils import GeometrySimplifier
from ..monitoring.metrics import MetricsCollector


@dataclass
class TileSpec:
    """Specification for a single tile."""
    x: int
    y: int
    z: int
    bbox: Tuple[float, float, float, float]  # (minx, miny, maxx, maxy)
    
    @property
    def tile_id(self) -> str:
        """Get unique tile identifier."""
        return f"{self.z}/{self.x}/{self.y}"


@dataclass
class LayerConfig:
    """Configuration for a tile layer."""
    name: str
    source_table: str
    min_zoom: int
    max_zoom: int
    geometry_column: str = "geometry"
    attributes: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None
    simplification_tolerance: Optional[float] = None
    buffer_size: int = 64  # Buffer in tile units
    
    def __post_init__(self):
        if self.attributes is None:
            self.attributes = []
        if self.filters is None:
            self.filters = {}


class VectorTileGenerator:
    """
    High-performance vector tile generator for basemap data.
    
    Generates Mapbox Vector Tiles (MVT) from geospatial data with
    optimized processing for large-scale basemap applications.
    """
    
    # Tile size in pixels (standard for web mercator)
    TILE_SIZE = 256
    
    # Earth circumference at equator in meters
    EARTH_CIRCUMFERENCE = 40075016.686
    
    # Web Mercator EPSG code
    WEB_MERCATOR_EPSG = 3857
    WGS84_EPSG = 4326
    
    def __init__(
        self,
        config: Config,
        layer_configs: List[LayerConfig],
        output_format: str = "mvt",
        tile_size: int = 256,
        buffer_size: int = 64
    ):
        """
        Initialize the vector tile generator.
        
        Args:
            config: Configuration object
            layer_configs: List of layer configurations
            output_format: Output tile format (mvt, geojson)
            tile_size: Tile size in pixels
            buffer_size: Buffer size in tile units
        """
        self.config = config
        self.layer_configs = {layer.name: layer for layer in layer_configs}
        self.output_format = output_format.lower()
        self.tile_size = tile_size
        self.buffer_size = buffer_size
        
        # Set up logging and metrics
        self.logger = structlog.get_logger(
            generator_type="VectorTileGenerator",
            output_format=output_format
        )
        self.metrics = MetricsCollector()
        
        # Initialize utilities
        self.spatial_utils = SpatialUtils()
        self.geometry_simplifier = GeometrySimplifier()
        
        # Set up coordinate transformations
        self.wgs84_to_mercator = Transformer.from_crs(
            self.WGS84_EPSG, 
            self.WEB_MERCATOR_EPSG, 
            always_xy=True
        )
        self.mercator_to_wgs84 = Transformer.from_crs(
            self.WEB_MERCATOR_EPSG, 
            self.WGS84_EPSG, 
            always_xy=True
        )
        
        # Tile generation statistics
        self.stats = {
            'tiles_generated': 0,
            'total_features_processed': 0,
            'total_processing_time': 0.0,
            'errors': []
        }
        
        self.logger.info(
            "Vector tile generator initialized",
            layers=list(self.layer_configs.keys()),
            output_format=output_format,
            tile_size=tile_size
        )
    
    def generate_tileset(
        self,
        data_sources: Dict[str, gpd.GeoDataFrame],
        zoom_levels: List[int],
        bbox: Optional[Tuple[float, float, float, float]] = None,
        output_dir: Optional[Path] = None,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Generate a complete tileset for specified zoom levels.
        
        Args:
            data_sources: Dictionary mapping layer names to GeoDataFrames
            zoom_levels: List of zoom levels to generate
            bbox: Optional bounding box to limit tile generation (minx, miny, maxx, maxy)
            output_dir: Optional output directory for tiles
            max_workers: Maximum number of worker threads
            
        Returns:
            Dictionary containing generation results and statistics
        """
        start_time = time.time()
        
        try:
            self.logger.info(
                "Starting tileset generation",
                zoom_levels=zoom_levels,
                layers=list(data_sources.keys()),
                bbox=bbox
            )
            
            # Validate inputs
            self._validate_inputs(data_sources, zoom_levels)
            
            # Prepare data sources
            prepared_data = self._prepare_data_sources(data_sources)
            
            # Generate tiles for each zoom level
            total_tiles = 0
            generation_results = {}
            
            for zoom in sorted(zoom_levels):
                self.logger.info(f"Generating tiles for zoom level {zoom}")
                
                # Get tile bounds for this zoom level
                tile_bounds = self._get_tile_bounds(zoom, bbox)
                
                # Generate tiles for this zoom level
                zoom_results = self._generate_zoom_level(
                    prepared_data,
                    zoom,
                    tile_bounds,
                    output_dir,
                    max_workers
                )
                
                generation_results[zoom] = zoom_results
                total_tiles += zoom_results['tiles_generated']
                
                self.logger.info(
                    f"Completed zoom level {zoom}",
                    tiles_generated=zoom_results['tiles_generated'],
                    processing_time=zoom_results['processing_time']
                )
            
            # Calculate overall statistics
            total_time = time.time() - start_time
            
            self.stats['tiles_generated'] += total_tiles
            self.stats['total_processing_time'] += total_time
            
            # Collect metrics
            self.metrics.record_histogram('tileset_generation_duration', total_time)
            self.metrics.increment_counter('tilesets_generated')
            
            result = {
                'success': True,
                'total_tiles': total_tiles,
                'zoom_levels': zoom_levels,
                'processing_time': total_time,
                'zoom_results': generation_results,
                'output_dir': str(output_dir) if output_dir else None
            }
            
            self.logger.info(
                "Tileset generation completed",
                total_tiles=total_tiles,
                processing_time=total_time
            )
            
            return result
            
        except Exception as e:
            error_msg = f"Tileset generation failed: {str(e)}"
            self.logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            
            # Collect error metrics
            self.metrics.increment_counter('tileset_generation_failures')
            
            return {
                'success': False,
                'error': error_msg,
                'processing_time': time.time() - start_time
            }
    
    def generate_single_tile(
        self,
        tile_spec: TileSpec,
        data_sources: Dict[str, gpd.GeoDataFrame]
    ) -> Optional[bytes]:
        """
        Generate a single vector tile.
        
        Args:
            tile_spec: Tile specification (x, y, z, bbox)
            data_sources: Dictionary mapping layer names to GeoDataFrames
            
        Returns:
            Tile data as bytes, or None if tile is empty
        """
        try:
            self.logger.debug(
                "Generating single tile",
                tile_id=tile_spec.tile_id,
                bbox=tile_spec.bbox
            )
            
            # Create tile bounding box with buffer
            tile_bbox = self._create_buffered_bbox(tile_spec)
            
            # Extract and process data for each layer
            tile_layers = {}
            
            for layer_name, layer_config in self.layer_configs.items():
                if layer_name not in data_sources:
                    continue
                
                # Check if layer should be included at this zoom level
                if not (layer_config.min_zoom <= tile_spec.z <= layer_config.max_zoom):
                    continue
                
                # Extract features for this tile
                layer_features = self._extract_tile_features(
                    data_sources[layer_name],
                    tile_bbox,
                    layer_config,
                    tile_spec.z
                )
                
                if not layer_features.empty:
                    # Convert to tile coordinates
                    tile_features = self._convert_to_tile_coordinates(
                        layer_features,
                        tile_spec
                    )
                    
                    # Prepare layer data for MVT
                    layer_data = self._prepare_layer_data(
                        tile_features,
                        layer_config
                    )
                    
                    if layer_data['features']:
                        tile_layers[layer_name] = layer_data
            
            # Generate tile if it has content
            if tile_layers:
                if self.output_format == 'mvt':
                    tile_data = mapbox_vector_tile.encode(tile_layers)
                    return tile_data
                elif self.output_format == 'geojson':
                    # Convert to GeoJSON format
                    geojson_data = self._convert_to_geojson(tile_layers)
                    return json.dumps(geojson_data).encode('utf-8')
                else:
                    raise ValueError(f"Unsupported output format: {self.output_format}")
            
            return None
            
        except Exception as e:
            self.logger.error(
                "Failed to generate tile",
                tile_id=tile_spec.tile_id,
                error=str(e)
            )
            return None
    
    def _validate_inputs(
        self,
        data_sources: Dict[str, gpd.GeoDataFrame],
        zoom_levels: List[int]
    ) -> None:
        """Validate input parameters."""
        # Check zoom levels
        if not zoom_levels:
            raise ValueError("No zoom levels specified")
        
        if min(zoom_levels) < 0 or max(zoom_levels) > 22:
            raise ValueError("Zoom levels must be between 0 and 22")
        
        # Check data sources
        for layer_name, layer_config in self.layer_configs.items():
            if layer_name not in data_sources:
                self.logger.warning(f"No data source provided for layer: {layer_name}")
                continue
            
            gdf = data_sources[layer_name]
            
            # Check if GeoDataFrame has geometry
            if not isinstance(gdf, gpd.GeoDataFrame):
                raise ValueError(f"Data source for layer {layer_name} must be a GeoDataFrame")
            
            if gdf.empty:
                self.logger.warning(f"Data source for layer {layer_name} is empty")
                continue
            
            # Check CRS
            if gdf.crs is None:
                raise ValueError(f"Data source for layer {layer_name} has no CRS defined")
    
    def _prepare_data_sources(
        self,
        data_sources: Dict[str, gpd.GeoDataFrame]
    ) -> Dict[str, gpd.GeoDataFrame]:
        """Prepare data sources for tile generation."""
        prepared_data = {}
        
        for layer_name, gdf in data_sources.items():
            if layer_name not in self.layer_configs:
                continue
            
            self.logger.info(f"Preparing data source for layer: {layer_name}")
            
            # Ensure data is in WGS84
            if gdf.crs.to_epsg() != self.WGS84_EPSG:
                gdf = gdf.to_crs(epsg=self.WGS84_EPSG)
            
            # Apply layer filters if specified
            layer_config = self.layer_configs[layer_name]
            if layer_config.filters:
                gdf = self._apply_filters(gdf, layer_config.filters)
            
            # Create spatial index for efficient querying
            gdf.sindex  # This creates the spatial index
            
            prepared_data[layer_name] = gdf
            
            self.logger.info(
                f"Prepared layer {layer_name}",
                feature_count=len(gdf),
                crs=gdf.crs.to_epsg()
            )
        
        return prepared_data
    
    def _get_tile_bounds(
        self,
        zoom: int,
        bbox: Optional[Tuple[float, float, float, float]] = None
    ) -> List[Tuple[int, int]]:
        """Get list of tile coordinates for a zoom level."""
        if bbox:
            # Calculate tile bounds for the specified bounding box
            min_x, min_y, max_x, max_y = bbox
            
            # Convert to tile coordinates
            min_tile_x, max_tile_y = self._deg_to_tile(min_x, min_y, zoom)
            max_tile_x, min_tile_y = self._deg_to_tile(max_x, max_y, zoom)
            
            tile_bounds = []
            for x in range(min_tile_x, max_tile_x + 1):
                for y in range(min_tile_y, max_tile_y + 1):
                    tile_bounds.append((x, y))
            
            return tile_bounds
        else:
            # Generate all tiles for the zoom level (global)
            num_tiles = 2 ** zoom
            tile_bounds = []
            
            for x in range(num_tiles):
                for y in range(num_tiles):
                    tile_bounds.append((x, y))
            
            return tile_bounds
    
    def _generate_zoom_level(
        self,
        data_sources: Dict[str, gpd.GeoDataFrame],
        zoom: int,
        tile_bounds: List[Tuple[int, int]],
        output_dir: Optional[Path],
        max_workers: int
    ) -> Dict[str, Any]:
        """Generate all tiles for a specific zoom level."""
        start_time = time.time()
        tiles_generated = 0
        
        # Create tile specifications
        tile_specs = []
        for x, y in tile_bounds:
            bbox = self._tile_to_bbox(x, y, zoom)
            tile_spec = TileSpec(x=x, y=y, z=zoom, bbox=bbox)
            tile_specs.append(tile_spec)
        
        # Generate tiles using thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tile generation tasks
            future_to_tile = {
                executor.submit(self.generate_single_tile, tile_spec, data_sources): tile_spec
                for tile_spec in tile_specs
            }
            
            # Process completed tiles
            for future in concurrent.futures.as_completed(future_to_tile):
                tile_spec = future_to_tile[future]
                
                try:
                    tile_data = future.result()
                    
                    if tile_data:
                        # Save tile to disk if output directory specified
                        if output_dir:
                            self._save_tile(tile_data, tile_spec, output_dir)
                        
                        tiles_generated += 1
                        
                        # Update statistics
                        self.stats['total_features_processed'] += self._count_tile_features(tile_data)
                        
                except Exception as e:
                    self.logger.error(
                        "Error processing tile",
                        tile_id=tile_spec.tile_id,
                        error=str(e)
                    )
                    self.stats['errors'].append(f"Tile {tile_spec.tile_id}: {str(e)}")
        
        processing_time = time.time() - start_time
        
        return {
            'tiles_generated': tiles_generated,
            'processing_time': processing_time,
            'total_tile_specs': len(tile_specs)
        }
    
    def _extract_tile_features(
        self,
        gdf: gpd.GeoDataFrame,
        tile_bbox: Tuple[float, float, float, float],
        layer_config: LayerConfig,
        zoom: int
    ) -> gpd.GeoDataFrame:
        """Extract features that intersect with the tile bounding box."""
        try:
            # Create bounding box geometry
            minx, miny, maxx, maxy = tile_bbox
            bbox_geom = box(minx, miny, maxx, maxy)
            
            # Use spatial index for efficient querying
            possible_matches_index = list(gdf.sindex.intersection(tile_bbox))
            possible_matches = gdf.iloc[possible_matches_index]
            
            # Filter features that actually intersect
            intersecting_features = possible_matches[
                possible_matches.geometry.intersects(bbox_geom)
            ]
            
            if intersecting_features.empty:
                return intersecting_features
            
            # Apply zoom-based filtering
            filtered_features = self._apply_zoom_filtering(
                intersecting_features, 
                layer_config, 
                zoom
            )
            
            # Simplify geometries if needed
            if layer_config.simplification_tolerance:
                tolerance = self._calculate_simplification_tolerance(
                    layer_config.simplification_tolerance, 
                    zoom
                )
                filtered_features = filtered_features.copy()
                filtered_features.geometry = filtered_features.geometry.simplify(tolerance)
            
            # Select only required attributes
            if layer_config.attributes:
                columns_to_keep = [layer_config.geometry_column] + layer_config.attributes
                available_columns = [col for col in columns_to_keep if col in filtered_features.columns]
                filtered_features = filtered_features[available_columns]
            
            return filtered_features
            
        except Exception as e:
            self.logger.error(f"Error extracting tile features: {str(e)}")
            return gpd.GeoDataFrame()
    
    def _convert_to_tile_coordinates(
        self,
        gdf: gpd.GeoDataFrame,
        tile_spec: TileSpec
    ) -> gpd.GeoDataFrame:
        """Convert geometries to tile coordinate system (0-4096 range)."""
        try:
            # Get tile bounds in degrees
            minx, miny, maxx, maxy = tile_spec.bbox
            
            # Calculate scale factors
            tile_width = maxx - minx
            tile_height = maxy - miny
            
            # Create a copy to avoid modifying original data
            tile_gdf = gdf.copy()
            
            # Transform geometries to tile coordinates
            def transform_to_tile_coords(geom):
                if geom is None or geom.is_empty:
                    return geom
                
                # Scale and translate geometry to tile coordinates
                # MVT uses 4096x4096 coordinate system
                extent = 4096
                
                def coord_transform(x, y, z=None):
                    # Normalize to 0-1 range
                    norm_x = (x - minx) / tile_width
                    norm_y = (y - miny) / tile_height
                    
                    # Scale to tile extent
                    tile_x = norm_x * extent
                    tile_y = (1 - norm_y) * extent  # Flip Y axis for tile coordinates
                    
                    if z is not None:
                        return tile_x, tile_y, z
                    return tile_x, tile_y
                
                return transform(coord_transform, geom)
            
            tile_gdf.geometry = tile_gdf.geometry.apply(transform_to_tile_coords)
            
            return tile_gdf
            
        except Exception as e:
            self.logger.error(f"Error converting to tile coordinates: {str(e)}")
            return gdf
    
    def _prepare_layer_data(
        self,
        gdf: gpd.GeoDataFrame,
        layer_config: LayerConfig
    ) -> Dict[str, Any]:
        """Prepare layer data for MVT encoding."""
        features = []
        
        for idx, row in gdf.iterrows():
            try:
                geom = row.geometry
                
                if geom is None or geom.is_empty:
                    continue
                
                # Convert geometry to appropriate format
                if hasattr(geom, '__geo_interface__'):
                    geometry_dict = geom.__geo_interface__
                else:
                    # Fallback for complex geometries
                    geometry_dict = self._geometry_to_dict(geom)
                
                # Prepare properties
                properties = {}
                for attr in layer_config.attributes:
                    if attr in row.index and attr != layer_config.geometry_column:
                        value = row[attr]
                        # Convert numpy types to Python types for JSON serialization
                        if hasattr(value, 'item'):
                            value = value.item()
                        elif pd.isna(value):
                            value = None
                        properties[attr] = value
                
                feature = {
                    'geometry': geometry_dict,
                    'properties': properties
                }
                
                features.append(feature)
                
            except Exception as e:
                self.logger.warning(f"Error processing feature {idx}: {str(e)}")
                continue
        
        return {
            'features': features,
            'extent': 4096,  # MVT standard extent
            'version': 2     # MVT version
        }
    
    def _apply_filters(
        self,
        gdf: gpd.GeoDataFrame,
        filters: Dict[str, Any]
    ) -> gpd.GeoDataFrame:
        """Apply filters to GeoDataFrame."""
        filtered_gdf = gdf.copy()
        
        for column, filter_value in filters.items():
            if column not in filtered_gdf.columns:
                continue
            
            if isinstance(filter_value, list):
                # Filter by list of values
                filtered_gdf = filtered_gdf[filtered_gdf[column].isin(filter_value)]
            elif isinstance(filter_value, dict):
                # Handle range filters
                if 'min' in filter_value:
                    filtered_gdf = filtered_gdf[filtered_gdf[column] >= filter_value['min']]
                if 'max' in filter_value:
                    filtered_gdf = filtered_gdf[filtered_gdf[column] <= filter_value['max']]
            else:
                # Exact match filter
                filtered_gdf = filtered_gdf[filtered_gdf[column] == filter_value]
        
        return filtered_gdf
    
    def _apply_zoom_filtering(
        self,
        gdf: gpd.GeoDataFrame,
        layer_config: LayerConfig,
        zoom: int
    ) -> gpd.GeoDataFrame:
        """Apply zoom-level specific filtering."""
        # Basic implementation - can be extended for more sophisticated filtering
        
        # Filter by geometry size at different zoom levels
        if zoom <= 8:
            # At low zoom levels, filter out very small features
            if hasattr(gdf.geometry.iloc[0], 'area'):
                # For polygons, filter by area
                min_area = 1e-6  # Adjust based on requirements
                gdf = gdf[gdf.geometry.area >= min_area]
            elif hasattr(gdf.geometry.iloc[0], 'length'):
                # For lines, filter by length
                min_length = 1e-4  # Adjust based on requirements
                gdf = gdf[gdf.geometry.length >= min_length]
        
        return gdf
    
    def _calculate_simplification_tolerance(
        self,
        base_tolerance: float,
        zoom: int
    ) -> float:
        """Calculate simplification tolerance based on zoom level."""
        # Higher zoom = more detail = lower tolerance
        zoom_factor = 2 ** (14 - zoom)  # Adjust base zoom as needed
        return base_tolerance * zoom_factor
    
    def _create_buffered_bbox(
        self,
        tile_spec: TileSpec
    ) -> Tuple[float, float, float, float]:
        """Create a buffered bounding box for tile extraction."""
        minx, miny, maxx, maxy = tile_spec.bbox
        
        # Calculate buffer in degrees based on tile size
        width = maxx - minx
        height = maxy - miny
        
        # Buffer as percentage of tile size
        buffer_percent = self.buffer_size / self.tile_size  # Convert pixel buffer to percentage
        
        buffer_x = width * buffer_percent
        buffer_y = height * buffer_percent
        
        return (
            minx - buffer_x,
            miny - buffer_y,
            maxx + buffer_x,
            maxy + buffer_y
        )
    
    def _tile_to_bbox(
        self,
        x: int,
        y: int,
        zoom: int
    ) -> Tuple[float, float, float, float]:
        """Convert tile coordinates to bounding box in degrees."""
        n = 2.0 ** zoom
        
        # Calculate longitude bounds
        lon_min = x / n * 360.0 - 180.0
        lon_max = (x + 1) / n * 360.0 - 180.0
        
        # Calculate latitude bounds
        lat_rad_min = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
        lat_rad_max = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        
        lat_min = math.degrees(lat_rad_min)
        lat_max = math.degrees(lat_rad_max)
        
        return (lon_min, lat_min, lon_max, lat_max)
    
    def _deg_to_tile(
        self,
        lon: float,
        lat: float,
        zoom: int
    ) -> Tuple[int, int]:
        """Convert longitude/latitude to tile coordinates."""
        n = 2.0 ** zoom
        
        x = int((lon + 180.0) / 360.0 * n)
        
        lat_rad = math.radians(lat)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        
        return (x, y)
    
    def _save_tile(
        self,
        tile_data: bytes,
        tile_spec: TileSpec,
        output_dir: Path
    ) -> None:
        """Save tile data to disk."""
        try:
            # Create directory structure
            tile_dir = output_dir / str(tile_spec.z) / str(tile_spec.x)
            tile_dir.mkdir(parents=True, exist_ok=True)
            
            # Determine file extension
            if self.output_format == 'mvt':
                extension = 'mvt'
            elif self.output_format == 'geojson':
                extension = 'geojson'
            else:
                extension = 'tile'
            
            # Save tile file
            tile_path = tile_dir / f"{tile_spec.y}.{extension}"
            
            with open(tile_path, 'wb') as f:
                f.write(tile_data)
                
        except Exception as e:
            self.logger.error(f"Error saving tile {tile_spec.tile_id}: {str(e)}")
    
    def _count_tile_features(self, tile_data: bytes) -> int:
        """Count features in a tile (approximate)."""
        try:
            if self.output_format == 'mvt':
                # Decode MVT to count features
                decoded = mapbox_vector_tile.decode(tile_data)
                total_features = sum(len(layer['features']) for layer in decoded.values())
                return total_features
            elif self.output_format == 'geojson':
                # Parse GeoJSON to count features
                geojson_data = json.loads(tile_data.decode('utf-8'))
                if 'features' in geojson_data:
                    return len(geojson_data['features'])
                else:
                    # Count features in all layers
                    total_features = 0
                    for layer_data in geojson_data.values():
                        if isinstance(layer_data, dict) and 'features' in layer_data:
                            total_features += len(layer_data['features'])
                    return total_features
        except Exception:
            pass
        
        return 0
    
    def _geometry_to_dict(self, geom) -> Dict[str, Any]:
        """Convert Shapely geometry to dictionary format."""
        try:
            return geom.__geo_interface__
        except Exception:
            # Fallback for complex geometries
            if hasattr(geom, 'geom_type'):
                if geom.geom_type == 'Point':
                    return {
                        'type': 'Point',
                        'coordinates': [geom.x, geom.y]
                    }
                elif geom.geom_type == 'LineString':
                    return {
                        'type': 'LineString',
                        'coordinates': list(geom.coords)
                    }
                elif geom.geom_type == 'Polygon':
                    exterior_coords = list(geom.exterior.coords)
                    interior_coords = [list(interior.coords) for interior in geom.interiors]
                    coordinates = [exterior_coords] + interior_coords
                    return {
                        'type': 'Polygon',
                        'coordinates': coordinates
                    }
            
            # Default fallback
            return {'type': 'Point', 'coordinates': [0, 0]}
    
    def _convert_to_geojson(self, tile_layers: Dict[str, Any]) -> Dict[str, Any]:
        """Convert tile layers to GeoJSON format."""
        if len(tile_layers) == 1:
            # Single layer - return as FeatureCollection
            layer_data = list(tile_layers.values())[0]
            return {
                'type': 'FeatureCollection',
                'features': layer_data['features']
            }
        else:
            # Multiple layers - return as dictionary of FeatureCollections
            geojson_data = {}
            for layer_name, layer_data in tile_layers.items():
                geojson_data[layer_name] = {
                    'type': 'FeatureCollection',
                    'features': layer_data['features']
                }
            return geojson_data
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get tile generation statistics."""
        return self.stats.copy()
    
    def reset_stats(self) -> None:
        """Reset generation statistics."""
        self.stats = {
            'tiles_generated': 0,
            'total_features_processed': 0,
            'total_processing_time': 0.0,
            'errors': []
        }


# Example usage and configuration
if __name__ == "__main__":
    # Example layer configurations
    layer_configs = [
        LayerConfig(
            name="roads",
            source_table="osm_roads",
            min_zoom=0,
            max_zoom=14,
            attributes=["name", "highway", "surface"],
            filters={"highway": ["motorway", "trunk", "primary", "secondary"]},
            simplification_tolerance=0.001
        ),
        LayerConfig(
            name="buildings",
            source_table="osm_buildings",
            min_zoom=12,
            max_zoom=18,
            attributes=["name", "building", "height"],
            simplification_tolerance=0.0001
        ),
        LayerConfig(
            name="pois",
            source_table="osm_pois",
            min_zoom=8,
            max_zoom=16,
            attributes=["name", "amenity", "shop"],
            filters={"amenity": ["restaurant", "hospital", "school"]}
        )
    ]
    
    # Mock configuration
    class MockConfig:
        def __init__(self):
            self.tile_generation = {
                'max_workers': 4,
                'buffer_size': 64,
                'output_format': 'mvt'
            }
    
    # Initialize generator
    config = MockConfig()
    generator = VectorTileGenerator(
        config=config,
        layer_configs=layer_configs,
        output_format="mvt"
    )
    
    print("Vector Tile Generator initialized successfully!")
    print(f"Configured layers: {list(generator.layer_configs.keys())}")
    print(f"Output format: {generator.output_format}")
    print(f"Tile size: {generator.tile_size}x{generator.tile_size}")
    
    # Example of generating a single tile
    tile_spec = TileSpec(x=1024, y=1024, z=11, bbox=(-1.0, 51.0, 0.0, 52.0))
    print(f"Example tile spec: {tile_spec.tile_id}")
    print(f"Tile bbox: {tile_spec.bbox}")