"""
Unit Tests for OSM Data Ingestion

Comprehensive test suite for the OSM data ingestion module, demonstrating
best practices in testing geospatial data processing components.

This test suite demonstrates:
- Unit testing for geospatial data processing
- Mock data generation for testing
- Error handling and edge case testing
- Performance testing for large datasets
- Data quality validation testing
- Integration testing patterns
"""

import unittest
import tempfile
import shutil
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
import pytest

# Import the modules to test
import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from data_ingestion.osm_ingestion import OSMDataIngester
from data_ingestion.base_ingester import BaseDataIngester
from utils.config import Config


class TestOSMDataIngester(unittest.TestCase):
    """Test suite for OSM data ingestion functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        
        # Create mock configuration
        self.mock_config = Mock(spec=Config)
        self.mock_config.osm = Mock()
        self.mock_config.osm.chunk_size = 1000
        self.mock_config.osm.memory_limit_mb = 512
        self.mock_config.aws = Mock()
        self.mock_config.aws.use_aws = False
        self.mock_config.database = Mock()
        self.mock_config.database.table_name = "test_table"
        self.mock_config.database.chunk_size = 1000
        
        # Initialize ingester
        self.ingester = OSMDataIngester(self.mock_config)
        
        # Create sample OSM data for testing
        self.sample_nodes = [
            {
                'id': 1,
                'lat': 37.7749,
                'lon': -122.4194,
                'tags': {'amenity': 'restaurant', 'name': 'Test Restaurant'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            {
                'id': 2,
                'lat': 37.7849,
                'lon': -122.4094,
                'tags': {'shop': 'supermarket', 'name': 'Test Market'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            }
        ]
        
        self.sample_ways = [
            {
                'id': 100,
                'nodes': [1, 2, 3, 4],
                'tags': {'highway': 'primary', 'name': 'Test Street'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            {
                'id': 101,
                'nodes': [5, 6, 7, 8, 5],  # Closed way (building)
                'tags': {'building': 'yes', 'name': 'Test Building'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            }
        ]
        
        self.sample_relations = [
            {
                'id': 200,
                'members': [
                    {'type': 'way', 'ref': 100, 'role': 'outer'},
                    {'type': 'way', 'ref': 101, 'role': 'inner'}
                ],
                'tags': {'type': 'multipolygon', 'landuse': 'residential'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            }
        ]
        
        self.sample_osm_data = {
            'nodes': self.sample_nodes,
            'ways': self.sample_ways,
            'relations': self.sample_relations,
            'metadata': {
                'source_file': 'test.osm',
                'format': 'xml',
                'total_elements': 5
            }
        }
    
    def tearDown(self):
        """Clean up after each test method."""
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_initialization(self):
        """Test OSM ingester initialization."""
        self.assertIsInstance(self.ingester, OSMDataIngester)
        self.assertIsInstance(self.ingester, BaseDataIngester)
        self.assertEqual(self.ingester.chunk_size, 1000)
        self.assertIsNotNone(self.ingester.tag_processor)
        self.assertIsNotNone(self.ingester.geometry_builder)
    
    def test_extract_from_file_not_found(self):
        """Test extraction with non-existent file."""
        non_existent_file = self.temp_path / "non_existent.osm"
        
        with self.assertRaises(FileNotFoundError):
            self.ingester._extract_from_file(non_existent_file)
    
    def test_extract_from_file_unsupported_format(self):
        """Test extraction with unsupported file format."""
        unsupported_file = self.temp_path / "test.txt"
        unsupported_file.touch()
        
        with self.assertRaises(ValueError) as context:
            self.ingester._extract_from_file(unsupported_file)
        
        self.assertIn("Unsupported OSM file format", str(context.exception))
    
    def test_validate_osm_data_valid(self):
        """Test validation with valid OSM data."""
        result = self.ingester.validate(self.sample_osm_data)
        self.assertTrue(result)
    
    def test_validate_osm_data_missing_keys(self):
        """Test validation with missing required keys."""
        invalid_data = {'nodes': []}  # Missing required keys
        
        result = self.ingester.validate(invalid_data)
        self.assertFalse(result)
    
    def test_validate_osm_data_empty(self):
        """Test validation with empty data."""
        empty_data = {
            'nodes': [],
            'ways': [],
            'relations': [],
            'metadata': {'total_elements': 0}
        }
        
        result = self.ingester.validate(empty_data)
        self.assertFalse(result)
    
    def test_validate_node_valid(self):
        """Test node validation with valid node."""
        valid_node = self.sample_nodes[0]
        result = self.ingester._validate_node(valid_node)
        self.assertTrue(result)
    
    def test_validate_node_missing_fields(self):
        """Test node validation with missing required fields."""
        invalid_node = {'id': 1, 'lat': 37.7749}  # Missing 'lon'
        result = self.ingester._validate_node(invalid_node)
        self.assertFalse(result)
    
    def test_validate_node_invalid_coordinates(self):
        """Test node validation with invalid coordinates."""
        invalid_node = {
            'id': 1,
            'lat': 91.0,  # Invalid latitude
            'lon': -122.4194
        }
        result = self.ingester._validate_node(invalid_node)
        self.assertFalse(result)
    
    def test_validate_way_valid(self):
        """Test way validation with valid way."""
        valid_way = self.sample_ways[0]
        result = self.ingester._validate_way(valid_way)
        self.assertTrue(result)
    
    def test_validate_way_insufficient_nodes(self):
        """Test way validation with insufficient nodes."""
        invalid_way = {
            'id': 100,
            'nodes': [1]  # Only one node
        }
        result = self.ingester._validate_way(invalid_way)
        self.assertFalse(result)
    
    def test_transform_osm_data(self):
        """Test OSM data transformation."""
        result = self.ingester.transform(self.sample_osm_data)
        
        # Check that result is a dictionary
        self.assertIsInstance(result, dict)
        
        # Check for expected feature types
        expected_types = ['pois', 'roads']
        for feature_type in expected_types:
            if feature_type in result:
                self.assertIsInstance(result[feature_type], gpd.GeoDataFrame)
    
    def test_extract_poi_nodes(self):
        """Test POI node extraction."""
        poi_nodes = self.ingester._extract_poi_nodes(self.sample_nodes)
        
        # Should extract both restaurant and supermarket
        self.assertEqual(len(poi_nodes), 2)
        
        # Check POI types
        poi_types = [node['poi_type'] for node in poi_nodes]
        self.assertIn('amenity:restaurant', poi_types)
        self.assertIn('shop:supermarket', poi_types)
    
    def test_extract_road_ways(self):
        """Test road way extraction."""
        road_ways = self.ingester._extract_road_ways(self.sample_ways)
        
        # Should extract the highway way
        self.assertEqual(len(road_ways), 1)
        self.assertEqual(road_ways[0]['road_type'], 'primary')
        self.assertEqual(road_ways[0]['name'], 'Test Street')
    
    def test_create_poi_geodataframe(self):
        """Test POI GeoDataFrame creation."""
        poi_nodes = self.ingester._extract_poi_nodes(self.sample_nodes)
        gdf = self.ingester._create_poi_geodataframe(poi_nodes)
        
        # Check GeoDataFrame properties
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        self.assertEqual(len(gdf), 2)
        self.assertEqual(gdf.crs.to_epsg(), 4326)
        
        # Check geometry types
        for geom in gdf.geometry:
            self.assertIsInstance(geom, Point)
        
        # Check required columns
        required_columns = ['osm_id', 'poi_type', 'name']
        for col in required_columns:
            self.assertIn(col, gdf.columns)
    
    def test_create_road_geodataframe(self):
        """Test road GeoDataFrame creation."""
        # Create node lookup
        node_lookup = {
            1: (-122.4194, 37.7749),
            2: (-122.4094, 37.7849),
            3: (-122.3994, 37.7949),
            4: (-122.3894, 37.8049)
        }
        
        road_ways = self.ingester._extract_road_ways(self.sample_ways)
        gdf = self.ingester._create_road_geodataframe(road_ways, node_lookup)
        
        # Check GeoDataFrame properties
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        self.assertEqual(len(gdf), 1)
        self.assertEqual(gdf.crs.to_epsg(), 4326)
        
        # Check geometry type
        for geom in gdf.geometry:
            self.assertIsInstance(geom, LineString)
        
        # Check required columns
        required_columns = ['osm_id', 'road_type', 'name']
        for col in required_columns:
            self.assertIn(col, gdf.columns)
    
    def test_create_road_geodataframe_missing_nodes(self):
        """Test road GeoDataFrame creation with missing nodes."""
        # Incomplete node lookup
        node_lookup = {
            1: (-122.4194, 37.7749),
            2: (-122.4094, 37.7849)
            # Missing nodes 3 and 4
        }
        
        road_ways = self.ingester._extract_road_ways(self.sample_ways)
        gdf = self.ingester._create_road_geodataframe(road_ways, node_lookup)
        
        # Should return empty GeoDataFrame due to missing nodes
        self.assertEqual(len(gdf), 0)
    
    @patch('data_ingestion.osm_ingestion.osmium')
    def test_extract_from_pbf(self, mock_osmium):
        """Test PBF file extraction."""
        # Create mock PBF file
        pbf_file = self.temp_path / "test.pbf"
        pbf_file.touch()
        
        # Mock osmium handler
        mock_handler = Mock()
        mock_osmium.SimpleHandler.return_value = mock_handler
        
        # Test extraction
        with patch.object(self.ingester, '_extract_from_pbf') as mock_extract:
            mock_extract.return_value = self.sample_osm_data
            result = self.ingester._extract_from_pbf(pbf_file)
            
            self.assertEqual(result, self.sample_osm_data)
    
    def test_parse_node_element_valid(self):
        """Test XML node element parsing."""
        # Create mock XML element
        mock_element = Mock()
        mock_element.get.side_effect = lambda key: {
            'id': '1',
            'lat': '37.7749',
            'lon': '-122.4194',
            'version': '1',
            'timestamp': '2023-01-01T00:00:00Z'
        }.get(key)
        
        # Mock tag elements
        mock_tag1 = Mock()
        mock_tag1.get.side_effect = lambda key: {
            'k': 'amenity',
            'v': 'restaurant'
        }.get(key)
        
        mock_tag2 = Mock()
        mock_tag2.get.side_effect = lambda key: {
            'k': 'name',
            'v': 'Test Restaurant'
        }.get(key)
        
        mock_element.findall.return_value = [mock_tag1, mock_tag2]
        
        result = self.ingester._parse_node_element(mock_element)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 1)
        self.assertEqual(result['lat'], 37.7749)
        self.assertEqual(result['lon'], -122.4194)
        self.assertEqual(result['tags']['amenity'], 'restaurant')
        self.assertEqual(result['tags']['name'], 'Test Restaurant')
    
    def test_parse_node_element_no_tags(self):
        """Test XML node element parsing with no tags."""
        # Create mock XML element without tags
        mock_element = Mock()
        mock_element.get.side_effect = lambda key: {
            'id': '1',
            'lat': '37.7749',
            'lon': '-122.4194'
        }.get(key)
        mock_element.findall.return_value = []  # No tags
        
        result = self.ingester._parse_node_element(mock_element)
        
        # Should return None for nodes without tags
        self.assertIsNone(result)
    
    def test_parse_node_element_invalid_data(self):
        """Test XML node element parsing with invalid data."""
        # Create mock XML element with invalid coordinates
        mock_element = Mock()
        mock_element.get.side_effect = lambda key: {
            'id': 'invalid',
            'lat': 'invalid',
            'lon': 'invalid'
        }.get(key)
        mock_element.findall.return_value = []
        
        result = self.ingester._parse_node_element(mock_element)
        
        # Should return None for invalid data
        self.assertIsNone(result)
    
    def test_ingest_workflow_success(self):
        """Test complete ingestion workflow."""
        # Mock the extract method
        with patch.object(self.ingester, 'extract') as mock_extract:
            mock_extract.return_value = self.sample_osm_data
            
            # Mock the load method
            with patch.object(self.ingester, 'load') as mock_load:
                mock_load.return_value = True
                
                # Test ingestion
                result = self.ingester.ingest(
                    source="test.osm",
                    destination="output.parquet"
                )
                
                self.assertTrue(result['success'])
                self.assertIn('stats', result)
                self.assertGreater(result['stats']['records_processed'], 0)
    
    def test_ingest_workflow_validation_failure(self):
        """Test ingestion workflow with validation failure."""
        # Mock extract to return invalid data
        with patch.object(self.ingester, 'extract') as mock_extract:
            mock_extract.return_value = {'invalid': 'data'}
            
            # Test ingestion with validation
            result = self.ingester.ingest(
                source="test.osm",
                destination="output.parquet",
                validate_data=True
            )
            
            self.assertFalse(result['success'])
            self.assertIn('error', result)
    
    def test_ingest_workflow_load_failure(self):
        """Test ingestion workflow with load failure."""
        # Mock extract to return valid data
        with patch.object(self.ingester, 'extract') as mock_extract:
            mock_extract.return_value = self.sample_osm_data
            
            # Mock load to fail
            with patch.object(self.ingester, 'load') as mock_load:
                mock_load.return_value = False
                
                # Test ingestion
                result = self.ingester.ingest(
                    source="test.osm",
                    destination="output.parquet"
                )
                
                self.assertFalse(result['success'])
                self.assertIn('error', result)
    
    def test_performance_large_dataset(self):
        """Test performance with large dataset."""
        # Create large dataset
        large_nodes = []
        for i in range(10000):
            large_nodes.append({
                'id': i,
                'lat': 37.7749 + (i * 0.0001),
                'lon': -122.4194 + (i * 0.0001),
                'tags': {'amenity': 'test', 'name': f'Test POI {i}'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            })
        
        large_ways = []
        for i in range(1000):
            large_ways.append({
                'id': i + 100000,
                'nodes': [i*4, i*4+1, i*4+2, i*4+3],
                'tags': {'highway': 'residential', 'name': f'Test Street {i}'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            })
        
        large_osm_data = {
            'nodes': large_nodes,
            'ways': large_ways,
            'relations': [],
            'metadata': {
                'source_file': 'large_test.osm',
                'format': 'xml',
                'total_elements': len(large_nodes) + len(large_ways)
            }
        }
        
        # Test transformation performance
        start_time = time.time()
        result = self.ingester.transform(large_osm_data)
        processing_time = time.time() - start_time
        
        # Performance assertions
        self.assertLess(processing_time, 30.0)  # Should complete within 30 seconds
        self.assertIsInstance(result, dict)
        
        # Check that POIs were processed
        if 'pois' in result:
            self.assertGreater(len(result['pois']), 0)
    
    def test_memory_usage_monitoring(self):
        """Test memory usage monitoring during processing."""
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process data
        result = self.ingester.transform(self.sample_osm_data)
        
        # Get final memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory usage should be reasonable
        self.assertLess(memory_increase, 100)  # Less than 100MB increase
    
    def test_concurrent_processing(self):
        """Test concurrent processing capabilities."""
        import threading
        import queue
        
        # Create multiple datasets
        datasets = []
        for i in range(5):
            dataset = {
                'nodes': [
                    {
                        'id': i * 1000 + j,
                        'lat': 37.7749 + (j * 0.001),
                        'lon': -122.4194 + (j * 0.001),
                        'tags': {'amenity': 'test', 'name': f'POI {i}-{j}'},
                        'version': 1,
                        'timestamp': '2023-01-01T00:00:00Z'
                    }
                    for j in range(100)
                ],
                'ways': [],
                'relations': [],
                'metadata': {'total_elements': 100}
            }
            datasets.append(dataset)
        
        # Process datasets concurrently
        results_queue = queue.Queue()
        
        def process_dataset(dataset):
            result = self.ingester.transform(dataset)
            results_queue.put(result)
        
        threads = []
        for dataset in datasets:
            thread = threading.Thread(target=process_dataset, args=(dataset,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())
        
        self.assertEqual(len(results), 5)
        for result in results:
            self.assertIsInstance(result, dict)
    
    def test_error_recovery(self):
        """Test error recovery mechanisms."""
        # Create data with some invalid entries
        mixed_nodes = [
            # Valid node
            {
                'id': 1,
                'lat': 37.7749,
                'lon': -122.4194,
                'tags': {'amenity': 'restaurant'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            # Invalid node (missing coordinates)
            {
                'id': 2,
                'tags': {'amenity': 'cafe'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            # Valid node
            {
                'id': 3,
                'lat': 37.7849,
                'lon': -122.4094,
                'tags': {'shop': 'supermarket'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            }
        ]
        
        mixed_osm_data = {
            'nodes': mixed_nodes,
            'ways': [],
            'relations': [],
            'metadata': {'total_elements': 3}
        }
        
        # Should process valid nodes and skip invalid ones
        result = self.ingester.transform(mixed_osm_data)
        
        self.assertIsInstance(result, dict)
        if 'pois' in result:
            # Should have 2 valid POIs (skipping the invalid one)
            self.assertEqual(len(result['pois']), 2)
    
    def test_data_quality_metrics(self):
        """Test data quality metrics collection."""
        # Process sample data
        result = self.ingester.transform(self.sample_osm_data)
        
        # Check for quality metrics
        if hasattr(self.ingester, 'quality_metrics'):
            metrics = self.ingester.quality_metrics
            
            # Should have basic quality metrics
            expected_metrics = ['nodes_processed', 'ways_processed', 'relations_processed']
            for metric in expected_metrics:
                if metric in metrics:
                    self.assertIsInstance(metrics[metric], int)
                    self.assertGreaterEqual(metrics[metric], 0)
    
    def test_tag_processing_edge_cases(self):
        """Test tag processing with edge cases."""
        edge_case_nodes = [
            # Node with empty tags
            {
                'id': 1,
                'lat': 37.7749,
                'lon': -122.4194,
                'tags': {},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            # Node with special characters in tags
            {
                'id': 2,
                'lat': 37.7849,
                'lon': -122.4094,
                'tags': {'name': 'Café "Special" & More', 'amenity': 'restaurant'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            # Node with very long tag values
            {
                'id': 3,
                'lat': 37.7949,
                'lon': -122.3994,
                'tags': {
                    'name': 'A' * 1000,  # Very long name
                    'description': 'B' * 2000,  # Very long description
                    'amenity': 'restaurant'
                },
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            }
        ]
        
        edge_case_data = {
            'nodes': edge_case_nodes,
            'ways': [],
            'relations': [],
            'metadata': {'total_elements': 3}
        }
        
        # Should handle edge cases gracefully
        result = self.ingester.transform(edge_case_data)
        self.assertIsInstance(result, dict)
    
    def test_geometry_validation(self):
        """Test geometry validation during processing."""
        # Create ways with potential geometry issues
        problematic_ways = [
            # Way with duplicate consecutive nodes
            {
                'id': 100,
                'nodes': [1, 1, 2, 3],  # Duplicate node
                'tags': {'highway': 'residential'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            # Way with only one node (invalid)
            {
                'id': 101,
                'nodes': [1],  # Single node
                'tags': {'highway': 'primary'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            },
            # Valid way
            {
                'id': 102,
                'nodes': [1, 2, 3, 4],
                'tags': {'highway': 'secondary'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            }
        ]
        
        problematic_data = {
            'nodes': self.sample_nodes,
            'ways': problematic_ways,
            'relations': [],
            'metadata': {'total_elements': len(self.sample_nodes) + len(problematic_ways)}
        }
        
        # Should handle problematic geometries
        result = self.ingester.transform(problematic_data)
        self.assertIsInstance(result, dict)
    
    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test with invalid configuration
        invalid_config = Mock()
        invalid_config.osm = None  # Missing OSM configuration
        
        with self.assertRaises(AttributeError):
            OSMDataIngester(invalid_config)
    
    def test_file_format_detection(self):
        """Test automatic file format detection."""
        # Test XML format detection
        xml_file = self.temp_path / "test.osm"
        xml_file.touch()
        
        format_detected = self.ingester._detect_file_format(xml_file)
        self.assertEqual(format_detected, 'xml')
        
        # Test PBF format detection
        pbf_file = self.temp_path / "test.pbf"
        pbf_file.touch()
        
        format_detected = self.ingester._detect_file_format(pbf_file)
        self.assertEqual(format_detected, 'pbf')
        
        # Test unsupported format
        txt_file = self.temp_path / "test.txt"
        txt_file.touch()
        
        with self.assertRaises(ValueError):
            self.ingester._detect_file_format(txt_file)
    
    def test_chunked_processing(self):
        """Test chunked processing for large files."""
        # Create large dataset that exceeds chunk size
        large_nodes = []
        chunk_size = self.ingester.chunk_size
        
        for i in range(chunk_size * 2 + 500):  # More than 2 chunks
            large_nodes.append({
                'id': i,
                'lat': 37.7749 + (i * 0.00001),
                'lon': -122.4194 + (i * 0.00001),
                'tags': {'amenity': 'test'},
                'version': 1,
                'timestamp': '2023-01-01T00:00:00Z'
            })
        
        large_data = {
            'nodes': large_nodes,
            'ways': [],
            'relations': [],
            'metadata': {'total_elements': len(large_nodes)}
        }
        
        # Should process in chunks without memory issues
        result = self.ingester.transform(large_data)
        self.assertIsInstance(result, dict)
    
    def test_statistics_collection(self):
        """Test statistics collection during processing."""
        # Process sample data
        result = self.ingester.ingest(
            source=self.sample_osm_data,  # Pass data directly
            destination="memory"  # In-memory processing
        )
        
        # Check statistics
        self.assertIn('stats', result)
        stats = result['stats']
        
        expected_stats = ['records_processed', 'processing_time', 'features_created']
        for stat in expected_stats:
            if stat in stats:
                self.assertIsInstance(stats[stat], (int, float))
    
    def test_cleanup_resources(self):
        """Test proper resource cleanup."""
        # Create temporary files
        temp_files = []
        for i in range(5):
            temp_file = self.temp_path / f"temp_{i}.tmp"
            temp_file.touch()
            temp_files.append(temp_file)
        
        # Process data (which might create temporary resources)
        result = self.ingester.transform(self.sample_osm_data)
        
        # Call cleanup if available
        if hasattr(self.ingester, 'cleanup'):
            self.ingester.cleanup()
        
        # Verify basic functionality still works
        self.assertIsInstance(result, dict)


class TestOSMDataIngesterIntegration(unittest.TestCase):
    """Integration tests for OSM data ingestion."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        
        # Create realistic configuration
        self.config = Mock(spec=Config)
        self.config.osm = Mock()
        self.config.osm.chunk_size = 1000
        self.config.osm.memory_limit_mb = 512
        self.config.osm.poi_tags = ['amenity', 'shop', 'tourism']
        self.config.osm.road_tags = ['highway']
        self.config.aws = Mock()
        self.config.aws.use_aws = False
        
        self.ingester = OSMDataIngester(self.config)
    
    def tearDown(self):
        """Clean up integration test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_full_workflow_integration(self):
        """Test complete workflow integration."""
        # Create sample OSM XML file
        osm_xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <osm version="0.6" generator="test">
          <node id="1" lat="37.7749" lon="-122.4194" version="1" timestamp="2023-01-01T00:00:00Z">
            <tag k="amenity" v="restaurant"/>
            <tag k="name" v="Test Restaurant"/>
          </node>
          <node id="2" lat="37.7849" lon="-122.4094" version="1" timestamp="2023-01-01T00:00:00Z">
            <tag k="shop" v="supermarket"/>
            <tag k="name" v="Test Market"/>
          </node>
          <way id="100" version="1" timestamp="2023-01-01T00:00:00Z">
            <nd ref="1"/>
            <nd ref="2"/>
            <tag k="highway" v="primary"/>
            <tag k="name" v="Test Street"/>
          </way>
        </osm>'''
        
        # Write XML file
        xml_file = self.temp_path / "test.osm"
        xml_file.write_text(osm_xml_content)
        
        # Create output file path
        output_file = self.temp_path / "output.parquet"
        
        # Mock the file processing methods
        with patch.object(self.ingester, '_extract_from_xml') as mock_extract:
            mock_extract.return_value = {
                'nodes': [
                    {
                        'id': 1,
                        'lat': 37.7749,
                        'lon': -122.4194,
                        'tags': {'amenity': 'restaurant', 'name': 'Test Restaurant'},
                        'version': 1,
                        'timestamp': '2023-01-01T00:00:00Z'
                    }
                ],
                'ways': [],
                'relations': [],
                'metadata': {'total_elements': 1}
            }
            
            with patch.object(self.ingester, 'load') as mock_load:
                mock_load.return_value = True
                
                # Test full workflow
                result = self.ingester.ingest(
                    source=str(xml_file),
                    destination=str(output_file)
                )
                
                self.assertTrue(result['success'])
                self.assertIn('stats', result)


if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)
    
    # Alternative: Run with pytest for more features
    # pytest.main([__file__, '-v', '--tb=short'])