"""
Basemap Data Processing Pipeline

A comprehensive data engineering solution for processing geospatial data
and generating high-quality map tiles for automotive, logistics, consumer,
outdoor, and gaming applications.

Author: Adebisi Ayokunle
Project: Basemap Data Processing Pipeline
"""

__version__ = "1.0.0"
__author__ = "Adebisi Ayokunle"
__email__ = "adeways2000@gmail.com"

# Core modules
from . import data_ingestion
from . import processing
from . import tile_generation
from . import monitoring
from . import utils

__all__ = [
    "data_ingestion",
    "processing", 
    "tile_generation",
    "monitoring",
    "utils"
]