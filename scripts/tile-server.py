#!/usr/bin/env python3
"""
Basemap Tile Server

A FastAPI-based tile server for serving generated map tiles.
Supports MVT (Mapbox Vector Tiles) and provides tile metadata.
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
import asyncio
import aiofiles
from fastapi import FastAPI, HTTPException, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import uvicorn
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Configuration
TILE_DIR = Path(os.getenv("TILE_DIR", "/app/tiles"))
PORT = int(os.getenv("PORT", "8000"))
HOST = os.getenv("HOST", "0.0.0.0")

# Create FastAPI app
app = FastAPI(
    title="Basemap Tile Server",
    description="High-performance tile server for basemap data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Tile cache for metadata
tile_cache: Dict[str, Dict[str, Any]] = {}

@app.on_event("startup")
async def startup_event():
    """Initialize the tile server."""
    logger.info("Starting Basemap Tile Server", tile_dir=str(TILE_DIR), port=PORT)
    
    # Create tile directory if it doesn't exist
    TILE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load tile metadata cache
    await load_tile_metadata()
    
    logger.info("Tile server initialized successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down Basemap Tile Server")

async def load_tile_metadata():
    """Load tile metadata into cache."""
    try:
        metadata_file = TILE_DIR / "metadata.json"
        if metadata_file.exists():
            async with aiofiles.open(metadata_file, 'r') as f:
                content = await f.read()
                global tile_cache
                tile_cache = json.loads(content)
                logger.info("Loaded tile metadata", tiles_count=len(tile_cache))
        else:
            logger.info("No metadata file found, starting with empty cache")
    except Exception as e:
        logger.error("Failed to load tile metadata", error=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "basemap-tile-server",
        "version": "1.0.0",
        "tile_directory": str(TILE_DIR),
        "tiles_cached": len(tile_cache)
    }

@app.get("/")
async def root():
    """Root endpoint with server information."""
    return {
        "service": "Basemap Tile Server",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "tiles": "/tiles/{z}/{x}/{y}.{format}",
            "metadata": "/metadata",
            "tile_info": "/tiles/{z}/{x}/{y}/info",
            "docs": "/docs"
        },
        "supported_formats": ["mvt", "pbf", "geojson"],
        "tile_directory": str(TILE_DIR)
    }

@app.get("/tiles/{z}/{x}/{y}.{format}")
async def get_tile(
    z: int,
    x: int,
    y: int,
    format: str,
    response: Response
):
    """
    Serve a map tile.
    
    Args:
        z: Zoom level
        x: Tile X coordinate
        y: Tile Y coordinate
        format: Tile format (mvt, pbf, geojson)
    """
    # Validate parameters
    if z < 0 or z > 18:
        raise HTTPException(status_code=400, detail="Invalid zoom level")
    
    if format not in ["mvt", "pbf", "geojson"]:
        raise HTTPException(status_code=400, detail="Unsupported format")
    
    # Construct tile path
    tile_path = TILE_DIR / str(z) / str(x) / f"{y}.{format}"
    
    # Check if tile exists
    if not tile_path.exists():
        # Try alternative extensions
        if format in ["mvt", "pbf"]:
            alt_format = "pbf" if format == "mvt" else "mvt"
            alt_path = TILE_DIR / str(z) / str(x) / f"{y}.{alt_format}"
            if alt_path.exists():
                tile_path = alt_path
            else:
                raise HTTPException(status_code=404, detail="Tile not found")
        else:
            raise HTTPException(status_code=404, detail="Tile not found")
    
    # Set appropriate headers
    if format in ["mvt", "pbf"]:
        response.headers["Content-Type"] = "application/x-protobuf"
        response.headers["Content-Encoding"] = "gzip"
    elif format == "geojson":
        response.headers["Content-Type"] = "application/json"
    
    # Set caching headers
    response.headers["Cache-Control"] = "public, max-age=3600"
    response.headers["Access-Control-Allow-Origin"] = "*"
    
    # Log tile request
    logger.info("Serving tile", z=z, x=x, y=y, format=format, path=str(tile_path))
    
    return FileResponse(tile_path)

@app.get("/tiles/{z}/{x}/{y}/info")
async def get_tile_info(z: int, x: int, y: int):
    """
    Get metadata information for a specific tile.
    
    Args:
        z: Zoom level
        x: Tile X coordinate
        y: Tile Y coordinate
    """
    tile_key = f"{z}/{x}/{y}"
    
    if tile_key in tile_cache:
        return tile_cache[tile_key]
    
    # Try to find tile file and extract basic info
    tile_formats = ["mvt", "pbf", "geojson"]
    tile_info = None
    
    for format in tile_formats:
        tile_path = TILE_DIR / str(z) / str(x) / f"{y}.{format}"
        if tile_path.exists():
            stat = tile_path.stat()
            tile_info = {
                "z": z,
                "x": x,
                "y": y,
                "format": format,
                "size_bytes": stat.st_size,
                "modified": stat.st_mtime,
                "path": str(tile_path.relative_to(TILE_DIR))
            }
            break
    
    if tile_info is None:
        raise HTTPException(status_code=404, detail="Tile not found")
    
    return tile_info

@app.get("/metadata")
async def get_metadata(
    z: Optional[int] = Query(None, description="Filter by zoom level"),
    limit: Optional[int] = Query(100, description="Limit number of results")
):
    """
    Get tile metadata.
    
    Args:
        z: Optional zoom level filter
        limit: Maximum number of tiles to return
    """
    filtered_tiles = tile_cache
    
    if z is not None:
        filtered_tiles = {
            k: v for k, v in tile_cache.items()
            if v.get("z") == z
        }
    
    # Limit results
    limited_tiles = dict(list(filtered_tiles.items())[:limit])
    
    return {
        "total_tiles": len(tile_cache),
        "filtered_tiles": len(filtered_tiles),
        "returned_tiles": len(limited_tiles),
        "tiles": limited_tiles
    }

@app.get("/stats")
async def get_stats():
    """Get server statistics."""
    try:
        # Count tiles by zoom level
        zoom_counts = {}
        format_counts = {}
        total_size = 0
        
        for tile_path in TILE_DIR.rglob("*.*"):
            if tile_path.is_file():
                parts = tile_path.relative_to(TILE_DIR).parts
                if len(parts) >= 3:
                    z = int(parts[0])
                    format = tile_path.suffix[1:]  # Remove the dot
                    
                    zoom_counts[z] = zoom_counts.get(z, 0) + 1
                    format_counts[format] = format_counts.get(format, 0) + 1
                    total_size += tile_path.stat().st_size
        
        return {
            "total_tiles": sum(zoom_counts.values()),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "zoom_levels": sorted(zoom_counts.keys()),
            "tiles_by_zoom": zoom_counts,
            "tiles_by_format": format_counts,
            "tile_directory": str(TILE_DIR)
        }
    
    except Exception as e:
        logger.error("Failed to generate stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to generate statistics")

@app.post("/refresh")
async def refresh_metadata():
    """Refresh tile metadata cache."""
    try:
        await load_tile_metadata()
        return {
            "status": "success",
            "message": "Metadata cache refreshed",
            "tiles_loaded": len(tile_cache)
        }
    except Exception as e:
        logger.error("Failed to refresh metadata", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to refresh metadata")

@app.get("/tiles/{z}")
async def list_tiles_by_zoom(z: int):
    """List all tiles for a specific zoom level."""
    if z < 0 or z > 18:
        raise HTTPException(status_code=400, detail="Invalid zoom level")
    
    zoom_dir = TILE_DIR / str(z)
    if not zoom_dir.exists():
        return {"tiles": [], "count": 0}
    
    tiles = []
    for x_dir in zoom_dir.iterdir():
        if x_dir.is_dir():
            x = int(x_dir.name)
            for tile_file in x_dir.iterdir():
                if tile_file.is_file():
                    y = int(tile_file.stem)
                    format = tile_file.suffix[1:]
                    tiles.append({
                        "z": z,
                        "x": x,
                        "y": y,
                        "format": format,
                        "url": f"/tiles/{z}/{x}/{y}.{format}"
                    })
    
    return {
        "zoom_level": z,
        "tiles": tiles,
        "count": len(tiles)
    }

@app.get("/bounds/{z}/{x}/{y}")
async def get_tile_bounds(z: int, x: int, y: int):
    """Get geographic bounds for a tile."""
    import math
    
    def tile_to_lat_lon(z, x, y):
        """Convert tile coordinates to lat/lon bounds."""
        n = 2.0 ** z
        lon_left = x / n * 360.0 - 180.0
        lon_right = (x + 1) / n * 360.0 - 180.0
        lat_top = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
        lat_bottom = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
        
        return {
            "west": lon_left,
            "south": lat_bottom,
            "east": lon_right,
            "north": lat_top
        }
    
    bounds = tile_to_lat_lon(z, x, y)
    
    return {
        "z": z,
        "x": x,
        "y": y,
        "bounds": bounds,
        "bbox": [bounds["west"], bounds["south"], bounds["east"], bounds["north"]]
    }

if __name__ == "__main__":
    # Run the server
    uvicorn.run(
        "tile-server:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
        access_log=True
    )
