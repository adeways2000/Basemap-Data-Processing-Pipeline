-- Initialize PostGIS extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS basemap;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS staging;

-- Create tables for basemap data
CREATE TABLE IF NOT EXISTS basemap.processing_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) UNIQUE NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    source_file VARCHAR(500),
    records_processed INTEGER DEFAULT 0,
    tiles_generated INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create spatial indexes
CREATE INDEX IF NOT EXISTS idx_processing_runs_run_id ON basemap.processing_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_processing_runs_status ON basemap.processing_runs(status);
CREATE INDEX IF NOT EXISTS idx_processing_runs_start_time ON basemap.processing_runs(start_time);

-- Create monitoring tables
CREATE TABLE IF NOT EXISTS monitoring.metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    labels JSONB,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_name_timestamp ON monitoring.metrics(metric_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON monitoring.metrics(timestamp);

-- Create data quality tracking table
CREATE TABLE IF NOT EXISTS basemap.data_quality_checks (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    check_type VARCHAR(100) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    result_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    details JSONB,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_checks_run_id ON basemap.data_quality_checks(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_checks_type ON basemap.data_quality_checks(check_type);

-- Create tile metadata table
CREATE TABLE IF NOT EXISTS basemap.tile_metadata (
    id SERIAL PRIMARY KEY,
    tile_id VARCHAR(255) UNIQUE NOT NULL,
    zoom_level INTEGER NOT NULL,
    tile_x INTEGER NOT NULL,
    tile_y INTEGER NOT NULL,
    format VARCHAR(10) NOT NULL,
    size_bytes INTEGER,
    feature_count INTEGER,
    bbox GEOMETRY(POLYGON, 4326),
    generated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tile_metadata_zoom ON basemap.tile_metadata(zoom_level);
CREATE INDEX IF NOT EXISTS idx_tile_metadata_coords ON basemap.tile_metadata(zoom_level, tile_x, tile_y);
CREATE INDEX IF NOT EXISTS idx_tile_metadata_bbox ON basemap.tile_metadata USING GIST(bbox);

-- Create user and permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow_user') THEN
        CREATE USER airflow_user WITH PASSWORD 'airflow_password';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA basemap TO airflow_user;
GRANT USAGE ON SCHEMA monitoring TO airflow_user;
GRANT USAGE ON SCHEMA staging TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA basemap TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA basemap TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA monitoring TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO airflow_user;

-- Create functions for common operations
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers
CREATE TRIGGER update_processing_runs_updated_at 
    BEFORE UPDATE ON basemap.processing_runs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert sample data for testing
INSERT INTO basemap.processing_runs (run_id, start_time, status, source_file, records_processed)
VALUES 
    ('test-run-001', NOW() - INTERVAL '1 hour', 'completed', 'test.osm', 1000),
    ('test-run-002', NOW() - INTERVAL '30 minutes', 'running', 'sample.pbf', 500)
ON CONFLICT (run_id) DO NOTHING;
