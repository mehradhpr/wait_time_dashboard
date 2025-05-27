-- Normalized schema for Canadian healthcare wait time data

-- Create database
CREATE DATABASE healthcare_analytics;

-- Use the database
\c healthcare_analytics;

-- Enable extensions for advanced features
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- DIMENSION TABLES =============================================

-- Provinces and territories lookup table
CREATE TABLE dim_provinces (
    province_id SERIAL PRIMARY KEY,
    province_code VARCHAR(3) UNIQUE NOT NULL,
    province_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    population_estimate INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medical procedures/indicators lookup table
CREATE TABLE dim_procedures (
    procedure_id SERIAL PRIMARY KEY,
    procedure_code VARCHAR(20) UNIQUE NOT NULL,
    procedure_name VARCHAR(100) NOT NULL,
    procedure_category VARCHAR(50) NOT NULL,
    description TEXT,
    is_surgery BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metrics types lookup table
CREATE TABLE dim_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_code VARCHAR(20) UNIQUE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_type VARCHAR(20) NOT NULL, -- 'percentile', 'volume', 'benchmark'
    unit_of_measurement VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reporting levels lookup table
CREATE TABLE dim_reporting_levels (
    level_id SERIAL PRIMARY KEY,
    level_code VARCHAR(20) UNIQUE NOT NULL,
    level_name VARCHAR(50) NOT NULL,
    description TEXT
);

-- FACT TABLE =============================================

-- Main fact table for wait time data
CREATE TABLE fact_wait_times (
    wait_time_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    province_id INTEGER NOT NULL REFERENCES dim_provinces(province_id),
    procedure_id INTEGER NOT NULL REFERENCES dim_procedures(procedure_id),
    metric_id INTEGER NOT NULL REFERENCES dim_metrics(metric_id),
    reporting_level_id INTEGER NOT NULL REFERENCES dim_reporting_levels(level_id),
    data_year INTEGER NOT NULL,
    indicator_result DECIMAL(10,2),
    is_estimate BOOLEAN DEFAULT FALSE,
    data_quality_flag VARCHAR(10), -- 'good', 'fair', 'poor', 'n/a'
    region_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- INDEXES FOR PERFORMANCE =============================================

-- Primary query patterns indexes
CREATE INDEX idx_wait_times_year ON fact_wait_times(data_year);
CREATE INDEX idx_wait_times_province ON fact_wait_times(province_id);
CREATE INDEX idx_wait_times_procedure ON fact_wait_times(procedure_id);
CREATE INDEX idx_wait_times_metric ON fact_wait_times(metric_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_wait_times_year_province ON fact_wait_times(data_year, province_id);
CREATE INDEX idx_wait_times_procedure_metric ON fact_wait_times(procedure_id, metric_id);
CREATE INDEX idx_wait_times_year_procedure_province ON fact_wait_times(data_year, procedure_id, province_id);

-- Index for non-null results
CREATE INDEX idx_wait_times_with_data ON fact_wait_times(province_id, procedure_id, data_year) 
WHERE indicator_result IS NOT NULL;

-- CONSTRAINTS AND VALIDATION =============================================

-- Data year constraints
ALTER TABLE fact_wait_times ADD CONSTRAINT chk_data_year 
CHECK (data_year BETWEEN 2000 AND 2030);

-- Positive values for wait times and volumes
ALTER TABLE fact_wait_times ADD CONSTRAINT chk_positive_result 
CHECK (indicator_result >= 0 OR indicator_result IS NULL);

-- Data quality flag validation
ALTER TABLE fact_wait_times ADD CONSTRAINT chk_quality_flag 
CHECK (data_quality_flag IN ('good', 'fair', 'poor', 'n/a', NULL));

-- AUDIT AND METADATA TABLES =============================================

-- Data load audit table
CREATE TABLE audit_data_loads (
    load_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    load_status VARCHAR(20) DEFAULT 'in_progress',
    error_message TEXT,
    load_duration_seconds INTEGER
);

-- Comments for documentation
COMMENT ON TABLE dim_provinces IS 'Lookup table for Canadian provinces and territories';
COMMENT ON TABLE dim_procedures IS 'Lookup table for medical procedures and treatments';
COMMENT ON TABLE dim_metrics IS 'Lookup table for measurement metrics (percentiles, volumes, benchmarks)';
COMMENT ON TABLE fact_wait_times IS 'Main fact table containing wait time measurements';
COMMENT ON COLUMN fact_wait_times.indicator_result IS 'Numeric result value - wait time in days, volume count, or percentage';
COMMENT ON COLUMN fact_wait_times.data_quality_flag IS 'Data quality indicator based on completeness and reliability';