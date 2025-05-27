
-- Purpose: Normalized database design for analyzing Canadian healthcare wait times (2008-2023)
-- Data Source: CIHI Wait Times for Priority Procedures dataset

-- DROP TABLES (for clean reinstallation)
DROP TABLE IF EXISTS fact_wait_times;
DROP TABLE IF EXISTS dim_time_periods;
DROP TABLE IF EXISTS dim_procedures;
DROP TABLE IF EXISTS dim_provinces;
DROP TABLE IF EXISTS dim_metrics;
DROP TABLE IF EXISTS ref_benchmarks;
DROP TABLE IF EXISTS ref_population_data;

-- DIMENSION TABLES
-- ================================================================

-- Province/Territory Dimension
CREATE TABLE dim_provinces (
    province_id INT PRIMARY KEY IDENTITY(1,1),
    province_code VARCHAR(5) NOT NULL UNIQUE,
    province_name VARCHAR(50) NOT NULL,
    region VARCHAR(20) NOT NULL, -- Atlantic, Central, Western, etc.
    population_2023 INT,
    is_territory TINYINT DEFAULT 0, -- 1 for territories, 0 for provinces
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Medical Procedures Dimension
CREATE TABLE dim_procedures (
    procedure_id INT PRIMARY KEY IDENTITY(1,1),
    procedure_code VARCHAR(10) NOT NULL UNIQUE,
    procedure_name VARCHAR(100) NOT NULL,
    procedure_category VARCHAR(50) NOT NULL, -- Cancer Surgery, Cardiac, Diagnostic, etc.
    clinical_priority VARCHAR(20) NOT NULL, -- High, Medium, Low
    is_active TINYINT DEFAULT 1,
    description TEXT,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Metrics Dimension
CREATE TABLE dim_metrics (
    metric_id INT PRIMARY KEY IDENTITY(1,1),
    metric_code VARCHAR(20) NOT NULL UNIQUE,
    metric_name VARCHAR(50) NOT NULL,
    metric_type VARCHAR(30) NOT NULL, -- Percentile, Benchmark_Compliance, Volume
    unit_of_measurement VARCHAR(30) NOT NULL,
    is_performance_indicator TINYINT DEFAULT 1,
    higher_is_better TINYINT, -- 1 if higher values are better, 0 if lower is better, NULL if neutral
    created_date DATETIME2 DEFAULT GETDATE()
);

-- Time Periods Dimension
CREATE TABLE dim_time_periods (
    time_id INT PRIMARY KEY IDENTITY(1,1),
    fiscal_year INT NOT NULL,
    calendar_year INT NOT NULL,
    quarter INT NOT NULL, -- 1-4
    year_quarter VARCHAR(7) NOT NULL, -- Format: 2023-Q1
    is_current_year TINYINT DEFAULT 0,
    fiscal_year_start_date DATE,
    fiscal_year_end_date DATE,
    created_date DATETIME2 DEFAULT GETDATE()
);

-- REFERENCE TABLES
-- ================================================================

-- Benchmark Standards by Procedure
CREATE TABLE ref_benchmarks (
    benchmark_id INT PRIMARY KEY IDENTITY(1,1),
    procedure_id INT NOT NULL,
    metric_id INT NOT NULL,
    benchmark_value DECIMAL(10,2) NOT NULL,
    benchmark_description VARCHAR(200),
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    source VARCHAR(100) DEFAULT 'CIHI',
    created_date DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (procedure_id) REFERENCES dim_procedures(procedure_id),
    FOREIGN KEY (metric_id) REFERENCES dim_metrics(metric_id)
);

-- Population Reference Data (for per-capita analysis)
CREATE TABLE ref_population_data (
    population_id INT PRIMARY KEY IDENTITY(1,1),
    province_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    total_population INT NOT NULL,
    population_65_plus INT,
    healthcare_spending_per_capita DECIMAL(12,2),
    data_source VARCHAR(100) DEFAULT 'Statistics Canada',
    created_date DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (province_id) REFERENCES dim_provinces(province_id),
    UNIQUE (province_id, fiscal_year)
);

-- FACT TABLE
-- ================================================================

-- Main Fact Table for Wait Times
CREATE TABLE fact_wait_times (
    wait_time_id BIGINT PRIMARY KEY IDENTITY(1,1),
    province_id INT NOT NULL,
    procedure_id INT NOT NULL,
    metric_id INT NOT NULL,
    time_id INT NOT NULL,
    
    -- Measures
    result_value DECIMAL(10,2), -- The actual wait time or percentage
    volume_cases INT, -- Number of cases (when applicable)
    
    -- Flags and Status
    is_benchmark_met TINYINT, -- 1 if benchmark met, 0 if not, NULL if not applicable
    data_quality_flag VARCHAR(20) DEFAULT 'VALID', -- VALID, ESTIMATED, MISSING, SUPPRESSED
    reporting_level VARCHAR(20) NOT NULL, -- Provincial, Regional, National
    
    -- Audit fields
    source_file VARCHAR(100),
    load_date DATETIME2 DEFAULT GETDATE(),
    created_date DATETIME2 DEFAULT GETDATE(),
    
    -- Foreign Keys
    FOREIGN KEY (province_id) REFERENCES dim_provinces(province_id),
    FOREIGN KEY (procedure_id) REFERENCES dim_procedures(procedure_id),
    FOREIGN KEY (metric_id) REFERENCES dim_metrics(metric_id),
    FOREIGN KEY (time_id) REFERENCES dim_time_periods(time_id)
);

-- INDEXES FOR PERFORMANCE
-- ================================================================

-- Fact table indexes
CREATE NONCLUSTERED INDEX IX_fact_wait_times_province_procedure ON fact_wait_times(province_id, procedure_id);
CREATE NONCLUSTERED INDEX IX_fact_wait_times_time_metric ON fact_wait_times(time_id, metric_id);
CREATE NONCLUSTERED INDEX IX_fact_wait_times_benchmark_flag ON fact_wait_times(is_benchmark_met) WHERE is_benchmark_met IS NOT NULL;

-- Dimension table indexes
CREATE NONCLUSTERED INDEX IX_dim_procedures_category ON dim_procedures(procedure_category);
CREATE NONCLUSTERED INDEX IX_dim_time_fiscal_year ON dim_time_periods(fiscal_year);
CREATE NONCLUSTERED INDEX IX_ref_benchmarks_procedure ON ref_benchmarks(procedure_id);

-- DATA VALIDATION CONSTRAINTS
-- ================================================================

-- Ensure valid percentage values
ALTER TABLE fact_wait_times ADD CONSTRAINT CK_result_value_percentage 
    CHECK (result_value >= 0 AND (result_value <= 100 OR result_value > 365)); -- Allow percentages 0-100 or days > 365

-- Ensure valid fiscal years
ALTER TABLE dim_time_periods ADD CONSTRAINT CK_fiscal_year_range 
    CHECK (fiscal_year BETWEEN 2000 AND 2030);

-- Ensure valid quarters
ALTER TABLE dim_time_periods ADD CONSTRAINT CK_quarter_range 
    CHECK (quarter BETWEEN 1 AND 4);

-- VIEWS FOR COMMON QUERIES
-- ================================================================

-- View: Current Year Performance Summary
CREATE VIEW vw_current_year_performance AS
SELECT 
    p.province_name,
    pr.procedure_name,
    m.metric_name,
    f.result_value,
    m.unit_of_measurement,
    f.is_benchmark_met,
    t.fiscal_year
FROM fact_wait_times f
JOIN dim_provinces p ON f.province_id = p.province_id
JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
JOIN dim_metrics m ON f.metric_id = m.metric_id
JOIN dim_time_periods t ON f.time_id = t.time_id
WHERE t.is_current_year = 1 
    AND f.data_quality_flag = 'VALID'
    AND f.result_value IS NOT NULL;

-- View: Benchmark Compliance Summary
CREATE VIEW vw_benchmark_compliance AS
SELECT 
    p.province_name,
    pr.procedure_name,
    pr.procedure_category,
    t.fiscal_year,
    COUNT(*) as total_metrics,
    SUM(CASE WHEN f.is_benchmark_met = 1 THEN 1 ELSE 0 END) as benchmarks_met,
    CAST(SUM(CASE WHEN f.is_benchmark_met = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as compliance_percentage
FROM fact_wait_times f
JOIN dim_provinces p ON f.province_id = p.province_id
JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
JOIN dim_time_periods t ON f.time_id = t.time_id
WHERE f.is_benchmark_met IS NOT NULL
GROUP BY p.province_name, pr.procedure_name, pr.procedure_category, t.fiscal_year;

-- View: Year-over-Year Trends
CREATE VIEW vw_wait_time_trends AS
SELECT 
    p.province_name,
    pr.procedure_name,
    m.metric_name,
    t.fiscal_year,
    f.result_value,
    LAG(f.result_value) OVER (PARTITION BY f.province_id, f.procedure_id, f.metric_id ORDER BY t.fiscal_year) as previous_year_value,
    f.result_value - LAG(f.result_value) OVER (PARTITION BY f.province_id, f.procedure_id, f.metric_id ORDER BY t.fiscal_year) as year_over_year_change,
    CASE 
        WHEN LAG(f.result_value) OVER (PARTITION BY f.province_id, f.procedure_id, f.metric_id ORDER BY t.fiscal_year) IS NOT NULL
        THEN CAST((f.result_value - LAG(f.result_value) OVER (PARTITION BY f.province_id, f.procedure_id, f.metric_id ORDER BY t.fiscal_year)) * 100.0 / 
                  LAG(f.result_value) OVER (PARTITION BY f.province_id, f.procedure_id, f.metric_id ORDER BY t.fiscal_year) AS DECIMAL(8,2))
        ELSE NULL 
    END as percent_change
FROM fact_wait_times f
JOIN dim_provinces p ON f.province_id = p.province_id
JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
JOIN dim_metrics m ON f.metric_id = m.metric_id
JOIN dim_time_periods t ON f.time_id = t.time_id
WHERE f.result_value IS NOT NULL 
    AND f.data_quality_flag = 'VALID';

-- STORED PROCEDURES FOR DATA MANAGEMENT
-- ================================================================

-- Procedure to refresh current year flag
CREATE PROCEDURE sp_refresh_current_year
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Reset all current year flags
    UPDATE dim_time_periods SET is_current_year = 0;
    
    -- Set current year flag for the most recent fiscal year with data
    UPDATE dim_time_periods 
    SET is_current_year = 1 
    WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM dim_time_periods);
    
    PRINT 'Current year flags refreshed successfully.';
END;

-- Procedure to calculate benchmark compliance
CREATE PROCEDURE sp_calculate_benchmark_compliance
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE f 
    SET is_benchmark_met = 
        CASE 
            WHEN b.benchmark_value IS NULL THEN NULL
            WHEN m.higher_is_better = 1 AND f.result_value >= b.benchmark_value THEN 1
            WHEN m.higher_is_better = 0 AND f.result_value <= b.benchmark_value THEN 1
            ELSE 0
        END
    FROM fact_wait_times f
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    LEFT JOIN ref_benchmarks b ON f.procedure_id = b.procedure_id 
                               AND f.metric_id = b.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE b.effective_start_date <= DATEFROMPARTS(t.fiscal_year, 3, 31) -- Assuming fiscal year ends March 31
        AND (b.effective_end_date IS NULL OR b.effective_end_date >= DATEFROMPARTS(t.fiscal_year, 4, 1));
    
    PRINT 'Benchmark compliance calculations completed.';
END;

-- SAMPLE DATA LOADING TEMPLATE
-- ================================================================

-- Example INSERT statements (to be populated with actual data)
-- These show the structure for data loading scripts

/*
-- Sample Province Data
INSERT INTO dim_provinces (province_code, province_name, region, population_2023, is_territory) VALUES
('AB', 'Alberta', 'Western', 4756408, 0),
('BC', 'British Columbia', 'Western', 5399118, 0),
('MB', 'Manitoba', 'Central', 1418129, 0),
('NB', 'New Brunswick', 'Atlantic', 808718, 0),
('NL', 'Newfoundland and Labrador', 'Atlantic', 540418, 0),
('NS', 'Nova Scotia', 'Atlantic', 1030890, 0),
('ON', 'Ontario', 'Central', 15801768, 0),
('PE', 'Prince Edward Island', 'Atlantic', 173787, 0),
('QC', 'Quebec', 'Central', 8604495, 0),
('SK', 'Saskatchewan', 'Western', 1214618, 0);

-- Sample Procedure Data
INSERT INTO dim_procedures (procedure_code, procedure_name, procedure_category, clinical_priority) VALUES
('BLAD_SURG', 'Bladder Cancer Surgery', 'Cancer Surgery', 'High'),
('BRST_SURG', 'Breast Cancer Surgery', 'Cancer Surgery', 'High'),
('CABG', 'CABG', 'Cardiac Surgery', 'High'),
('CT_SCAN', 'CT Scan', 'Diagnostic Imaging', 'Medium'),
('CAT_SURG', 'Cataract Surgery', 'Ophthalmology', 'Medium');

-- Sample Metrics Data
INSERT INTO dim_metrics (metric_code, metric_name, metric_type, unit_of_measurement, higher_is_better) VALUES
('PCT_50', '50th Percentile', 'Percentile', 'Days', 0),
('PCT_90', '90th Percentile', 'Percentile', 'Days', 0),
('BENCH_MET', '% Meeting Benchmark', 'Benchmark_Compliance', 'Proportion', 1),
('VOLUME', 'Volume', 'Volume', 'Number of cases', NULL);
*/

-- DATABASE DOCUMENTATION SUMMARY
-- ================================================================

/*
SCHEMA OVERVIEW:
===============
This database implements a star schema optimized for healthcare wait time analytics:

FACT TABLE:
- fact_wait_times: Central table storing all wait time measurements

DIMENSION TABLES:
- dim_provinces: Geographic information
- dim_procedures: Medical procedure details
- dim_metrics: Measurement types and units
- dim_time_periods: Temporal dimension

REFERENCE TABLES:
- ref_benchmarks: Performance targets by procedure
- ref_population_data: Population statistics for per-capita analysis

VIEWS:
- vw_current_year_performance: Latest year performance data
- vw_benchmark_compliance: Compliance rates by province/procedure
- vw_wait_time_trends: Year-over-year trend analysis

FEATURES:
- Normalized design preventing data redundancy
- Comprehensive indexing for query performance  
- Data quality flags and validation constraints
- Audit trails with load dates and source tracking
- Flexible benchmark system supporting changing standards
- Support for both provincial and national reporting levels

SCALABILITY:
- Identity columns for efficient primary keys
- Partitioning-ready design for large datasets
- Optimized for both OLTP and OLAP workloads
*/