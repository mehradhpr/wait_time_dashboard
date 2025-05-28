# docs/database_design.md
"""
# Healthcare Wait Times Database Design

## Overview
The database follows a dimensional modeling approach with a star schema design optimized for analytical queries and reporting.

## Schema Architecture

### Dimension Tables

#### dim_provinces
Stores province and territory information:
- `province_id` (Primary Key)
- `province_code` (Unique identifier)
- `province_name` (Full name)
- `region` (Geographic region)
- `population_estimate` (Population data)

#### dim_procedures  
Medical procedures and treatments:
- `procedure_id` (Primary Key)
- `procedure_code` (Unique identifier)
- `procedure_name` (Full name)
- `procedure_category` (Grouping category)
- `is_surgery` (Boolean flag)
- `description` (Detailed description)

#### dim_metrics
Measurement types:
- `metric_id` (Primary Key)
- `metric_code` (Unique identifier)  
- `metric_name` (Full name)
- `metric_type` (Type: percentile, volume, benchmark)
- `unit_of_measurement` (Units)

#### dim_reporting_levels
Data granularity levels:
- `level_id` (Primary Key)
- `level_code` (Unique identifier)
- `level_name` (Description)

### Fact Table

#### fact_wait_times
Central fact table containing measurements:
- `wait_time_id` (Primary Key - UUID)
- `province_id` (Foreign Key)
- `procedure_id` (Foreign Key)
- `metric_id` (Foreign Key)  
- `reporting_level_id` (Foreign Key)
- `data_year` (Year of measurement)
- `indicator_result` (Numeric result)
- `is_estimate` (Data quality flag)
- `data_quality_flag` (Quality indicator)
- `region_name` (Additional grouping)

## Analytical Views

### v_wait_times_detail
Comprehensive view joining all dimensions with fact data for easy querying.

### v_provincial_performance  
Province-level summary statistics and performance metrics.

### v_procedure_performance
Procedure-level aggregations and national comparisons.

### mv_wait_time_trends (Materialized)
Pre-calculated trend analysis with year-over-year changes for dashboard performance.

### mv_dashboard_summary (Materialized)
Summary statistics for dashboard header cards.

## Indexes and Performance

### Primary Indexes
- Foreign key indexes on fact table
- Composite indexes for common query patterns:
  - `(data_year, province_id)`
  - `(procedure_id, metric_id)`
  - `(data_year, procedure_id, province_id)`

### Specialized Indexes
- Partial index for non-null results
- GIN indexes for text search capabilities

## Data Quality Features

### Constraints
- Check constraints for data year ranges (2000-2030)
- Check constraints for positive numeric values
- Enumerated values for data quality flags

### Audit Trail
- `audit_data_loads` table tracks ETL operations
- Timestamps on all dimension tables
- Load statistics and error tracking

## Stored Procedures

### sp_wait_time_trends()
Calculates trend analysis with year-over-year changes and trend direction classification.

### sp_provincial_comparison()  
Compares provincial performance for specific procedures with statistical ranking.

### sp_benchmark_analysis()
Analyzes benchmark compliance and categorizes performance levels.

### sp_procedure_statistics()
Comprehensive procedure-level statistics including best/worst performers.

## Security Considerations

- Row-level security can be implemented for multi-tenant scenarios
- Stored procedures prevent SQL injection
- Parameterized queries in application layer
- Separate read-only users for reporting applications

## Maintenance Operations

### Daily Tasks
- Refresh materialized views
- Update table statistics
- Check data quality metrics

### Weekly Tasks  
- Reindex heavily updated tables
- Analyze query performance
- Review audit logs

## Scalability Design

The schema supports:
- Horizontal partitioning by year for large datasets
- Read replicas for reporting workloads
- Connection pooling for concurrent users
- Materialized views for complex aggregations
"""
