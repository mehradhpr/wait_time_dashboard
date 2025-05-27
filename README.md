# Canadian Healthcare Wait Times Analytics System

## Project Overview

This comprehensive healthcare analytics system demonstrates end-to-end business intelligence capabilities using real Canadian healthcare wait times data. The project showcases advanced database design, ETL processes, statistical analysis, and interactive data visualization skills suitable for a professional portfolio.

### Key Business Value
- **Evidence-based decision making** for healthcare administrators
- **Performance benchmarking** across provinces and procedures
- **Trend analysis and forecasting** for strategic planning
- **Resource optimization** through capacity analysis
- **Real-time monitoring** of critical performance indicators

---

## Technical Skills Demonstrated

### Database & Data Management
- **Normalized database design** (Star schema with fact and dimension tables)
- **SQL Server integration** with proper indexing and constraints
- **Data quality management** with validation and cleansing processes
- **ETL pipeline development** using Python and pandas
- **Stored procedures** for automated data processing

### Analytics & Business Intelligence
- **Advanced SQL queries** with CTEs, window functions, and statistical analysis
- **Crystal Reports development** with parameterized, professional reports
- **PowerBI dashboard design** with interactive visualizations
- **Statistical analysis** including correlation, trend detection, and forecasting
- **Performance optimization** for large datasets

### Programming & Automation
- **Python programming** with object-oriented design
- **Error handling and logging** for production-ready code
- **Data validation and quality assurance** processes
- **Configuration management** and deployment practices

---

## Project Structure

```
healthcare-analytics-system/
├── README.md                           # This comprehensive guide
├── database/
│   ├── schema/
│   │   └── healthcare_schema.sql       # Complete database schema
│   ├── queries/
│   │   └── analytics_queries.sql       # SQL query library
│   └── sample_data/
│       └── waittimes_sample.xlsx       # Sample dataset
├── etl/
│   ├── healthcare_etl.py              # Main ETL script
│   ├── data_quality_checker.py        # Data validation utilities
│   ├── config.py                      # Configuration management
│   └── requirements.txt               # Python dependencies
├── reports/
│   ├── crystal_reports/
│   │   ├── executive_dashboard.rpt     # Crystal Reports templates
│   │   ├── provincial_scorecard.rpt
│   │   ├── trend_analysis.rpt
│   │   ├── benchmark_compliance.rpt
│   │   ├── volume_analysis.rpt
│   │   └── quarterly_monitor.rpt
│   └── templates/
│       └── crystal_report_specs.md    # Report specifications
├── dashboards/
│   ├── powerbi/
│   │   ├── healthcare_analytics.pbix   # PowerBI dashboard file
│   │   └── data_model.json            # Data model configuration
│   └── design/
│       └── dashboard_specifications.md # Dashboard design guide
├── documentation/
│   ├── user_guides/                   # End-user documentation
│   ├── technical_specs/               # Technical specifications
│   └── deployment_guide.md            # Deployment instructions
└── assets/
    ├── images/                        # Screenshots and diagrams
    └── sample_outputs/                # Example reports and outputs
```

---

## Quick Start

### Prerequisites
```bash
- SQL Server 2019+ (or SQL Server Express)
- Python 3.8+
- Crystal Reports Developer (or SAP Crystal Reports)
- Power BI Desktop
- Excel 2016+ (for data source)
```

### Installation Steps

#### 1. Database Setup
```sql
-- Execute the database schema
sqlcmd -S localhost -i database/schema/healthcare_schema.sql

-- Verify installation
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'dbo';
-- Should return 8 tables
```

#### 2. Python Environment Setup
```bash
# Create virtual environment
python -m venv healthcare_analytics
source healthcare_analytics/bin/activate  # Linux/Mac
# or
healthcare_analytics\Scripts\activate     # Windows

# Install dependencies
pip install -r etl/requirements.txt
```

#### 3. Data Loading
```bash
# Update database connection in config.py
# Place your Excel file in the project root
python etl/healthcare_etl.py

# Verify data load
# Check logs in healthcare_etl.log
```

#### 4. Report Deployment
```bash
# Crystal Reports
# Import .rpt files into Crystal Reports Developer
# Update data source connections
# Test parameter functionality

# Power BI
# Open healthcare_analytics.pbix
# Update data source connections
# Refresh data model
# Publish to Power BI Service
```

---

## Data Sources & Schema

### Primary Dataset
- **Source**: Canadian Institute for Health Information (CIHI)
- **Coverage**: 2008-2023 fiscal years
- **Scope**: 10 provinces, 16 medical procedures
- **Records**: ~17,000 data points
- **Metrics**: Wait times (50th/90th percentile), volumes, benchmark compliance

### Database Schema

#### Fact Table
```sql
fact_wait_times
├── wait_time_id (PK)        -- Unique identifier
├── province_id (FK)         -- Links to dim_provinces
├── procedure_id (FK)        -- Links to dim_procedures  
├── metric_id (FK)           -- Links to dim_metrics
├── time_id (FK)             -- Links to dim_time_periods
├── result_value             -- Actual measurement
├── volume_cases             -- Number of cases
├── is_benchmark_met         -- Compliance flag
├── data_quality_flag        -- Quality indicator
└── reporting_level          -- Provincial/National
```

#### Dimension Tables
```sql
dim_provinces       -- Geographic information
dim_procedures      -- Medical procedure details
dim_metrics         -- Measurement definitions
dim_time_periods    -- Temporal dimension
ref_benchmarks      -- Performance targets
```

---

## Configuration Guide

### Database Connection
```python
# etl/config.py
class DatabaseConfig:
    def __init__(self):
        self.server = "your_server_name"
        self.database = "HealthcareWaitTimes"
        self.driver = "{ODBC Driver 17 for SQL Server}"
        self.trusted_connection = "yes"  # or use username/password
```

### ETL Configuration
```python
# Key parameters to adjust
DATA_QUALITY_THRESHOLD = 0.8    # Minimum data completeness
OUTLIER_THRESHOLD = 3.0          # Z-score for outlier detection
TREND_SIGNIFICANCE = 0.05        # P-value threshold
LOG_LEVEL = "INFO"               # Logging verbosity
```

### Report Parameters
```sql
-- Default parameter values
@Year = 2023                     -- Reporting year
@Province = "All"                -- Province filter
@ComplianceThreshold = 80        -- Benchmark compliance %
@TrendPeriod = 5                 -- Years for trend analysis
```

---

## Analytics Capabilities

### Performance Analysis
- **Provincial Rankings**: Comparative performance across provinces
- **Procedure Benchmarking**: Target vs actual performance tracking
- **Volume Analysis**: Capacity utilization and patient flow
- **Trend Detection**: Statistical significance testing for trends

### Advanced Analytics
- **Correlation Analysis**: Volume vs wait time relationships
- **Outlier Detection**: Statistical anomaly identification
- **Forecasting**: Predictive modeling for future performance
- **Scenario Planning**: What-if analysis for interventions

### Key Performance Indicators
```sql
-- Sample KPI calculations
Overall_Compliance = Benchmarks_Met / Total_Benchmarks * 100
Avg_Wait_Time = SUM(50th_Percentile) / COUNT(Procedures)
YoY_Change = Current_Year - Previous_Year
Capacity_Utilization = Actual_Volume / Optimal_Volume
```

---

## Reporting Suite

### Crystal Reports Portfolio

#### 1. Executive Performance Dashboard
- **Purpose**: Senior leadership overview
- **Key Features**: KPI summary, trend analysis, priority alerts
- **Frequency**: Monthly
- **Distribution**: C-suite, board members

#### 2. Provincial Performance Scorecard  
- **Purpose**: Individual province deep-dive
- **Key Features**: Comparative analysis, procedure details
- **Frequency**: Monthly per province
- **Distribution**: Provincial health ministers, administrators

#### 3. Trend Analysis Report
- **Purpose**: Multi-year statistical analysis
- **Key Features**: Regression analysis, forecasting
- **Frequency**: Quarterly
- **Distribution**: Policy analysts, strategic planners

#### 4. Benchmark Compliance Report
- **Purpose**: Performance standards monitoring
- **Key Features**: Compliance matrix, action items
- **Frequency**: Monthly
- **Distribution**: Quality improvement teams

#### 5. Volume and Capacity Analysis
- **Purpose**: Resource optimization insights
- **Key Features**: Capacity utilization, bottleneck identification
- **Frequency**: Quarterly
- **Distribution**: Operations managers, resource planners

#### 6. Quarterly Performance Monitor
- **Purpose**: Operational tracking
- **Key Features**: Real-time alerts, variance analysis
- **Frequency**: Quarterly
- **Distribution**: Department heads, front-line managers

### Power BI Dashboard Suite

#### 1. Executive Overview Dashboard
- **Real-time KPIs** with interactive province map
- **Trend visualization** with drill-down capabilities
- **Priority alerts** with automated flagging
- **Mobile-responsive** design for executive access

#### 2. Provincial Deep Dive Dashboard
- **Detailed performance analysis** by province
- **Peer comparison** and benchmarking
- **Historical trends** with statistical analysis
- **Procedure-level** drill-down capabilities

#### 3. Operational Analytics Dashboard
- **Real-time monitoring** with auto-refresh
- **Capacity analysis** and resource optimization
- **Predictive analytics** with forecasting
- **Alert management** system

#### 4. Comparative Analysis Dashboard
- **Multi-province** side-by-side comparison
- **Performance rankings** with statistical significance
- **Benchmark analysis** across dimensions
- **Custom comparison** configurations

#### 5. Trend and Forecasting Dashboard
- **Advanced statistical modeling** with R integration
- **Scenario planning** and what-if analysis
- **Confidence intervals** and uncertainty quantification
- **Seasonal pattern** detection and analysis

---

## Quality Assurance

### Data Quality Framework
```python
# Automated quality checks
class DataQualityChecker:
    def check_completeness(self):
        # Verify >80% data completeness
    
    def check_referential_integrity(self):
        # Ensure FK relationships valid
    
    def check_business_rules(self):
        # Validate realistic wait times, percentages
    
    def check_statistical_outliers(self):
        # Identify potential data errors
```

### Testing Procedures
- **Unit Testing**: Individual component validation
- **Integration Testing**: End-to-end data flow verification
- **User Acceptance Testing**: Business user validation
- **Performance Testing**: Load and response time verification

### Validation Checkpoints
- Data extraction accuracy (source vs target)
- Transformation logic correctness
- Load completeness and integrity
- Report calculation accuracy
- Dashboard interactivity functionality

---

## Deployment Guide

### Environment Setup

#### Development Environment
```bash
# Local development setup
1. Install SQL Server Developer Edition
2. Set up Python virtual environment
3. Install Crystal Reports Developer
4. Configure Power BI Desktop
5. Load sample data for testing
```

#### Production Environment
```bash
# Production deployment checklist
1. SQL Server Enterprise/Standard Edition
2. Crystal Reports Server
3. Power BI Premium capacity
4. Automated ETL scheduling
5. Backup and recovery procedures
```

### Deployment Steps

#### 1. Database Deployment
```sql
-- Create production database
CREATE DATABASE HealthcareWaitTimes_Prod;

-- Deploy schema
EXEC sp_executesql @sql = 'healthcare_schema.sql';

-- Configure security
CREATE LOGIN healthcare_user WITH PASSWORD = 'SecurePassword';
CREATE USER healthcare_user FOR LOGIN healthcare_user;
GRANT SELECT, INSERT, UPDATE ON SCHEMA::dbo TO healthcare_user;
```

#### 2. ETL Deployment
```bash
# Schedule ETL job
# Windows Task Scheduler or SQL Server Agent
# Daily execution at 6:00 AM
python /path/to/healthcare_etl.py >> /logs/etl.log 2>&1
```

#### 3. Reports Deployment
```bash
# Crystal Reports
# Deploy to Crystal Reports Server
# Configure data sources and security
# Set up automated distribution

# Power BI
# Publish to Power BI Service
# Configure refresh schedules
# Set up row-level security
# Create user access groups
```

### Monitoring and Maintenance
- **Daily**: ETL job monitoring and data quality checks
- **Weekly**: Performance monitoring and optimization
- **Monthly**: User feedback review and report updates
- **Quarterly**: System capacity planning and upgrades

---

## User Guide

### Getting Started

#### For Executives
1. **Access Executive Dashboard** via Power BI Service or emailed PDF
2. **Review KPI summary** for overall system performance
3. **Click on provinces** in the map for detailed analysis
4. **Focus on priority alerts** for immediate action items
5. **Use trend analysis** for strategic planning discussions

#### For Provincial Administrators
1. **Open Provincial Scorecard** for your province
2. **Compare performance** with peer provinces
3. **Identify best and worst** performing procedures
4. **Review detailed metrics** and benchmark compliance
5. **Export data** for local analysis and reporting

#### For Operations Managers
1. **Monitor Operational Dashboard** for real-time status
2. **Check capacity utilization** metrics regularly
3. **Respond to automated alerts** promptly
4. **Use optimization insights** for resource planning
5. **Track performance trends** month-over-month

### Common Tasks

#### Generating Reports
```bash
# Crystal Reports - Automated generation
1. Open Crystal Reports Developer
2. Select report template
3. Set parameters (Year, Province, etc.)
4. Preview and validate
5. Export to PDF/Excel
6. Distribute via email/portal
```

#### Dashboard Navigation
```bash
# Power BI - Interactive analysis
1. Apply filters using slicers
2. Click visuals for cross-filtering
3. Use drill-through for details
4. Create custom views with bookmarks
5. Export data for offline analysis
```

#### Data Refresh
```bash
# Manual refresh process
1. Verify data source availability
2. Run ETL script: python healthcare_etl.py
3. Check logs for errors
4. Refresh Power BI datasets
5. Validate report outputs
```

---

## Troubleshooting Guide

### Common Issues

#### Database Connection Errors
```bash
Problem: "Login failed for user"
Solution: 
1. Check connection string in config.py
2. Verify SQL Server authentication mode
3. Ensure user has proper permissions
4. Test connection using SSMS
```

#### ETL Process Failures
```bash
Problem: "Data extraction failed"
Solution:
1. Check Excel file path and permissions
2. Verify file format matches expected schema
3. Review data quality thresholds
4. Check available disk space
5. Examine detailed logs in healthcare_etl.log
```

#### Report Generation Issues
```bash
Problem: "Report parameters not working"
Solution:
1. Verify parameter data types
2. Check parameter default values
3. Ensure pick list queries execute properly
4. Test with simple parameter values first
```

#### Dashboard Performance Issues
```bash
Problem: "Dashboard loading slowly"
Solution:
1. Optimize DAX measures for performance
2. Reduce visual complexity
3. Implement aggregation tables
4. Consider DirectQuery vs Import mode
5. Check Power BI capacity limits
```

### Error Codes Reference

#### ETL Error Codes
- **E001**: Database connection failed
- **E002**: Excel file not found or corrupted
- **E003**: Data validation failed
- **E004**: Insufficient disk space
- **E005**: Duplicate key constraint violation

#### Report Error Codes  
- **R001**: Parameter validation failed
- **R002**: Data source connection timeout
- **R003**: Formula syntax error
- **R004**: Export format not supported
- **R005**: Security permissions insufficient

### Performance Optimization

#### Database Optimization
```sql
-- Index maintenance
EXEC sp_updatestats;
REINDEX TABLES WITH REBUILD;

-- Query optimization
UPDATE STATISTICS fact_wait_times;
CHECK INDEX performance WITH EXECUTION_PLAN;
```

#### ETL Optimization
```python
# Batch processing for large datasets
BATCH_SIZE = 10000
chunk_size = len(dataframe) // BATCH_SIZE
for chunk in chunks:
    process_batch(chunk)
```

#### Report Optimization
```bash
# Crystal Reports optimization
1. Use database views for complex queries
2. Implement summary tables
3. Optimize sub-report usage
4. Cache frequently used data
5. Minimize cross-tab complexity
```