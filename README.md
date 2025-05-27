# Healthcare Wait Times Analytics Dashboard

A comprehensive analytics platform for Canadian healthcare wait time data, demonstrating database design, ETL pipelines, SQL analytics, and interactive visualization.

## Project Overview

This project analyzes Canadian healthcare wait times (2008-2023) across 15+ medical procedures and 11 provinces/territories, providing insights into:

- Wait time trends and patterns
- Provincial performance comparisons  
- Benchmark compliance analysis
- Statistical significance testing
- Automated insights and recommendations

## Skills Demonstrated

- **Database Design**: Normalized PostgreSQL schema with proper indexing
- **SQL Expertise**: Complex queries, stored procedures, window functions, CTEs
- **Python Development**: ETL pipelines, statistical analysis, data processing
- **Data Visualization**: Interactive dashboards with Plotly and Dash
- **Analytics**: Trend analysis, benchmarking, statistical tests

## Data Source

Canadian Institute for Health Information (CIHI) wait times data:
- **17,286 records** across 16 years (2008-2023)
- **15 medical procedures** including surgeries, diagnostics, treatments
- **11 provinces/territories** plus national aggregates
- **4 key metrics**: 50th/90th percentiles, volumes, benchmark compliance

## Architecture

Excel Data (Raw Source) -> ETL Pipeline (Python/Pandas) -> PostgreSQL (Normalized) -> Analytcis (Python/SQL) -> Dash App (Visualization)

## Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Git

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/mehradhpr/healthcare-wait-times-analytics.git
cd healthcare-wait-times-analytics
```

2. **Run automated setup**
```bash
python setup.py
```

3. **Configure environment**
```bash
# Edit .env file with your database credentials
nano .env
```

4. **Place data file**
```bash
# Copy your wait_times_data.xlsx to data/raw/
cp /path/to/wait_times_data.xlsx data/raw/
```

5. **Load data**
```bash
python scripts/run_etl.py
```

6. **Start dashboard**
```bash
python dashboard/app.py
```

7. **Access application**
   - Dashboard: http://localhost:8050
   - API endpoints: http://localhost:5000 (if running Flask API)

## Database Schema

### Dimension Tables
- `dim_provinces`: Province/territory lookup
- `dim_procedures`: Medical procedure definitions  
- `dim_metrics`: Measurement types (percentiles, volumes, benchmarks)
- `dim_reporting_levels`: Data granularity levels

### Fact Table
- `fact_wait_times`: Central fact table with wait time measurements

### Key Views
- `v_wait_times_detail`: Comprehensive analytical view
- `v_provincial_performance`: Province-level summaries
- `mv_wait_time_trends`: Materialized view for trend analysis

## 📈 Analytics Features

### Stored Procedures
- `sp_wait_time_trends()`: Trend analysis with year-over-year changes
- `sp_provincial_comparison()`: Statistical province comparisons
- `sp_benchmark_analysis()`: Compliance monitoring
- `sp_procedure_statistics()`: Comprehensive procedure analytics

### Python Analytics
```python
from src.analytics.wait_time_analyzer import WaitTimeAnalyzer

analyzer = WaitTimeAnalyzer(db_connection)

# Trend analysis
trends = analyzer.calculate_trend_analysis(data)

# Provincial comparison  
comparison = analyzer.provincial_comparison('Hip Replacement', 2023)

# Statistical testing
test_result = analyzer.statistical_significance_test('Ontario', 'Quebec', 'CABG', [2020, 2021, 2022, 2023])
```

### Dashboard Features
- **Overview**: Distribution analysis and summary statistics
- **Trends**: Time series analysis with trend classification
- **Comparisons**: Provincial performance benchmarking
- **Benchmarks**: Compliance monitoring and alerts
- **Insights**: Automated findings and recommendations

## 🔧 Configuration

### Environment Variables
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=healthcare_analytics
DB_USER=postgres
DB_PASSWORD=your_password

# Application
FLASK_ENV=development
DASH_DEBUG=True
LOG_LEVEL=INFO
```

### Database Performance
```sql
-- Optimized indexes for common queries
CREATE INDEX idx_wait_times_year_province ON fact_wait_times(data_year, province_id);
CREATE INDEX idx_wait_times_procedure_metric ON fact_wait_times(procedure_id, metric_id);

-- Materialized views for dashboard performance
REFRESH MATERIALIZED VIEW mv_wait_time_trends;
```

## Usage Examples

### SQL Analytics
```sql
-- Provincial comparison for Hip Replacement in 2023
SELECT * FROM sp_provincial_comparison('Hip Replacement', 2023, '50th Percentile');

-- Trend analysis for Ontario procedures
SELECT * FROM sp_wait_time_trends(NULL, 'Ontario', 2018, 2023);

-- Benchmark compliance summary
SELECT * FROM sp_benchmark_analysis('British Columbia', 2023);
```

### Python Analysis
```python
# Load and analyze wait time data
df = analyzer.get_wait_time_data(
    province='Ontario',
    procedure='Cataract Surgery',
    start_year=2020,
    end_year=2023
)

# Calculate trends
trends = analyzer.calculate_trend_analysis(df)

# Generate insights
insights = analyzer.generate_insights('Ontario', 'Cataract Surgery')
```

## Testing

```bash
# Run all tests
python -m pytest tests/

# Run specific test categories
python -m pytest tests/test_etl/
python -m pytest tests/test_analytics/

# Run with coverage
python -m pytest --cov=src tests/
```

## Performance Optimizations

### Database
- Normalized schema reduces redundancy
- Strategic indexing for query performance
- Materialized views for dashboard speed
- Partitioning for large datasets (future enhancement)

### Application
- Data caching in analytics module
- Batch processing in ETL pipeline
- Asynchronous dashboard updates
- Connection pooling

## Security Considerations

- Environment-based configuration
- SQL injection prevention with parameterized queries
- Input validation and sanitization
- Database user permissions (least privilege)
- Audit logging for data changes

## Maintenance

### Daily Tasks
```bash
# Refresh materialized views
python setup.py refresh-views

# Check data quality
python scripts/data_quality_check.py
```

### Weekly Tasks
```bash
# Full maintenance routine
python setup.py maintenance

# Database backup
python setup.py backup
```

## Deployment

### Development
```bash
python dashboard/app.py
```

### Production
```bash
# Deploy with production settings
python setup.py deploy

# Run with Gunicorn
gunicorn -w 4 -b 0.0.0.0:8050 dashboard.app:server
```

## Documentation

- [Database Design](docs/database_design.md)
- [API Documentation](docs/api_documentation.md)
- [User Guide](docs/user_guide.md)
- [Architecture Overview](docs/architecture.md)

## Acknowledgments

- Canadian Institute for Health Information (CIHI) for the data
- PostgreSQL and Python communities
- Plotly/Dash for visualization capabilities