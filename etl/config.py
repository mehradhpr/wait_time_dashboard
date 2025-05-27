# Purpose: Centralized configuration for database, file paths, and ETL settings

import os
from typing import Dict, Tuple, Optional
from datetime import date


class DatabaseConfig:
    """Database connection configuration"""
    
    def __init__(self):
        # Database connection settings
        self.server = "localhost,1433"
        self.database = "HealthcareWaitTimes"
        self.username = "sa"
        self.password = "YourStrong@Password123"
        self.driver = "{ODBC Driver 18 for SQL Server}"
        
        # Connection options
        self.trust_server_certificate = "yes"
        self.encrypt = "no"
        self.connection_timeout = 30
        
    def get_connection_string(self) -> str:
        """Get formatted connection string for pyodbc"""
        return (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate={self.trust_server_certificate};"
            f"Encrypt={self.encrypt};"
            f"Connection Timeout={self.connection_timeout};"
        )
    
    def get_connection_params(self) -> Dict[str, str]:
        """Get connection parameters as dictionary"""
        return {
            'server': self.server,
            'database': self.database,
            'username': self.username,
            'password': self.password,
            'driver': self.driver
        }


class FileConfig:
    """File paths and data source configuration"""
    
    def __init__(self):
        # Base directory (project root)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Data files
        self.excel_filename = "waittimespriorityproceduresincanada2024datatablesen.xlsx"
        self.excel_file_path = os.path.join(self.base_dir, self.excel_filename)
        
        # Excel sheet configuration
        self.excel_sheet_name = "Wait times 2008 to 2023"
        self.excel_header_row = 2  # 0-indexed row where data starts
        
        # Column mapping for Excel data
        self.excel_column_names = [
            'Reporting_Level', 'Province_Territory', 'Region', 'Indicator', 
            'Metric', 'Data_Year', 'Unit', 'Result'
        ]
        
        # Output directories
        self.logs_dir = os.path.join(self.base_dir, 'logs')
        self.reports_dir = os.path.join(self.base_dir, 'reports')
        self.exports_dir = os.path.join(self.base_dir, 'exports')
        
        # Create directories if they don't exist
        self._create_directories()
        
        # Log files
        self.etl_log_file = os.path.join(self.logs_dir, 'healthcare_etl.log')
        self.quality_log_file = os.path.join(self.logs_dir, 'data_quality.log')
        
    def _create_directories(self):
        """Create necessary directories"""
        directories = [self.logs_dir, self.reports_dir, self.exports_dir]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def get_excel_config(self) -> Dict:
        """Get Excel file configuration"""
        return {
            'file_path': self.excel_file_path,
            'sheet_name': self.excel_sheet_name,
            'header': self.excel_header_row,
            'names': self.excel_column_names
        }
    
    def file_exists(self, file_path: str = None) -> bool:
        """Check if Excel file exists"""
        path = file_path or self.excel_file_path
        return os.path.isfile(path)


class DataConfig:
    """Data processing and validation configuration"""
    
    def __init__(self):
        # Data quality thresholds
        self.min_data_completeness = 0.70  # 70% minimum completeness
        self.outlier_threshold = 3.0  # Z-score threshold for outliers
        self.max_wait_time_days = 730  # 2 years maximum wait time
        self.trend_significance_level = 0.05  # P-value threshold
        
        # Data validation ranges
        self.valid_year_range = (2008, 2025)
        self.valid_percentage_range = (0, 100)
        self.batch_size = 1000  # Processing batch size
        
        # Province metadata mapping
        self.province_data = {
            'Alberta': ('AB', 'Western', 4756408, 0),
            'British Columbia': ('BC', 'Western', 5399118, 0),
            'Manitoba': ('MB', 'Central', 1418129, 0),
            'New Brunswick': ('NB', 'Atlantic', 808718, 0),
            'Newfoundland and Labrador': ('NL', 'Atlantic', 540418, 0),
            'Nova Scotia': ('NS', 'Atlantic', 1030890, 0),
            'Ontario': ('ON', 'Central', 15801768, 0),
            'Prince Edward Island': ('PE', 'Atlantic', 173787, 0),
            'Quebec': ('QC', 'Central', 8604495, 0),
            'Saskatchewan': ('SK', 'Western', 1214618, 0),
            'Canada': ('CA', 'National', 39858480, 0)  # National aggregate
        }
        
        # Procedure categorization mapping
        self.procedure_categories = {
            'Bladder Cancer Surgery': ('BLAD_SURG', 'Cancer Surgery', 'High'),
            'Breast Cancer Surgery': ('BRST_SURG', 'Cancer Surgery', 'High'),
            'Colorectal Cancer Surgery': ('CLRC_SURG', 'Cancer Surgery', 'High'),
            'CABG': ('CABG', 'Cardiac Surgery', 'High'),
            'CT Scan': ('CT_SCAN', 'Diagnostic Imaging', 'Medium'),
            'Cataract Surgery': ('CAT_SURG', 'Ophthalmology', 'Medium'),
            'Cataract surgery': ('CAT_SURG_2', 'Ophthalmology', 'Medium'),
            'Hip Fracture Repair': ('HIP_FRAC', 'Orthopedic Surgery', 'High'),
            'Hip Replacement': ('HIP_REPL', 'Orthopedic Surgery', 'Medium'),
            'Knee Replacement': ('KNEE_REPL', 'Orthopedic Surgery', 'Medium'),
            'MRI Scan': ('MRI_SCAN', 'Diagnostic Imaging', 'Medium'),
            'Prostate Cancer Surgery': ('PROS_SURG', 'Cancer Surgery', 'High'),
            'Diagnostic CT': ('DIAG_CT', 'Diagnostic Imaging', 'Medium'),
            'Diagnostic MRI': ('DIAG_MRI', 'Diagnostic Imaging', 'Medium'),
            'Lung Cancer Surgery': ('LUNG_SURG', 'Cancer Surgery', 'High'),
            'Cardiac Catheterization': ('CARD_CATH', 'Cardiac Surgery', 'High')
        }
        
        # Metrics definition
        self.metrics_data = [
            ('PCT_50', '50th Percentile', 'Percentile', 'Days', 0),
            ('PCT_90', '90th Percentile', 'Percentile', 'Days', 0),
            ('BENCH_MET', '% Meeting Benchmark', 'Benchmark_Compliance', 'Percentage', 1),
            ('VOLUME', 'Volume', 'Volume', 'Number of cases', None)
        ]
        
        # Metric name standardization mapping
        self.metric_name_mapping = {
            '50th Percentile': '50th Percentile',
            '50th percentile': '50th Percentile',
            '90th Percentile': '90th Percentile', 
            '90th percentile': '90th Percentile',
            '% Meeting Benchmark': '% Meeting Benchmark',
            '% meeting benchmark': '% Meeting Benchmark',
            'Volume': 'Volume'
        }
    
    def get_province_info(self, province_name: str) -> Optional[Tuple[str, str, int, int]]:
        """Get province information by name"""
        return self.province_data.get(province_name)
    
    def get_procedure_info(self, procedure_name: str) -> Tuple[str, str, str]:
        """Get procedure information, with fallback for unknown procedures"""
        if procedure_name in self.procedure_categories:
            return self.procedure_categories[procedure_name]
        else:
            # Generate code for unknown procedures
            code = procedure_name.upper().replace(' ', '_')[:10]
            return (code, 'Other', 'Medium')
    
    def standardize_metric_name(self, metric_name: str) -> str:
        """Standardize metric names"""
        return self.metric_name_mapping.get(metric_name, metric_name)


class ETLConfig:
    """ETL processing configuration"""
    
    def __init__(self):
        # Processing settings
        self.chunk_size = 10000  # Records per processing chunk
        self.max_errors_to_log = 10  # Maximum errors to log in detail
        self.retry_attempts = 3  # Number of retry attempts for failed operations
        self.timeout_seconds = 300  # 5 minutes timeout for long operations
        
        # Logging configuration
        self.log_level = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        self.log_format = '%(asctime)s - %(levelname)s - %(message)s'
        self.max_log_file_size = 10 * 1024 * 1024  # 10MB
        self.backup_count = 5  # Number of backup log files
        
        # Data quality flags
        self.quality_flags = {
            'VALID': 'Data passes all validation checks',
            'MISSING': 'Required data is missing',
            'INVALID': 'Data fails validation rules',
            'ESTIMATED': 'Data is estimated/interpolated',
            'SUPPRESSED': 'Data suppressed for privacy/confidentiality'
        }
        
        # Source file tracking
        self.source_file_name = "waittimespriorityproceduresincanada2024datatablesen.xlsx"
        self.data_source = "Canadian Institute for Health Information (CIHI)"
        self.load_timestamp_format = "%Y-%m-%d %H:%M:%S"


class ReportConfig:
    """Reporting and output configuration"""
    
    def __init__(self):
        # Report generation settings
        self.default_fiscal_year = 2023
        self.report_title = "Canadian Healthcare Wait Times Analytics"
        self.organization = "Healthcare Analytics System"
        
        # Export formats
        self.supported_export_formats = ['PDF', 'Excel', 'CSV', 'JSON']
        self.default_export_format = 'PDF'
        
        # Dashboard settings
        self.dashboard_refresh_interval = 15  # minutes
        self.max_dashboard_data_points = 1000
        self.chart_color_palette = [
            '#1f77b4',  # Blue
            '#2ca02c',  # Green
            '#ff7f0e',  # Orange
            '#d62728',  # Red
            '#9467bd',  # Purple
            '#8c564b',  # Brown
            '#e377c2',  # Pink
            '#7f7f7f',  # Gray
            '#bcbd22',  # Olive
            '#17becf'   # Cyan
        ]
        
        # Performance thresholds for color coding
        self.performance_thresholds = {
            'excellent': 80,  # >= 80% compliance
            'good': 60,       # 60-79% compliance
            'fair': 40,       # 40-59% compliance
            'poor': 0         # < 40% compliance
        }


class MasterConfig:
    """Master configuration class combining all configurations"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.files = FileConfig()
        self.data = DataConfig()
        self.etl = ETLConfig()
        self.reports = ReportConfig()
        
        # Environment settings
        self.environment = os.getenv('HEALTHCARE_ENV', 'development')  # development, testing, production
        self.debug_mode = os.getenv('DEBUG', 'False').lower() == 'true'
        
    def get_all_configs(self) -> Dict:
        """Get all configuration objects"""
        return {
            'database': self.database,
            'files': self.files,
            'data': self.data,
            'etl': self.etl,
            'reports': self.reports
        }
    
    def validate_configuration(self) -> bool:
        """Validate all configuration settings"""
        issues = []
        
        # Check if Excel file exists
        if not self.files.file_exists():
            issues.append(f"Excel file not found: {self.files.excel_file_path}")
        
        # Check database connection settings
        if not all([self.database.server, self.database.database, 
                   self.database.username, self.database.password]):
            issues.append("Database connection parameters incomplete")
        
        # Check data validation ranges
        if self.data.valid_year_range[0] >= self.data.valid_year_range[1]:
            issues.append("Invalid year range configuration")
        
        if issues:
            print("Configuration validation issues:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        return True
    
    def print_configuration_summary(self):
        """Print configuration summary for debugging"""
        print("=" * 60)
        print("HEALTHCARE ANALYTICS - CONFIGURATION SUMMARY")
        print("=" * 60)
        print(f"Environment: {self.environment}")
        print(f"Debug Mode: {self.debug_mode}")
        print(f"Database: {self.database.server}/{self.database.database}")
        print(f"Excel File: {self.files.excel_filename}")
        print(f"File Exists: {self.files.file_exists()}")
        print(f"Log Directory: {self.files.logs_dir}")
        print(f"Data Quality Threshold: {self.data.min_data_completeness}")
        print(f"Batch Size: {self.etl.chunk_size}")
        print("=" * 60)


# Global configuration instance
config = MasterConfig()

# Convenience functions for easy access
def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return config.database

def get_file_config() -> FileConfig:
    """Get file configuration"""
    return config.files

def get_data_config() -> DataConfig:
    """Get data configuration"""
    return config.data

def get_etl_config() -> ETLConfig:
    """Get ETL configuration"""
    return config.etl

def get_report_config() -> ReportConfig:
    """Get report configuration"""
    return config.reports

# Example usage and testing
if __name__ == "__main__":
    # Test configuration
    master_config = MasterConfig()
    
    # Print summary
    master_config.print_configuration_summary()
    
    # Validate configuration
    is_valid = master_config.validate_configuration()
    print(f"\nConfiguration Valid: {is_valid}")
    
    # Test specific configurations
    print(f"\nDatabase Connection String: {master_config.database.get_connection_string()}")
    print(f"Excel Config: {master_config.files.get_excel_config()}")
    print(f"Province Info for Ontario: {master_config.data.get_province_info('Ontario')}")
    print(f"Procedure Info for CABG: {master_config.data.get_procedure_info('CABG')}")