
# Purpose: ETL pipeline to load Excel data into SQL Server database
# Data cleaning, validation, ETL processes, error handling

import pandas as pd
import numpy as np
import pyodbc
import logging
from datetime import datetime, date
import re
from typing import Dict, List, Tuple, Optional
import warnings
from config import DatabaseConfig
warnings.filterwarnings('ignore')


# SETUP
class DataProcessor:
    """Main class for processing healthcare wait times data"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.setup_logging()
        self.data_quality_issues = []
        
    def setup_logging(self):
        """Configure logging for data processing activities"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('healthcare_etl.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def connect_to_database(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = pyodbc.connect(self.config.get_connection_string())
            self.logger.info("Database connection established successfully")
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            return False
            
    def disconnect_from_database(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

# DATA EXTRACTION AND CLEANING
# ================================================================

    def extract_excel_data(self, file_path: str) -> pd.DataFrame:
        """Extract data from the Excel file with proper handling"""
        try:
            self.logger.info(f"Starting data extraction from {file_path}")
            
            # Read the specific sheet with wait times data
            df = pd.read_excel(
                file_path, 
                sheet_name='Wait times 2008 to 2023',
                header=2,  # Data starts at row 3 (0-indexed as 2)
                names=['Reporting_Level', 'Province_Territory', 'Region', 'Indicator', 
                       'Metric', 'Data_Year', 'Unit', 'Result']
            )
            
            self.logger.info(f"Extracted {len(df)} records from Excel file")
            return df
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {str(e)}")
            raise
            
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the extracted data"""
        self.logger.info("Starting data cleaning process")
        
        original_count = len(df)
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove header rows that may have been included
        df = df[df['Province_Territory'] != 'Province/territory']
        df = df[df['Indicator'] != 'Indicator']
        
        # Clean province names
        df['Province_Territory'] = df['Province_Territory'].str.strip()
        df = df[df['Province_Territory'].notna()]
        df = df[~df['Province_Territory'].isin(['', 'Province/territory'])]
        
        # Clean procedure names
        df['Indicator'] = df['Indicator'].str.strip()
        df = df[df['Indicator'].notna()]
        
        # Standardize metric names
        metric_mapping = {
            '50th Percentile': '50th Percentile',
            '50th percentile': '50th Percentile',
            '90th Percentile': '90th Percentile', 
            '90th percentile': '90th Percentile',
            '% Meeting Benchmark': '% Meeting Benchmark',
            '% meeting benchmark': '% Meeting Benchmark',
            'Volume': 'Volume'
        }
        df['Metric'] = df['Metric'].map(metric_mapping).fillna(df['Metric'])
        
        # Clean and validate years
        df['Data_Year'] = pd.to_numeric(df['Data_Year'], errors='coerce')
        df = df[(df['Data_Year'] >= 2000) & (df['Data_Year'] <= 2030)]
        
        # Handle result values
        df['Result'] = df['Result'].replace(['n/a', 'N/A', ''], np.nan)
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        
        # Add data quality flags
        df['Data_Quality_Flag'] = 'VALID'
        df.loc[df['Result'].isna(), 'Data_Quality_Flag'] = 'MISSING'
        df.loc[df['Result'] < 0, 'Data_Quality_Flag'] = 'INVALID'
        
        # Log data quality issues
        missing_count = df[df['Data_Quality_Flag'] == 'MISSING'].shape[0]
        invalid_count = df[df['Data_Quality_Flag'] == 'INVALID'].shape[0]
        
        self.logger.info(f"Data cleaning completed:")
        self.logger.info(f"  - Original records: {original_count}")
        self.logger.info(f"  - Clean records: {len(df)}")
        self.logger.info(f"  - Missing values: {missing_count}")
        self.logger.info(f"  - Invalid values: {invalid_count}")
        
        return df
        
    def validate_data(self, df: pd.DataFrame) -> List[str]:
        """Validate data quality and business rules"""
        self.logger.info("Starting data validation")
        issues = []
        
        # Check for required fields
        required_fields = ['Province_Territory', 'Indicator', 'Metric', 'Data_Year']
        for field in required_fields:
            null_count = df[field].isna().sum()
            if null_count > 0:
                issues.append(f"{field}: {null_count} null values found")
        
        # Validate year range
        invalid_years = df[(df['Data_Year'] < 2008) | (df['Data_Year'] > 2023)].shape[0]
        if invalid_years > 0:
            issues.append(f"Found {invalid_years} records with invalid years")
        
        # Check for unrealistic wait times (> 2 years)
        unrealistic_waits = df[
            (df['Metric'].str.contains('Percentile', na=False)) & 
            (df['Result'] > 730)
        ].shape[0]
        if unrealistic_waits > 0:
            issues.append(f"Found {unrealistic_waits} records with wait times > 2 years")
        
        # Check for impossible percentages
        invalid_percentages = df[
            (df['Metric'].str.contains('Benchmark', na=False)) & 
            ((df['Result'] < 0) | (df['Result'] > 100))
        ].shape[0]
        if invalid_percentages > 0:
            issues.append(f"Found {invalid_percentages} records with invalid percentages")
        
        self.data_quality_issues = issues
        
        for issue in issues:
            self.logger.warning(f"Data quality issue: {issue}")
            
        return issues

# DIMENSION TABLE POPULATION
# ================================================================

    def populate_provinces(self, df: pd.DataFrame):
        """Populate the provinces dimension table"""
        self.logger.info("Populating provinces dimension table")
        
        # Get unique provinces from data
        provinces = df['Province_Territory'].unique()
        provinces = [p for p in provinces if pd.notna(p) and p != 'Canada']
        
        # Province metadata mapping
        province_data = {
            'Alberta': ('AB', 'Western', 4756408, 0),
            'British Columbia': ('BC', 'Western', 5399118, 0),
            'Manitoba': ('MB', 'Central', 1418129, 0),
            'New Brunswick': ('NB', 'Atlantic', 808718, 0),
            'Newfoundland and Labrador': ('NL', 'Atlantic', 540418, 0),
            'Nova Scotia': ('NS', 'Atlantic', 1030890, 0),
            'Ontario': ('ON', 'Central', 15801768, 0),
            'Prince Edward Island': ('PE', 'Atlantic', 173787, 0),
            'Quebec': ('QC', 'Central', 8604495, 0),
            'Saskatchewan': ('SK', 'Western', 1214618, 0)
        }
        
        # Add Canada as a special entry
        province_data['Canada'] = ('CA', 'National', 39858480, 0)
        
        cursor = self.connection.cursor()
        
        try:
            # Clear existing data
            cursor.execute("DELETE FROM dim_provinces")
            
            # Insert province data
            insert_sql = """
                INSERT INTO dim_provinces (province_code, province_name, region, population_2023, is_territory)
                VALUES (?, ?, ?, ?, ?)
            """
            
            for province in set(list(provinces) + ['Canada']):
                if province in province_data:
                    code, region, population, is_territory = province_data[province]
                    cursor.execute(insert_sql, (code, province, region, population, is_territory))
                else:
                    # Handle unknown provinces
                    cursor.execute(insert_sql, ('UNK', province, 'Unknown', None, 0))
            
            self.connection.commit()
            self.logger.info(f"Inserted {len(set(list(provinces) + ['Canada']))} provinces")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating provinces: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def populate_procedures(self, df: pd.DataFrame):
        """Populate the procedures dimension table"""
        self.logger.info("Populating procedures dimension table")
        
        # Get unique procedures
        procedures = df['Indicator'].dropna().unique()
        
        # Procedure categorization
        procedure_categories = {
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
            'MRI Scan': ('MRI_SCAN', 'Diagnostic Imaging', 'Medium')
        }
        
        cursor = self.connection.cursor()
        
        try:
            # Clear existing data
            cursor.execute("DELETE FROM dim_procedures")
            
            # Insert procedure data
            insert_sql = """
                INSERT INTO dim_procedures (procedure_code, procedure_name, procedure_category, clinical_priority)
                VALUES (?, ?, ?, ?)
            """
            
            for procedure in procedures:
                if procedure in procedure_categories:
                    code, category, priority = procedure_categories[procedure]
                else:
                    # Generate code for unknown procedures
                    code = re.sub(r'[^A-Z0-9]', '_', procedure.upper())[:10]
                    category = 'Other'
                    priority = 'Medium'
                
                cursor.execute(insert_sql, (code, procedure, category, priority))
            
            self.connection.commit()
            self.logger.info(f"Inserted {len(procedures)} procedures")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating procedures: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def populate_metrics(self, df: pd.DataFrame):
        """Populate the metrics dimension table"""
        self.logger.info("Populating metrics dimension table")
        
        # Define metrics with their properties
        metrics_data = [
            ('PCT_50', '50th Percentile', 'Percentile', 'Days', 0),
            ('PCT_90', '90th Percentile', 'Percentile', 'Days', 0),
            ('BENCH_MET', '% Meeting Benchmark', 'Benchmark_Compliance', 'Percentage', 1),
            ('VOLUME', 'Volume', 'Volume', 'Number of cases', None)
        ]
        
        cursor = self.connection.cursor()
        
        try:
            # Clear existing data
            cursor.execute("DELETE FROM dim_metrics")
            
            # Insert metrics data
            insert_sql = """
                INSERT INTO dim_metrics (metric_code, metric_name, metric_type, unit_of_measurement, higher_is_better)
                VALUES (?, ?, ?, ?, ?)
            """
            
            for code, name, metric_type, unit, higher_better in metrics_data:
                cursor.execute(insert_sql, (code, name, metric_type, unit, higher_better))
            
            self.connection.commit()
            self.logger.info(f"Inserted {len(metrics_data)} metrics")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating metrics: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def populate_time_periods(self, df: pd.DataFrame):
        """Populate the time periods dimension table"""
        self.logger.info("Populating time periods dimension table")
        
        # Get unique years from data
        years = sorted(df['Data_Year'].dropna().unique())
        
        cursor = self.connection.cursor()
        
        try:
            # Clear existing data
            cursor.execute("DELETE FROM dim_time_periods")
            
            # Insert time period data
            insert_sql = """
                INSERT INTO dim_time_periods (fiscal_year, calendar_year, quarter, year_quarter, 
                                            is_current_year, fiscal_year_start_date, fiscal_year_end_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            current_year = max(years)
            
            for year in years:
                for quarter in range(1, 5):
                    year_quarter = f"{int(year)}-Q{quarter}"
                    is_current = 1 if year == current_year else 0
                    
                    # Canadian fiscal year runs April 1 to March 31
                    fiscal_start = date(int(year), 4, 1)
                    fiscal_end = date(int(year) + 1, 3, 31)
                    
                    cursor.execute(insert_sql, (
                        int(year), int(year), quarter, year_quarter, 
                        is_current, fiscal_start, fiscal_end
                    ))
            
            self.connection.commit()
            self.logger.info(f"Inserted time periods for {len(years)} years")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating time periods: {str(e)}")
            raise
        finally:
            cursor.close()

# FACT TABLE POPULATION
# ================================================================

    def populate_fact_table(self, df: pd.DataFrame):
        """Populate the main fact table with wait times data"""
        self.logger.info("Populating fact table")
        
        cursor = self.connection.cursor()
        
        try:
            # Get dimension mappings
            province_map = self.get_dimension_mapping('dim_provinces', 'province_name', 'province_id')
            procedure_map = self.get_dimension_mapping('dim_procedures', 'procedure_name', 'procedure_id')
            metric_map = self.get_dimension_mapping('dim_metrics', 'metric_name', 'metric_id')
            
            # Clear existing fact data
            cursor.execute("DELETE FROM fact_wait_times")
            
            # Insert fact data
            insert_sql = """
                INSERT INTO fact_wait_times (
                    province_id, procedure_id, metric_id, time_id, result_value, 
                    volume_cases, data_quality_flag, reporting_level, source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            successful_inserts = 0
            failed_inserts = 0
            
            for _, row in df.iterrows():
                try:
                    # Get dimension IDs
                    province_id = province_map.get(row['Province_Territory'])
                    procedure_id = procedure_map.get(row['Indicator'])
                    metric_id = metric_map.get(row['Metric'])
                    
                    if not all([province_id, procedure_id, metric_id]):
                        failed_inserts += 1
                        continue
                    
                    # Get time_id (using year and Q1 as default)
                    time_id = self.get_time_id(int(row['Data_Year']), 1)
                    
                    # Prepare values
                    result_value = row['Result'] if pd.notna(row['Result']) else None
                    volume_cases = result_value if row['Metric'] == 'Volume' else None
                    
                    cursor.execute(insert_sql, (
                        province_id, procedure_id, metric_id, time_id,
                        result_value, volume_cases, row['Data_Quality_Flag'],
                        row['Reporting_Level'], 'waittimespriorityproceduresincanada2024datatablesen.xlsx'
                    ))
                    
                    successful_inserts += 1
                    
                except Exception as e:
                    failed_inserts += 1
                    if failed_inserts < 10:  # Log first few errors
                        self.logger.warning(f"Failed to insert row: {str(e)}")
            
            self.connection.commit()
            self.logger.info(f"Fact table population completed:")
            self.logger.info(f"  - Successful inserts: {successful_inserts}")
            self.logger.info(f"  - Failed inserts: {failed_inserts}")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating fact table: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def get_dimension_mapping(self, table_name: str, name_column: str, id_column: str) -> Dict:
        """Get mapping dictionary for dimension lookups"""
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT {name_column}, {id_column} FROM {table_name}")
        return {row[0]: row[1] for row in cursor.fetchall()}
        
    def get_time_id(self, year: int, quarter: int) -> Optional[int]:
        """Get time_id for given year and quarter"""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT time_id FROM dim_time_periods WHERE fiscal_year = ? AND quarter = ?",
            (year, quarter)
        )
        result = cursor.fetchone()
        return result[0] if result else None

# MAIN ETL PIPELINE
# ================================================================

    def run_etl_pipeline(self, excel_file_path: str) -> bool:
        """Execute the complete ETL pipeline"""
        try:
            self.logger.info("Starting Healthcare Wait Times ETL Pipeline")
            
            # Step 1: Connect to database
            if not self.connect_to_database():
                return False
            
            # Step 2: Extract data from Excel
            raw_data = self.extract_excel_data(excel_file_path)
            
            # Step 3: Clean and validate data
            clean_data = self.clean_data(raw_data)
            validation_issues = self.validate_data(clean_data)
            
            # Step 4: Populate dimension tables
            self.populate_provinces(clean_data)
            self.populate_procedures(clean_data)
            self.populate_metrics(clean_data)
            self.populate_time_periods(clean_data)
            
            # Step 5: Populate fact table
            self.populate_fact_table(clean_data)
            
            # Step 6: Generate summary report
            self.generate_load_summary()
            
            self.logger.info("ETL Pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            return False
        finally:
            self.disconnect_from_database()
            
    def generate_load_summary(self):
        """Generate a summary report of the data load"""
        cursor = self.connection.cursor()
        
        # Get record counts
        cursor.execute("SELECT COUNT(*) FROM dim_provinces")
        province_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_procedures")
        procedure_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_metrics")
        metric_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dim_time_periods")
        time_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM fact_wait_times")
        fact_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM fact_wait_times WHERE result_value IS NOT NULL")
        valid_fact_count = cursor.fetchone()[0]
        
        # Log summary
        self.logger.info("=" * 50)
        self.logger.info("DATA LOAD SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"Provinces loaded: {province_count}")
        self.logger.info(f"Procedures loaded: {procedure_count}")
        self.logger.info(f"Metrics loaded: {metric_count}")
        self.logger.info(f"Time periods loaded: {time_count}")
        self.logger.info(f"Fact records loaded: {fact_count}")
        self.logger.info(f"Valid fact records: {valid_fact_count}")
        self.logger.info(f"Data completeness: {(valid_fact_count/fact_count*100):.1f}%")
        
        if self.data_quality_issues:
            self.logger.info(f"Data quality issues found: {len(self.data_quality_issues)}")
        
        self.logger.info("=" * 50)

# UTILITY FUNCTIONS
# ================================================================

def main():
    """Main execution function"""
    # Configuration
    config = DatabaseConfig()
    processor = DataProcessor(config)
    
    # File path to your Excel file
    excel_file_path = "waittimespriorityproceduresincanada2024datatablesen.xlsx"
    
    # Run the ETL pipeline
    success = processor.run_etl_pipeline(excel_file_path)
    
    if success:
        print("ETL Pipeline completed successfully!")
        print("Check 'healthcare_etl.log' for detailed processing information.")
    else:
        print("ETL Pipeline failed. Check logs for details.")

if __name__ == "__main__":
    main()

# ADDITIONAL UTILITY SCRIPTS
# ================================================================

class DataQualityChecker:
    """Additional data quality checking utilities"""
    
    def __init__(self, connection):
        self.connection = connection
        
    def run_quality_checks(self):
        """Run comprehensive data quality checks"""
        cursor = self.connection.cursor()
        
        checks = [
            ("Referential Integrity", self.check_referential_integrity),
            ("Data Completeness", self.check_data_completeness),
            ("Business Rule Validation", self.check_business_rules),
            ("Statistical Outliers", self.check_statistical_outliers)
        ]
        
        results = {}
        for check_name, check_function in checks:
            try:
                results[check_name] = check_function()
                print(f"✓ {check_name}: PASSED")
            except Exception as e:
                results[check_name] = f"FAILED: {str(e)}"
                print(f"✗ {check_name}: FAILED - {str(e)}")
                
        return results
        
    def check_referential_integrity(self):
        """Check foreign key relationships"""
        cursor = self.connection.cursor()
        
        # Check for orphaned fact records
        cursor.execute("""
            SELECT COUNT(*) FROM fact_wait_times f
            LEFT JOIN dim_provinces p ON f.province_id = p.province_id
            WHERE p.province_id IS NULL
        """)
        orphaned_provinces = cursor.fetchone()[0]
        
        if orphaned_provinces > 0:
            raise Exception(f"Found {orphaned_provinces} fact records with invalid province_id")
            
        return "All referential integrity checks passed"
        
    def check_data_completeness(self):
        """Check for data completeness across dimensions"""
        cursor = self.connection.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(result_value) as records_with_values,
                CAST(COUNT(result_value) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct
            FROM fact_wait_times
        """)
        
        total, with_values, completeness = cursor.fetchone()
        
        if completeness < 70:  # Less than 70% completeness
            raise Exception(f"Data completeness too low: {completeness}%")
            
        return f"Data completeness: {completeness}% ({with_values}/{total} records)"
        
    def check_business_rules(self):
        """Validate business-specific rules"""
        cursor = self.connection.cursor()
        
        # Check for impossible wait times (negative or > 3 years)
        cursor.execute("""
            SELECT COUNT(*) FROM fact_wait_times f
            JOIN dim_metrics m ON f.metric_id = m.metric_id
            WHERE m.metric_name LIKE '%Percentile%' 
            AND (f.result_value < 0 OR f.result_value > 1095)
        """)
        
        invalid_waits = cursor.fetchone()[0]
        
        if invalid_waits > 0:
            raise Exception(f"Found {invalid_waits} records with impossible wait times")
            
        return "All business rules validated"
        
    def check_statistical_outliers(self):
        """Identify statistical outliers that may indicate data issues"""
        cursor = self.connection.cursor()
        
        cursor.execute("""
            WITH stats AS (
                SELECT 
                    AVG(result_value) as mean_val,
                    STDEV(result_value) as std_val
                FROM fact_wait_times 
                WHERE result_value IS NOT NULL
            )
            SELECT COUNT(*) FROM fact_wait_times f, stats s
            WHERE f.result_value IS NOT NULL
            AND ABS(f.result_value - s.mean_val) > 4 * s.std_val
        """)
        
        extreme_outliers = cursor.fetchone()[0]
        
        return f"Found {extreme_outliers} extreme statistical outliers (>4 standard deviations)"

# Usage example for data quality checking
def run_quality_checks():
    config = DatabaseConfig()
    connection = pyodbc.connect(config.get_connection_string())
    
    checker = DataQualityChecker(connection)
    results = checker.run_quality_checks()
    
    connection.close()
    return results