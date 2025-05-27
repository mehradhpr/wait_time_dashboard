# ================================================================
# HEALTHCARE WAIT TIMES - STREAMLINED ETL PIPELINE
# ================================================================
# Purpose: Clean, modular ETL pipeline using configuration management

import pandas as pd
import numpy as np
import pyodbc
import logging
import re
from datetime import datetime, date
from typing import Dict, List, Optional
import warnings

# Import configurations
from config import (
    get_database_config, get_file_config, get_data_config, 
    get_etl_config, MasterConfig
)

warnings.filterwarnings('ignore')


class HealthcareETLProcessor:
    """Streamlined ETL processor for healthcare wait times data"""
    
    def __init__(self):
        # Load configurations
        self.config = MasterConfig()
        self.db_config = self.config.database
        self.file_config = self.config.files
        self.data_config = self.config.data
        self.etl_config = self.config.etl
        
        # Initialize connection and logging
        self.connection = None
        self.setup_logging()
        self.data_quality_issues = []
        
    def setup_logging(self):
        """Configure logging using ETL configuration"""
        logging.basicConfig(
            level=getattr(logging, self.etl_config.log_level),
            format=self.etl_config.log_format,
            handlers=[
                logging.FileHandler(self.file_config.etl_log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def connect_to_database(self) -> bool:
        """Establish database connection using configuration"""
        try:
            self.connection = pyodbc.connect(self.db_config.get_connection_string())
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

    def extract_excel_data(self) -> pd.DataFrame:
        """Extract data from Excel file using file configuration"""
        try:
            self.logger.info(f"Starting data extraction from {self.file_config.excel_filename}")
            
            # Check if file exists
            if not self.file_config.file_exists():
                raise FileNotFoundError(f"Excel file not found: {self.file_config.excel_file_path}")
            
            # Load Excel configuration
            excel_config = self.file_config.get_excel_config()
            
            # Read Excel file
            df = pd.read_excel(
                excel_config['file_path'],
                sheet_name=excel_config['sheet_name'],
                header=excel_config['header'],
                names=excel_config['names']
            )
            
            self.logger.info(f"Extracted {len(df)} records from Excel file")
            return df
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {str(e)}")
            raise
            
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize data using data configuration"""
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
        
        # Standardize metric names using configuration
        df['Metric'] = df['Metric'].apply(self.data_config.standardize_metric_name)
        
        # Clean and validate years using configuration
        df['Data_Year'] = pd.to_numeric(df['Data_Year'], errors='coerce')
        min_year, max_year = self.data_config.valid_year_range
        df = df[(df['Data_Year'] >= min_year) & (df['Data_Year'] <= max_year)]
        
        # Handle result values
        df['Result'] = df['Result'].replace(['n/a', 'N/A', ''], np.nan)
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        
        # Add data quality flags using ETL configuration
        df['Data_Quality_Flag'] = self.etl_config.quality_flags['VALID']
        df.loc[df['Result'].isna(), 'Data_Quality_Flag'] = self.etl_config.quality_flags['MISSING']
        df.loc[df['Result'] < 0, 'Data_Quality_Flag'] = self.etl_config.quality_flags['INVALID']
        
        # Log cleaning results
        missing_count = df[df['Data_Quality_Flag'] == self.etl_config.quality_flags['MISSING']].shape[0]
        invalid_count = df[df['Data_Quality_Flag'] == self.etl_config.quality_flags['INVALID']].shape[0]
        
        self.logger.info(f"Data cleaning completed:")
        self.logger.info(f"  - Original records: {original_count}")
        self.logger.info(f"  - Clean records: {len(df)}")
        self.logger.info(f"  - Missing values: {missing_count}")
        self.logger.info(f"  - Invalid values: {invalid_count}")
        
        return df
        
    def validate_data(self, df: pd.DataFrame) -> List[str]:
        """Validate data quality using data configuration"""
        self.logger.info("Starting data validation")
        issues = []
        
        # Check for required fields
        required_fields = ['Province_Territory', 'Indicator', 'Metric', 'Data_Year']
        for field in required_fields:
            null_count = df[field].isna().sum()
            if null_count > 0:
                issues.append(f"{field}: {null_count} null values found")
        
        # Validate year range using configuration
        min_year, max_year = self.data_config.valid_year_range
        invalid_years = df[(df['Data_Year'] < min_year) | (df['Data_Year'] > max_year)].shape[0]
        if invalid_years > 0:
            issues.append(f"Found {invalid_years} records with invalid years")
        
        # Check for unrealistic wait times using configuration
        unrealistic_waits = df[
            (df['Metric'].str.contains('Percentile', na=False)) & 
            (df['Result'] > self.data_config.max_wait_time_days)
        ].shape[0]
        if unrealistic_waits > 0:
            issues.append(f"Found {unrealistic_waits} records with wait times > {self.data_config.max_wait_time_days} days")
        
        # Check for impossible percentages using configuration
        min_pct, max_pct = self.data_config.valid_percentage_range
        invalid_percentages = df[
            (df['Metric'].str.contains('Benchmark', na=False)) & 
            ((df['Result'] < min_pct) | (df['Result'] > max_pct))
        ].shape[0]
        if invalid_percentages > 0:
            issues.append(f"Found {invalid_percentages} records with invalid percentages")
        
        self.data_quality_issues = issues
        
        for issue in issues:
            self.logger.warning(f"Data quality issue: {issue}")
            
        return issues

    def populate_provinces(self, df: pd.DataFrame):
        """Populate provinces dimension using data configuration"""
        self.logger.info("Populating provinces dimension table")
        
        # Get unique provinces from data
        provinces = df['Province_Territory'].unique()
        provinces = [p for p in provinces if pd.notna(p)]
        
        # Add Canada if not present
        all_provinces = set(list(provinces) + ['Canada'])
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("DELETE FROM dim_provinces")
            
            insert_sql = """
                INSERT INTO dim_provinces (province_code, province_name, region, population_2023, is_territory)
                VALUES (?, ?, ?, ?, ?)
            """
            
            for province in all_provinces:
                province_info = self.data_config.get_province_info(province)
                if province_info:
                    code, region, population, is_territory = province_info
                    cursor.execute(insert_sql, (code, province, region, population, is_territory))
                else:
                    # Handle unknown provinces
                    cursor.execute(insert_sql, ('UNK', province, 'Unknown', None, 0))
            
            self.connection.commit()
            self.logger.info(f"Inserted {len(all_provinces)} provinces")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating provinces: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def populate_procedures(self, df: pd.DataFrame):
        """Populate procedures dimension using data configuration"""
        self.logger.info("Populating procedures dimension table")
        
        # Get unique procedures
        procedures = df['Indicator'].dropna().unique()
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("DELETE FROM dim_procedures")
            
            insert_sql = """
                INSERT INTO dim_procedures (procedure_code, procedure_name, procedure_category, clinical_priority)
                VALUES (?, ?, ?, ?)
            """
            
            for procedure in procedures:
                code, category, priority = self.data_config.get_procedure_info(procedure)
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
        """Populate metrics dimension using data configuration"""
        self.logger.info("Populating metrics dimension table")
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("DELETE FROM dim_metrics")
            
            insert_sql = """
                INSERT INTO dim_metrics (metric_code, metric_name, metric_type, unit_of_measurement, higher_is_better)
                VALUES (?, ?, ?, ?, ?)
            """
            
            for code, name, metric_type, unit, higher_better in self.data_config.metrics_data:
                cursor.execute(insert_sql, (code, name, metric_type, unit, higher_better))
            
            self.connection.commit()
            self.logger.info(f"Inserted {len(self.data_config.metrics_data)} metrics")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error populating metrics: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def populate_time_periods(self, df: pd.DataFrame):
        """Populate time periods dimension"""
        self.logger.info("Populating time periods dimension table")
        
        # Get unique years from data
        years = sorted(df['Data_Year'].dropna().unique())
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("DELETE FROM dim_time_periods")
            
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

    def populate_fact_table(self, df: pd.DataFrame):
        """Populate fact table with optimized batch processing"""
        self.logger.info("Populating fact table")
        
        cursor = self.connection.cursor()
        
        try:
            # Get dimension mappings
            province_map = self.get_dimension_mapping('dim_provinces', 'province_name', 'province_id')
            procedure_map = self.get_dimension_mapping('dim_procedures', 'procedure_name', 'procedure_id')
            metric_map = self.get_dimension_mapping('dim_metrics', 'metric_name', 'metric_id')
            
            # Clear existing fact data
            cursor.execute("DELETE FROM fact_wait_times")
            
            # Prepare batch insert
            insert_sql = """
                INSERT INTO fact_wait_times (
                    province_id, procedure_id, metric_id, time_id, result_value, 
                    volume_cases, data_quality_flag, reporting_level, source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            successful_inserts = 0
            failed_inserts = 0
            batch_data = []
            
            # Process data in batches using configuration
            for _, row in df.iterrows():
                try:
                    # Get dimension IDs
                    province_id = province_map.get(row['Province_Territory'])
                    procedure_id = procedure_map.get(row['Indicator'])
                    metric_id = metric_map.get(row['Metric'])
                    
                    if not all([province_id, procedure_id, metric_id]):
                        failed_inserts += 1
                        continue
                    
                    # Get time_id
                    time_id = self.get_time_id(int(row['Data_Year']), 1)
                    
                    # Prepare values
                    result_value = row['Result'] if pd.notna(row['Result']) else None
                    volume_cases = result_value if row['Metric'] == 'Volume' else None
                    
                    # Add to batch
                    batch_data.append((
                        province_id, procedure_id, metric_id, time_id,
                        result_value, volume_cases, row['Data_Quality_Flag'],
                        row['Reporting_Level'], self.etl_config.source_file_name
                    ))
                    
                    # Execute batch when reaching batch size
                    if len(batch_data) >= self.data_config.batch_size:
                        cursor.executemany(insert_sql, batch_data)
                        successful_inserts += len(batch_data)
                        batch_data = []
                    
                except Exception as e:
                    failed_inserts += 1
                    if failed_inserts <= self.etl_config.max_errors_to_log:
                        self.logger.warning(f"Failed to insert row: {str(e)}")
            
            # Execute remaining batch
            if batch_data:
                cursor.executemany(insert_sql, batch_data)
                successful_inserts += len(batch_data)
            
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

    def run_etl_pipeline(self) -> bool:
        """Execute the complete ETL pipeline"""
        try:
            self.logger.info("Starting Healthcare Wait Times ETL Pipeline")
            
            # Validate configuration
            if not self.config.validate_configuration():
                self.logger.error("Configuration validation failed")
                return False
            
            # Step 1: Connect to database
            if not self.connect_to_database():
                return False
            
            # Step 2: Extract data from Excel
            raw_data = self.extract_excel_data()
            
            # Step 3: Clean and validate data
            clean_data = self.clean_data(raw_data)
            validation_issues = self.validate_data(clean_data)
            
            # Check if data quality meets minimum standards
            data_completeness = len(clean_data[clean_data['Data_Quality_Flag'] == self.etl_config.quality_flags['VALID']]) / len(clean_data)
            if data_completeness < self.data_config.min_data_completeness:
                self.logger.warning(f"Data completeness ({data_completeness:.1%}) below minimum threshold ({self.data_config.min_data_completeness:.1%})")
            
            # Step 4: Populate dimension tables
            self.populate_provinces(clean_data)
            self.populate_procedures(clean_data)
            self.populate_metrics(clean_data)
            self.populate_time_periods(clean_data)
            
            # Step 5: Populate fact table
            self.populate_fact_table(clean_data)
            
            # Step 6: Generate summary report
            self.generate_load_summary()
            
            # Step 7: Run basic quality checks
            self.run_post_load_validation()
            
            self.logger.info("ETL Pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            return False
        finally:
            self.disconnect_from_database()
            
    def generate_load_summary(self):
        """Generate comprehensive load summary using configuration"""
        cursor = self.connection.cursor()
        
        # Get record counts for all tables
        table_counts = {}
        tables_to_check = ['dim_provinces', 'dim_procedures', 'dim_metrics', 'dim_time_periods', 'fact_wait_times']
        
        for table in tables_to_check:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            table_counts[table] = cursor.fetchone()[0]
        
        # Get valid fact records count
        cursor.execute("SELECT COUNT(*) FROM fact_wait_times WHERE result_value IS NOT NULL")
        valid_fact_count = cursor.fetchone()[0]
        
        # Calculate data completeness
        fact_count = table_counts['fact_wait_times']
        completeness = (valid_fact_count / fact_count * 100) if fact_count > 0 else 0
        
        # Log detailed summary
        self.logger.info("=" * 60)
        self.logger.info("ETL PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Configuration: {self.config.environment.upper()} environment")
        self.logger.info(f"Data Source: {self.file_config.excel_filename}")
        self.logger.info(f"Database: {self.db_config.server}/{self.db_config.database}")
        self.logger.info(f"Load Timestamp: {datetime.now().strftime(self.etl_config.load_timestamp_format)}")
        self.logger.info("")
        self.logger.info("Table Population Results:")
        for table, count in table_counts.items():
            self.logger.info(f"  - {table}: {count:,} records")
        self.logger.info("")
        self.logger.info(f"Data Quality Metrics:")
        self.logger.info(f"  - Total fact records: {fact_count:,}")
        self.logger.info(f"  - Valid fact records: {valid_fact_count:,}")
        self.logger.info(f"  - Data completeness: {completeness:.1f}%")
        
        if self.data_quality_issues:
            self.logger.info(f"  - Data quality issues: {len(self.data_quality_issues)}")
            for issue in self.data_quality_issues[:5]:  # Show first 5 issues
                self.logger.info(f"    • {issue}")
        
        self.logger.info("=" * 60)
    
    def run_post_load_validation(self):
        """Run basic validation checks after data load"""
        self.logger.info("Running post-load validation checks")
        
        try:
            # Import and run quick validation
            from data_quality_checker import quick_data_validation
            
            is_valid = quick_data_validation(self.connection)
            
            if is_valid:
                self.logger.info("✓ Post-load validation passed")
            else:
                self.logger.warning("⚠ Post-load validation issues detected")
                
        except ImportError:
            self.logger.info("Data quality checker not available - skipping post-load validation")
        except Exception as e:
            self.logger.warning(f"Post-load validation failed: {str(e)}")


def main():
    """Main execution function with enhanced error handling"""
    try:
        # Print configuration summary if in debug mode
        config = MasterConfig()
        if config.debug_mode:
            config.print_configuration_summary()
        
        # Initialize and run ETL processor
        processor = HealthcareETLProcessor()
        success = processor.run_etl_pipeline()
        
        if success:
            print("✅ ETL Pipeline completed successfully!")
            print(f"📊 Check '{processor.file_config.etl_log_file}' for detailed processing information.")
            print(f"📁 Reports available in: {processor.file_config.reports_dir}")
            
            # Optionally run comprehensive quality checks
            try:
                from data_quality_checker import run_quality_checks
                print("\n🔍 Running comprehensive data quality checks...")
                quality_results = run_quality_checks(export_report=True)
                
                passed_checks = sum(1 for r in quality_results.values() 
                                   if isinstance(r, dict) and r.get('status') == 'PASSED')
                total_checks = len([r for r in quality_results.values() 
                                   if isinstance(r, dict) and 'status' in r])
                
                print(f"✅ Quality checks completed: {passed_checks}/{total_checks} passed")
                if 'report_exported_to' in quality_results:
                    print(f"📋 Quality report: {quality_results['report_exported_to']}")
                    
            except ImportError:
                print("📝 Comprehensive quality checker not available")
            except Exception as e:
                print(f"⚠️ Quality check failed: {str(e)}")
                
        else:
            print("❌ ETL Pipeline failed!")
            print(f"🔍 Check logs for details: {processor.file_config.etl_log_file}")
            return 1
            
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)