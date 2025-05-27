"""
Healthcare Wait Times ETL Pipeline
Author: Data Analytics Team
Created: 2025-05-27
Description: Extract, Transform, Load pipeline for Canadian healthcare wait time data
"""

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import logging
from datetime import datetime
import uuid
import os
from typing import Dict, List, Optional, Tuple
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manages database connections and operations"""
    
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
            
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
            
    def execute_query(self, query: str, params: Optional[Tuple] = None):
        """Execute a single query"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchall()
            return None
            
    def execute_batch(self, query: str, data: List[Tuple]):
        """Execute batch insert/update operations"""
        with self.connection.cursor() as cursor:
            execute_batch(cursor, query, data, page_size=1000)

class WaitTimeETL:
    """Main ETL class for healthcare wait time data"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.load_id = str(uuid.uuid4())
        self.stats = {
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_failed': 0
        }
        
    def extract_data(self, file_path: str) -> pd.DataFrame:
        """Extract data from Excel file"""
        logger.info(f"Extracting data from {file_path}")
        
        try:
            # Read the specific worksheet with wait time data
            df = pd.read_excel(
                file_path, 
                sheet_name='Wait times 2008 to 2023',
                skiprows=2,  # Skip header rows
                engine='openpyxl'
            )
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            # Remove empty rows
            df = df.dropna(how='all')
            
            # Filter out rows where all key columns are empty
            key_columns = ['Province/territory', 'Indicator', 'Metric', 'Data year']
            df = df.dropna(subset=key_columns, how='all')
            
            logger.info(f"Extracted {len(df)} records from source file")
            return df
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            raise
            
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean the data"""
        logger.info("Starting data transformation")
        
        # Create a copy to avoid modifying original
        transformed_df = df.copy()
        
        # Standardize column names
        column_mapping = {
            'Province/territory': 'province_name',
            'Reporting level': 'reporting_level',
            'Region': 'region_name',
            'Indicator': 'procedure_name',
            'Metric': 'metric_name',
            'Data year': 'data_year',
            'Unit of measurement': 'unit_of_measurement',
            'Indicator result': 'indicator_result'
        }
        transformed_df = transformed_df.rename(columns=column_mapping)
        
        # Clean and standardize data types
        transformed_df['data_year'] = pd.to_numeric(transformed_df['data_year'], errors='coerce')
        transformed_df['indicator_result'] = pd.to_numeric(transformed_df['indicator_result'], errors='coerce')
        
        # Handle missing values and data quality flags
        transformed_df['data_quality_flag'] = 'good'
        transformed_df.loc[transformed_df['indicator_result'].isna(), 'data_quality_flag'] = 'n/a'
        
        # Clean text fields
        text_columns = ['province_name', 'procedure_name', 'metric_name', 'reporting_level']
        for col in text_columns:
            if col in transformed_df.columns:
                transformed_df[col] = transformed_df[col].astype(str).str.strip()
        
        # Filter out invalid years
        transformed_df = transformed_df[
            (transformed_df['data_year'] >= 2008) & 
            (transformed_df['data_year'] <= 2023)
        ]
        
        # Add processing metadata
        transformed_df['load_id'] = self.load_id
        transformed_df['processed_at'] = datetime.now()
        
        logger.info(f"Transformation completed. {len(transformed_df)} records ready for load")
        return transformed_df
        
    def get_lookup_mappings(self) -> Dict[str, Dict]:
        """Get ID mappings for dimension tables"""
        logger.info("Loading lookup table mappings")
        
        mappings = {}
        
        # Province mappings
        province_query = "SELECT province_id, province_name FROM dim_provinces"
        province_results = self.db.execute_query(province_query)
        mappings['provinces'] = {row['province_name']: row['province_id'] for row in province_results}
        
        # Procedure mappings
        procedure_query = "SELECT procedure_id, procedure_name FROM dim_procedures"
        procedure_results = self.db.execute_query(procedure_query)
        mappings['procedures'] = {row['procedure_name']: row['procedure_id'] for row in procedure_results}
        
        # Metric mappings
        metric_query = "SELECT metric_id, metric_name FROM dim_metrics"
        metric_results = self.db.execute_query(metric_query)
        mappings['metrics'] = {row['metric_name']: row['metric_id'] for row in metric_results}
        
        # Reporting level mappings
        level_query = "SELECT level_id, level_name FROM dim_reporting_levels"
        level_results = self.db.execute_query(level_query)
        mappings['levels'] = {row['level_name']: row['level_id'] for row in level_results}
        
        return mappings
        
    def load_data(self, df: pd.DataFrame):
        """Load transformed data into database"""
        logger.info("Starting data load process")
        
        try:
            # Start audit record
            self.start_load_audit(df.shape[0])
            
            # Get lookup mappings
            mappings = self.get_lookup_mappings()
            
            # Prepare data for insertion
            insert_data = []
            failed_records = []
            
            for idx, row in df.iterrows():
                try:
                    # Map dimension IDs
                    province_id = mappings['provinces'].get(row['province_name'])
                    procedure_id = mappings['procedures'].get(row['procedure_name'])
                    metric_id = mappings['metrics'].get(row['metric_name'])
                    level_id = mappings['levels'].get(row['reporting_level'])
                    
                    # Skip records with missing dimension mappings
                    if not all([province_id, procedure_id, metric_id, level_id]):
                        failed_records.append(f"Row {idx}: Missing dimension mapping")
                        continue
                    
                    # Prepare insert tuple
                    insert_tuple = (
                        province_id,
                        procedure_id,
                        metric_id,
                        level_id,
                        int(row['data_year']) if pd.notna(row['data_year']) else None,
                        float(row['indicator_result']) if pd.notna(row['indicator_result']) else None,
                        False,  # is_estimate
                        row['data_quality_flag'],
                        row.get('region_name', 'n/a')
                    )
                    
                    insert_data.append(insert_tuple)
                    
                except Exception as e:
                    failed_records.append(f"Row {idx}: {str(e)}")
                    continue
            
            # Batch insert data
            if insert_data:
                insert_query = """
                INSERT INTO fact_wait_times 
                (province_id, procedure_id, metric_id, reporting_level_id, 
                 data_year, indicator_result, is_estimate, data_quality_flag, region_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                self.db.execute_batch(insert_query, insert_data)
                self.db.connection.commit()
                
                self.stats['records_inserted'] = len(insert_data)
                logger.info(f"Successfully inserted {len(insert_data)} records")
            
            # Log failed records
            if failed_records:
                self.stats['records_failed'] = len(failed_records)
                logger.warning(f"Failed to process {len(failed_records)} records")
                for error in failed_records[:10]:  # Log first 10 errors
                    logger.warning(error)
            
            # Complete audit record
            self.complete_load_audit('completed')
            
        except Exception as e:
            self.db.connection.rollback()
            self.complete_load_audit('failed', str(e))
            logger.error(f"Data load failed: {e}")
            raise
            
    def start_load_audit(self, record_count: int):
        """Start load audit record"""
        audit_query = """
        INSERT INTO audit_data_loads 
        (load_id, source_file, records_processed, load_status)
        VALUES (%s, %s, %s, %s)
        """
        
        self.db.execute_query(
            audit_query, 
            (self.load_id, 'wait_times_data.xlsx', record_count, 'in_progress')
        )
        self.db.connection.commit()
        
    def complete_load_audit(self, status: str, error_message: str = None):
        """Complete load audit record"""
        audit_query = """
        UPDATE audit_data_loads 
        SET load_status = %s,
            records_inserted = %s,
            records_failed = %s,
            error_message = %s,
            load_duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - load_timestamp))
        WHERE load_id = %s
        """
        
        self.db.execute_query(
            audit_query,
            (status, self.stats['records_inserted'], self.stats['records_failed'], 
             error_message, self.load_id)
        )
        self.db.connection.commit()
        
    def run_etl_pipeline(self, file_path: str):
        """Execute complete ETL pipeline"""
        start_time = datetime.now()
        logger.info(f"Starting ETL pipeline for {file_path}")
        
        try:
            # Extract
            raw_data = self.extract_data(file_path)
            self.stats['records_processed'] = len(raw_data)
            
            # Transform
            clean_data = self.transform_data(raw_data)
            
            # Load
            self.load_data(clean_data)
            
            # Summary
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
            logger.info(f"Stats: {self.stats}")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

def main():
    """Main execution function"""
    
    # Database connection parameters
    db_params = {
        'host': 'localhost',
        'database': 'healthcare_analytics',
        'user': 'postgres',
        'password': 'your_password',
        'port': 5432
    }
    
    # Initialize database connection
    db_conn = DatabaseConnection(db_params)
    
    try:
        # Connect to database
        db_conn.connect()
        
        # Initialize ETL processor
        etl_processor = WaitTimeETL(db_conn)
        
        # Run ETL pipeline
        etl_processor.run_etl_pipeline('wait_times_data.xlsx')
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
    finally:
        db_conn.disconnect()

if __name__ == "__main__":
    main()