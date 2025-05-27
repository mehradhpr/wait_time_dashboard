"""
Healthcare Wait Times ETL Pipeline
Description: Orchestrates ETL process using modular components
"""

import logging
from datetime import datetime
import uuid
from typing import Dict
from .extract import extract_data, validate_extracted_data
from .transform import transform_data, validate_transformed_data
from .load import get_lookup_mappings, prepare_fact_data, load_data
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

class WaitTimeETL:
    """Main ETL orchestrator using split modules"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.load_id = str(uuid.uuid4())
        self.stats = {
            'records_processed': 0,
            'records_inserted': 0,
            'records_failed': 0
        }
        
    def run_etl_pipeline(self, file_path: str) -> Dict[str, int]:
        """Execute complete ETL pipeline using modular components"""
        start_time = datetime.now()
        logger.info(f"Starting ETL pipeline for {file_path}")
        
        try:
            # Extract phase
            logger.info("Phase 1: Extract")
            raw_data = extract_data(file_path)
            
            if not validate_extracted_data(raw_data):
                raise ValueError("Data extraction validation failed")
            
            self.stats['records_processed'] = len(raw_data)
            
            # Transform phase  
            logger.info("Phase 2: Transform")
            clean_data = transform_data(raw_data, self.load_id)
            
            if not validate_transformed_data(clean_data):
                raise ValueError("Data transformation validation failed")
            
            # Load phase
            logger.info("Phase 3: Load")
            mappings = get_lookup_mappings(self.db)
            insert_data, failed_records = prepare_fact_data(clean_data, mappings)
            
            self.stats['records_failed'] = len(failed_records)
            
            if insert_data:
                load_stats = load_data(self.db, insert_data, self.load_id)
                self.stats['records_inserted'] = load_stats['records_inserted']
            
            # Summary
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
            logger.info(f"Stats: {self.stats}")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

def run_etl(file_path: str, db_params: Dict[str, str]) -> Dict[str, int]:
    """Convenience function to run ETL pipeline"""
    # Initialize database connection
    db_conn = DatabaseConnection(db_params)
    
    try:
        # Connect to database
        db_conn.connect()
        
        # Initialize ETL processor
        etl_processor = WaitTimeETL(db_conn)
        
        # Run ETL pipeline
        return etl_processor.run_etl_pipeline(file_path)
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
    finally:
        db_conn.disconnect()

if __name__ == "__main__":
    # Example usage
    db_params = {
        'host': 'localhost',
        'database': 'healthcare_analytics',
        'user': 'postgres', 
        'password': 'your_password',
        'port': 5432
    }
    
    try:
        stats = run_etl('data/raw/wait_times_data.xlsx', db_params)
        print(f"ETL completed: {stats}")
    except Exception as e:
        print(f"ETL failed: {e}")