"""
ETL Pipeline Execution Script
"""

import sys
import os
from pathlib import Path
import argparse
import logging
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from config.settings import DATABASE_CONFIG, DATA_CONFIG
from etl.pipeline import run_etl
from utils.logging_config import setup_logging

def main():
    """Main ETL execution function"""
    parser = argparse.ArgumentParser(description='Run Healthcare Wait Times ETL Pipeline')
    parser.add_argument('--file', '-f', 
                       default=str(DATA_CONFIG['raw_data_path'] / DATA_CONFIG['source_file']),
                       help='Path to source Excel file')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting ETL pipeline execution")
    logger.info(f"Source file: {args.file}")
    
    # Check if source file exists
    if not os.path.exists(args.file):
        logger.error(f"Source file not found: {args.file}")
        sys.exit(1)
    
    try:
        # Run ETL pipeline
        start_time = datetime.now()
        stats = run_etl(args.file, DATABASE_CONFIG)
        end_time = datetime.now()
        
        # Log results
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
        logger.info(f"Processing statistics:")
        logger.info(f"  - Records processed: {stats['records_processed']}")
        logger.info(f"  - Records inserted: {stats['records_inserted']}")
        logger.info(f"  - Records failed: {stats['records_failed']}")
        
        print(f"ETL pipeline completed successfully!")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Records processed: {stats['records_processed']}")
        print(f"Records inserted: {stats['records_inserted']}")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        print(f"ETL pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()