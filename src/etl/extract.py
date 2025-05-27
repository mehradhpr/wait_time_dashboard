"""
ETL Extract Module
Description: Data extraction utilities split from ETL pipeline
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def extract_data(file_path: str) -> pd.DataFrame:
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

def validate_extracted_data(df: pd.DataFrame) -> bool:
    """Validate extracted data structure"""
    required_columns = [
        'Province/territory', 'Reporting level', 'Region',
        'Indicator', 'Metric', 'Data year', 
        'Unit of measurement', 'Indicator result'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    if len(df) == 0:
        logger.error("No data extracted")
        return False
    
    logger.info("Data validation passed")
    return True