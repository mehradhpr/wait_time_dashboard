"""
ETL Transform Module
Description: Data transformation utilities split from ETL pipeline
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame, load_id: str) -> pd.DataFrame:
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
    transformed_df['load_id'] = load_id
    transformed_df['processed_at'] = datetime.now()
    
    logger.info(f"Transformation completed. {len(transformed_df)} records ready for load")
    return transformed_df

def clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and convert data types"""
    df_clean = df.copy()
    
    # Convert numeric columns
    numeric_columns = ['data_year', 'indicator_result']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Clean text columns
    text_columns = ['province_name', 'procedure_name', 'metric_name']
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
    
    return df_clean

def validate_transformed_data(df: pd.DataFrame) -> bool:
    """Validate transformed data quality"""
    # Check for required columns after transformation
    required_columns = ['province_name', 'procedure_name', 'metric_name', 'data_year']
    
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing transformed column: {col}")
            return False
    
    # Check data ranges
    if df['data_year'].min() < 2008 or df['data_year'].max() > 2023:
        logger.warning("Data year outside expected range")
    
    logger.info("Transformed data validation passed")
    return True