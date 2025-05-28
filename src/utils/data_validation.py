"""
Data Validation Utilities
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Data validation utilities for healthcare wait time data"""
    
    @staticmethod
    def validate_excel_structure(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate Excel file structure"""
        required_columns = [
            'Province/territory', 'Reporting level', 'Region',
            'Indicator', 'Metric', 'Data year', 
            'Unit of measurement', 'Indicator result'
        ]
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"Missing required columns: {missing_columns}")
        
        # Check data types and ranges
        if 'Data year' in df.columns:
            invalid_years = df[
                (pd.to_numeric(df['Data year'], errors='coerce') < 2008) |
                (pd.to_numeric(df['Data year'], errors='coerce') > 2023)
            ]
            if len(invalid_years) > 0:
                validation_result['warnings'].append(f"Found {len(invalid_years)} records with invalid years")
        
        # Check for completely empty rows
        empty_rows = df.dropna(how='all')
        if len(empty_rows) < len(df):
            validation_result['warnings'].append(f"Removed {len(df) - len(empty_rows)} empty rows")
        
        validation_result['summary'] = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_columns': len(missing_columns),
            'data_completeness': (df.notna().sum().sum() / (len(df) * len(df.columns))) * 100
        }
        
        return validation_result
    
    @staticmethod
    def validate_transformed_data(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate transformed data before database load"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        
        required_columns = ['province_name', 'procedure_name', 'metric_name', 'data_year']
        
        # Check required columns after transformation
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"Missing transformed columns: {missing_columns}")
        
        # Validate data year
        if 'data_year' in df.columns:
            invalid_years = df[
                (df['data_year'] < 2008) | (df['data_year'] > 2023) | df['data_year'].isna()
            ]
            if len(invalid_years) > 0:
                validation_result['warnings'].append(f"Found {len(invalid_years)} records with invalid data years")
        
        # Validate numeric results
        if 'indicator_result' in df.columns:
            negative_results = df[df['indicator_result'] < 0]
            if len(negative_results) > 0:
                validation_result['warnings'].append(f"Found {len(negative_results)} records with negative results")
        
        validation_result['summary'] = {
            'total_records': len(df),
            'valid_records': len(df.dropna(subset=required_columns)),
            'data_years_range': f"{df['data_year'].min():.0f}-{df['data_year'].max():.0f}" if 'data_year' in df.columns else 'N/A',
            'unique_provinces': df['province_name'].nunique() if 'province_name' in df.columns else 0,
            'unique_procedures': df['procedure_name'].nunique() if 'procedure_name' in df.columns else 0
        }
        
        return validation_result
    
    @staticmethod
    def validate_database_mappings(df: pd.DataFrame, mappings: Dict[str, Dict]) -> Dict[str, Any]:
        """Validate that data can be mapped to database dimensions"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'mapping_stats': {}
        }
        
        # Check province mappings
        if 'province_name' in df.columns:
            unmapped_provinces = df[~df['province_name'].isin(mappings['provinces'].keys())]['province_name'].unique()
            if len(unmapped_provinces) > 0:
                validation_result['warnings'].append(f"Unmapped provinces: {list(unmapped_provinces)}")
            
            validation_result['mapping_stats']['provinces'] = {
                'total_unique': df['province_name'].nunique(),
                'mapped': df[df['province_name'].isin(mappings['provinces'].keys())]['province_name'].nunique(),
                'unmapped': len(unmapped_provinces)
            }
        
        # Check procedure mappings
        if 'procedure_name' in df.columns:
            unmapped_procedures = df[~df['procedure_name'].isin(mappings['procedures'].keys())]['procedure_name'].unique()
            if len(unmapped_procedures) > 0:
                validation_result['warnings'].append(f"Unmapped procedures: {list(unmapped_procedures)}")
            
            validation_result['mapping_stats']['procedures'] = {
                'total_unique': df['procedure_name'].nunique(),
                'mapped': df[df['procedure_name'].isin(mappings['procedures'].keys())]['procedure_name'].nunique(),
                'unmapped': len(unmapped_procedures)
            }
        
        return validation_result