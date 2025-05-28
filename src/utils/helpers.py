"""
General Utility Functions
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize column names"""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def safe_numeric_conversion(value, default=None):
    """Safely convert value to numeric"""
    try:
        return pd.to_numeric(value)
    except (ValueError, TypeError):
        return default

def calculate_percentage_change(old_value, new_value):
    """Calculate percentage change between two values"""
    if old_value == 0:
        return float('inf') if new_value > 0 else 0
    return ((new_value - old_value) / old_value) * 100

def format_number(value, decimal_places=1):
    """Format number with specified decimal places"""
    if pd.isna(value):
        return 'N/A'
    return f"{value:.{decimal_places}f}"

def get_trend_description(slope, r_squared):
    """Get human-readable trend description"""
    if r_squared < 0.3:
        return "No clear trend"
    elif abs(slope) < 0.5:
        return "Stable"
    elif slope > 0:
        return "Increasing" if r_squared > 0.6 else "Slightly increasing"
    else:
        return "Decreasing" if r_squared > 0.6 else "Slightly decreasing"

def validate_data_completeness(df: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
    """Validate data completeness"""
    validation_results = {
        'is_valid': True,
        'missing_columns': [],
        'empty_columns': [],
        'completeness_scores': {}
    }
    
    # Check for missing columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        validation_results['is_valid'] = False
        validation_results['missing_columns'] = missing_cols
    
    # Check completeness for existing columns
    for col in required_columns:
        if col in df.columns:
            completeness = (df[col].notna().sum() / len(df)) * 100
            validation_results['completeness_scores'][col] = completeness
            
            if completeness == 0:
                validation_results['empty_columns'].append(col)
                validation_results['is_valid'] = False
    
    return validation_results