"""
Application Configuration Settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = BASE_DIR / 'logs'

# Database configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'healthcare_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# Application configuration
APP_CONFIG = {
    'debug': os.getenv('FLASK_ENV', 'development') == 'development',
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'dashboard_host': os.getenv('DASHBOARD_HOST', '0.0.0.0'),
    'dashboard_port': int(os.getenv('DASHBOARD_PORT', 8050)),
}

# Data configuration
DATA_CONFIG = {
    'raw_data_path': DATA_DIR / 'raw',
    'processed_data_path': DATA_DIR / 'processed',
    'exports_path': DATA_DIR / 'exports',
    'source_file': 'wait_times_data.xlsx',
    'sheet_name': 'Wait times 2008 to 2023',
}

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': APP_CONFIG['log_level'],
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': LOGS_DIR / 'application.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': APP_CONFIG['log_level'],
            'propagate': False
        }
    }
}

# ===================================

# src/config/database.py
"""
Database Configuration and Connection Management
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import logging
from .settings import DATABASE_CONFIG

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database connection manager with connection pooling"""
    
    def __init__(self, config=None):
        self.config = config or DATABASE_CONFIG
        self._connection = None
    
    def get_connection(self):
        """Get database connection"""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self.config)
        return self._connection
    
    @contextmanager
    def get_cursor(self, dict_cursor=True):
        """Context manager for database cursor"""
        conn = self.get_connection()
        cursor_factory = RealDictCursor if dict_cursor else None
        
        try:
            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                yield cursor
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("Database connection closed")

# Global database manager instance
db_manager = DatabaseManager()

# ===================================

# src/utils/logging_config.py
"""
Logging Configuration Utilities
"""

import logging.config
import os
from pathlib import Path
from .settings import LOGGING_CONFIG, LOGS_DIR

def setup_logging():
    """Setup application logging"""
    # Ensure logs directory exists
    LOGS_DIR.mkdir(exist_ok=True)
    
    # Configure logging
    logging.config.dictConfig(LOGGING_CONFIG)
    
    logger = logging.getLogger(__name__)
    logger.info("Logging configured successfully")

def get_logger(name):
    """Get logger with specified name"""
    return logging.getLogger(name)

# ===================================

# src/utils/helpers.py
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