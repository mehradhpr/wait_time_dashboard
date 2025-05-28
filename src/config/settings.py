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
            'filename': str(LOGS_DIR / 'application.log'),
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