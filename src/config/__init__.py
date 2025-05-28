"""
Configuration Module
Contains application settings and database configuration
"""

from .settings import DATABASE_CONFIG, APP_CONFIG, DATA_CONFIG
from .database import DatabaseManager, db_manager

__all__ = [
    'DATABASE_CONFIG',
    'APP_CONFIG', 
    'DATA_CONFIG',
    'DatabaseManager',
    'db_manager'
]