"""
Database Connection Management
"""

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import logging
from typing import Dict, List, Optional, Tuple, Any
import os

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
            
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Execute a single query"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchall()
            return []
            
    def execute_batch(self, query: str, data: List[Tuple]):
        """Execute batch insert/update operations"""
        with self.connection.cursor() as cursor:
            execute_batch(cursor, query, data, page_size=1000)

def get_db_connection():
    """Factory function for database connections"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'healthcare_analytics'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'your_password'),
        port=os.getenv('DB_PORT', 5432)
    )