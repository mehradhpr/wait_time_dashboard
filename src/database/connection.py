
# Handles database connections and connection pooling

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_batch
import logging
from contextlib import contextmanager
from typing import Dict, Optional, Generator, Any
import os

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manages PostgreSQL database connections with connection pooling"""
    
    def __init__(self, connection_params: Dict[str, Any], pool_size: int = 5):
        self.connection_params = connection_params
        self.pool_size = pool_size
        self.connection_pool = None
        self._single_connection = None
        
    def initialize_pool(self):
        """Initialize connection pool for high-concurrency applications"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, self.pool_size,
                **self.connection_params
            )
            logger.info(f"Database connection pool initialized with {self.pool_size} connections")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    def connect(self):
        """Establish single database connection for simple applications"""
        try:
            self._single_connection = psycopg2.connect(**self.connection_params)
            self._single_connection.autocommit = False
            logger.info("Single database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close single connection or all pool connections"""
        if self._single_connection:
            self._single_connection.close()
            self._single_connection = None
            logger.info("Database connection closed")
        
        if self.connection_pool:
            self.connection_pool.closeall()
            self.connection_pool = None
            logger.info("Connection pool closed")
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager for getting database connections"""
        connection = None
        try:
            if self.connection_pool:
                connection = self.connection_pool.getconn()
            else:
                connection = self._single_connection
                
            if not connection:
                raise Exception("No database connection available")
                
            yield connection
            
        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if self.connection_pool and connection:
                self.connection_pool.putconn(connection)
    
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch: bool = True):
        """Execute a single query with optional parameters"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch and cursor.description:
                    return cursor.fetchall()
                conn.commit()
                return cursor.rowcount if not fetch else None
    
    def execute_batch(self, query: str, data_list: list, page_size: int = 1000):
        """Execute batch operations for better performance"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, query, data_list, page_size=page_size)
                conn.commit()
                return cursor.rowcount
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT 1')
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

class DatabaseConfig:
    """Database configuration helper"""
    
    @staticmethod
    def from_env() -> Dict[str, Any]:
        """Load database configuration from environment variables"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'healthcare_analytics'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': int(os.getenv('DB_PORT', 5432))
        }
    
    @staticmethod
    def create_connection(use_pool: bool = False) -> DatabaseConnection:
        """Create a database connection instance"""
        config = DatabaseConfig.from_env()
        db_conn = DatabaseConnection(config)
        
        if use_pool:
            db_conn.initialize_pool()
        else:
            db_conn.connect()
            
        return db_conn

def get_database_connection(use_pool: bool = False) -> DatabaseConnection:
    """Factory function for creating database connections"""
    return DatabaseConfig.create_connection(use_pool)