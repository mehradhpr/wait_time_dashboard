"""
Database Setup Script
Sets up the complete database schema, reference data, and stored procedures
"""

import sys
import os
from pathlib import Path
import argparse
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from config.settings import DATABASE_CONFIG
from utils.logging_config import setup_logging

def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    db_params = DATABASE_CONFIG.copy()
    db_name = db_params.pop('database')
    
    conn = psycopg2.connect(**db_params, database='postgres')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if not cursor.fetchone():
            cursor.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database {db_name} created successfully")
        else:
            print(f"Database {db_name} already exists")
    
    conn.close()

def execute_sql_file(file_path):
    """Execute SQL file against the database"""
    if not os.path.exists(file_path):
        print(f"Warning: SQL file not found: {file_path}")
        return False
    
    conn = psycopg2.connect(**DATABASE_CONFIG)
    try:
        with open(file_path, 'r') as file:
            sql_content = file.read()
        
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
            conn.commit()
        
        print(f"Successfully executed: {file_path}")
        return True
    except Exception as e:
        print(f"Error executing {file_path}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Setup Healthcare Analytics Database')
    parser.add_argument('--recreate', action='store_true', help='Drop and recreate database')
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Create database
        create_database_if_not_exists()
        
        # Execute SQL files in order
        sql_files = [
            'database/schema/01_create_tables.sql',
            'database/schema/02_reference_data.sql', 
            'database/stored_procedures/sp_wait_time_trends.sql',
            'database/views/analytical_views.sql'
        ]
        
        success_count = 0
        for sql_file in sql_files:
            if execute_sql_file(sql_file):
                success_count += 1
        
        print(f"Database setup completed. {success_count}/{len(sql_files)} files executed successfully.")
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()