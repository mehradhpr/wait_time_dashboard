#!/usr/bin/env python3
"""
Healthcare Analytics Database Setup Script
Author: Data Analytics Team
Created: 2025-05-27
Description: Automated database setup and initial data load
"""

import os
import sys
import subprocess
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from pathlib import Path
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseSetup:
    """Database setup and initialization"""
    
    def __init__(self):
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', 5432)
        }
        self.db_name = os.getenv('DB_NAME', 'healthcare_analytics')
        
    def create_database(self):
        """Create the database if it doesn't exist"""
        logger.info(f"Creating database: {self.db_name}")
        
        try:
            # Connect to default postgres database
            conn = psycopg2.connect(**self.db_params, database='postgres')
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn.cursor() as cursor:
                # Check if database exists
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.db_name,)
                )
                
                if not cursor.fetchone():
                    cursor.execute(f'CREATE DATABASE "{self.db_name}"')
                    logger.info(f"Database {self.db_name} created successfully")
                else:
                    logger.info(f"Database {self.db_name} already exists")
            
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def execute_sql_file(self, file_path):
        """Execute SQL file"""
        logger.info(f"Executing SQL file: {file_path}")
        
        try:
            conn = psycopg2.connect(**self.db_params, database=self.db_name)
            
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                conn.commit()
            
            conn.close()
            logger.info(f"Successfully executed {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing {file_path}: {e}")
            return False
    
    def setup_schema(self):
        """Setup database schema"""
        sql_files = [
            'database/schema/01_create_tables.sql',
            'database/schema/02_reference_data.sql',
            'database/stored_procedures/sp_wait_time_trends.sql',
            'database/views/analytical_views.sql'
        ]
        
        for sql_file in sql_files:
            if os.path.exists(sql_file):
                if not self.execute_sql_file(sql_file):
                    return False
            else:
                logger.warning(f"SQL file not found: {sql_file}")
        
        return True
    
    def load_initial_data(self):
        """Load initial data using ETL pipeline"""
        logger.info("Loading initial data...")
        
        try:
            # Import and run ETL pipeline
            from src.etl.pipeline import WaitTimeETL
            from src.database.connection import DatabaseConnection
            
            db_conn = DatabaseConnection(dict(self.db_params, database=self.db_name))
            db_conn.connect()
            
            etl_processor = WaitTimeETL(db_conn)
            etl_processor.run_etl_pipeline('data/raw/wait_times_data.xlsx')
            
            db_conn.disconnect()
            logger.info("Initial data load completed")
            return True
            
        except Exception as e:
            logger.error(f"Error loading initial data: {e}")
            return False

def install_requirements():
    """Install Python requirements"""
    logger.info("Installing Python requirements...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
        ])
        logger.info("Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error installing requirements: {e}")
        return False

def create_environment_file():
    """Create .env file from template"""
    env_template = """
# Healthcare Analytics Environment Configuration

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=healthcare_analytics
DB_USER=postgres
DB_PASSWORD=your_password_here

# Application Configuration
FLASK_ENV=development
DASH_DEBUG=True
LOG_LEVEL=INFO

# Dashboard Configuration
DASHBOARD_HOST=0.0.0.0
DASHBOARD_PORT=8050

# Security (generate secure keys for production)
SECRET_KEY=dev_secret_key_change_in_production
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_template.strip())
        logger.info("Created .env file - please update with your configuration")
    else:
        logger.info(".env file already exists")

def setup_directory_structure():
    """Create necessary directories"""
    directories = [
        'data/raw',
        'data/processed', 
        'data/exports',
        'logs',
        'database/migrations',
        'tests/test_data',
        'dashboard/assets',
        'dashboard/static/images'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    logger.info("Directory structure created")

def run_tests():
    """Run basic tests to verify setup"""
    logger.info("Running setup verification tests...")
    
    try:
        # Test database connection
        db_setup = DatabaseSetup()
        conn = psycopg2.connect(**db_setup.db_params, database=db_setup.db_name)
        
        with conn.cursor() as cursor:
            # Test basic queries
            cursor.execute("SELECT COUNT(*) FROM dim_provinces")
            province_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_procedures") 
            procedure_count = cursor.fetchone()[0]
            
            logger.info(f"Database test passed: {province_count} provinces, {procedure_count} procedures")
        
        conn.close()
        
        # Test imports
        from src.analytics.wait_time_analyzer import WaitTimeAnalyzer
        from src.etl.pipeline import WaitTimeETL
        logger.info("Import tests passed")
        
        return True
        
    except Exception as e:
        logger.error(f"Setup verification failed: {e}")
        return False

def main():
    """Main setup function"""
    parser = argparse.ArgumentParser(description='Healthcare Analytics Setup')
    parser.add_argument('--skip-data', action='store_true', 
                       help='Skip initial data loading')
    parser.add_argument('--test-only', action='store_true',
                       help='Only run verification tests')
    
    args = parser.parse_args()
    
    logger.info("Starting Healthcare Analytics setup...")
    
    if args.test_only:
        success = run_tests()
        sys.exit(0 if success else 1)
    
    # Setup steps
    steps = [
        ("Creating directory structure", setup_directory_structure),
        ("Creating environment file", create_environment_file),
        ("Installing requirements", install_requirements),
        ("Creating database", lambda: DatabaseSetup().create_database()),
        ("Setting up schema", lambda: DatabaseSetup().setup_schema()),
    ]
    
    if not args.skip_data:
        steps.append(("Loading initial data", lambda: DatabaseSetup().load_initial_data()))
    
    steps.append(("Running verification tests", run_tests))
    
    # Execute setup steps
    for step_name, step_function in steps:
        logger.info(f"Step: {step_name}")
        try:
            success = step_function()
            if not success:
                logger.error(f"Setup failed at step: {step_name}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Setup failed at step {step_name}: {e}")
            sys.exit(1)
    
    logger.info("Healthcare Analytics setup completed successfully!")
    logger.info("Next steps:")
    logger.info("1. Update .env file with your database credentials")
    logger.info("2. Place your wait_times_data.xlsx file in data/raw/")
    logger.info("3. Run: python dashboard/app.py to start the dashboard")
    logger.info("4. Access dashboard at: http://localhost:8050")

if __name__ == "__main__":
    main()


# Additional utility scripts

class DataMaintenance:
    """Data maintenance and refresh utilities"""
    
    def __init__(self):
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'healthcare_analytics'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', 5432)
        }
    
    def refresh_materialized_views(self):
        """Refresh all materialized views"""
        logger.info("Refreshing materialized views...")
        
        try:
            conn = psycopg2.connect(**self.db_params)
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT refresh_materialized_views()")
                result = cursor.fetchone()[0]
                conn.commit()
            
            conn.close()
            logger.info(f"Materialized views refreshed: {result}")
            return True
            
        except Exception as e:
            logger.error(f"Error refreshing materialized views: {e}")
            return False
    
    def cleanup_old_logs(self, days_to_keep=30):
        """Clean up old log entries"""
        logger.info(f"Cleaning up logs older than {days_to_keep} days...")
        
        try:
            conn = psycopg2.connect(**self.db_params)
            
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM audit_data_loads 
                    WHERE load_timestamp < NOW() - INTERVAL '%s days'
                """, (days_to_keep,))
                
                deleted_count = cursor.rowcount
                conn.commit()
            
            conn.close()
            logger.info(f"Cleaned up {deleted_count} old log entries")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning up logs: {e}")
            return False
    
    def backup_database(self, backup_path=None):
        """Create database backup"""
        if not backup_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"backups/healthcare_analytics_{timestamp}.sql"
        
        logger.info(f"Creating database backup: {backup_path}")
        
        try:
            # Create backups directory
            Path("backups").mkdir(exist_ok=True)
            
            # Use pg_dump
            cmd = [
                'pg_dump',
                '-h', self.db_params['host'],
                '-p', str(self.db_params['port']),
                '-U', self.db_params['user'],
                '-d', self.db_params['database'],
                '-f', backup_path,
                '--verbose'
            ]
            
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_params['password']
            
            subprocess.run(cmd, env=env, check=True)
            logger.info(f"Database backup created successfully: {backup_path}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating backup: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during backup: {e}")
            return False

def run_maintenance():
    """Run routine maintenance tasks"""
    maintenance = DataMaintenance()
    
    logger.info("Starting routine maintenance...")
    
    # Refresh materialized views
    maintenance.refresh_materialized_views()
    
    # Clean up old logs
    maintenance.cleanup_old_logs()
    
    # Create backup (weekly)
    from datetime import datetime
    if datetime.now().weekday() == 0:  # Monday
        maintenance.backup_database()
    
    logger.info("Maintenance completed")

# Deployment script
def deploy_to_production():
    """Deploy application to production"""
    logger.info("Starting production deployment...")
    
    deployment_steps = [
        "Backing up current database",
        "Running database migrations", 
        "Refreshing materialized views",
        "Installing/updating dependencies",
        "Restarting application services"
    ]
    
    try:
        # Create backup before deployment
        maintenance = DataMaintenance()
        maintenance.backup_database("backups/pre_deployment_backup.sql")
        
        # Update dependencies
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        
        # Refresh views
        maintenance.refresh_materialized_views()
        
        # Create deployment log
        with open("logs/deployment.log", "a") as f:
            f.write(f"{datetime.now().isoformat()}: Deployment completed successfully\n")
        
        logger.info("Production deployment completed successfully")
        
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        sys.exit(1)

# Command line interface for maintenance tasks
if __name__ == "__main__" and len(sys.argv) > 1:
    command = sys.argv[1]
    
    if command == "maintenance":
        run_maintenance()
    elif command == "backup":
        DataMaintenance().backup_database()
    elif command == "refresh-views":
        DataMaintenance().refresh_materialized_views()
    elif command == "deploy":
        deploy_to_production()
    elif command == "setup":
        main()
    else:
        print("Available commands: setup, maintenance, backup, refresh-views, deploy")
        sys.exit(1)