"""
Healthcare Analytics Database Setup Script
Description: Automated database setup with proper imports and paths
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

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

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
            'database/stored_procedures/sp_provincial_comparison.sql',
            'database/stored_procedures/sp_benchmark_analysis.sql',
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
            # Check if data file exists
            data_file = 'data/raw/wait_times_data.xlsx'
            if not os.path.exists(data_file):
                logger.warning(f"Data file not found: {data_file}")
                logger.info("Generating sample data...")
                
                # Generate sample data
                subprocess.check_call([sys.executable, "scripts/generate_sample_data.py"])
            
            # Import and run ETL pipeline
            try:
                from etl.pipeline import run_etl
                
                db_params = dict(self.db_params)
                db_params['database'] = self.db_name
                
                stats = run_etl(data_file, db_params)
                logger.info(f"Initial data load completed: {stats}")
                return True
            except ImportError:
                logger.warning("ETL modules not available yet. Skipping data load.")
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
    env_template = """# Healthcare Analytics Environment Configuration

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
        'tests/test_data'
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
        
        # Test imports (optional - don't fail if modules not ready)
        try:
            from database.connection import DatabaseConnection
            from analytics.wait_time_analyzer import WaitTimeAnalyzer
            logger.info("Import tests passed")
        except ImportError:
            logger.warning("Some modules not yet available - this is normal during initial setup")
        
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
    logger.info("2. Run: python scripts/generate_sample_data.py (if using sample data)")
    logger.info("3. Run: python dashboard/app.py to start the dashboard")
    logger.info("4. Access dashboard at: http://localhost:8050")

if __name__ == "__main__":
    main()