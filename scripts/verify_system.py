"""
System Verification Script
Tests all major components to ensure they work together correctly
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

def test_imports():
    """Test that all major modules can be imported"""
    print("Testing imports...")
    
    try:
        from config.settings import DATABASE_CONFIG, APP_CONFIG, DATA_CONFIG
        from config.database import db_manager
        from utils.logging_config import setup_logging
        from utils.helpers import format_number, calculate_percentage_change
        from analytics.wait_time_analyzer import WaitTimeAnalyzer
        print("✓ All imports successful")
        return True
    except Exception as e:
        print(f"✗ Import error: {e}")
        return False

def test_database_connection():
    """Test database connectivity"""
    print("Testing database connection...")
    
    try:
        from config.database import db_manager
        
        # Try to execute a simple query
        result = db_manager.execute_query("SELECT 1 as test")
        if result and result[0]['test'] == 1:
            print("✓ Database connection successful")
            return True
        else:
            print("✗ Database query failed")
            return False
    except Exception as e:
        print(f"✗ Database connection error: {e}")
        return False

def test_reference_data():
    """Test that reference data exists"""
    print("Testing reference data...")
    
    try:
        from config.database import db_manager
        
        # Check provinces
        provinces = db_manager.execute_query("SELECT COUNT(*) as count FROM dim_provinces")
        province_count = provinces[0]['count'] if provinces else 0
        
        # Check procedures
        procedures = db_manager.execute_query("SELECT COUNT(*) as count FROM dim_procedures")
        procedure_count = procedures[0]['count'] if procedures else 0
        
        # Check metrics
        metrics = db_manager.execute_query("SELECT COUNT(*) as count FROM dim_metrics")
        metric_count = metrics[0]['count'] if metrics else 0
        
        if province_count > 0 and procedure_count > 0 and metric_count > 0:
            print(f"✓ Reference data exists: {province_count} provinces, {procedure_count} procedures, {metric_count} metrics")
            return True
        else:
            print(f"✗ Missing reference data: {province_count} provinces, {procedure_count} procedures, {metric_count} metrics")
            return False
            
    except Exception as e:
        print(f"✗ Reference data check error: {e}")
        return False

def test_analytics():
    """Test analytics functionality"""
    print("Testing analytics...")
    
    try:
        from config.database import db_manager
        from analytics.wait_time_analyzer import WaitTimeAnalyzer
        
        analyzer = WaitTimeAnalyzer(db_manager)
        
        # Test data retrieval
        df = analyzer.get_wait_time_data(start_year=2020, end_year=2023)
        
        if not df.empty:
            print(f"✓ Analytics working: Retrieved {len(df)} records")
            
            # Test a simple calculation
            avg_wait = df['wait_time_value'].mean()
            print(f"  Average wait time: {avg_wait:.1f} days")
            return True
        else:
            print("✗ No data retrieved by analytics")
            return False
            
    except Exception as e:
        print(f"✗ Analytics error: {e}")
        return False

def test_stored_procedures():
    """Test stored procedures"""
    print("Testing stored procedures...")
    
    try:
        from config.database import db_manager
        
        # Test trend analysis procedure
        result = db_manager.execute_query("SELECT * FROM sp_wait_time_trends() LIMIT 5")
        
        if result and len(result) > 0:
            print(f"✓ Stored procedures working: Got {len(result)} trend records")
            return True
        else:
            print("✗ Stored procedures returned no data")
            return False
            
    except Exception as e:
        print(f"✗ Stored procedure error: {e}")
        return False

def main():
    """Run all verification tests"""
    print("Healthcare Analytics System Verification")
    print("=" * 50)
    
    tests = [
        ("Import Tests", test_imports),
        ("Database Connection", test_database_connection),
        ("Reference Data", test_reference_data),
        ("Analytics Module", test_analytics),
        ("Stored Procedures", test_stored_procedures)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"Verification Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All systems working correctly!")
        return True
    else:
        print("⚠️  Some issues found. Check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)