"""
Report Generation Script
Generates various analytical reports from the healthcare wait times data
"""

import sys
import os
from pathlib import Path
import argparse
import logging
from datetime import datetime
import pandas as pd

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from config.settings import DATABASE_CONFIG, DATA_CONFIG
from database.connection import get_db_connection
from analytics.wait_time_analyzer import WaitTimeAnalyzer
from utils.logging_config import setup_logging

def generate_provincial_summary_report():
    """Generate provincial performance summary report"""
    conn = get_db_connection()
    analyzer = WaitTimeAnalyzer(conn)
    
    try:
        # Get latest data for all provinces
        df = analyzer.get_wait_time_data(start_year=2022, end_year=2023)
        
        if df.empty:
            print("No data available for report generation")
            return
        
        # Calculate summary statistics by province
        provincial_summary = df.groupby('province_name').agg({
            'wait_time_value': ['mean', 'median', 'count'],
            'procedure_name': 'nunique'
        }).round(1)
        
        # Flatten column names
        provincial_summary.columns = ['avg_wait_time', 'median_wait_time', 'total_records', 'procedures_count']
        provincial_summary = provincial_summary.reset_index()
        
        # Export to CSV
        output_path = DATA_CONFIG['exports_path'] / f"provincial_summary_{datetime.now().strftime('%Y%m%d')}.csv"
        provincial_summary.to_csv(output_path, index=False)
        
        print(f"Provincial summary report saved to: {output_path}")
        print(f"Report includes {len(provincial_summary)} provinces")
        
        return output_path
        
    finally:
        conn.close()

def generate_trend_analysis_report():
    """Generate trend analysis report"""
    conn = get_db_connection()
    analyzer = WaitTimeAnalyzer(conn)
    
    try:
        # Get multi-year data for trend analysis
        df = analyzer.get_wait_time_data(start_year=2018, end_year=2023)
        
        if df.empty:
            print("No data available for trend analysis")
            return
        
        # Calculate trends
        trends = analyzer.calculate_trend_analysis(df)
        
        if not trends:
            print("No trends could be calculated")
            return
        
        # Convert to DataFrame for export
        trend_records = []
        for key, trend_data in trends.items():
            trend_records.append({
                'province': trend_data['province'],
                'procedure': trend_data['procedure'],
                'trend_category': trend_data['trend_category'],
                'percent_change': trend_data['percent_change'],
                'r_squared': trend_data['r_squared'],
                'years_of_data': trend_data['years_of_data'],
                'average_wait': trend_data['average_wait']
            })
        
        trend_df = pd.DataFrame(trend_records)
        
        # Export to CSV
        output_path = DATA_CONFIG['exports_path'] / f"trend_analysis_{datetime.now().strftime('%Y%m%d')}.csv"
        trend_df.to_csv(output_path, index=False)
        
        print(f"Trend analysis report saved to: {output_path}")
        print(f"Analyzed trends for {len(trend_df)} province-procedure combinations")
        
        return output_path
        
    finally:
        conn.close()

def generate_benchmark_compliance_report():
    """Generate benchmark compliance report"""
    conn = get_db_connection()
    analyzer = WaitTimeAnalyzer(conn)
    
    try:
        # Get benchmark analysis for latest year
        benchmark_data = analyzer.benchmark_analysis(None, 2023)
        
        if 'error' in benchmark_data:
            print(f"Error generating benchmark report: {benchmark_data['error']}")
            return
        
        # Convert to DataFrame
        benchmark_df = pd.DataFrame(benchmark_data['by_procedure'])
        
        if benchmark_df.empty:
            print("No benchmark data available")
            return
        
        # Export to CSV
        output_path = DATA_CONFIG['exports_path'] / f"benchmark_compliance_{datetime.now().strftime('%Y%m%d')}.csv"
        benchmark_df.to_csv(output_path, index=False)
        
        print(f"Benchmark compliance report saved to: {output_path}")
        print(f"Report includes {len(benchmark_df)} procedure assessments")
        
        # Print summary
        summary = benchmark_data['summary']
        print(f"Average compliance: {summary['avg_compliance']:.1f}%")
        print(f"Procedures above 90%: {summary['procedures_above_90pct']}")
        
        return output_path
        
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Generate Healthcare Analytics Reports')
    parser.add_argument('--type', choices=['provincial', 'trends', 'benchmark', 'all'], 
                       default='all', help='Type of report to generate')
    
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Ensure exports directory exists
    DATA_CONFIG['exports_path'].mkdir(parents=True, exist_ok=True)
    
    print(f"Starting report generation: {args.type}")
    
    try:
        if args.type in ['provincial', 'all']:
            generate_provincial_summary_report()
        
        if args.type in ['trends', 'all']:
            generate_trend_analysis_report()
        
        if args.type in ['benchmark', 'all']:
            generate_benchmark_compliance_report()
        
        print("Report generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()