# ================================================================
# HEALTHCARE ANALYTICS - DATA QUALITY CHECKER
# ================================================================
# Purpose: Comprehensive data quality validation and monitoring

import pyodbc
import pandas as pd
import numpy as np
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Any
from config import get_database_config, get_data_config, get_etl_config, get_file_config


class DataQualityChecker:
    """Comprehensive data quality checking and validation"""
    
    def __init__(self, connection=None):
        self.connection = connection
        self.db_config = get_database_config()
        self.data_config = get_data_config()
        self.etl_config = get_etl_config()
        self.file_config = get_file_config()
        
        # Setup logging
        self.setup_logging()
        
        # Quality check results
        self.quality_results = {}
        self.issues_found = []
        
    def setup_logging(self):
        """Setup logging for quality checks"""
        logging.basicConfig(
            level=getattr(logging, self.etl_config.log_level),
            format=self.etl_config.log_format,
            handlers=[
                logging.FileHandler(self.file_config.quality_log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def connect_to_database(self) -> bool:
        """Establish database connection if not provided"""
        if self.connection is None:
            try:
                self.connection = pyodbc.connect(self.db_config.get_connection_string())
                self.logger.info("Database connection established for quality checks")
                return True
            except Exception as e:
                self.logger.error(f"Database connection failed: {str(e)}")
                return False
        return True
        
    def disconnect_from_database(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

    def run_all_quality_checks(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks"""
        self.logger.info("Starting comprehensive data quality assessment")
        
        if not self.connect_to_database():
            return {"error": "Failed to connect to database"}
        
        # Define all quality checks
        quality_checks = [
            ("Database Schema Validation", self.check_database_schema),
            ("Referential Integrity", self.check_referential_integrity),
            ("Data Completeness", self.check_data_completeness),
            ("Business Rule Validation", self.check_business_rules),
            ("Statistical Outliers", self.check_statistical_outliers),
            ("Data Consistency", self.check_data_consistency),
            ("Temporal Data Validation", self.check_temporal_data),
            ("Province Data Validation", self.check_province_data),
            ("Procedure Data Validation", self.check_procedure_data),
            ("Performance Metrics", self.calculate_performance_metrics)
        ]
        
        # Execute all checks
        for check_name, check_function in quality_checks:
            try:
                self.logger.info(f"Running check: {check_name}")
                result = check_function()
                self.quality_results[check_name] = {
                    "status": "PASSED",
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                }
                self.logger.info(f"✓ {check_name}: PASSED")
                
            except Exception as e:
                error_msg = str(e)
                self.quality_results[check_name] = {
                    "status": "FAILED",
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat()
                }
                self.issues_found.append(f"{check_name}: {error_msg}")
                self.logger.error(f"✗ {check_name}: FAILED - {error_msg}")
        
        # Generate summary
        self.generate_quality_summary()
        
        return self.quality_results
    
    def check_database_schema(self) -> str:
        """Validate database schema and table structure"""
        cursor = self.connection.cursor()
        
        # Expected tables
        expected_tables = [
            'dim_provinces', 'dim_procedures', 'dim_metrics', 'dim_time_periods',
            'fact_wait_times', 'ref_benchmarks', 'ref_population_data'
        ]
        
        # Check if all tables exist
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
        """)
        
        existing_tables = [row[0] for row in cursor.fetchall()]
        missing_tables = set(expected_tables) - set(existing_tables)
        
        if missing_tables:
            raise Exception(f"Missing tables: {', '.join(missing_tables)}")
        
        # Check table row counts
        table_counts = {}
        for table in expected_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            table_counts[table] = count
        
        return f"All {len(expected_tables)} tables exist. Row counts: {table_counts}"
    
    def check_referential_integrity(self) -> str:
        """Check foreign key relationships and referential integrity"""
        cursor = self.connection.cursor()
        integrity_issues = []
        
        # Check fact table foreign keys
        fk_checks = [
            ("fact_wait_times -> dim_provinces", """
                SELECT COUNT(*) FROM fact_wait_times f
                LEFT JOIN dim_provinces p ON f.province_id = p.province_id
                WHERE p.province_id IS NULL
            """),
            ("fact_wait_times -> dim_procedures", """
                SELECT COUNT(*) FROM fact_wait_times f
                LEFT JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
                WHERE pr.procedure_id IS NULL
            """),
            ("fact_wait_times -> dim_metrics", """
                SELECT COUNT(*) FROM fact_wait_times f
                LEFT JOIN dim_metrics m ON f.metric_id = m.metric_id
                WHERE m.metric_id IS NULL
            """),
            ("fact_wait_times -> dim_time_periods", """
                SELECT COUNT(*) FROM fact_wait_times f
                LEFT JOIN dim_time_periods t ON f.time_id = t.time_id
                WHERE t.time_id IS NULL
            """)
        ]
        
        for check_name, query in fk_checks:
            cursor.execute(query)
            orphaned_records = cursor.fetchone()[0]
            if orphaned_records > 0:
                integrity_issues.append(f"{check_name}: {orphaned_records} orphaned records")
        
        if integrity_issues:
            raise Exception("; ".join(integrity_issues))
        
        return "All referential integrity checks passed"
    
    def check_data_completeness(self) -> Dict[str, float]:
        """Check data completeness across all dimensions"""
        cursor = self.connection.cursor()
        
        # Overall fact table completeness
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(result_value) as records_with_values,
                CAST(COUNT(result_value) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct
            FROM fact_wait_times
        """)
        
        total, with_values, completeness = cursor.fetchone()
        
        if completeness < self.data_config.min_data_completeness * 100:
            raise Exception(f"Data completeness too low: {completeness}% (minimum: {self.data_config.min_data_completeness * 100}%)")
        
        # Completeness by dimension
        dimension_completeness = {}
        
        # By province
        cursor.execute("""
            SELECT 
                p.province_name,
                COUNT(*) as total_records,
                COUNT(f.result_value) as valid_records,
                CAST(COUNT(f.result_value) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct
            FROM fact_wait_times f
            JOIN dim_provinces p ON f.province_id = p.province_id
            GROUP BY p.province_name
            ORDER BY completeness_pct ASC
        """)
        
        province_completeness = {row[0]: row[3] for row in cursor.fetchall()}
        dimension_completeness['provinces'] = province_completeness
        
        # By procedure
        cursor.execute("""
            SELECT 
                pr.procedure_name,
                COUNT(*) as total_records,
                COUNT(f.result_value) as valid_records,
                CAST(COUNT(f.result_value) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct
            FROM fact_wait_times f
            JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
            GROUP BY pr.procedure_name
            ORDER BY completeness_pct ASC
        """)
        
        procedure_completeness = {row[0]: row[3] for row in cursor.fetchall()}
        dimension_completeness['procedures'] = procedure_completeness
        
        return {
            'overall_completeness': float(completeness),
            'total_records': total,
            'valid_records': with_values,
            'dimension_completeness': dimension_completeness
        }
    
    def check_business_rules(self) -> Dict[str, int]:
        """Validate business-specific rules"""
        cursor = self.connection.cursor()
        business_rule_violations = {}
        
        # Rule 1: Wait times should be realistic (0 to 2 years)
        cursor.execute("""
            SELECT COUNT(*) FROM fact_wait_times f
            JOIN dim_metrics m ON f.metric_id = m.metric_id
            WHERE m.metric_name LIKE '%Percentile%' 
            AND (f.result_value < 0 OR f.result_value > ?)
        """, (self.data_config.max_wait_time_days,))
        
        invalid_waits = cursor.fetchone()[0]
        business_rule_violations['unrealistic_wait_times'] = invalid_waits
        
        # Rule 2: Percentages should be 0-100
        cursor.execute("""
            SELECT COUNT(*) FROM fact_wait_times f
            JOIN dim_metrics m ON f.metric_id = m.metric_id
            WHERE m.metric_name LIKE '%Benchmark%' 
            AND (f.result_value < 0 OR f.result_value > 100)
        """)
        
        invalid_percentages = cursor.fetchone()[0]
        business_rule_violations['invalid_percentages'] = invalid_percentages
        
        # Rule 3: Volume should be positive integers
        cursor.execute("""
            SELECT COUNT(*) FROM fact_wait_times f
            JOIN dim_metrics m ON f.metric_id = m.metric_id
            WHERE m.metric_name = 'Volume' 
            AND (f.result_value < 0 OR f.result_value != FLOOR(f.result_value))
        """)
        
        invalid_volumes = cursor.fetchone()[0]
        business_rule_violations['invalid_volumes'] = invalid_volumes
        
        # Rule 4: 90th percentile should be >= 50th percentile
        cursor.execute("""
            WITH percentile_comparison AS (
                SELECT 
                    f1.province_id,
                    f1.procedure_id,
                    t.fiscal_year,
                    f1.result_value as pct_50,
                    f2.result_value as pct_90
                FROM fact_wait_times f1
                JOIN fact_wait_times f2 ON f1.province_id = f2.province_id 
                    AND f1.procedure_id = f2.procedure_id 
                    AND f1.time_id = f2.time_id
                JOIN dim_metrics m1 ON f1.metric_id = m1.metric_id AND m1.metric_name = '50th Percentile'
                JOIN dim_metrics m2 ON f2.metric_id = m2.metric_id AND m2.metric_name = '90th Percentile'
                JOIN dim_time_periods t ON f1.time_id = t.time_id
                WHERE f1.result_value IS NOT NULL AND f2.result_value IS NOT NULL
            )
            SELECT COUNT(*) FROM percentile_comparison
            WHERE pct_90 < pct_50
        """)
        
        invalid_percentiles = cursor.fetchone()[0]
        business_rule_violations['invalid_percentile_relationships'] = invalid_percentiles
        
        # Check if any critical violations exist
        critical_violations = sum([
            invalid_waits, invalid_percentages, invalid_volumes, invalid_percentiles
        ])
        
        if critical_violations > 0:
            violation_details = []
            for rule, count in business_rule_violations.items():
                if count > 0:
                    violation_details.append(f"{rule}: {count} violations")
            
            if len(violation_details) > 0:
                self.logger.warning(f"Business rule violations found: {'; '.join(violation_details)}")
        
        return business_rule_violations
    
    def check_statistical_outliers(self) -> Dict[str, Any]:
        """Identify statistical outliers that may indicate data issues"""
        cursor = self.connection.cursor()
        
        # Calculate statistical outliers for wait times
        cursor.execute("""
            WITH wait_time_stats AS (
                SELECT 
                    f.procedure_id,
                    f.metric_id,
                    AVG(f.result_value) as mean_val,
                    STDEV(f.result_value) as std_val,
                    COUNT(*) as sample_size
                FROM fact_wait_times f
                JOIN dim_metrics m ON f.metric_id = m.metric_id
                WHERE f.result_value IS NOT NULL 
                AND m.metric_name LIKE '%Percentile%'
                GROUP BY f.procedure_id, f.metric_id
                HAVING COUNT(*) >= 10 AND STDEV(f.result_value) > 0
            ),
            outliers AS (
                SELECT 
                    f.*,
                    p.province_name,
                    pr.procedure_name,
                    m.metric_name,
                    t.fiscal_year,
                    s.mean_val,
                    s.std_val,
                    ABS(f.result_value - s.mean_val) / s.std_val as z_score
                FROM fact_wait_times f
                JOIN wait_time_stats s ON f.procedure_id = s.procedure_id AND f.metric_id = s.metric_id
                JOIN dim_provinces p ON f.province_id = p.province_id
                JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
                JOIN dim_metrics m ON f.metric_id = m.metric_id
                JOIN dim_time_periods t ON f.time_id = t.time_id
                WHERE f.result_value IS NOT NULL
            )
            SELECT 
                COUNT(*) as total_outliers,
                COUNT(CASE WHEN z_score > 3 THEN 1 END) as extreme_outliers,
                COUNT(CASE WHEN z_score > 2 THEN 1 END) as significant_outliers,
                AVG(z_score) as avg_z_score,
                MAX(z_score) as max_z_score
            FROM outliers
            WHERE z_score > ?
        """, (self.data_config.outlier_threshold,))
        
        outlier_stats = cursor.fetchone()
        
        # Get top outliers for detailed analysis
        cursor.execute("""
            WITH wait_time_stats AS (
                SELECT 
                    f.procedure_id,
                    f.metric_id,
                    AVG(f.result_value) as mean_val,
                    STDEV(f.result_value) as std_val
                FROM fact_wait_times f
                WHERE f.result_value IS NOT NULL 
                GROUP BY f.procedure_id, f.metric_id
                HAVING COUNT(*) >= 10 AND STDEV(f.result_value) > 0
            )
            SELECT TOP 10
                p.province_name,
                pr.procedure_name,
                m.metric_name,
                t.fiscal_year,
                f.result_value,
                s.mean_val,
                ABS(f.result_value - s.mean_val) / s.std_val as z_score
            FROM fact_wait_times f
            JOIN wait_time_stats s ON f.procedure_id = s.procedure_id AND f.metric_id = s.metric_id
            JOIN dim_provinces p ON f.province_id = p.province_id
            JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
            JOIN dim_metrics m ON f.metric_id = m.metric_id
            JOIN dim_time_periods t ON f.time_id = t.time_id
            WHERE f.result_value IS NOT NULL
            AND ABS(f.result_value - s.mean_val) / s.std_val > ?
            ORDER BY ABS(f.result_value - s.mean_val) / s.std_val DESC
        """, (self.data_config.outlier_threshold,))
        
        top_outliers = []
        for row in cursor.fetchall():
            top_outliers.append({
                'province': row[0],
                'procedure': row[1],
                'metric': row[2],
                'year': row[3],
                'value': float(row[4]),
                'mean': float(row[5]),
                'z_score': float(row[6])
            })
        
        return {
            'total_outliers': outlier_stats[0],
            'extreme_outliers': outlier_stats[1],
            'significant_outliers': outlier_stats[2],
            'avg_z_score': float(outlier_stats[3]) if outlier_stats[3] else 0,
            'max_z_score': float(outlier_stats[4]) if outlier_stats[4] else 0,
            'top_outliers': top_outliers
        }
    
    def check_data_consistency(self) -> Dict[str, Any]:
        """Check for data consistency issues"""
        cursor = self.connection.cursor()
        consistency_issues = {}
        
        # Check for duplicate records
        cursor.execute("""
            SELECT 
                province_id, procedure_id, metric_id, time_id, COUNT(*) as duplicate_count
            FROM fact_wait_times
            GROUP BY province_id, procedure_id, metric_id, time_id
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cursor.fetchall()
        consistency_issues['duplicate_records'] = len(duplicates)
        
        # Check for missing complementary data (e.g., if 50th percentile exists, 90th should too)
        cursor.execute("""
            WITH metric_pairs AS (
                SELECT DISTINCT
                    province_id, procedure_id, time_id,
                    COUNT(CASE WHEN m.metric_name = '50th Percentile' THEN 1 END) as has_50th,
                    COUNT(CASE WHEN m.metric_name = '90th Percentile' THEN 1 END) as has_90th
                FROM fact_wait_times f
                JOIN dim_metrics m ON f.metric_id = m.metric_id
                WHERE m.metric_name IN ('50th Percentile', '90th Percentile')
                AND f.result_value IS NOT NULL
                GROUP BY province_id, procedure_id, time_id
            )
            SELECT COUNT(*) FROM metric_pairs
            WHERE (has_50th = 1 AND has_90th = 0) OR (has_50th = 0 AND has_90th = 1)
        """)
        
        incomplete_pairs = cursor.fetchone()[0]
        consistency_issues['incomplete_percentile_pairs'] = incomplete_pairs
        
        # Check for province name consistency
        cursor.execute("""
            SELECT province_name, COUNT(DISTINCT province_code) as code_variations
            FROM dim_provinces
            GROUP BY province_name
            HAVING COUNT(DISTINCT province_code) > 1
        """)
        
        province_inconsistencies = cursor.fetchall()
        consistency_issues['province_name_inconsistencies'] = len(province_inconsistencies)
        
        return consistency_issues
    
    def check_temporal_data(self) -> Dict[str, Any]:
        """Validate temporal data patterns"""
        cursor = self.connection.cursor()
        temporal_issues = {}
        
        # Check for gaps in yearly data
        cursor.execute("""
            WITH year_coverage AS (
                SELECT 
                    pr.procedure_name,
                    p.province_name,
                    MIN(t.fiscal_year) as first_year,
                    MAX(t.fiscal_year) as last_year,
                    COUNT(DISTINCT t.fiscal_year) as years_reported,
                    MAX(t.fiscal_year) - MIN(t.fiscal_year) + 1 as expected_years
                FROM fact_wait_times f
                JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
                JOIN dim_provinces p ON f.province_id = p.province_id
                JOIN dim_time_periods t ON f.time_id = t.time_id
                WHERE f.result_value IS NOT NULL
                GROUP BY pr.procedure_name, p.province_name
            )
            SELECT 
                COUNT(*) as total_series,
                COUNT(CASE WHEN years_reported < expected_years THEN 1 END) as series_with_gaps,
                AVG(CAST(years_reported AS FLOAT) / expected_years) as avg_coverage_ratio
            FROM year_coverage
            WHERE expected_years > 1
        """)
        
        temporal_stats = cursor.fetchone()
        temporal_issues.update({
            'total_time_series': temporal_stats[0],
            'series_with_gaps': temporal_stats[1],
            'average_coverage_ratio': float(temporal_stats[2]) if temporal_stats[2] else 0
        })
        
        # Check for unrealistic year-over-year changes
        cursor.execute("""
            WITH yoy_changes AS (
                SELECT 
                    f1.result_value as current_value,
                    f2.result_value as previous_value,
                    ABS(f1.result_value - f2.result_value) as absolute_change,
                    ABS(f1.result_value - f2.result_value) / f2.result_value * 100 as percent_change
                FROM fact_wait_times f1
                JOIN fact_wait_times f2 ON f1.province_id = f2.province_id 
                    AND f1.procedure_id = f2.procedure_id 
                    AND f1.metric_id = f2.metric_id
                JOIN dim_time_periods t1 ON f1.time_id = t1.time_id
                JOIN dim_time_periods t2 ON f2.time_id = t2.time_id
                WHERE f1.result_value IS NOT NULL 
                AND f2.result_value IS NOT NULL
                AND f2.result_value > 0
                AND t1.fiscal_year = t2.fiscal_year + 1
            )
            SELECT 
                COUNT(*) as total_yoy_comparisons,
                COUNT(CASE WHEN percent_change > 100 THEN 1 END) as extreme_changes,
                AVG(percent_change) as avg_percent_change,
                MAX(percent_change) as max_percent_change
            FROM yoy_changes
        """)
        
        yoy_stats = cursor.fetchone()
        temporal_issues.update({
            'total_yoy_comparisons': yoy_stats[0],
            'extreme_yoy_changes': yoy_stats[1],
            'avg_yoy_change_percent': float(yoy_stats[2]) if yoy_stats[2] else 0,
            'max_yoy_change_percent': float(yoy_stats[3]) if yoy_stats[3] else 0
        })
        
        return temporal_issues
    
    def check_province_data(self) -> Dict[str, Any]:
        """Validate province-specific data"""
        cursor = self.connection.cursor()
        
        # Check if all expected provinces are present
        expected_provinces = set(self.data_config.province_data.keys())
        
        cursor.execute("SELECT province_name FROM dim_provinces")
        actual_provinces = set(row[0] for row in cursor.fetchall())
        
        missing_provinces = expected_provinces - actual_provinces
        unexpected_provinces = actual_provinces - expected_provinces
        
        # Check province data distribution
        cursor.execute("""
            SELECT 
                p.province_name,
                COUNT(*) as total_records,
                COUNT(f.result_value) as valid_records,
                COUNT(DISTINCT pr.procedure_id) as procedures_covered,
                COUNT(DISTINCT t.fiscal_year) as years_covered
            FROM dim_provinces p
            LEFT JOIN fact_wait_times f ON p.province_id = f.province_id
            LEFT JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
            LEFT JOIN dim_time_periods t ON f.time_id = t.time_id
            GROUP BY p.province_name
            ORDER BY valid_records DESC
        """)
        
        province_coverage = []
        for row in cursor.fetchall():
            province_coverage.append({
                'province': row[0],
                'total_records': row[1],
                'valid_records': row[2],
                'procedures_covered': row[3],
                'years_covered': row[4]
            })
        
        return {
            'missing_provinces': list(missing_provinces),
            'unexpected_provinces': list(unexpected_provinces),
            'province_coverage': province_coverage
        }
    
    def check_procedure_data(self) -> Dict[str, Any]:
        """Validate procedure-specific data"""
        cursor = self.connection.cursor()
        
        # Check procedure categorization
        cursor.execute("""
            SELECT 
                procedure_name,
                procedure_category,
                clinical_priority,
                COUNT(*) as usage_count
            FROM dim_procedures pr
            LEFT JOIN fact_wait_times f ON pr.procedure_id = f.procedure_id
            GROUP BY procedure_name, procedure_category, clinical_priority
            ORDER BY usage_count DESC
        """)
        
        procedure_usage = []
        for row in cursor.fetchall():
            procedure_usage.append({
                'procedure': row[0],
                'category': row[1],
                'priority': row[2],
                'usage_count': row[3]
            })
        
        # Check for procedures with no data
        unused_procedures = [p for p in procedure_usage if p['usage_count'] == 0]
        
        return {
            'total_procedures': len(procedure_usage),
            'unused_procedures': len(unused_procedures),
            'procedure_usage': procedure_usage[:10],  # Top 10
            'unused_procedure_list': [p['procedure'] for p in unused_procedures]
        }
    
    def calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate overall system performance metrics"""
        cursor = self.connection.cursor()
        
        # Overall system metrics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT f.province_id) as provinces_with_data,
                COUNT(DISTINCT f.procedure_id) as procedures_with_data,
                COUNT(DISTINCT t.fiscal_year) as years_with_data,
                COUNT(*) as total_fact_records,
                COUNT(f.result_value) as valid_fact_records,
                MIN(t.fiscal_year) as earliest_year,
                MAX(t.fiscal_year) as latest_year
            FROM fact_wait_times f
            JOIN dim_time_periods t ON f.time_id = t.time_id
        """)
        
        system_metrics = cursor.fetchone()
        
        # Calculate benchmark compliance rate
        cursor.execute("""
            SELECT 
                COUNT(*) as total_benchmark_records,
                COUNT(CASE WHEN is_benchmark_met = 1 THEN 1 END) as benchmarks_met,
                CAST(COUNT(CASE WHEN is_benchmark_met = 1 THEN 1 END) * 100.0 / 
                     COUNT(*) AS DECIMAL(5,2)) as compliance_rate
            FROM fact_wait_times
            WHERE is_benchmark_met IS NOT NULL
        """)
        
        benchmark_stats = cursor.fetchone()
        
        return {
            'provinces_with_data': system_metrics[0],
            'procedures_with_data': system_metrics[1],
            'years_with_data': system_metrics[2],
            'total_fact_records': system_metrics[3],
            'valid_fact_records': system_metrics[4],
            'data_span_years': f"{system_metrics[5]}-{system_metrics[6]}",
            'benchmark_records': benchmark_stats[0] if benchmark_stats[0] else 0,
            'benchmarks_met': benchmark_stats[1] if benchmark_stats[1] else 0,
            'overall_compliance_rate': float(benchmark_stats[2]) if benchmark_stats[2] else 0
        }
    
    def generate_quality_summary(self):
        """Generate comprehensive quality assessment summary"""
        self.logger.info("=" * 70)
        self.logger.info("DATA QUALITY ASSESSMENT SUMMARY")
        self.logger.info("=" * 70)
        
        # Count passed and failed checks
        passed_checks = sum(1 for result in self.quality_results.values() if result['status'] == 'PASSED')
        failed_checks = sum(1 for result in self.quality_results.values() if result['status'] == 'FAILED')
        total_checks = len(self.quality_results)
        
        self.logger.info(f"Total Quality Checks: {total_checks}")
        self.logger.info(f"Passed: {passed_checks}")
        self.logger.info(f"Failed: {failed_checks}")
        self.logger.info(f"Success Rate: {(passed_checks/total_checks*100):.1f}%")
        
        # Log critical issues
        if self.issues_found:
            self.logger.info(f"\nCritical Issues Found ({len(self.issues_found)}):")
            for issue in self.issues_found:
                self.logger.warning(f"  - {issue}")
        else:
            self.logger.info("\n✓ No critical data quality issues found")
        
        # Performance metrics summary
        if 'Performance Metrics' in self.quality_results:
            perf_metrics = self.quality_results['Performance Metrics']['result']
            self.logger.info(f"\nSystem Performance Summary:")
            self.logger.info(f"  - Data Coverage: {perf_metrics['data_span_years']}")
            self.logger.info(f"  - Valid Records: {perf_metrics['valid_fact_records']:,}")
            self.logger.info(f"  - Provinces: {perf_metrics['provinces_with_data']}")
            self.logger.info(f"  - Procedures: {perf_metrics['procedures_with_data']}")
            if perf_metrics['overall_compliance_rate'] > 0:
                self.logger.info(f"  - Benchmark Compliance: {perf_metrics['overall_compliance_rate']:.1f}%")
        
        self.logger.info("=" * 70)
    
    def export_quality_report(self, output_path: str = None) -> str:
        """Export detailed quality report to file"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(
                self.file_config.reports_dir, 
                f"data_quality_report_{timestamp}.json"
            )
        
        import json
        
        # Prepare report data
        report_data = {
            'report_metadata': {
                'generated_timestamp': datetime.now().isoformat(),
                'database': self.db_config.database,
                'server': self.db_config.server,
                'total_checks_performed': len(self.quality_results),
                'checks_passed': sum(1 for r in self.quality_results.values() if r['status'] == 'PASSED'),
                'checks_failed': sum(1 for r in self.quality_results.values() if r['status'] == 'FAILED')
            },
            'quality_check_results': self.quality_results,
            'critical_issues': self.issues_found,
            'configuration_used': {
                'min_data_completeness': self.data_config.min_data_completeness,
                'outlier_threshold': self.data_config.outlier_threshold,
                'max_wait_time_days': self.data_config.max_wait_time_days
            }
        }
        
        # Write report to file
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        self.logger.info(f"Quality report exported to: {output_path}")
        return output_path


# Convenience functions for easy usage
def run_quality_checks(connection=None, export_report=True) -> Dict[str, Any]:
    """Run all quality checks and optionally export report"""
    checker = DataQualityChecker(connection)
    results = checker.run_all_quality_checks()
    
    if export_report:
        report_path = checker.export_quality_report()
        results['report_exported_to'] = report_path
    
    return results

def quick_data_validation(connection=None) -> bool:
    """Quick validation check - returns True if data quality is acceptable"""
    checker = DataQualityChecker(connection)
    
    try:
        # Run essential checks only
        checker.connect_to_database()
        checker.check_database_schema()
        checker.check_referential_integrity()
        completeness = checker.check_data_completeness()
        
        # Check if minimum standards are met
        if completeness['overall_completeness'] >= checker.data_config.min_data_completeness * 100:
            return True
        else:
            return False
            
    except Exception as e:
        checker.logger.error(f"Quick validation failed: {str(e)}")
        return False
    finally:
        checker.disconnect_from_database()


# Example usage
if __name__ == "__main__":
    # Run comprehensive quality checks
    results = run_quality_checks(export_report=True)
    
    # Print summary
    print("\nQuality Check Summary:")
    print(f"Total Checks: {len(results) - 1}")  # -1 for report_exported_to key
    passed = sum(1 for r in results.values() if isinstance(r, dict) and r.get('status') == 'PASSED')
    print(f"Passed: {passed}")
    print(f"Failed: {len(results) - 1 - passed}")
    
    # Quick validation example
    is_valid = quick_data_validation()
    print(f"\nData Quality Acceptable: {is_valid}")