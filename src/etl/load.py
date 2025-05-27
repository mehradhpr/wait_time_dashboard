"""
ETL Load Module
Description: Data loading utilities split from ETL pipeline
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

def get_lookup_mappings(db_connection) -> Dict[str, Dict]:
    """Get ID mappings for dimension tables"""
    logger.info("Loading lookup table mappings")
    
    mappings = {}
    
    # Province mappings
    province_query = "SELECT province_id, province_name FROM dim_provinces"
    province_results = db_connection.execute_query(province_query)
    mappings['provinces'] = {row['province_name']: row['province_id'] for row in province_results}
    
    # Procedure mappings
    procedure_query = "SELECT procedure_id, procedure_name FROM dim_procedures"
    procedure_results = db_connection.execute_query(procedure_query)
    mappings['procedures'] = {row['procedure_name']: row['procedure_id'] for row in procedure_results}
    
    # Metric mappings
    metric_query = "SELECT metric_id, metric_name FROM dim_metrics"
    metric_results = db_connection.execute_query(metric_query)
    mappings['metrics'] = {row['metric_name']: row['metric_id'] for row in metric_results}
    
    # Reporting level mappings
    level_query = "SELECT level_id, level_name FROM dim_reporting_levels"
    level_results = db_connection.execute_query(level_query)
    mappings['levels'] = {row['level_name']: row['level_id'] for row in level_results}
    
    return mappings

def prepare_fact_data(df: pd.DataFrame, mappings: Dict[str, Dict]) -> Tuple[List[Tuple], List[str]]:
    """Prepare data for loading into fact table"""
    logger.info("Preparing fact table data")
    
    insert_data = []
    failed_records = []
    
    for idx, row in df.iterrows():
        try:
            # Map dimension IDs
            province_id = mappings['provinces'].get(row['province_name'])
            procedure_id = mappings['procedures'].get(row['procedure_name'])
            metric_id = mappings['metrics'].get(row['metric_name'])
            level_id = mappings['levels'].get(row['reporting_level'])
            
            # Skip records with missing dimension mappings
            if not all([province_id, procedure_id, metric_id, level_id]):
                failed_records.append(f"Row {idx}: Missing dimension mapping")
                continue
            
            # Prepare insert tuple
            insert_tuple = (
                province_id,
                procedure_id,
                metric_id,
                level_id,
                int(row['data_year']) if pd.notna(row['data_year']) else None,
                float(row['indicator_result']) if pd.notna(row['indicator_result']) else None,
                False,  # is_estimate
                row['data_quality_flag'],
                row.get('region_name', 'n/a')
            )
            
            insert_data.append(insert_tuple)
            
        except Exception as e:
            failed_records.append(f"Row {idx}: {str(e)}")
            continue
    
    logger.info(f"Prepared {len(insert_data)} records for loading")
    return insert_data, failed_records

def load_data(db_connection, insert_data: List[Tuple], load_id: str) -> Dict[str, int]:
    """Load transformed data into database"""
    logger.info("Starting data load process")
    
    stats = {
        'records_inserted': 0,
        'records_failed': 0
    }
    
    try:
        # Start audit record
        start_load_audit(db_connection, load_id, len(insert_data))
        
        if insert_data:
            insert_query = """
            INSERT INTO fact_wait_times 
            (province_id, procedure_id, metric_id, reporting_level_id, 
             data_year, indicator_result, is_estimate, data_quality_flag, region_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            db_connection.execute_batch(insert_query, insert_data)
            db_connection.connection.commit()
            
            stats['records_inserted'] = len(insert_data)
            logger.info(f"Successfully inserted {len(insert_data)} records")
        
        # Complete audit record
        complete_load_audit(db_connection, load_id, stats['records_inserted'], stats['records_failed'], 'completed')
        
        return stats
        
    except Exception as e:
        db_connection.connection.rollback()
        complete_load_audit(db_connection, load_id, 0, len(insert_data), 'failed', str(e))
        logger.error(f"Data load failed: {e}")
        raise

def start_load_audit(db_connection, load_id: str, record_count: int):
    """Start load audit record"""
    audit_query = """
    INSERT INTO audit_data_loads 
    (load_id, source_file, records_processed, load_status)
    VALUES (%s, %s, %s, %s)
    """
    
    db_connection.execute_query(
        audit_query, 
        (load_id, 'wait_times_data.xlsx', record_count, 'in_progress')
    )
    db_connection.connection.commit()

def complete_load_audit(db_connection, load_id: str, records_inserted: int, 
                       records_failed: int, status: str, error_message: str = None):
    """Complete load audit record"""
    audit_query = """
    UPDATE audit_data_loads 
    SET load_status = %s,
        records_inserted = %s,
        records_failed = %s,
        error_message = %s,
        load_duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - load_timestamp))
    WHERE load_id = %s
    """
    
    db_connection.execute_query(
        audit_query,
        (status, records_inserted, records_failed, error_message, load_id)
    )
    db_connection.connection.commit()