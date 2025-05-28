"""
Database Query Templates and Constants
"""

# Basic dimension queries
PROVINCE_QUERIES = {
    'get_all': "SELECT * FROM dim_provinces ORDER BY province_name",
    'get_by_name': "SELECT * FROM dim_provinces WHERE province_name ILIKE %s",
    'get_by_region': "SELECT * FROM dim_provinces WHERE region = %s ORDER BY province_name"
}

PROCEDURE_QUERIES = {
    'get_all': "SELECT * FROM dim_procedures ORDER BY procedure_name",
    'get_by_category': "SELECT * FROM dim_procedures WHERE procedure_category = %s ORDER BY procedure_name",
    'get_surgeries': "SELECT * FROM dim_procedures WHERE is_surgery = TRUE ORDER BY procedure_name"
}

METRIC_QUERIES = {
    'get_all': "SELECT * FROM dim_metrics ORDER BY metric_name",
    'get_by_type': "SELECT * FROM dim_metrics WHERE metric_type = %s ORDER BY metric_name"
}

# Analytical queries
ANALYTICAL_QUERIES = {
    'wait_times_summary': """
        SELECT 
            dp.province_name,
            dpr.procedure_name,
            dm.metric_name,
            wt.data_year,
            wt.indicator_result,
            wt.data_quality_flag
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        WHERE wt.indicator_result IS NOT NULL
        ORDER BY dp.province_name, dpr.procedure_name, wt.data_year
    """,
    
    'latest_year_data': """
        SELECT 
            dp.province_name,
            dpr.procedure_name,
            dm.metric_name,
            wt.indicator_result
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        WHERE wt.data_year = (SELECT MAX(data_year) FROM fact_wait_times)
        AND wt.indicator_result IS NOT NULL
        AND dp.province_name != 'Canada'
        ORDER BY dp.province_name, dpr.procedure_name
    """,
    
    'data_quality_summary': """
        SELECT 
            dm.metric_name,
            COUNT(*) as total_records,
            COUNT(wt.indicator_result) as records_with_data,
            ROUND((COUNT(wt.indicator_result)::DECIMAL / COUNT(*) * 100), 1) as completeness_pct
        FROM fact_wait_times wt
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        GROUP BY dm.metric_name
        ORDER BY completeness_pct DESC
    """
}

# Dashboard specific queries
DASHBOARD_QUERIES = {
    'summary_stats': """
        WITH latest_year AS (
            SELECT MAX(data_year) as max_year FROM fact_wait_times
        )
        SELECT 
            'Total Procedures' as metric,
            COUNT(DISTINCT dpr.procedure_id)::TEXT as value
        FROM dim_procedures dpr
        UNION ALL
        SELECT 
            'Provinces Reporting' as metric,
            COUNT(DISTINCT dp.province_id)::TEXT as value
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        CROSS JOIN latest_year ly
        WHERE wt.data_year = ly.max_year
        AND dp.province_name != 'Canada'
    """,
    
    'filter_options': {
        'provinces': """
            SELECT DISTINCT province_name 
            FROM dim_provinces 
            WHERE province_name != 'Canada' 
            ORDER BY province_name
        """,
        'procedures': """
            SELECT DISTINCT procedure_name 
            FROM dim_procedures 
            ORDER BY procedure_name
        """,
        'years': """
            SELECT DISTINCT data_year 
            FROM fact_wait_times 
            WHERE data_year IS NOT NULL 
            ORDER BY data_year DESC
        """
    }
}