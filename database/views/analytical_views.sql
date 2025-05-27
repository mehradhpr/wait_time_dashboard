-- Analytical views for common reporting and dashboard queries

-- CORE ANALYTICAL VIEWS =============================================

-- Comprehensive wait time data view with all dimensions
CREATE OR REPLACE VIEW v_wait_times_detail AS
SELECT 
    wt.wait_time_id,
    wt.data_year,
    dp.province_name,
    dp.province_code,
    dp.region,
    dpr.procedure_name,
    dpr.procedure_category,
    dpr.is_surgery,
    dm.metric_name,
    dm.metric_type,
    dm.unit_of_measurement,
    wt.indicator_result,
    wt.data_quality_flag,
    drl.level_name as reporting_level,
    wt.region_name,
    wt.created_at,
    wt.updated_at
FROM fact_wait_times wt
JOIN dim_provinces dp ON wt.province_id = dp.province_id
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id  
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
JOIN dim_reporting_levels drl ON wt.reporting_level_id = drl.level_id;

-- Latest year summary view
CREATE OR REPLACE VIEW v_latest_wait_times AS
WITH latest_year AS (
    SELECT MAX(data_year) as max_year 
    FROM fact_wait_times
)
SELECT 
    vtd.*
FROM v_wait_times_detail vtd
CROSS JOIN latest_year ly
WHERE vtd.data_year = ly.max_year
AND vtd.indicator_result IS NOT NULL;

-- Provincial performance summary
CREATE OR REPLACE VIEW v_provincial_performance AS
SELECT 
    dp.province_name,
    dp.province_code,
    dp.region,
    COUNT(DISTINCT dpr.procedure_id) as procedures_offered,
    COUNT(DISTINCT CASE WHEN wt.indicator_result IS NOT NULL THEN dpr.procedure_id END) as procedures_with_data,
    AVG(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as avg_median_wait_time,
    AVG(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as avg_benchmark_compliance,
    SUM(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result ELSE 0 END) as total_procedure_volume,
    MAX(wt.data_year) as latest_data_year
FROM fact_wait_times wt
JOIN dim_provinces dp ON wt.province_id = dp.province_id
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
WHERE dp.province_name != 'Canada'
GROUP BY dp.province_name, dp.province_code, dp.region;

-- Procedure performance summary
CREATE OR REPLACE VIEW v_procedure_performance AS
SELECT 
    dpr.procedure_name,
    dpr.procedure_category,
    dpr.is_surgery,
    COUNT(DISTINCT dp.province_id) as provinces_reporting,
    AVG(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as national_avg_median_wait,
    MIN(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as best_median_wait,
    MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as worst_median_wait,
    AVG(CASE WHEN dm.metric_name = '90th Percentile' THEN wt.indicator_result END) as national_avg_p90_wait,
    AVG(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as national_benchmark_compliance,
    SUM(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result ELSE 0 END) as total_national_volume,
    MAX(wt.data_year) as latest_data_year
FROM fact_wait_times wt
JOIN dim_provinces dp ON wt.province_id = dp.province_id
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
WHERE dp.province_name != 'Canada'
AND wt.data_year = (SELECT MAX(data_year) FROM fact_wait_times)
GROUP BY dpr.procedure_name, dpr.procedure_category, dpr.is_surgery;

-- MATERIALIZED VIEWS FOR PERFORMANCE =============================================

-- Materialized view for trend analysis (refreshed nightly)
CREATE MATERIALIZED VIEW mv_wait_time_trends AS
SELECT 
    dp.province_name,
    dpr.procedure_name,
    dm.metric_name,
    wt.data_year,
    wt.indicator_result as current_value,
    LAG(wt.indicator_result, 1) OVER (
        PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
        ORDER BY wt.data_year
    ) as previous_year_value,
    LAG(wt.indicator_result, 2) OVER (
        PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
        ORDER BY wt.data_year
    ) as two_years_ago_value,
    -- Calculate year-over-year change
    CASE 
        WHEN LAG(wt.indicator_result, 1) OVER (
            PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
            ORDER BY wt.data_year
        ) IS NOT NULL AND LAG(wt.indicator_result, 1) OVER (
            PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
            ORDER BY wt.data_year
        ) > 0
        THEN ((wt.indicator_result - LAG(wt.indicator_result, 1) OVER (
            PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
            ORDER BY wt.data_year
        )) / LAG(wt.indicator_result, 1) OVER (
            PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
            ORDER BY wt.data_year
        ) * 100)
        ELSE NULL
    END as yoy_change_percent,
    -- Calculate 3-year trend
    CASE 
        WHEN COUNT(*) OVER (
            PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
            ORDER BY wt.data_year 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) = 3
        THEN CASE
            WHEN wt.indicator_result > LAG(wt.indicator_result, 1) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) AND LAG(wt.indicator_result, 1) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) > LAG(wt.indicator_result, 2) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) THEN 'Increasing'
            WHEN wt.indicator_result < LAG(wt.indicator_result, 1) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) AND LAG(wt.indicator_result, 1) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) < LAG(wt.indicator_result, 2) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) THEN 'Decreasing'
            ELSE 'Variable'
        END
        ELSE 'Insufficient Data'
    END as trend_direction
FROM fact_wait_times wt
JOIN dim_provinces dp ON wt.province_id = dp.province_id
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
WHERE wt.indicator_result IS NOT NULL
AND dp.province_name != 'Canada';

-- Create indexes on materialized view
CREATE INDEX idx_mv_trends_province ON mv_wait_time_trends(province_name);
CREATE INDEX idx_mv_trends_procedure ON mv_wait_time_trends(procedure_name);
CREATE INDEX idx_mv_trends_year ON mv_wait_time_trends(data_year);

-- Materialized view for dashboard summary stats
CREATE MATERIALIZED VIEW mv_dashboard_summary AS
WITH current_year AS (
    SELECT MAX(data_year) as max_year FROM fact_wait_times
),
summary_stats AS (
    SELECT 
        'Total Procedures Tracked' as metric,
        COUNT(DISTINCT dpr.procedure_id)::TEXT as value,
        'procedures' as unit
    FROM dim_procedures dpr
    
    UNION ALL
    
    SELECT 
        'Provinces Reporting' as metric,
        COUNT(DISTINCT dp.province_id)::TEXT as value,
        'provinces' as unit
    FROM fact_wait_times wt
    JOIN dim_provinces dp ON wt.province_id = dp.province_id
    CROSS JOIN current_year cy
    WHERE wt.data_year = cy.max_year
    AND dp.province_name != 'Canada'
    
    UNION ALL
    
    SELECT 
        'Latest Data Year' as metric,
        MAX(wt.data_year)::TEXT as value,
        'year' as unit
    FROM fact_wait_times wt
    
    UNION ALL
    
    SELECT 
        'Total Data Points' as metric,
        COUNT(*)::TEXT as value,
        'records' as unit
    FROM fact_wait_times wt
    WHERE wt.indicator_result IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Average National Median Wait' as metric,
        ROUND(AVG(wt.indicator_result)::DECIMAL, 1)::TEXT as value,
        'days' as unit
    FROM fact_wait_times wt
    JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
    JOIN dim_provinces dp ON wt.province_id = dp.province_id
    CROSS JOIN current_year cy
    WHERE dm.metric_name = '50th Percentile'
    AND wt.data_year = cy.max_year
    AND dp.province_name != 'Canada'
    AND wt.indicator_result IS NOT NULL
)
SELECT * FROM summary_stats;

-- SPECIALIZED REPORTING VIEWS =============================================

-- Surgery vs Non-Surgery wait times comparison
CREATE OR REPLACE VIEW v_surgery_comparison AS
SELECT 
    dpr.is_surgery,
    CASE WHEN dpr.is_surgery THEN 'Surgical Procedures' ELSE 'Non-Surgical Procedures' END as procedure_type,
    COUNT(DISTINCT dpr.procedure_id) as procedure_count,
    AVG(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as avg_median_wait,
    AVG(CASE WHEN dm.metric_name = '90th Percentile' THEN wt.indicator_result END) as avg_p90_wait,
    AVG(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as avg_benchmark_compliance,
    SUM(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result ELSE 0 END) as total_volume
FROM fact_wait_times wt
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
JOIN dim_provinces dp ON wt.province_id = dp.province_id
WHERE wt.data_year = (SELECT MAX(data_year) FROM fact_wait_times)
AND dp.province_name != 'Canada'
GROUP BY dpr.is_surgery;

-- Regional performance comparison
CREATE OR REPLACE VIEW v_regional_performance AS
SELECT 
    dp.region,
    COUNT(DISTINCT dp.province_id) as provinces_in_region,
    COUNT(DISTINCT dpr.procedure_id) as procedures_tracked,
    AVG(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as avg_median_wait,
    MIN(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as best_median_wait,
    MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as worst_median_wait,
    AVG(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as avg_benchmark_compliance,
    SUM(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result ELSE 0 END) as total_regional_volume
FROM fact_wait_times wt
JOIN dim_provinces dp ON wt.province_id = dp.province_id
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
WHERE wt.data_year = (SELECT MAX(data_year) FROM fact_wait_times)
AND dp.province_name != 'Canada'
AND dp.region IS NOT NULL
GROUP BY dp.region;

-- Cancer surgery wait times focus view
CREATE OR REPLACE VIEW v_cancer_surgery_waits AS
SELECT 
    dp.province_name,
    dpr.procedure_name,
    wt.data_year,
    MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as median_wait_days,
    MAX(CASE WHEN dm.metric_name = '90th Percentile' THEN wt.indicator_result END) as p90_wait_days,
    MAX(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as benchmark_compliance,
    MAX(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result END) as annual_volume,
    -- Cancer surgery benchmark is typically 28 days
    CASE 
        WHEN MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) <= 28 THEN 'Meeting Target'
        WHEN MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) <= 42 THEN 'Close to Target'
        ELSE 'Above Target'
    END as performance_status
FROM fact_wait_times wt
JOIN dim_provinces dp ON wt.province_id = dp.province_id
JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
WHERE dpr.procedure_category = 'Cancer Surgery'
AND dp.province_name != 'Canada'
GROUP BY dp.province_name, dpr.procedure_name, wt.data_year
HAVING MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) IS NOT NULL;

-- DATA QUALITY AND MONITORING VIEWS =============================================

-- Data freshness monitoring
CREATE OR REPLACE VIEW v_data_freshness AS
SELECT 
    'Wait Times Data' as data_source,
    MAX(wt.data_year) as latest_data_year,
    COUNT(DISTINCT wt.data_year) as years_of_data,
    COUNT(*) as total_records,
    COUNT(wt.indicator_result) as records_with_values,
    ROUND((COUNT(wt.indicator_result)::DECIMAL / COUNT(*) * 100), 1) as data_completeness_pct,
    MAX(wt.created_at) as last_loaded_at
FROM fact_wait_times wt

UNION ALL

SELECT 
    'ETL Load History' as data_source,
    EXTRACT(YEAR FROM MAX(load_timestamp))::INTEGER as latest_data_year,
    COUNT(DISTINCT DATE(load_timestamp)) as years_of_data,
    COUNT(*) as total_records,
    COUNT(CASE WHEN load_status = 'completed' THEN 1 END) as records_with_values,
    ROUND((COUNT(CASE WHEN load_status = 'completed' THEN 1 END)::DECIMAL / COUNT(*) * 100), 1) as data_completeness_pct,
    MAX(load_timestamp) as last_loaded_at
FROM audit_data_loads;

-- Missing data analysis
CREATE OR REPLACE VIEW v_missing_data_analysis AS
SELECT 
    dp.province_name,
    dpr.procedure_name,
    dm.metric_name,
    COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 THEN 1 END) as expected_recent_records,
    COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 AND wt.indicator_result IS NOT NULL THEN 1 END) as actual_records_with_data,
    CASE 
        WHEN COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 THEN 1 END) = 0 THEN 'No Data'
        WHEN COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 AND wt.indicator_result IS NOT NULL THEN 1 END) = 0 THEN 'All Missing'
        WHEN COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 AND wt.indicator_result IS NOT NULL THEN 1 END) < 
             COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 THEN 1 END) THEN 'Partial Data'
        ELSE 'Complete Data'
    END as data_status
FROM dim_provinces dp
CROSS JOIN dim_procedures dpr
CROSS JOIN dim_metrics dm
LEFT JOIN fact_wait_times wt ON dp.province_id = wt.province_id 
    AND dpr.procedure_id = wt.procedure_id 
    AND dm.metric_id = wt.metric_id
WHERE dp.province_name != 'Canada'
GROUP BY dp.province_name, dpr.procedure_name, dm.metric_name
HAVING COUNT(CASE WHEN wt.data_year BETWEEN 2020 AND 2023 THEN 1 END) > 0
ORDER BY dp.province_name, dpr.procedure_name, dm.metric_name;

-- REFRESH FUNCTIONS FOR MATERIALIZED VIEWS =============================================

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS TEXT AS $
DECLARE
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    end_time TIMESTAMP;
    result_message TEXT;
BEGIN
    -- Refresh trend analysis view
    REFRESH MATERIALIZED VIEW mv_wait_time_trends;
    
    -- Refresh dashboard summary view
    REFRESH MATERIALIZED VIEW mv_dashboard_summary;
    
    end_time := CURRENT_TIMESTAMP;
    result_message := 'Materialized views refreshed successfully in ' || 
                     EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER || ' seconds';
    
    -- Log the refresh
    INSERT INTO audit_data_loads (source_file, records_processed, load_status, error_message)
    VALUES ('materialized_view_refresh', 0, 'completed', result_message);
    
    RETURN result_message;
EXCEPTION
    WHEN OTHERS THEN
        -- Log the error
        INSERT INTO audit_data_loads (source_file, records_processed, load_status, error_message)
        VALUES ('materialized_view_refresh', 0, 'failed', SQLERRM);
        
        RETURN 'Materialized view refresh failed: ' || SQLERRM;
END;
$ LANGUAGE plpgsql;

-- Schedule materialized view refresh (example cron job entry)
-- 0 2 * * * /usr/bin/psql -d healthcare_analytics -c "SELECT refresh_materialized_views();"

-- Grant permissions for application user
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO healthcare_app_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO healthcare_app_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO healthcare_app_user;