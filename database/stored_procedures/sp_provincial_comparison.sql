-- PROVINCIAL PERFORMANCE COMPARISON =============================================

CREATE OR REPLACE FUNCTION sp_provincial_comparison(
    p_procedure_name VARCHAR,
    p_year INTEGER DEFAULT 2023,
    p_metric_type VARCHAR DEFAULT '50th Percentile'
)
RETURNS TABLE (
    province_name VARCHAR,
    wait_time_days DECIMAL,
    national_average DECIMAL,
    variance_from_average DECIMAL,
    percentile_rank INTEGER,
    performance_category VARCHAR,
    volume_cases INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH provincial_data AS (
        SELECT 
            dp.province_name,
            wt.indicator_result as wait_time_days,
            vol.indicator_result as volume_cases
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        LEFT JOIN fact_wait_times vol ON vol.province_id = wt.province_id 
            AND vol.procedure_id = wt.procedure_id 
            AND vol.data_year = wt.data_year
            AND vol.metric_id = (SELECT metric_id FROM dim_metrics WHERE metric_name = 'Volume')
        WHERE dpr.procedure_name = p_procedure_name
        AND wt.data_year = p_year
        AND dm.metric_name = p_metric_type
        AND dp.province_name != 'Canada'
        AND wt.indicator_result IS NOT NULL
    ),
    national_stats AS (
        SELECT AVG(wait_time_days) as avg_wait_time
        FROM provincial_data
    )
    SELECT 
        pd.province_name,
        pd.wait_time_days,
        ns.avg_wait_time as national_average,
        ROUND((pd.wait_time_days - ns.avg_wait_time)::DECIMAL, 2) as variance_from_average,
        (PERCENT_RANK() OVER (ORDER BY pd.wait_time_days DESC) * 100)::INTEGER as percentile_rank,
        CASE 
            WHEN pd.wait_time_days <= ns.avg_wait_time * 0.9 THEN 'Excellent'
            WHEN pd.wait_time_days <= ns.avg_wait_time * 1.1 THEN 'Good'
            WHEN pd.wait_time_days <= ns.avg_wait_time * 1.3 THEN 'Fair'
            ELSE 'Needs Improvement'
        END as performance_category,
        COALESCE(pd.volume_cases::INTEGER, 0) as volume_cases
    FROM provincial_data pd
    CROSS JOIN national_stats ns
    ORDER BY pd.wait_time_days;
END;
$$ LANGUAGE plpgsql;

-- REGIONAL COMPARISON FUNCTION =============================================

CREATE OR REPLACE FUNCTION sp_regional_comparison(
    p_procedure_name VARCHAR,
    p_year INTEGER DEFAULT 2023
)
RETURNS TABLE (
    region VARCHAR,
    avg_wait_time DECIMAL,
    min_wait_time DECIMAL,
    max_wait_time DECIMAL,
    provinces_count INTEGER,
    total_volume INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        dp.region,
        ROUND(AVG(wt.indicator_result)::DECIMAL, 1) as avg_wait_time,
        MIN(wt.indicator_result) as min_wait_time,
        MAX(wt.indicator_result) as max_wait_time,
        COUNT(DISTINCT dp.province_id)::INTEGER as provinces_count,
        COALESCE(SUM(vol.indicator_result)::INTEGER, 0) as total_volume
    FROM fact_wait_times wt
    JOIN dim_provinces dp ON wt.province_id = dp.province_id
    JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
    JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
    LEFT JOIN fact_wait_times vol ON vol.province_id = wt.province_id 
        AND vol.procedure_id = wt.procedure_id 
        AND vol.data_year = wt.data_year
        AND vol.metric_id = (SELECT metric_id FROM dim_metrics WHERE metric_name = 'Volume')
    WHERE dpr.procedure_name = p_procedure_name
    AND wt.data_year = p_year
    AND dm.metric_name = '50th Percentile'
    AND dp.province_name != 'Canada'
    AND dp.region IS NOT NULL
    AND wt.indicator_result IS NOT NULL
    GROUP BY dp.region
    ORDER BY avg_wait_time;
END;
$$ LANGUAGE plpgsql;