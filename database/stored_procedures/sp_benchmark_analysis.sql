-- BENCHMARK COMPLIANCE ANALYSIS =============================================

CREATE OR REPLACE FUNCTION sp_benchmark_analysis(
    p_province_name VARCHAR DEFAULT NULL,
    p_year INTEGER DEFAULT 2023
)
RETURNS TABLE (
    province_name VARCHAR,
    procedure_name VARCHAR,
    benchmark_compliance DECIMAL,
    median_wait_time DECIMAL,
    p90_wait_time DECIMAL,
    total_volume INTEGER,
    compliance_category VARCHAR,
    improvement_needed DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    WITH benchmark_data AS (
        SELECT 
            dp.province_name,
            dpr.procedure_name,
            MAX(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as benchmark_pct,
            MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as median_wait,
            MAX(CASE WHEN dm.metric_name = '90th Percentile' THEN wt.indicator_result END) as p90_wait,
            MAX(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result END) as volume_count
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        WHERE wt.data_year = p_year
        AND (p_province_name IS NULL OR dp.province_name ILIKE '%' || p_province_name || '%')
        AND dp.province_name != 'Canada'
        GROUP BY dp.province_name, dpr.procedure_name
        HAVING MAX(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) IS NOT NULL
    )
    SELECT 
        bd.province_name,
        bd.procedure_name,
        bd.benchmark_pct as benchmark_compliance,
        bd.median_wait as median_wait_time,
        bd.p90_wait as p90_wait_time,
        COALESCE(bd.volume_count::INTEGER, 0) as total_volume,
        CASE 
            WHEN bd.benchmark_pct >= 90 THEN 'Excellent'
            WHEN bd.benchmark_pct >= 75 THEN 'Good'
            WHEN bd.benchmark_pct >= 50 THEN 'Fair'
            ELSE 'Poor'
        END as compliance_category,
        GREATEST(0, 90 - bd.benchmark_pct) as improvement_needed
    FROM benchmark_data bd
    ORDER BY bd.province_name, bd.benchmark_pct DESC;
END;
$$ LANGUAGE plpgsql;