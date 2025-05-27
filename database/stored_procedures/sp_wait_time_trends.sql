-- Business logic procedures for wait time analysis

-- WAIT TIME TREND ANALYSIS =============================================

CREATE OR REPLACE FUNCTION sp_wait_time_trends(
    p_procedure_name VARCHAR DEFAULT NULL,
    p_province_name VARCHAR DEFAULT NULL,
    p_start_year INTEGER DEFAULT 2008,
    p_end_year INTEGER DEFAULT 2023,
    p_metric_type VARCHAR DEFAULT '50th Percentile'
)
RETURNS TABLE (
    province_name VARCHAR,
    procedure_name VARCHAR,
    data_year INTEGER,
    metric_name VARCHAR,
    wait_time_days DECIMAL,
    year_over_year_change DECIMAL,
    trend_direction VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    WITH trend_data AS (
        SELECT 
            dp.province_name,
            dpr.procedure_name,
            wt.data_year,
            dm.metric_name,
            wt.indicator_result as wait_time_days,
            LAG(wt.indicator_result) OVER (
                PARTITION BY dp.province_id, dpr.procedure_id, dm.metric_id 
                ORDER BY wt.data_year
            ) as previous_year_value
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        WHERE wt.indicator_result IS NOT NULL
        AND wt.data_year BETWEEN p_start_year AND p_end_year
        AND (p_procedure_name IS NULL OR dpr.procedure_name ILIKE '%' || p_procedure_name || '%')
        AND (p_province_name IS NULL OR dp.province_name ILIKE '%' || p_province_name || '%')
        AND dm.metric_name = p_metric_type
        ORDER BY dp.province_name, dpr.procedure_name, wt.data_year
    )
    SELECT 
        td.province_name,
        td.procedure_name,
        td.data_year,
        td.metric_name,
        td.wait_time_days,
        CASE 
            WHEN td.previous_year_value IS NOT NULL AND td.previous_year_value > 0 
            THEN ROUND(((td.wait_time_days - td.previous_year_value) / td.previous_year_value * 100)::DECIMAL, 2)
            ELSE NULL 
        END as year_over_year_change,
        CASE 
            WHEN td.previous_year_value IS NULL THEN 'No Previous Data'
            WHEN td.wait_time_days > td.previous_year_value THEN 'Increasing'
            WHEN td.wait_time_days < td.previous_year_value THEN 'Decreasing'
            ELSE 'Stable'
        END as trend_direction
    FROM trend_data td;
END;
$$ LANGUAGE plpgsql;

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
        PERCENT_RANK() OVER (ORDER BY pd.wait_time_days DESC) * 100 as percentile_rank_calc,
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

-- WAIT TIME STATISTICS BY PROCEDURE =============================================

CREATE OR REPLACE FUNCTION sp_procedure_statistics(
    p_procedure_name VARCHAR DEFAULT NULL,
    p_year INTEGER DEFAULT 2023
)
RETURNS TABLE (
    procedure_name VARCHAR,
    procedure_category VARCHAR,
    total_national_volume INTEGER,
    avg_median_wait_time DECIMAL,
    min_median_wait_time DECIMAL,
    max_median_wait_time DECIMAL,
    provinces_reporting INTEGER,
    best_performing_province VARCHAR,
    worst_performing_province VARCHAR,
    national_benchmark_compliance DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    WITH procedure_stats AS (
        SELECT 
            dpr.procedure_name,
            dpr.procedure_category,
            SUM(CASE WHEN dm.metric_name = 'Volume' THEN wt.indicator_result ELSE 0 END) as total_volume,
            AVG(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as avg_median,
            MIN(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as min_median,
            MAX(CASE WHEN dm.metric_name = '50th Percentile' THEN wt.indicator_result END) as max_median,
            COUNT(DISTINCT CASE WHEN dm.metric_name = '50th Percentile' AND wt.indicator_result IS NOT NULL THEN dp.province_id END) as reporting_provinces,
            AVG(CASE WHEN dm.metric_name = '% Meeting Benchmark' THEN wt.indicator_result END) as avg_benchmark
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        WHERE wt.data_year = p_year
        AND dp.province_name != 'Canada'
        AND (p_procedure_name IS NULL OR dpr.procedure_name ILIKE '%' || p_procedure_name || '%')
        GROUP BY dpr.procedure_name, dpr.procedure_category
    ),
    best_worst AS (
        SELECT 
            dpr.procedure_name,
            dp.province_name,
            wt.indicator_result as wait_time,
            ROW_NUMBER() OVER (PARTITION BY dpr.procedure_name ORDER BY wt.indicator_result ASC) as best_rank,
            ROW_NUMBER() OVER (PARTITION BY dpr.procedure_name ORDER BY wt.indicator_result DESC) as worst_rank
        FROM fact_wait_times wt
        JOIN dim_provinces dp ON wt.province_id = dp.province_id
        JOIN dim_procedures dpr ON wt.procedure_id = dpr.procedure_id
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        WHERE wt.data_year = p_year
        AND dm.metric_name = '50th Percentile'
        AND dp.province_name != 'Canada'
        AND wt.indicator_result IS NOT NULL
        AND (p_procedure_name IS NULL OR dpr.procedure_name ILIKE '%' || p_procedure_name || '%')
    )
    SELECT 
        ps.procedure_name,
        ps.procedure_category,
        ps.total_volume::INTEGER as total_national_volume,
        ROUND(ps.avg_median::DECIMAL, 1) as avg_median_wait_time,
        ps.min_median as min_median_wait_time,
        ps.max_median as max_median_wait_time,
        ps.reporting_provinces as provinces_reporting,
        (SELECT province_name FROM best_worst WHERE procedure_name = ps.procedure_name AND best_rank = 1) as best_performing_province,
        (SELECT province_name FROM best_worst WHERE procedure_name = ps.procedure_name AND worst_rank = 1) as worst_performing_province,
        ROUND(ps.avg_benchmark::DECIMAL, 1) as national_benchmark_compliance
    FROM procedure_stats ps
    ORDER BY ps.avg_median DESC;
END;
$$ LANGUAGE plpgsql;

-- DATA QUALITY REPORT =============================================

CREATE OR REPLACE FUNCTION sp_data_quality_report()
RETURNS TABLE (
    metric_name VARCHAR,
    total_expected_records INTEGER,
    records_with_data INTEGER,
    data_completeness_pct DECIMAL,
    latest_data_year INTEGER,
    provinces_reporting INTEGER,
    procedures_covered INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH quality_stats AS (
        SELECT 
            dm.metric_name,
            COUNT(*) as total_records,
            COUNT(wt.indicator_result) as records_with_data,
            MAX(wt.data_year) as latest_year,
            COUNT(DISTINCT wt.province_id) as unique_provinces,
            COUNT(DISTINCT wt.procedure_id) as unique_procedures
        FROM fact_wait_times wt
        JOIN dim_metrics dm ON wt.metric_id = dm.metric_id
        GROUP BY dm.metric_name
    )
    SELECT 
        qs.metric_name,
        qs.total_records as total_expected_records,
        qs.records_with_data as records_with_data,
        ROUND((qs.records_with_data::DECIMAL / qs.total_records * 100), 1) as data_completeness_pct,
        qs.latest_year as latest_data_year,
        qs.unique_provinces as provinces_reporting,
        qs.unique_procedures as procedures_covered
    FROM quality_stats qs
    ORDER BY qs.metric_name;
END;
$$ LANGUAGE plpgsql;