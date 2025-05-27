
-- Purpose: Essential SQL queries and stored procedures for healthcare analytics portfolio
-- Complex joins, window functions, CTEs, statistical analysis, stored procedures

-- SECTION 1: DATA QUALITY & EXPLORATION
-- ================================================================

-- Query 1: Data Coverage Dashboard
SELECT 
    'System Overview' as metric_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN result_value IS NOT NULL THEN 1 END) as valid_records,
    CAST(COUNT(CASE WHEN result_value IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct,
    COUNT(DISTINCT f.province_id) as provinces,
    COUNT(DISTINCT f.procedure_id) as procedures,
    MIN(t.fiscal_year) as earliest_year,
    MAX(t.fiscal_year) as latest_year
FROM fact_wait_times f
JOIN dim_time_periods t ON f.time_id = t.time_id;

-- Query 2: Provincial Data Quality Assessment
SELECT 
    p.province_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN f.result_value IS NOT NULL THEN 1 END) as valid_records,
    CAST(COUNT(CASE WHEN f.result_value IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as data_quality_score,
    COUNT(CASE WHEN f.data_quality_flag = 'MISSING' THEN 1 END) as missing_data_points
FROM fact_wait_times f
JOIN dim_provinces p ON f.province_id = p.province_id
GROUP BY p.province_name
ORDER BY data_quality_score DESC;

-- SECTION 2: PERFORMANCE ANALYSIS
-- ================================================================

-- Query 3: Provincial Performance Scorecard (Latest Year)
WITH performance_metrics AS (
    SELECT 
        p.province_name,
        COUNT(CASE WHEN f.is_benchmark_met = 1 THEN 1 END) as benchmarks_met,
        COUNT(CASE WHEN f.is_benchmark_met = 0 THEN 1 END) as benchmarks_missed,
        AVG(CASE WHEN m.metric_name LIKE '%50th Percentile%' THEN f.result_value END) as avg_median_wait,
        COUNT(DISTINCT f.procedure_id) as procedures_tracked
    FROM fact_wait_times f
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE t.fiscal_year = (SELECT MAX(fiscal_year) FROM dim_time_periods)
        AND f.result_value IS NOT NULL
        AND p.province_name != 'Canada'
    GROUP BY p.province_name
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY 
        CAST(benchmarks_met * 100.0 / NULLIF(benchmarks_met + benchmarks_missed, 0) AS DECIMAL(5,2)) DESC,
        avg_median_wait ASC
    ) as performance_rank,
    province_name,
    procedures_tracked,
    benchmarks_met,
    benchmarks_missed,
    CAST(benchmarks_met * 100.0 / NULLIF(benchmarks_met + benchmarks_missed, 0) AS DECIMAL(5,2)) as compliance_rate,
    CAST(avg_median_wait AS DECIMAL(6,1)) as avg_wait_days,
    CASE 
        WHEN benchmarks_met * 100.0 / NULLIF(benchmarks_met + benchmarks_missed, 0) >= 80 THEN 'Excellent'
        WHEN benchmarks_met * 100.0 / NULLIF(benchmarks_met + benchmarks_missed, 0) >= 60 THEN 'Good'
        ELSE 'Needs Improvement'
    END as performance_grade
FROM performance_metrics
ORDER BY performance_rank;

-- Query 4: Best & Worst Performing Procedures by Province
WITH procedure_rankings AS (
    SELECT 
        p.province_name,
        pr.procedure_name,
        AVG(CASE WHEN m.metric_name LIKE '%50th Percentile%' THEN f.result_value END) as avg_wait_days,
        AVG(CASE WHEN f.is_benchmark_met IS NOT NULL THEN CAST(f.is_benchmark_met AS FLOAT) END) as compliance_rate,
        ROW_NUMBER() OVER (PARTITION BY p.province_name ORDER BY 
            AVG(CASE WHEN f.is_benchmark_met IS NOT NULL THEN CAST(f.is_benchmark_met AS FLOAT) END) DESC,
            AVG(CASE WHEN m.metric_name LIKE '%50th Percentile%' THEN f.result_value END) ASC
        ) as best_rank,
        ROW_NUMBER() OVER (PARTITION BY p.province_name ORDER BY 
            AVG(CASE WHEN f.is_benchmark_met IS NOT NULL THEN CAST(f.is_benchmark_met AS FLOAT) END) ASC,
            AVG(CASE WHEN m.metric_name LIKE '%50th Percentile%' THEN f.result_value END) DESC
        ) as worst_rank
    FROM fact_wait_times f
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE f.result_value IS NOT NULL 
        AND t.fiscal_year >= 2020
        AND p.province_name != 'Canada'
    GROUP BY p.province_name, pr.procedure_name
    HAVING COUNT(*) >= 3
)
SELECT 
    province_name,
    CASE WHEN best_rank = 1 THEN 'BEST' ELSE 'WORST' END as performance_type,
    procedure_name,
    CAST(avg_wait_days AS DECIMAL(6,1)) as wait_days,
    CAST(compliance_rate * 100 AS DECIMAL(5,1)) as compliance_pct
FROM procedure_rankings 
WHERE best_rank = 1 OR worst_rank = 1
ORDER BY province_name, performance_type DESC;

-- SECTION 3: TREND ANALYSIS
-- ================================================================

-- Query 5: Year-over-Year Trend Analysis
WITH yearly_trends AS (
    SELECT 
        pr.procedure_name,
        p.province_name,
        t.fiscal_year,
        AVG(f.result_value) as avg_wait_time,
        LAG(AVG(f.result_value)) OVER (PARTITION BY pr.procedure_name, p.province_name ORDER BY t.fiscal_year) as prev_year_wait
    FROM fact_wait_times f
    JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE f.result_value IS NOT NULL 
        AND m.metric_name LIKE '%50th Percentile%'
        AND t.fiscal_year >= 2018
        AND p.province_name != 'Canada'
    GROUP BY pr.procedure_name, p.province_name, t.fiscal_year
)
SELECT 
    procedure_name,
    province_name,
    fiscal_year,
    CAST(avg_wait_time AS DECIMAL(6,1)) as current_wait_days,
    CAST(prev_year_wait AS DECIMAL(6,1)) as previous_wait_days,
    CAST(avg_wait_time - prev_year_wait AS DECIMAL(6,1)) as change_days,
    CASE 
        WHEN prev_year_wait IS NULL THEN 'No Prior Data'
        WHEN ABS(avg_wait_time - prev_year_wait) < 2 THEN 'Stable'
        WHEN avg_wait_time > prev_year_wait THEN 'Worsening'
        ELSE 'Improving'
    END as trend_direction
FROM yearly_trends
WHERE fiscal_year >= 2020
ORDER BY procedure_name, province_name, fiscal_year DESC;

-- Query 6: Long-term Trend Identification (5-Year)
WITH long_term_trends AS (
    SELECT 
        pr.procedure_name,
        AVG(CASE WHEN t.fiscal_year BETWEEN 2019 AND 2021 THEN f.result_value END) as early_period_avg,
        AVG(CASE WHEN t.fiscal_year BETWEEN 2021 AND 2023 THEN f.result_value END) as recent_period_avg,
        COUNT(DISTINCT t.fiscal_year) as years_covered
    FROM fact_wait_times f
    JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE f.result_value IS NOT NULL 
        AND m.metric_name LIKE '%50th Percentile%'
        AND t.fiscal_year BETWEEN 2019 AND 2023
    GROUP BY pr.procedure_name
    HAVING COUNT(DISTINCT t.fiscal_year) >= 4
)
SELECT 
    procedure_name,
    CAST(early_period_avg AS DECIMAL(6,1)) as early_avg_days,
    CAST(recent_period_avg AS DECIMAL(6,1)) as recent_avg_days,
    CAST(recent_period_avg - early_period_avg AS DECIMAL(6,1)) as total_change_days,
    CAST((recent_period_avg - early_period_avg) * 100.0 / early_period_avg AS DECIMAL(6,2)) as percent_change,
    CASE 
        WHEN ABS(recent_period_avg - early_period_avg) < 5 THEN 'Stable'
        WHEN recent_period_avg > early_period_avg THEN 'Deteriorating'
        ELSE 'Improving'
    END as long_term_trend
FROM long_term_trends
WHERE early_period_avg IS NOT NULL AND recent_period_avg IS NOT NULL
ORDER BY ABS(percent_change) DESC;

-- SECTION 4: ADVANCED ANALYTICS
-- ================================================================

-- Query 7: Statistical Outlier Detection
WITH procedure_stats AS (
    SELECT 
        f.province_id,
        f.procedure_id,
        AVG(f.result_value) as mean_value,
        STDEV(f.result_value) as std_dev
    FROM fact_wait_times f
    WHERE f.result_value IS NOT NULL
    GROUP BY f.province_id, f.procedure_id
    HAVING COUNT(*) >= 8 AND STDEV(f.result_value) > 0
)
SELECT 
    p.province_name,
    pr.procedure_name,
    t.fiscal_year,
    CAST(f.result_value AS DECIMAL(8,2)) as actual_value,
    CAST(ps.mean_value AS DECIMAL(8,2)) as historical_average,
    CAST((f.result_value - ps.mean_value) / ps.std_dev AS DECIMAL(4,2)) as z_score,
    CASE 
        WHEN ABS((f.result_value - ps.mean_value) / ps.std_dev) > 2.5 THEN 'Extreme'
        WHEN ABS((f.result_value - ps.mean_value) / ps.std_dev) > 2 THEN 'Significant'
        ELSE 'Moderate'
    END as outlier_severity
FROM fact_wait_times f
JOIN procedure_stats ps ON f.province_id = ps.province_id AND f.procedure_id = ps.procedure_id
JOIN dim_provinces p ON f.province_id = p.province_id
JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
JOIN dim_time_periods t ON f.time_id = t.time_id
WHERE f.result_value IS NOT NULL
    AND ABS((f.result_value - ps.mean_value) / ps.std_dev) > 2
ORDER BY ABS((f.result_value - ps.mean_value) / ps.std_dev) DESC;

-- Query 8: Volume vs Wait Time Correlation
WITH volume_wait_data AS (
    SELECT 
        p.province_name,
        pr.procedure_name,
        t.fiscal_year,
        f1.result_value as median_wait_days,
        f2.result_value as case_volume
    FROM fact_wait_times f1
    JOIN fact_wait_times f2 ON f1.province_id = f2.province_id 
                            AND f1.procedure_id = f2.procedure_id 
                            AND f1.time_id = f2.time_id
    JOIN dim_provinces p ON f1.province_id = p.province_id
    JOIN dim_procedures pr ON f1.procedure_id = pr.procedure_id
    JOIN dim_time_periods t ON f1.time_id = t.time_id
    JOIN dim_metrics m1 ON f1.metric_id = m1.metric_id AND m1.metric_name LIKE '%50th Percentile%'
    JOIN dim_metrics m2 ON f2.metric_id = m2.metric_id AND m2.metric_name = 'Volume'
    WHERE f1.result_value IS NOT NULL AND f2.result_value IS NOT NULL
)
SELECT 
    procedure_name,
    COUNT(*) as data_points,
    CAST(AVG(median_wait_days) AS DECIMAL(6,1)) as avg_wait_days,
    CAST(AVG(case_volume) AS DECIMAL(8,0)) as avg_volume,
    CASE 
        WHEN COUNT(*) >= 20 THEN 
            CAST((COUNT(*) * SUM(median_wait_days * case_volume) - SUM(median_wait_days) * SUM(case_volume)) /
                 (SQRT(COUNT(*) * SUM(median_wait_days * median_wait_days) - SUM(median_wait_days) * SUM(median_wait_days)) *
                  SQRT(COUNT(*) * SUM(case_volume * case_volume) - SUM(case_volume) * SUM(case_volume))) AS DECIMAL(4,3))
        ELSE NULL
    END as correlation_coefficient
FROM volume_wait_data
GROUP BY procedure_name
HAVING COUNT(*) >= 15
ORDER BY ABS(correlation_coefficient) DESC;

-- SECTION 5: STORED PROCEDURES
-- ================================================================

-- Stored Procedure 1: Performance Dashboard Generator
CREATE PROCEDURE sp_performance_dashboard
    @province_filter VARCHAR(50) = NULL,
    @year_filter INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @year_filter IS NULL
        SET @year_filter = (SELECT MAX(fiscal_year) FROM dim_time_periods);
    
    -- Key Metrics Summary
    SELECT 
        'DASHBOARD_SUMMARY' as section,
        @year_filter as report_year,
        COUNT(DISTINCT f.province_id) as provinces_reporting,
        COUNT(DISTINCT f.procedure_id) as procedures_tracked,
        COUNT(CASE WHEN f.is_benchmark_met = 1 THEN 1 END) as benchmarks_met,
        COUNT(CASE WHEN f.is_benchmark_met = 0 THEN 1 END) as benchmarks_missed,
        CAST(COUNT(CASE WHEN f.is_benchmark_met = 1 THEN 1 END) * 100.0 / 
             NULLIF(COUNT(CASE WHEN f.is_benchmark_met IS NOT NULL THEN 1 END), 0) AS DECIMAL(5,2)) as overall_compliance_rate
    FROM fact_wait_times f
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE t.fiscal_year = @year_filter
        AND (@province_filter IS NULL OR p.province_name = @province_filter)
        AND p.province_name != 'Canada';
    
    -- Detailed Results
    SELECT 
        'DETAILED_RESULTS' as section,
        p.province_name,
        pr.procedure_name,
        m.metric_name,
        f.result_value,
        CASE WHEN f.is_benchmark_met = 1 THEN 'Met' 
             WHEN f.is_benchmark_met = 0 THEN 'Not Met' 
             ELSE 'N/A' END as benchmark_status
    FROM fact_wait_times f
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE t.fiscal_year = @year_filter
        AND f.result_value IS NOT NULL
        AND (@province_filter IS NULL OR p.province_name = @province_filter)
        AND p.province_name != 'Canada'
    ORDER BY p.province_name, pr.procedure_name, m.metric_name;
END;

-- Stored Procedure 2: Trend Analysis Generator
CREATE PROCEDURE sp_trend_analysis
    @analysis_years INT = 5,
    @trend_threshold DECIMAL(5,2) = 5.0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_year INT = (SELECT MAX(fiscal_year) FROM dim_time_periods) - @analysis_years + 1;
    
    WITH trend_analysis AS (
        SELECT 
            pr.procedure_name,
            p.province_name,
            MIN(t.fiscal_year) as start_year,
            MAX(t.fiscal_year) as end_year,
            AVG(CASE WHEN t.fiscal_year = MIN(t.fiscal_year) OVER (PARTITION BY pr.procedure_name, p.province_name) 
                     THEN f.result_value END) as start_value,
            AVG(CASE WHEN t.fiscal_year = MAX(t.fiscal_year) OVER (PARTITION BY pr.procedure_name, p.province_name) 
                     THEN f.result_value END) as end_value
        FROM fact_wait_times f
        JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
        JOIN dim_provinces p ON f.province_id = p.province_id
        JOIN dim_metrics m ON f.metric_id = m.metric_id
        JOIN dim_time_periods t ON f.time_id = t.time_id
        WHERE f.result_value IS NOT NULL
            AND t.fiscal_year >= @start_year
            AND m.metric_name LIKE '%50th Percentile%'
            AND p.province_name != 'Canada'
        GROUP BY pr.procedure_name, p.province_name
        HAVING COUNT(DISTINCT t.fiscal_year) >= 3
    )
    SELECT 
        procedure_name,
        province_name,
        start_year,
        end_year,
        CAST(start_value AS DECIMAL(6,1)) as initial_wait_days,
        CAST(end_value AS DECIMAL(6,1)) as final_wait_days,
        CAST(end_value - start_value AS DECIMAL(6,1)) as total_change_days,
        CAST((end_value - start_value) * 100.0 / start_value AS DECIMAL(6,2)) as percent_change,
        CASE 
            WHEN ABS((end_value - start_value) * 100.0 / start_value) < @trend_threshold THEN 'Stable'
            WHEN end_value > start_value THEN 'Worsening'
            ELSE 'Improving'
        END as trend_classification
    FROM trend_analysis
    WHERE start_value IS NOT NULL AND end_value IS NOT NULL
    ORDER BY ABS((end_value - start_value) * 100.0 / start_value) DESC;
END;

-- Stored Procedure 3: Alert System for Performance Issues
CREATE PROCEDURE sp_performance_alerts
    @alert_threshold_pct DECIMAL(5,2) = 25.0
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        'PERFORMANCE_ALERT' as alert_type,
        p.province_name,
        pr.procedure_name,
        t.fiscal_year,
        f.result_value as actual_wait_days,
        b.benchmark_value as benchmark_days,
        CAST((f.result_value - b.benchmark_value) * 100.0 / b.benchmark_value AS DECIMAL(6,2)) as percent_over_benchmark,
        CASE 
            WHEN (f.result_value - b.benchmark_value) * 100.0 / b.benchmark_value > 50 THEN 'CRITICAL'
            WHEN (f.result_value - b.benchmark_value) * 100.0 / b.benchmark_value > @alert_threshold_pct THEN 'HIGH'
            ELSE 'MODERATE'
        END as severity_level,
        f.volume_cases,
        GETDATE() as alert_timestamp
    FROM fact_wait_times f
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    JOIN ref_benchmarks b ON f.procedure_id = b.procedure_id AND f.metric_id = b.metric_id
    WHERE f.is_benchmark_met = 0
        AND t.fiscal_year = (SELECT MAX(fiscal_year) FROM dim_time_periods)
        AND (f.result_value - b.benchmark_value) * 100.0 / b.benchmark_value > @alert_threshold_pct
        AND p.province_name != 'Canada'
    ORDER BY percent_over_benchmark DESC;
END;

-- SECTION 6: BUSINESS INTELLIGENCE QUERIES
-- ================================================================

-- Query 9: Executive Summary Report
SELECT 
    'EXECUTIVE_SUMMARY' as report_type,
    (SELECT MAX(fiscal_year) FROM dim_time_periods) as reporting_year,
    (SELECT COUNT(DISTINCT province_id) FROM fact_wait_times f 
     JOIN dim_provinces p ON f.province_id = p.province_id 
     WHERE p.province_name != 'Canada') as provinces_in_system,
    (SELECT COUNT(DISTINCT procedure_id) FROM fact_wait_times) as procedures_monitored,
    (SELECT COUNT(*) FROM fact_wait_times f 
     JOIN dim_time_periods t ON f.time_id = t.time_id 
     WHERE t.fiscal_year = (SELECT MAX(fiscal_year) FROM dim_time_periods) 
     AND f.result_value IS NOT NULL) as current_year_measurements,
    (SELECT CAST(COUNT(CASE WHEN is_benchmark_met = 1 THEN 1 END) * 100.0 / 
                 NULLIF(COUNT(CASE WHEN is_benchmark_met IS NOT NULL THEN 1 END), 0) AS DECIMAL(5,2))
     FROM fact_wait_times f 
     JOIN dim_time_periods t ON f.time_id = t.time_id 
     WHERE t.fiscal_year = (SELECT MAX(fiscal_year) FROM dim_time_periods)) as overall_benchmark_compliance;

-- Query 10: Resource Optimization Opportunities
WITH optimization_analysis AS (
    SELECT 
        p.province_name,
        pr.procedure_name,
        AVG(CASE WHEN m.metric_name LIKE '%50th Percentile%' THEN f.result_value END) as avg_wait_days,
        AVG(CASE WHEN m.metric_name = 'Volume' THEN f.result_value END) as avg_volume,
        COUNT(CASE WHEN f.is_benchmark_met = 0 THEN 1 END) as benchmark_failures
    FROM fact_wait_times f
    JOIN dim_provinces p ON f.province_id = p.province_id
    JOIN dim_procedures pr ON f.procedure_id = pr.procedure_id
    JOIN dim_metrics m ON f.metric_id = m.metric_id
    JOIN dim_time_periods t ON f.time_id = t.time_id
    WHERE f.result_value IS NOT NULL
        AND t.fiscal_year >= (SELECT MAX(fiscal_year) FROM dim_time_periods) - 2
        AND p.province_name != 'Canada'
    GROUP BY p.province_name, pr.procedure_name
    HAVING AVG(CASE WHEN m.metric_name = 'Volume' THEN f.result_value END) > 100
)
SELECT 
    province_name,
    procedure_name,
    CAST(avg_wait_days AS DECIMAL(6,1)) as current_wait_days,
    CAST(avg_volume AS DECIMAL(8,0)) as annual_volume,
    benchmark_failures,
    CAST(avg_wait_days * avg_volume * 0.2 AS DECIMAL(10,0)) as potential_patient_days_saved,
    CASE 
        WHEN avg_wait_days * avg_volume > 50000 THEN 'HIGH IMPACT OPPORTUNITY'
        WHEN avg_wait_days * avg_volume > 20000 THEN 'MEDIUM IMPACT OPPORTUNITY'
        ELSE 'LOW IMPACT OPPORTUNITY'
    END as optimization_priority
FROM optimization_analysis
WHERE benchmark_failures > 0
ORDER BY avg_wait_days * avg_volume DESC;

-- QUERY EXECUTION EXAMPLES
-- ================================================================

/*
-- Execute Performance Dashboard for specific province
EXEC sp_performance_dashboard @province_filter = 'Ontario', @year_filter = 2023;

-- Generate 3-year trend analysis
EXEC sp_trend_analysis @analysis_years = 3, @trend_threshold = 10.0;

-- Run performance alerts with 20% threshold
EXEC sp_performance_alerts @alert_threshold_pct = 20.0;
*/