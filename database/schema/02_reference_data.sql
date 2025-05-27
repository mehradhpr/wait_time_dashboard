-- Reference Data Population Script

-- POPULATE REPORTING LEVELS =============================================

INSERT INTO dim_reporting_levels (level_code, level_name, description) VALUES
('PROV', 'Provincial', 'Provincial level reporting'),
('REG', 'Regional', 'Regional health authority level'),
('NAT', 'National', 'National/Canada-wide aggregation');

-- POPULATE PROVINCES =============================================

INSERT INTO dim_provinces (province_code, province_name, region, population_estimate) VALUES
('AB', 'Alberta', 'Western Canada', 4428000),
('BC', 'British Columbia', 'Western Canada', 5214000),
('MB', 'Manitoba', 'Central Canada', 1380000),
('NB', 'New Brunswick', 'Atlantic Canada', 781000),
('NL', 'Newfoundland and Labrador', 'Atlantic Canada', 521000),
('NS', 'Nova Scotia', 'Atlantic Canada', 992000),
('ON', 'Ontario', 'Central Canada', 14800000),
('PE', 'Prince Edward Island', 'Atlantic Canada', 164000),
('QC', 'Quebec', 'Central Canada', 8575000),
('SK', 'Saskatchewan', 'Western Canada', 1180000),
('CA', 'Canada', 'National', 38000000);

-- POPULATE PROCEDURES =============================================

INSERT INTO dim_procedures (procedure_code, procedure_name, procedure_category, description, is_surgery) VALUES
('BLAD_SURG', 'Bladder Cancer Surgery', 'Cancer Surgery', 'Surgical treatment for bladder cancer', TRUE),
('BRST_SURG', 'Breast Cancer Surgery', 'Cancer Surgery', 'Surgical treatment for breast cancer', TRUE),
('CABG', 'CABG', 'Cardiac Surgery', 'Coronary Artery Bypass Graft surgery', TRUE),
('CATA_SURG', 'Cataract Surgery', 'Ophthalmology', 'Surgical removal of cataracts', TRUE),
('COLR_SURG', 'Colorectal Cancer Surgery', 'Cancer Surgery', 'Surgical treatment for colorectal cancer', TRUE),
('CT_SCAN', 'CT Scan', 'Diagnostic Imaging', 'Computed Tomography imaging', FALSE),
('HIP_FRAC', 'Hip Fracture Repair', 'Orthopedic Surgery', 'Emergency repair of hip fractures', TRUE),
('HIP_FRAC_EI', 'Hip Fracture Repair/Emergency and Inpatient', 'Orthopedic Surgery', 'Hip fracture repair including emergency and inpatient care', TRUE),
('HIP_REPL', 'Hip Replacement', 'Orthopedic Surgery', 'Total hip replacement surgery', TRUE),
('KNEE_REPL', 'Knee Replacement', 'Orthopedic Surgery', 'Total knee replacement surgery', TRUE),
('LUNG_SURG', 'Lung Cancer Surgery', 'Cancer Surgery', 'Surgical treatment for lung cancer', TRUE),
('MRI_SCAN', 'MRI Scan', 'Diagnostic Imaging', 'Magnetic Resonance Imaging', FALSE),
('PROST_SURG', 'Prostate Cancer Surgery', 'Cancer Surgery', 'Surgical treatment for prostate cancer', TRUE),
('RAD_THER', 'Radiation Therapy', 'Cancer Treatment', 'Radiation treatment for cancer', FALSE);

-- POPULATE METRICS =============================================

INSERT INTO dim_metrics (metric_code, metric_name, metric_type, unit_of_measurement, description) VALUES
('P50', '50th Percentile', 'percentile', 'Days', 'Median wait time - 50% of patients wait this long or less'),
('P90', '90th Percentile', 'percentile', 'Days', '90% of patients wait this long or less'),
('VOL', 'Volume', 'volume', 'Number of cases', 'Total number of procedures performed'),
('BENCH', '% Meeting Benchmark', 'benchmark', 'Proportion', 'Percentage of cases meeting established wait time targets');

-- CREATE HELPER VIEWS FOR DATA VALIDATION =============================================

-- View to check reference data completeness
CREATE VIEW v_reference_data_summary AS
SELECT 
    'Provinces' as table_name, 
    COUNT(*) as record_count,
    STRING_AGG(province_name, ', ' ORDER BY province_name) as sample_values
FROM dim_provinces
UNION ALL
SELECT 
    'Procedures' as table_name, 
    COUNT(*) as record_count,
    STRING_AGG(procedure_name, ', ' ORDER BY procedure_name) as sample_values
FROM dim_procedures
UNION ALL
SELECT 
    'Metrics' as table_name, 
    COUNT(*) as record_count,
    STRING_AGG(metric_name, ', ' ORDER BY metric_name) as sample_values
FROM dim_metrics
UNION ALL
SELECT 
    'Reporting Levels' as table_name, 
    COUNT(*) as record_count,
    STRING_AGG(level_name, ', ' ORDER BY level_name) as sample_values
FROM dim_reporting_levels;

-- CREATE DATA MAPPING FUNCTIONS =============================================

-- Function to get province ID by name
CREATE OR REPLACE FUNCTION get_province_id(p_province_name VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    province_id_result INTEGER;
BEGIN
    -- Direct match first
    SELECT province_id INTO province_id_result
    FROM dim_provinces 
    WHERE LOWER(province_name) = LOWER(p_province_name);
    
    -- If not found, try partial match
    IF province_id_result IS NULL THEN
        SELECT province_id INTO province_id_result
        FROM dim_provinces 
        WHERE LOWER(province_name) LIKE '%' || LOWER(p_province_name) || '%'
        LIMIT 1;
    END IF;
    
    RETURN province_id_result;
END;
$$ LANGUAGE plpgsql;

-- Function to get procedure ID by name
CREATE OR REPLACE FUNCTION get_procedure_id(p_procedure_name VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    procedure_id_result INTEGER;
BEGIN
    SELECT procedure_id INTO procedure_id_result
    FROM dim_procedures 
    WHERE LOWER(procedure_name) = LOWER(p_procedure_name);
    
    RETURN procedure_id_result;
END;
$$ LANGUAGE plpgsql;

-- Function to get metric ID by name
CREATE OR REPLACE FUNCTION get_metric_id(p_metric_name VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    metric_id_result INTEGER;
BEGIN
    SELECT metric_id INTO metric_id_result
    FROM dim_metrics 
    WHERE LOWER(metric_name) = LOWER(p_metric_name);
    
    RETURN metric_id_result;
END;
$$ LANGUAGE plpgsql;

-- Verify reference data
SELECT * FROM v_reference_data_summary;