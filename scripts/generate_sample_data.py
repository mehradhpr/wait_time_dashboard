"""
Generate Sample Wait Times Data
Creates a sample Excel file for testing when real data is not available
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from config.settings import DATA_CONFIG

def generate_sample_data():
    """Generate sample wait times data"""
    
    provinces = [
        'Alberta', 'British Columbia', 'Manitoba', 'New Brunswick',
        'Newfoundland and Labrador', 'Nova Scotia', 'Ontario',
        'Prince Edward Island', 'Quebec', 'Saskatchewan', 'Canada'
    ]
    
    procedures = [
        'Hip Replacement', 'Knee Replacement', 'Cataract Surgery',
        'CABG', 'CT Scan', 'MRI Scan', 'Radiation Therapy',
        'Breast Cancer Surgery', 'Colorectal Cancer Surgery',
        'Lung Cancer Surgery', 'Prostate Cancer Surgery'
    ]
    
    metrics = ['50th Percentile', '90th Percentile', 'Volume', '% Meeting Benchmark']
    years = range(2008, 2024)
    
    data = []
    
    for year in years:
        for province in provinces:
            for procedure in procedures:
                for metric in metrics:
                    # Generate realistic values based on metric type
                    if metric == '50th Percentile':
                        value = np.random.normal(120, 40)
                        value = max(30, min(300, value))
                    elif metric == '90th Percentile':
                        value = np.random.normal(200, 60)
                        value = max(60, min(500, value))
                    elif metric == 'Volume':
                        value = np.random.poisson(1000) if province != 'Canada' else np.random.poisson(10000)
                    else:  # % Meeting Benchmark
                        value = np.random.beta(7, 3) * 100
                    
                    # Add some missing data randomly
                    if np.random.random() < 0.05:
                        value = None
                    
                    data.append({
                        'Province/territory': province,
                        'Reporting level': 'Provincial' if province != 'Canada' else 'National',
                        'Region': 'N/A',
                        'Indicator': procedure,
                        'Metric': metric,
                        'Data year': year,
                        'Unit of measurement': 'Days' if metric in ['50th Percentile', '90th Percentile'] else 
                                             ('Number of cases' if metric == 'Volume' else 'Proportion'),
                        'Indicator result': value
                    })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to Excel
    output_path = DATA_CONFIG['raw_data_path'] / 'wait_times_data.xlsx'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Add header rows
        header_df = pd.DataFrame([
            ['Canadian Healthcare Wait Times Data'],
            ['Source: Sample Data for Testing']
        ])
        header_df.to_excel(writer, sheet_name='Wait times 2008 to 2023', 
                          index=False, header=False)
        
        # Add main data
        df.to_excel(writer, sheet_name='Wait times 2008 to 2023', 
                   index=False, startrow=2)
    
    print(f"Sample data generated: {output_path}")
    print(f"Records created: {len(df)}")
    
    return output_path

if __name__ == "__main__":
    generate_sample_data()