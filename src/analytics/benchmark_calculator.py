"""
Benchmark Analysis and Calculation Module
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class BenchmarkCalculator:
    """Calculates benchmark compliance and performance metrics"""
    
    # Standard benchmark targets (days)
    BENCHMARK_TARGETS = {
        'Cataract Surgery': 182,  # 6 months
        'Hip Replacement': 182,   # 6 months
        'Knee Replacement': 182,  # 6 months
        'CABG': 14,              # 2 weeks
        'Breast Cancer Surgery': 28,   # 4 weeks
        'Colorectal Cancer Surgery': 28, # 4 weeks
        'Lung Cancer Surgery': 28,      # 4 weeks
        'Prostate Cancer Surgery': 28,  # 4 weeks
        'Bladder Cancer Surgery': 28,   # 4 weeks
        'CT Scan': 30,                  # 1 month
        'MRI Scan': 90,                # 3 months
        'Radiation Therapy': 28,        # 4 weeks
        'Hip Fracture Repair': 2,       # 48 hours
    }
    
    def __init__(self, db_connection=None):
        self.db = db_connection
    
    def calculate_benchmark_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate benchmark compliance for procedures"""
        if df.empty:
            return pd.DataFrame()
        
        results = []
        
        for procedure in df['procedure_name'].unique():
            proc_data = df[df['procedure_name'] == procedure]
            target_days = self.BENCHMARK_TARGETS.get(procedure)
            
            if target_days is None:
                logger.warning(f"No benchmark target defined for {procedure}")
                continue
            
            for province in proc_data['province_name'].unique():
                prov_proc_data = proc_data[proc_data['province_name'] == province]
                
                # Get median wait time
                median_data = prov_proc_data[prov_proc_data['metric_name'] == '50th Percentile']
                if median_data.empty:
                    continue
                
                latest_median = median_data.loc[median_data['data_year'].idxmax()]
                median_wait = latest_median['indicator_result']
                
                # Calculate compliance
                compliance = self._calculate_compliance_score(median_wait, target_days)
                
                # Get volume data
                volume_data = prov_proc_data[prov_proc_data['metric_name'] == 'Volume']
                volume = volume_data['indicator_result'].sum() if not volume_data.empty else 0
                
                results.append({
                    'province_name': province,
                    'procedure_name': procedure,
                    'benchmark_target': target_days,
                    'median_wait_time': median_wait,
                    'benchmark_compliance': compliance,
                    'compliance_category': self._get_compliance_category(compliance),
                    'volume': volume,
                    'data_year': latest_median['data_year']
                })
        
        return pd.DataFrame(results)
    
    def _calculate_compliance_score(self, actual_wait: float, target_wait: float) -> float:
        """Calculate compliance score based on actual vs target wait times"""
        if pd.isna(actual_wait) or target_wait <= 0:
            return 0.0
        
        if actual_wait <= target_wait:
            return 100.0
        else:
            # Sliding scale - compliance decreases as wait time increases beyond target
            return max(0.0, 100.0 * (target_wait / actual_wait))
    
    def _get_compliance_category(self, compliance: float) -> str:
        """Categorize compliance score"""
        if compliance >= 90:
            return 'Excellent'
        elif compliance >= 75:
            return 'Good'
        elif compliance >= 50:
            return 'Fair'
        else:
            return 'Poor'
    
    def generate_benchmark_report(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive benchmark analysis report"""
        benchmark_data = self.calculate_benchmark_compliance(df)
        
        if benchmark_data.empty:
            return {'error': 'No benchmark data available'}
        
        report = {
            'summary': {
                'total_assessments': len(benchmark_data),
                'avg_compliance': benchmark_data['benchmark_compliance'].mean(),
                'procedures_above_90pct': len(benchmark_data[benchmark_data['benchmark_compliance'] >= 90]),
                'procedures_below_50pct': len(benchmark_data[benchmark_data['benchmark_compliance'] < 50])
            },
            'by_category': benchmark_data.groupby('compliance_category').agg({
                'benchmark_compliance': ['count', 'mean'],
                'median_wait_time': 'mean'
            }).round(2).to_dict(),
            'by_procedure': benchmark_data.groupby('procedure_name').agg({
                'benchmark_compliance': 'mean',
                'median_wait_time': 'mean',
                'volume': 'sum'
            }).round(2).to_dict(),
            'detailed_results': benchmark_data.to_dict('records')
        }
        
        return report