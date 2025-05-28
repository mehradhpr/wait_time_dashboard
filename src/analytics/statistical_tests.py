"""
Statistical Testing Module for Wait Time Analysis
"""

import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class StatisticalTester:
    """Statistical analysis and hypothesis testing for wait times"""
    
    def __init__(self):
        self.alpha = 0.05  # Significance level
    
    def compare_provinces(self, df: pd.DataFrame, province1: str, province2: str, 
                         procedure: str, years: List[int] = None) -> Dict:
        """Compare wait times between two provinces statistically"""
        
        # Filter data for each province
        p1_data = df[(df['province_name'] == province1) & 
                     (df['procedure_name'] == procedure) & 
                     (df['metric_name'] == '50th Percentile')]
        p2_data = df[(df['province_name'] == province2) & 
                     (df['procedure_name'] == procedure) & 
                     (df['metric_name'] == '50th Percentile')]
        
        if years:
            p1_data = p1_data[p1_data['data_year'].isin(years)]
            p2_data = p2_data[p2_data['data_year'].isin(years)]
        
        if len(p1_data) == 0 or len(p2_data) == 0:
            return {'error': 'Insufficient data for comparison'}
        
        wait_times_1 = p1_data['indicator_result'].dropna()
        wait_times_2 = p2_data['indicator_result'].dropna()
        
        # Perform t-test
        t_stat, p_value = stats.ttest_ind(wait_times_1, wait_times_2)
        
        # Calculate effect size (Cohen's d)
        cohens_d = self._calculate_cohens_d(wait_times_1, wait_times_2)
        
        # Mann-Whitney U test (non-parametric)
        u_stat, u_p_value = stats.mannwhitneyu(wait_times_1, wait_times_2, alternative='two-sided')
        
        return {
            'province1': province1,
            'province2': province2,
            'procedure': procedure,
            'sample_size_p1': len(wait_times_1),
            'sample_size_p2': len(wait_times_2),
            'mean_wait_p1': float(wait_times_1.mean()),
            'mean_wait_p2': float(wait_times_2.mean()),
            'median_wait_p1': float(wait_times_1.median()),
            'median_wait_p2': float(wait_times_2.median()),
            'std_p1': float(wait_times_1.std()),
            'std_p2': float(wait_times_2.std()),
            't_test': {
                't_statistic': float(t_stat),
                'p_value': float(p_value),
                'is_significant': p_value < self.alpha
            },
            'mann_whitney': {
                'u_statistic': float(u_stat),
                'p_value': float(u_p_value),
                'is_significant': u_p_value < self.alpha
            },
            'effect_size': {
                'cohens_d': float(cohens_d),
                'magnitude': self._interpret_effect_size(cohens_d)
            },
            'interpretation': self._interpret_comparison(province1, province2, p_value, cohens_d)
        }
    
    def test_trend_significance(self, df: pd.DataFrame, province: str, procedure: str) -> Dict:
        """Test if a trend is statistically significant"""
        data = df[(df['province_name'] == province) & 
                  (df['procedure_name'] == procedure) & 
                  (df['metric_name'] == '50th Percentile')].copy()
        
        if len(data) < 3:
            return {'error': 'Insufficient data points for trend analysis'}
        
        data = data.sort_values('data_year')
        years = data['data_year'].values
        wait_times = data['indicator_result'].dropna().values
        
        if len(wait_times) < 3:
            return {'error': 'Insufficient non-null data points'}
        
        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(years, wait_times)
        
        # Mann-Kendall test for trend
        mk_stat, mk_p_value = self._mann_kendall_test(wait_times)
        
        return {
            'province': province,
            'procedure': procedure,
            'data_points': len(wait_times),
            'year_range': f"{years.min()}-{years.max()}",
            'linear_regression': {
                'slope': float(slope),
                'r_squared': float(r_value**2),
                'p_value': float(p_value),
                'is_significant': p_value < self.alpha
            },
            'mann_kendall': {
                'statistic': float(mk_stat),
                'p_value': float(mk_p_value),
                'is_significant': mk_p_value < self.alpha
            },
            'trend_interpretation': self._interpret_trend(slope, p_value, r_value**2)
        }
    
    def _calculate_cohens_d(self, group1: pd.Series, group2: pd.Series) -> float:
        """Calculate Cohen's d effect size"""
        n1, n2 = len(group1), len(group2)
        var1, var2 = group1.var(ddof=1), group2.var(ddof=1)
        
        # Pooled standard deviation
        pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
        
        return (group1.mean() - group2.mean()) / pooled_std
    
    def _interpret_effect_size(self, cohens_d: float) -> str:
        """Interpret Cohen's d effect size"""
        abs_d = abs(cohens_d)
        if abs_d < 0.2:
            return 'Small'
        elif abs_d < 0.8:
            return 'Medium'
        else:
            return 'Large'
    
    def _mann_kendall_test(self, data: np.ndarray) -> Tuple[float, float]:
        """Perform Mann-Kendall test for trend"""
        n = len(data)
        s = 0
        
        for i in range(n - 1):
            for j in range(i + 1, n):
                if data[j] > data[i]:
                    s += 1
                elif data[j] < data[i]:
                    s -= 1
        
        # Calculate variance
        var_s = n * (n - 1) * (2 * n + 5) / 18
        
        # Calculate z-statistic
        if s > 0:
            z = (s - 1) / np.sqrt(var_s)
        elif s < 0:
            z = (s + 1) / np.sqrt(var_s)
        else:
            z = 0
        
        # Calculate p-value (two-tailed)
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        return s, p_value
    
    def _interpret_comparison(self, prov1: str, prov2: str, p_value: float, cohens_d: float) -> str:
        """Interpret statistical comparison results"""
        if p_value >= self.alpha:
            return f"No statistically significant difference between {prov1} and {prov2} wait times"
        
        direction = "longer" if cohens_d > 0 else "shorter"
        magnitude = self._interpret_effect_size(cohens_d).lower()
        
        return f"{prov1} has significantly {direction} wait times than {prov2} with {magnitude} effect size"
    
    def _interpret_trend(self, slope: float, p_value: float, r_squared: float) -> str:
        """Interpret trend analysis results"""
        if p_value >= self.alpha:
            return "No statistically significant trend detected"
        
        direction = "increasing" if slope > 0 else "decreasing"
        strength = "strong" if r_squared > 0.7 else "moderate" if r_squared > 0.4 else "weak"
        
        return f"Statistically significant {direction} trend with {strength} correlation (R² = {r_squared:.3f})"
