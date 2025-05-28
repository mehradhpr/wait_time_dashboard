"""
Trend Analysis Module for Wait Time Data
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class TrendAnalyzer:
    """Advanced trend analysis for wait time data"""
    
    def __init__(self):
        self.min_data_points = 3
    
    def analyze_comprehensive_trends(self, df: pd.DataFrame) -> Dict:
        """Perform comprehensive trend analysis across all dimensions"""
        
        results = {
            'provincial_trends': {},
            'procedure_trends': {},
            'national_trends': {},
            'summary_statistics': {}
        }
        
        # Analyze trends by province
        for province in df['province_name'].unique():
            if province != 'Canada':
                prov_data = df[df['province_name'] == province]
                results['provincial_trends'][province] = self._analyze_province_trends(prov_data)
        
        # Analyze trends by procedure
        for procedure in df['procedure_name'].unique():
            proc_data = df[df['procedure_name'] == procedure]
            results['procedure_trends'][procedure] = self._analyze_procedure_trends(proc_data)
        
        # National trends
        national_data = df[df['province_name'] == 'Canada']
        if not national_data.empty:
            results['national_trends'] = self._analyze_national_trends(national_data)
        
        # Summary statistics
        results['summary_statistics'] = self._calculate_trend_summary(results)
        
        return results
    
    def _analyze_province_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze trends for a specific province"""
        trends = {}
        
        for procedure in df['procedure_name'].unique():
            proc_data = df[df['procedure_name'] == procedure]
            median_data = proc_data[proc_data['metric_name'] == '50th Percentile']
            
            if len(median_data) >= self.min_data_points:
                trend_result = self._calculate_trend_metrics(median_data)
                trends[procedure] = trend_result
        
        return trends
    
    def _analyze_procedure_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze trends for a specific procedure across provinces"""
        trends = {}
        
        for province in df['province_name'].unique():
            if province != 'Canada':
                prov_data = df[df['province_name'] == province]
                median_data = prov_data[prov_data['metric_name'] == '50th Percentile']
                
                if len(median_data) >= self.min_data_points:
                    trend_result = self._calculate_trend_metrics(median_data)
                    trends[province] = trend_result
        
        return trends
    
    def _analyze_national_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze national-level trends"""
        trends = {}
        
        for procedure in df['procedure_name'].unique():
            proc_data = df[df['procedure_name'] == procedure]
            median_data = proc_data[proc_data['metric_name'] == '50th Percentile']
            
            if len(median_data) >= self.min_data_points:
                trend_result = self._calculate_trend_metrics(median_data)
                trends[procedure] = trend_result
        
        return trends
    
    def _calculate_trend_metrics(self, data: pd.DataFrame) -> Dict:
        """Calculate comprehensive trend metrics for a dataset"""
        if len(data) < self.min_data_points:
            return {'error': 'Insufficient data points'}
        
        # Sort by year
        data_sorted = data.sort_values('data_year')
        years = data_sorted['data_year'].values.reshape(-1, 1)
        wait_times = data_sorted['indicator_result'].values
        
        # Remove any NaN values
        valid_mask = ~np.isnan(wait_times)
        years_clean = years[valid_mask]
        wait_times_clean = wait_times[valid_mask]
        
        if len(wait_times_clean) < self.min_data_points:
            return {'error': 'Insufficient valid data points'}
        
        # Linear trend
        linear_model = LinearRegression()
        linear_model.fit(years_clean, wait_times_clean)
        linear_pred = linear_model.predict(years_clean)
        linear_r2 = r2_score(wait_times_clean, linear_pred)
        
        # Polynomial trend (degree 2)
        poly_features = PolynomialFeatures(degree=2)
        years_poly = poly_features.fit_transform(years_clean)
        poly_model = LinearRegression()
        poly_model.fit(years_poly, wait_times_clean)
        poly_pred = poly_model.predict(years_poly)
        poly_r2 = r2_score(wait_times_clean, poly_pred)
        
        # Calculate additional metrics
        slope = linear_model.coef_[0]
        intercept = linear_model.intercept_
        
        # Percentage change
        first_year_wait = wait_times_clean[0]
        last_year_wait = wait_times_clean[-1]
        pct_change = ((last_year_wait - first_year_wait) / first_year_wait) * 100
        
        # Volatility (standard deviation of residuals)
        residuals = wait_times_clean - linear_pred
        volatility = np.std(residuals)
        
        # Trend classification
        trend_category = self._classify_trend(slope, linear_r2, pct_change)
        
        return {
            'data_points': len(wait_times_clean),
            'year_range': f"{int(years_clean.min())}-{int(years_clean.max())}",
            'linear_trend': {
                'slope': float(slope),
                'intercept': float(intercept),
                'r_squared': float(linear_r2)
            },
            'polynomial_trend': {
                'r_squared': float(poly_r2)
            },
            'change_metrics': {
                'first_year_wait': float(first_year_wait),
                'last_year_wait': float(last_year_wait),
                'absolute_change': float(last_year_wait - first_year_wait),
                'percent_change': float(pct_change)
            },
            'variability': {
                'volatility': float(volatility),
                'coefficient_of_variation': float(np.std(wait_times_clean) / np.mean(wait_times_clean))
            },
            'trend_classification': trend_category,
            'trend_strength': self._assess_trend_strength(linear_r2, abs(slope))
        }
    
    def _classify_trend(self, slope: float, r_squared: float, pct_change: float) -> str:
        """Classify trend based on slope, R-squared, and percentage change"""
        if r_squared < 0.3:
            return 'No Clear Trend'
        elif abs(slope) < 0.5 and abs(pct_change) < 5:
            return 'Stable'
        elif slope > 0:
            if r_squared > 0.6:
                return 'Strongly Increasing'
            else:
                return 'Moderately Increasing'
        else:
            if r_squared > 0.6:
                return 'Strongly Decreasing'
            else:
                return 'Moderately Decreasing'
    
    def _assess_trend_strength(self, r_squared: float, abs_slope: float) -> str:
        """Assess the strength of the trend"""
        if r_squared < 0.3:
            return 'Weak'
        elif r_squared < 0.6:
            return 'Moderate'
        else:
            return 'Strong'
    
    def _calculate_trend_summary(self, trend_results: Dict) -> Dict:
        """Calculate summary statistics across all trends"""
        all_trends = []
        
        # Collect all trend classifications
        for prov_trends in trend_results['provincial_trends'].values():
            for trend_data in prov_trends.values():
                if 'trend_classification' in trend_data:
                    all_trends.append(trend_data['trend_classification'])
        
        # Count trend types
        trend_counts = pd.Series(all_trends).value_counts().to_dict()
        
        return {
            'total_trends_analyzed': len(all_trends),
            'trend_distribution': trend_counts,
            'improving_trends': len([t for t in all_trends if 'Decreasing' in t]),
            'worsening_trends': len([t for t in all_trends if 'Increasing' in t]),
            'stable_trends': len([t for t in all_trends if 'Stable' in t or 'No Clear' in t])
        }