"""
Healthcare Wait Times Analytics Module
Author: Data Analytics Team
Created: 2025-05-27
Description: Core analytics functions for wait time analysis and calculations
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, timedelta
import scipy.stats as stats
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class WaitTimeAnalyzer:
    """Main analytics class for healthcare wait time analysis"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.cache = {}
        
    def get_wait_time_data(self, 
                          province: Optional[str] = None,
                          procedure: Optional[str] = None,
                          start_year: int = 2008,
                          end_year: int = 2023,
                          metric_type: str = '50th Percentile') -> pd.DataFrame:
        """
        Retrieve wait time data with optional filters
        """
        cache_key = f"{province}_{procedure}_{start_year}_{end_year}_{metric_type}"
        
        if cache_key in self.cache:
            logger.info(f"Returning cached data for {cache_key}")
            return self.cache[cache_key]
        
        query = """
        SELECT 
            vtd.province_name,
            vtd.procedure_name,
            vtd.procedure_category,
            vtd.metric_name,
            vtd.data_year,
            vtd.indicator_result as wait_time_value,
            vtd.unit_of_measurement,
            vtd.region
        FROM v_wait_times_detail vtd
        WHERE vtd.data_year BETWEEN %s AND %s
        AND vtd.metric_name = %s
        AND vtd.indicator_result IS NOT NULL
        AND vtd.province_name != 'Canada'
        """
        
        params = [start_year, end_year, metric_type]
        
        if province:
            query += " AND vtd.province_name ILIKE %s"
            params.append(f"%{province}%")
            
        if procedure:
            query += " AND vtd.procedure_name ILIKE %s"
            params.append(f"%{procedure}%")
            
        query += " ORDER BY vtd.province_name, vtd.procedure_name, vtd.data_year"
        
        try:
            with self.db.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
            df = pd.DataFrame(results)
            self.cache[cache_key] = df
            
            logger.info(f"Retrieved {len(df)} wait time records")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving wait time data: {e}")
            raise
    
    def calculate_trend_analysis(self, df: pd.DataFrame) -> Dict:
        """
        Calculate comprehensive trend analysis for wait times
        """
        if df.empty:
            return {'error': 'No data available for trend analysis'}
        
        trends = {}
        
        # Group by province and procedure for individual trend analysis
        for (province, procedure), group in df.groupby(['province_name', 'procedure_name']):
            if len(group) < 3:  # Need at least 3 years for meaningful trend
                continue
                
            group_sorted = group.sort_values('data_year')
            years = group_sorted['data_year'].values
            wait_times = group_sorted['wait_time_value'].values
            
            # Linear regression for trend
            X = years.reshape(-1, 1)
            y = wait_times
            
            model = LinearRegression()
            model.fit(X, y)
            
            # Calculate trend statistics
            slope = model.coef_[0]
            r_squared = r2_score(y, model.predict(X))
            
            # Percentage change from first to last year
            pct_change = ((wait_times[-1] - wait_times[0]) / wait_times[0]) * 100
            
            # Trend classification
            if abs(slope) < 0.5 and r_squared < 0.3:
                trend_category = 'Stable'
            elif slope > 0:
                trend_category = 'Increasing' if r_squared > 0.5 else 'Slightly Increasing'
            else:
                trend_category = 'Decreasing' if r_squared > 0.5 else 'Slightly Decreasing'
            
            trends[f"{province}_{procedure}"] = {
                'province': province,
                'procedure': procedure,
                'years_of_data': len(group_sorted),
                'slope': round(slope, 3),
                'r_squared': round(r_squared, 3),
                'percent_change': round(pct_change, 2),
                'trend_category': trend_category,
                'first_year_wait': wait_times[0],
                'last_year_wait': wait_times[-1],
                'average_wait': round(np.mean(wait_times), 1),
                'volatility': round(np.std(wait_times), 1)
            }
        
        return trends
    
    def benchmark_analysis(self, province: str = None, year: int = 2023) -> Dict:
        """
        Analyze benchmark compliance and performance
        """
        query = """
        SELECT * FROM sp_benchmark_analysis(%s, %s)
        """
        
        try:
            with self.db.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (province, year))
                results = cursor.fetchall()
                
            if not results:
                return {'error': 'No benchmark data available'}
            
            benchmark_df = pd.DataFrame(results)
            
            analysis = {
                'summary': {
                    'total_procedures': len(benchmark_df),
                    'avg_compliance': round(benchmark_df['benchmark_compliance'].mean(), 1),
                    'procedures_above_90pct': len(benchmark_df[benchmark_df['benchmark_compliance'] >= 90]),
                    'procedures_below_50pct': len(benchmark_df[benchmark_df['benchmark_compliance'] < 50])
                },
                'by_procedure': [],
                'compliance_distribution': {
                    'excellent': len(benchmark_df[benchmark_df['compliance_category'] == 'Excellent']),
                    'good': len(benchmark_df[benchmark_df['compliance_category'] == 'Good']),
                    'fair': len(benchmark_df[benchmark_df['compliance_category'] == 'Fair']),
                    'poor': len(benchmark_df[benchmark_df['compliance_category'] == 'Poor'])
                }
            }
            
            # Detailed procedure analysis
            for _, row in benchmark_df.iterrows():
                analysis['by_procedure'].append({
                    'province': row['province_name'],
                    'procedure': row['procedure_name'],
                    'compliance': row['benchmark_compliance'],
                    'median_wait': row['median_wait_time'],
                    'p90_wait': row['p90_wait_time'],
                    'volume': row['total_volume'],
                    'category': row['compliance_category'],
                    'improvement_needed': row['improvement_needed']
                })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in benchmark analysis: {e}")
            return {'error': str(e)}
    
    def provincial_comparison(self, procedure: str, year: int = 2023) -> Dict:
        """
        Compare provincial performance for a specific procedure
        """
        query = """
        SELECT * FROM sp_provincial_comparison(%s, %s, '50th Percentile')
        """
        
        try:
            with self.db.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (procedure, year))
                results = cursor.fetchall()
                
            if not results:
                return {'error': f'No data available for {procedure} in {year}'}
            
            comparison_df = pd.DataFrame(results)
            
            analysis = {
                'procedure': procedure,
                'year': year,
                'national_average': round(comparison_df['national_average'].iloc[0], 1),
                'best_province': {
                    'name': comparison_df.loc[comparison_df['wait_time_days'].idxmin(), 'province_name'],
                    'wait_time': comparison_df['wait_time_days'].min()
                },
                'worst_province': {
                    'name': comparison_df.loc[comparison_df['wait_time_days'].idxmax(), 'province_name'],
                    'wait_time': comparison_df['wait_time_days'].max()
                },
                'provincial_data': [],
                'statistics': {
                    'median': round(comparison_df['wait_time_days'].median(), 1),
                    'std_dev': round(comparison_df['wait_time_days'].std(), 1),
                    'range': round(comparison_df['wait_time_days'].max() - comparison_df['wait_time_days'].min(), 1)
                }
            }
            
            # Add detailed provincial data
            for _, row in comparison_df.iterrows():
                analysis['provincial_data'].append({
                    'province': row['province_name'],
                    'wait_time': row['wait_time_days'],
                    'variance_from_avg': row['variance_from_average'],
                    'percentile_rank': row['percentile_rank'],
                    'performance_category': row['performance_category'],
                    'volume': row['volume_cases']
                })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in provincial comparison: {e}")
            return {'error': str(e)}
    
    def statistical_significance_test(self, province1: str, province2: str, 
                                    procedure: str, years: List[int]) -> Dict:
        """
        Test statistical significance of wait time differences between provinces
        """
        try:
            # Get data for both provinces
            df1 = self.get_wait_time_data(province=province1, procedure=procedure)
            df2 = self.get_wait_time_data(province=province2, procedure=procedure)
            
            # Filter by years
            df1 = df1[df1['data_year'].isin(years)]
            df2 = df2[df2['data_year'].isin(years)]
            
            if len(df1) == 0 or len(df2) == 0:
                return {'error': 'Insufficient data for statistical test'}
            
            wait_times1 = df1['wait_time_value'].values
            wait_times2 = df2['wait_time_value'].values
            
            # Perform t-test
            t_stat, p_value = stats.ttest_ind(wait_times1, wait_times2)
            
            # Effect size (Cohen's d)
            pooled_std = np.sqrt(((len(wait_times1) - 1) * np.var(wait_times1, ddof=1) + 
                                 (len(wait_times2) - 1) * np.var(wait_times2, ddof=1)) / 
                                (len(wait_times1) + len(wait_times2) - 2))
            
            cohens_d = (np.mean(wait_times1) - np.mean(wait_times2)) / pooled_std
            
            # Interpret effect size
            if abs(cohens_d) < 0.2:
                effect_size = 'Small'
            elif abs(cohens_d) < 0.8:
                effect_size = 'Medium'
            else:
                effect_size = 'Large'
            
            return {
                'province1': province1,
                'province2': province2,
                'procedure': procedure,
                'years_tested': years,
                'mean_wait_province1': round(np.mean(wait_times1), 1),
                'mean_wait_province2': round(np.mean(wait_times2), 1),
                'mean_difference': round(np.mean(wait_times1) - np.mean(wait_times2), 1),
                't_statistic': round(t_stat, 3),
                'p_value': round(p_value, 4),
                'is_significant': p_value < 0.05,
                'cohens_d': round(cohens_d, 3),
                'effect_size': effect_size,
                'interpretation': self._interpret_significance_test(p_value, cohens_d, province1, province2)
            }
            
        except Exception as e:
            logger.error(f"Error in statistical significance test: {e}")
            return {'error': str(e)}
    
    def _interpret_significance_test(self, p_value: float, cohens_d: float, 
                                   province1: str, province2: str) -> str:
        """
        Provide interpretation of statistical test results
        """
        if p_value >= 0.05:
            return f"No statistically significant difference found between {province1} and {province2} wait times (p = {p_value:.3f})"
        
        direction = "longer" if cohens_d > 0 else "shorter"
        magnitude = "small" if abs(cohens_d) < 0.5 else "moderate" if abs(cohens_d) < 0.8 else "large"
        
        return f"{province1} has statistically significantly {direction} wait times than {province2} (p = {p_value:.3f}), with a {magnitude} effect size (d = {cohens_d:.2f})"
    
    def generate_insights(self, province: str = None, procedure: str = None) -> Dict:
        """
        Generate automated insights and recommendations
        """
        insights = {
            'key_findings': [],
            'recommendations': [],
            'alerts': []
        }
        
        try:
            # Get recent data for analysis
            recent_data = self.get_wait_time_data(
                province=province, 
                procedure=procedure,
                start_year=2020,
                end_year=2023
            )
            
            if recent_data.empty:
                return {'error': 'No recent data available for insights'}
            
            # Trend analysis insights
            trends = self.calculate_trend_analysis(recent_data)
            
            increasing_trends = [k for k, v in trends.items() if 'Increasing' in v['trend_category']]
            decreasing_trends = [k for k, v in trends.items() if 'Decreasing' in v['trend_category']]
            
            if increasing_trends:
                insights['alerts'].append(f"{len(increasing_trends)} procedure-province combinations show increasing wait times")
                
            if decreasing_trends:
                insights['key_findings'].append(f"{len(decreasing_trends)} procedure-province combinations show improving wait times")
            
            # Performance insights
            latest_year_data = recent_data[recent_data['data_year'] == recent_data['data_year'].max()]
            
            if not latest_year_data.empty:
                avg_wait = latest_year_data['wait_time_value'].mean()
                high_wait_procedures = latest_year_data[latest_year_data['wait_time_value'] > avg_wait * 1.5]
                
                if len(high_wait_procedures) > 0:
                    insights['alerts'].append(f"{len(high_wait_procedures)} procedures have wait times >50% above average")
                    
                    # Specific recommendations
                    for _, proc in high_wait_procedures.head(3).iterrows():
                        insights['recommendations'].append(
                            f"Focus on {proc['procedure_name']} in {proc['province_name']} "
                            f"(current wait: {proc['wait_time_value']:.1f} days)"
                        )
            
            # Benchmark insights
            if procedure:
                benchmark_data = self.benchmark_analysis(province, 2023)
                if 'summary' in benchmark_data:
                    if benchmark_data['summary']['avg_compliance'] < 75:
                        insights['alerts'].append(f"Average benchmark compliance is {benchmark_data['summary']['avg_compliance']:.1f}% - below target")
                        insights['recommendations'].append("Implement targeted improvement initiatives for procedures with <75% benchmark compliance")
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return {'error': str(e)}