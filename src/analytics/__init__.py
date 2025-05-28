"""
Analytics Module
Provides statistical analysis and calculations for healthcare wait time data
"""

from .wait_time_analyzer import WaitTimeAnalyzer
from .benchmark_calculator import BenchmarkCalculator
from .statistical_tests import StatisticalTester
from .trend_analysis import TrendAnalyzer

__all__ = [
    'WaitTimeAnalyzer',
    'BenchmarkCalculator', 
    'StatisticalTester',
    'TrendAnalyzer'
]