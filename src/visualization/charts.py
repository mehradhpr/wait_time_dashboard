"""
Visualization Charts Module  
Description: Chart creation utilities split from dashboard
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def create_wait_time_distribution_chart(df: pd.DataFrame, title: str = "Wait Time Distribution by Procedure") -> go.Figure:
    """Create box plot showing wait time distribution"""
    fig = px.box(
        df, 
        x='procedure_name', 
        y='wait_time_value',
        title=title,
        labels={'wait_time_value': 'Wait Time (Days)', 'procedure_name': 'Procedure'}
    )
    fig.update_xaxis(tickangle=45)
    return fig

def create_provincial_heatmap(df: pd.DataFrame, title: str = "Average Wait Times by Province and Procedure") -> go.Figure:
    """Create heatmap comparing provinces and procedures"""
    pivot_data = df.pivot_table(
        values='wait_time_value', 
        index='province_name', 
        columns='procedure_name', 
        aggfunc='mean'
    )
    
    fig = px.imshow(
        pivot_data,
        title=title,
        labels=dict(x="Procedure", y="Province", color="Days"),
        aspect="auto"
    )
    return fig

def create_trend_line_chart(df: pd.DataFrame, title: str = "Wait Time Trends") -> go.Figure:
    """Create line chart showing trends over time"""
    trend_data = df.groupby(['data_year', 'procedure_name'])['wait_time_value'].mean().reset_index()
    
    fig = px.line(
        trend_data,
        x='data_year',
        y='wait_time_value',
        color='procedure_name',
        title=title,
        labels={'wait_time_value': 'Average Wait Time (Days)', 'data_year': 'Year'}
    )
    return fig

def create_provincial_comparison_chart(comparison_data: dict) -> go.Figure:
    """Create provincial comparison bar chart"""
    df_comp = pd.DataFrame(comparison_data['provincial_data'])
    
    fig = px.bar(
        df_comp.sort_values('wait_time'),
        x='province',
        y='wait_time',
        color='performance_category',
        title=f"Provincial Comparison - {comparison_data['procedure']} ({comparison_data['year']})",
        labels={'wait_time': 'Wait Time (Days)', 'province': 'Province'}
    )
    
    # Add national average line
    fig.add_hline(
        y=comparison_data['national_average'],
        line_dash="dash",
        line_color="red",
        annotation_text=f"National Average: {comparison_data['national_average']:.1f} days"
    )
    
    return fig

def create_benchmark_scatter_chart(benchmark_data: dict) -> go.Figure:
    """Create scatter plot for benchmark analysis"""
    df_benchmark = pd.DataFrame(benchmark_data['by_procedure'])
    
    if df_benchmark.empty:
        return go.Figure().add_annotation(text="No benchmark data available")
    
    fig = px.scatter(
        df_benchmark,
        x='median_wait',
        y='compliance',
        size='volume',
        color='category',
        hover_data=['province', 'procedure'],
        title="Benchmark Compliance vs Median Wait Time",
        labels={'median_wait': 'Median Wait Time (Days)', 'compliance': 'Benchmark Compliance (%)'}
    )
    
    # Add benchmark target line
    fig.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="90% Target")
    
    return fig

def create_trend_direction_pie_chart(trend_data: dict) -> go.Figure:
    """Create pie chart showing trend directions"""
    if not trend_data:
        return go.Figure().add_annotation(text="No trend data available")
    
    trend_summary = []
    for key, trend_info in trend_data.items():
        trend_summary.append({
            'Trend': trend_info['trend_category']
        })
    
    trend_df = pd.DataFrame(trend_summary)
    trend_counts = trend_df['Trend'].value_counts()
    
    fig = px.pie(
        values=trend_counts.values,
        names=trend_counts.index,
        title="Distribution of Trend Directions"
    )
    
    return fig

def create_summary_cards_data(db_connection) -> list:
    """Create data for dashboard summary cards"""
    try:
        query = "SELECT * FROM mv_dashboard_summary"
        results = db_connection.execute_query(query)
        
        cards_data = []
        for item in results:
            cards_data.append({
                'metric': item['metric'],
                'value': item['value'],
                'unit': item.get('unit', '')
            })
        
        return cards_data
        
    except Exception as e:
        logger.error(f"Error creating summary cards data: {e}")
        return []

def style_chart_layout(fig: go.Figure, theme: str = 'plotly_white') -> go.Figure:
    """Apply consistent styling to charts"""
    fig.update_layout(
        template=theme,
        title_font_size=16,
        font=dict(size=12),
        margin=dict(l=50, r=50, t=50, b=50)
    )
    return fig

def create_multi_metric_chart(df: pd.DataFrame, metrics: list) -> go.Figure:
    """Create chart with multiple metrics"""
    fig = make_subplots(
        rows=len(metrics), cols=1,
        subplot_titles=metrics,
        shared_xaxis=True
    )
    
    for i, metric in enumerate(metrics, 1):
        metric_data = df[df['metric_name'] == metric]
        if not metric_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=metric_data['data_year'],
                    y=metric_data['wait_time_value'],
                    mode='lines+markers',
                    name=metric
                ),
                row=i, col=1
            )
    
    fig.update_layout(height=300*len(metrics), title_text="Multi-Metric Analysis")
    return fig