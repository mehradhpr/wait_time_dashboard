"""
Healthcare Wait Times Interactive Dashboard
Author: Data Analytics Team
Created: 2025-05-27
Description: Dash-based interactive dashboard for wait time analytics
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import dash
from dash import dcc, html, Input, Output, callback, dash_table
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
import dash_bootstrap_components as dbc
from analytics.wait_time_analyzer import WaitTimeAnalyzer
from config.database import db_manager
from config.settings import APP_CONFIG
import logging

logger = logging.getLogger(__name__)

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Healthcare Wait Times Analytics"

# Initialize analytics
analyzer = WaitTimeAnalyzer(db_manager)

# Define color scheme
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'danger': '#E74C3C',
    'light': '#ECF0F1',
    'dark': '#34495E'
}

def create_header():
    """Create dashboard header"""
    return dbc.Navbar(
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Healthcare Wait Times Analytics", 
                           className="text-white mb-0",
                           style={'fontSize': '1.8rem'})
                ], width=8),
                dbc.Col([
                    html.P(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                          className="text-white-50 mb-0 text-end small")
                ], width=4)
            ], align="center")
        ], fluid=True),
        color=COLORS['primary'],
        dark=True,
        className="mb-4"
    )

def create_summary_cards():
    """Create summary statistics cards"""
    try:
        query = "SELECT * FROM mv_dashboard_summary"
        summary_data = db_manager.execute_query(query)
        
        if not summary_data:
            return html.Div("No summary data available", className="alert alert-warning")
        
        cards = []
        for item in summary_data:
            cards.append(
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(item['value'], className="text-primary mb-1"),
                            html.P(item['metric'], className="mb-0 small text-muted")
                        ])
                    ], className="h-100 shadow-sm")
                ], width=12, md=6, lg=2)
            )
            
        return dbc.Row(cards, className="mb-4")
        
    except Exception as e:
        logger.error(f"Error creating summary cards: {e}")
        return html.Div("Error loading summary data", className="alert alert-danger")

def get_filter_options():
    """Get options for dropdown filters"""
    try:
        # Get provinces
        provinces_query = "SELECT DISTINCT province_name FROM dim_provinces WHERE province_name != 'Canada' ORDER BY province_name"
        provinces_data = db_manager.execute_query(provinces_query)
        provinces = [{'label': row['province_name'], 'value': row['province_name']} for row in provinces_data]
        
        # Get procedures
        procedures_query = "SELECT DISTINCT procedure_name FROM dim_procedures ORDER BY procedure_name"
        procedures_data = db_manager.execute_query(procedures_query)
        procedures = [{'label': row['procedure_name'], 'value': row['procedure_name']} for row in procedures_data]
        
        # Get years
        years_query = "SELECT DISTINCT data_year FROM fact_wait_times WHERE data_year IS NOT NULL ORDER BY data_year DESC"
        years_data = db_manager.execute_query(years_query)
        years = [{'label': str(row['data_year']), 'value': row['data_year']} for row in years_data]
        
        return provinces, procedures, years
        
    except Exception as e:
        logger.error(f"Error getting filter options: {e}")
        return [], [], []

def create_filters():
    """Create filter controls"""
    provinces, procedures, years = get_filter_options()
    
    return dbc.Card([
        dbc.CardHeader(html.H5("Filters", className="mb-0")),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Province/Territory", className="form-label"),
                    dcc.Dropdown(
                        id='province-dropdown',
                        options=[{'label': 'All Provinces', 'value': 'all'}] + provinces,
                        value='all',
                        className="mb-3"
                    )
                ], width=12, md=4),
                dbc.Col([
                    html.Label("Medical Procedure", className="form-label"),
                    dcc.Dropdown(
                        id='procedure-dropdown',
                        options=[{'label': 'All Procedures', 'value': 'all'}] + procedures,
                        value='all',
                        className="mb-3"
                    )
                ], width=12, md=4),
                dbc.Col([
                    html.Label("Year Range", className="form-label"),
                    dcc.RangeSlider(
                        id='year-range-slider',
                        min=2008,
                        max=2023,
                        step=1,
                        marks={year: str(year) for year in range(2008, 2024, 3)},
                        value=[2018, 2023],
                        className="mb-3"
                    )
                ], width=12, md=4)
            ])
        ])
    ], className="mb-4")

# Define the app layout
app.layout = dbc.Container([
    create_header(),
    create_summary_cards(),
    create_filters(),
    
    # Main content tabs
    dbc.Tabs([
        dbc.Tab(label="Overview", tab_id="overview-tab"),
        dbc.Tab(label="Trends", tab_id="trends-tab"),
        dbc.Tab(label="Provincial Comparison", tab_id="comparison-tab"),
        dbc.Tab(label="Insights", tab_id="insights-tab")
    ], id="main-tabs", active_tab="overview-tab", className="mb-4"),
    
    # Content area
    html.Div(id="tab-content"),
    
    # Footer
    html.Hr(),
    html.Footer([
        html.P("Healthcare Wait Times Analytics Dashboard - Built with Python, PostgreSQL, and Dash",
               className="text-center text-muted small")
    ])
], fluid=True)

# Callback for tab content
@app.callback(
    Output('tab-content', 'children'),
    [Input('main-tabs', 'active_tab'),
     Input('province-dropdown', 'value'),
     Input('procedure-dropdown', 'value'),
     Input('year-range-slider', 'value')]
)
def render_tab_content(active_tab, province, procedure, year_range):
    """Render content based on active tab and filters"""
    
    if active_tab == "overview-tab":
        return create_overview_content(province, procedure, year_range)
    elif active_tab == "trends-tab":
        return create_trends_content(province, procedure, year_range)
    elif active_tab == "comparison-tab":
        return create_comparison_content(province, procedure, year_range)
    elif active_tab == "insights-tab":
        return create_insights_content(province, procedure, year_range)
    
    return html.Div("Select a tab to view content")

def create_overview_content(province, procedure, year_range):
    """Create overview tab content"""
    try:
        # Get filtered data
        province_filter = None if province == 'all' else province
        procedure_filter = None if procedure == 'all' else procedure
        
        df = analyzer.get_wait_time_data(
            province=province_filter,
            procedure=procedure_filter,
            start_year=year_range[0],
            end_year=year_range[1]
        )
        
        if df.empty:
            return html.Div("No data available for selected filters", className="alert alert-warning")
        
        # Create simple bar chart showing average wait times by procedure
        avg_by_procedure = df.groupby('procedure_name')['wait_time_value'].mean().reset_index()
        avg_by_procedure = avg_by_procedure.sort_values('wait_time_value', ascending=True)
        
        fig_overview = px.bar(
            avg_by_procedure, 
            x='wait_time_value', 
            y='procedure_name',
            orientation='h',
            title="Average Wait Times by Procedure",
            labels={'wait_time_value': 'Wait Time (Days)', 'procedure_name': 'Procedure'}
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_overview)
                ], width=12, md=8),
                dbc.Col([
                    html.H5("Data Summary"),
                    html.P(f"Records: {len(df):,}"),
                    html.P(f"Average Wait Time: {df['wait_time_value'].mean():.1f} days"),
                    html.P(f"Median Wait Time: {df['wait_time_value'].median():.1f} days"),
                    html.P(f"Range: {df['wait_time_value'].min():.1f} - {df['wait_time_value'].max():.1f} days"),
                ], width=12, md=4)
            ])
        ])
        
    except Exception as e:
        logger.error(f"Error creating overview content: {e}")
        return html.Div(f"Error loading overview: {str(e)}", className="alert alert-danger")

def create_trends_content(province, procedure, year_range):
    """Create trends analysis tab content"""
    try:
        province_filter = None if province == 'all' else province
        procedure_filter = None if procedure == 'all' else procedure
        
        df = analyzer.get_wait_time_data(
            province=province_filter,
            procedure=procedure_filter,
            start_year=year_range[0],
            end_year=year_range[1]
        )
        
        if df.empty:
            return html.Div("No data available for trend analysis", className="alert alert-warning")
        
        # Simple trend chart
        trend_data = df.groupby(['data_year', 'procedure_name'])['wait_time_value'].mean().reset_index()
        
        fig_trend = px.line(
            trend_data,
            x='data_year',
            y='wait_time_value',
            color='procedure_name',
            title="Wait Time Trends Over Time",
            labels={'wait_time_value': 'Average Wait Time (Days)', 'data_year': 'Year'}
        )
        
        return html.Div([
            dcc.Graph(figure=fig_trend)
        ])
        
    except Exception as e:
        logger.error(f"Error creating trends content: {e}")
        return html.Div(f"Error loading trends: {str(e)}", className="alert alert-danger")

def create_comparison_content(province, procedure, year_range):
    """Create provincial comparison tab content"""
    if procedure == 'all':
        return html.Div("Please select a specific procedure for provincial comparison", className="alert alert-info")
    
    try:
        comparison_data = analyzer.provincial_comparison(procedure, year_range[1])
        
        if 'error' in comparison_data:
            return html.Div(f"Error: {comparison_data['error']}", className="alert alert-warning")
        
        # Create comparison chart
        df_comp = pd.DataFrame(comparison_data['provincial_data'])
        
        fig_comparison = px.bar(
            df_comp.sort_values('wait_time'),
            x='province',
            y='wait_time',
            color='performance_category',
            title=f"Provincial Comparison - {procedure} ({year_range[1]})",
            labels={'wait_time': 'Wait Time (Days)', 'province': 'Province'}
        )
        
        return html.Div([
            dcc.Graph(figure=fig_comparison)
        ])
        
    except Exception as e:
        logger.error(f"Error creating comparison content: {e}")
        return html.Div(f"Error loading comparison: {str(e)}", className="alert alert-danger")

def create_insights_content(province, procedure, year_range):
    """Create insights and recommendations tab content"""
    try:
        province_filter = None if province == 'all' else province
        procedure_filter = None if procedure == 'all' else procedure
        
        insights = analyzer.generate_insights(province_filter, procedure_filter)
        
        if 'error' in insights:
            return html.Div(f"Error: {insights['error']}", className="alert alert-warning")
        
        # Create insights layout
        insights_content = []
        
        if insights['key_findings']:
            insights_content.append(
                dbc.Card([
                    dbc.CardHeader(html.H5("Key Findings", className="mb-0 text-success")),
                    dbc.CardBody([
                        html.Ul([html.Li(finding) for finding in insights['key_findings']])
                    ])
                ], className="mb-3")
            )
        
        if insights['recommendations']:
            insights_content.append(
                dbc.Card([
                    dbc.CardHeader(html.H5("Recommendations", className="mb-0 text-primary")),
                    dbc.CardBody([
                        html.Ul([html.Li(rec) for rec in insights['recommendations']])
                    ])
                ], className="mb-3")
            )
        
        if insights['alerts']:
            insights_content.append(
                dbc.Card([
                    dbc.CardHeader(html.H5("Alerts", className="mb-0 text-warning")),
                    dbc.CardBody([
                        html.Ul([html.Li(alert) for alert in insights['alerts']])
                    ])
                ], className="mb-3")
            )
        
        if not insights_content:
            insights_content = [html.Div("No specific insights available for current selection", className="alert alert-info")]
        
        return html.Div(insights_content)
        
    except Exception as e:
        logger.error(f"Error creating insights content: {e}")
        return html.Div(f"Error loading insights: {str(e)}", className="alert alert-danger")

if __name__ == '__main__':
    app.run_server(
        debug=APP_CONFIG['debug'], 
        host=APP_CONFIG['dashboard_host'], 
        port=APP_CONFIG['dashboard_port']
    )