"""
Healthcare Wait Times Interactive Dashboard
Author: Data Analytics Team
Created: 2025-05-27
Description: Dash-based interactive dashboard for wait time analytics
"""

import dash
from dash import dcc, html, Input, Output, callback, dash_table
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, date
import dash_bootstrap_components as dbc
from src.analytics.wait_time_analyzer import WaitTimeAnalyzer
import logging

logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'healthcare_analytics'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'your_password'),
        port=os.getenv('DB_PORT', 5432)
    )

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Healthcare Wait Times Analytics"

# Initialize analytics
db_conn = get_db_connection()
analyzer = WaitTimeAnalyzer(db_conn)

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
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM mv_dashboard_summary")
            summary_data = cursor.fetchall()
            
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
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get provinces
            cursor.execute("SELECT DISTINCT province_name FROM dim_provinces WHERE province_name != 'Canada' ORDER BY province_name")
            provinces = [{'label': row['province_name'], 'value': row['province_name']} for row in cursor.fetchall()]
            
            # Get procedures
            cursor.execute("SELECT DISTINCT procedure_name FROM dim_procedures ORDER BY procedure_name")
            procedures = [{'label': row['procedure_name'], 'value': row['procedure_name']} for row in cursor.fetchall()]
            
            # Get years
            cursor.execute("SELECT DISTINCT data_year FROM fact_wait_times WHERE data_year IS NOT NULL ORDER BY data_year DESC")
            years = [{'label': str(row['data_year']), 'value': row['data_year']} for row in cursor.fetchall()]
            
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
        dbc.Tab(label="Benchmark Analysis", tab_id="benchmark-tab"),
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
    elif active_tab == "benchmark-tab":
        return create_benchmark_content(province, procedure, year_range)
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
        
        # Create visualizations
        # 1. Wait time distribution by procedure
        fig_distribution = px.box(
            df, 
            x='procedure_name', 
            y='wait_time_value',
            title="Wait Time Distribution by Procedure",
            labels={'wait_time_value': 'Wait Time (Days)', 'procedure_name': 'Procedure'}
        )
        fig_distribution.update_xaxis(tickangle=45)
        
        # 2. Provincial comparison heatmap
        if province == 'all':
            pivot_data = df.pivot_table(
                values='wait_time_value', 
                index='province_name', 
                columns='procedure_name', 
                aggfunc='mean'
            )
            
            fig_heatmap = px.imshow(
                pivot_data,
                title="Average Wait Times by Province and Procedure (Days)",
                labels=dict(x="Procedure", y="Province", color="Days"),
                aspect="auto"
            )
        else:
            fig_heatmap = html.Div("Provincial heatmap available when 'All Provinces' is selected")
        
        # 3. Recent trends
        recent_data = df[df['data_year'] >= max(df['data_year']) - 2]
        fig_recent = px.line(
            recent_data.groupby(['data_year', 'procedure_name'])['wait_time_value'].mean().reset_index(),
            x='data_year',
            y='wait_time_value',
            color='procedure_name',
            title="Recent Wait Time Trends (Last 3 Years)",
            labels={'wait_time_value': 'Average Wait Time (Days)', 'data_year': 'Year'}
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_distribution)
                ], width=12)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_heatmap) if not isinstance(fig_heatmap, html.Div) else fig_heatmap
                ], width=12, md=8),
                dbc.Col([
                    html.H5("Data Summary"),
                    html.P(f"Records: {len(df):,}"),
                    html.P(f"Average Wait Time: {df['wait_time_value'].mean():.1f} days"),
                    html.P(f"Median Wait Time: {df['wait_time_value'].median():.1f} days"),
                    html.P(f"Range: {df['wait_time_value'].min():.1f} - {df['wait_time_value'].max():.1f} days"),
                ], width=12, md=4)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_recent)
                ], width=12)
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
        
        # Calculate trends
        trends = analyzer.calculate_trend_analysis(df)
        
        if not trends:
            return html.Div("Insufficient data for trend analysis", className="alert alert-warning")
        
        # Create trend visualization
        trend_summary = []
        for key, trend_data in trends.items():
            trend_summary.append({
                'Province': trend_data['province'],
                'Procedure': trend_data['procedure'],
                'Trend': trend_data['trend_category'],
                'Change (%)': trend_data['percent_change'],
                'R²': trend_data['r_squared'],
                'Current Wait': trend_data['last_year_wait']
            })
        
        trend_df = pd.DataFrame(trend_summary)
        
        # Trend direction pie chart
        trend_counts = trend_df['Trend'].value_counts()
        fig_pie = px.pie(
            values=trend_counts.values,
            names=trend_counts.index,
            title="Distribution of Trend Directions"
        )
        
        # Scatter plot of change vs R-squared
        fig_scatter = px.scatter(
            trend_df,
            x='Change (%)',
            y='R²',
            color='Trend',
            hover_data=['Province', 'Procedure'],
            title="Trend Reliability vs Magnitude of Change"
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_pie)
                ], width=12, md=6),
                dbc.Col([
                    dcc.Graph(figure=fig_scatter)
                ], width=12, md=6)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    html.H5("Detailed Trend Analysis"),
                    dash_table.DataTable(
                        data=trend_df.to_dict('records'),
                        columns=[{"name": i, "id": i, "type": "numeric", "format": {"specifier": ".1f"}} 
                               if i in ['Change (%)', 'R²', 'Current Wait'] 
                               else {"name": i, "id": i} for i in trend_df.columns],
                        sort_action="native",
                        filter_action="native",
                        page_size=10,
                        style_cell={'textAlign': 'left'},
                        style_data_conditional=[
                            {
                                'if': {'filter_query': '{Trend} = Increasing'},
                                'backgroundColor': '#ffebee',
                                'color': 'black',
                            },
                            {
                                'if': {'filter_query': '{Trend} = Decreasing'},
                                'backgroundColor': '#e8f5e8',
                                'color': 'black',
                            }
                        ]
                    )
                ], width=12)
            ])
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
        
        # Add national average line
        fig_comparison.add_hline(
            y=comparison_data['national_average'],
            line_dash="dash",
            line_color="red",
            annotation_text=f"National Average: {comparison_data['national_average']:.1f} days"
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Best Performer"),
                            html.H3(comparison_data['best_province']['name'], className="text-success"),
                            html.P(f"{comparison_data['best_province']['wait_time']:.1f} days")
                        ])
                    ])
                ], width=6, md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("National Average"),
                            html.H3(f"{comparison_data['national_average']:.1f}", className="text-primary"),
                            html.P("days")
                        ])
                    ])
                ], width=6, md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Range"),
                            html.H3(f"{comparison_data['statistics']['range']:.1f}", className="text-warning"),
                            html.P("days")
                        ])
                    ])
                ], width=6, md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Needs Focus"),
                            html.H3(comparison_data['worst_province']['name'], className="text-danger"),
                            html.P(f"{comparison_data['worst_province']['wait_time']:.1f} days")
                        ])
                    ])
                ], width=6, md=3)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_comparison)
                ], width=12)
            ])
        ])
        
    except Exception as e:
        logger.error(f"Error creating comparison content: {e}")
        return html.Div(f"Error loading comparison: {str(e)}", className="alert alert-danger")

def create_benchmark_content(province, procedure, year_range):
    """Create benchmark analysis tab content"""
    try:
        province_filter = None if province == 'all' else province
        benchmark_data = analyzer.benchmark_analysis(province_filter, year_range[1])
        
        if 'error' in benchmark_data:
            return html.Div(f"Error: {benchmark_data['error']}", className="alert alert-warning")
        
        # Create benchmark compliance chart
        df_benchmark = pd.DataFrame(benchmark_data['by_procedure'])
        
        if df_benchmark.empty:
            return html.Div("No benchmark data available", className="alert alert-warning")
        
        fig_benchmark = px.scatter(
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
        fig_benchmark.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="90% Target")
        
        return html.Div([
            # Summary cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Average Compliance"),
                            html.H3(f"{benchmark_data['summary']['avg_compliance']:.1f}%", 
                                   className="text-primary"),
                        ])
                    ])
                ], width=6, md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Excellent (≥90%)"),
                            html.H3(str(benchmark_data['compliance_distribution']['excellent']), 
                                   className="text-success"),
                        ])
                    ])
                ], width=6, md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Poor (<50%)"),
                            html.H3(str(benchmark_data['compliance_distribution']['poor']), 
                                   className="text-danger"),
                        ])
                    ])
                ], width=6, md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Total Procedures"),
                            html.H3(str(benchmark_data['summary']['total_procedures']), 
                                   className="text-info"),
                        ])
                    ])
                ], width=6, md=3)
            ], className="mb-4"),
            
            # Benchmark chart
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_benchmark)
                ], width=12)
            ])
        ])
        
    except Exception as e:
        logger.error(f"Error creating benchmark content: {e}")
        return html.Div(f"Error loading benchmark data: {str(e)}", className="alert alert-danger")

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
    app.run_server(debug=True, host='0.0.0.0', port=8050)