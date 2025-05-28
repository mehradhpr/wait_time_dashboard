"""
Main Dashboard Layout Structure
"""

import dash_bootstrap_components as dbc
from dash import html, dcc
from datetime import datetime
from .components import create_metric_card, create_filter_section
from .sidebar import create_sidebar

def create_main_layout():
    """Create the main dashboard layout"""
    return dbc.Container([
        # Header Row
        dbc.Row([
            dbc.Col([
                html.H1("Healthcare Wait Times Analytics", 
                       className="text-primary mb-2"),
                html.P([
                    "Comprehensive analysis of Canadian healthcare wait times (2008-2023). ",
                    html.Small(f"Last updated: {datetime.now().strftime('%B %d, %Y')}", 
                             className="text-muted")
                ], className="lead mb-4")
            ], width=12)
        ]),
        
        # Main Content Row
        dbc.Row([
            # Sidebar
            dbc.Col([
                create_sidebar()
            ], width=12, lg=3, className="mb-4"),
            
            # Main Content Area
            dbc.Col([
                # Summary Cards Row
                html.Div(id="summary-cards", className="mb-4"),
                
                # Filters Section
                html.Div(id="filters-section", className="mb-4"),
                
                # Tab Navigation
                dbc.Tabs([
                    dbc.Tab(label="📊 Overview", tab_id="overview-tab"),
                    dbc.Tab(label="📈 Trends", tab_id="trends-tab"),
                    dbc.Tab(label="🗺️ Provincial Comparison", tab_id="comparison-tab"),
                    dbc.Tab(label="🎯 Benchmark Analysis", tab_id="benchmark-tab"),
                    dbc.Tab(label="💡 Insights", tab_id="insights-tab")
                ], id="main-tabs", active_tab="overview-tab", className="mb-4"),
                
                # Dynamic Content Area
                html.Div(id="tab-content", className="min-vh-50")
            ], width=12, lg=9)
        ]),
        
        # Footer
        html.Hr(className="mt-5"),
        html.Footer([
            dbc.Row([
                dbc.Col([
                    html.P([
                        "Healthcare Wait Times Analytics Dashboard • ",
                        "Built with Python, PostgreSQL, and Dash • ",
                        html.A("View Source", href="#", className="text-decoration-none")
                    ], className="text-center text-muted small mb-0")
                ], width=12)
            ])
        ], className="py-3")
        
    ], fluid=True, className="px-4")

def create_tab_content_layout(tab_id):
    """Create layout for specific tab content"""
    layouts = {
        "overview-tab": create_overview_layout(),
        "trends-tab": create_trends_layout(),
        "comparison-tab": create_comparison_layout(),
        "benchmark-tab": create_benchmark_layout(),
        "insights-tab": create_insights_layout()
    }
    
    return layouts.get(tab_id, html.Div("Select a tab to view content"))

def create_overview_layout():
    """Create overview tab layout"""
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Div(id="overview-distribution-chart")
            ], width=12, lg=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Data Summary"),
                    dbc.CardBody(id="overview-summary-stats")
                ])
            ], width=12, lg=4)
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                html.Div(id="overview-heatmap")
            ], width=12)
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                html.Div(id="overview-trends")
            ], width=12)
        ])
    ])

def create_trends_layout():
    """Create trends tab layout"""
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Div(id="trends-pie-chart")
            ], width=12, md=6),
            dbc.Col([
                html.Div(id="trends-scatter-chart")
            ], width=12, md=6)
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                html.Div(id="trends-detailed-table")
            ], width=12)
        ])
    ])

def create_comparison_layout():
    """Create comparison tab layout"""
    return html.Div([
        html.Div(id="comparison-info-cards", className="mb-4"),
        html.Div(id="comparison-chart")
    ])

def create_benchmark_layout():
    """Create benchmark tab layout"""
    return html.Div([
        html.Div(id="benchmark-summary-cards", className="mb-4"),
        html.Div(id="benchmark-chart")
    ])

def create_insights_layout():
    """Create insights tab layout"""
    return html.Div([
        html.Div(id="insights-content")
    ])