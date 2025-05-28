"""
Dashboard Sidebar Layout
"""

import dash_bootstrap_components as dbc
from dash import html, dcc

def create_sidebar():
    """Create dashboard sidebar with navigation and information"""
    return dbc.Card([
        dbc.CardHeader([
            html.H6("Healthcare Analytics", className="mb-0 text-white"),
            html.Small("Wait Times Dashboard", className="text-white-50")
        ], className="bg-primary text-white"),
        dbc.CardBody([
            # Navigation Section
            html.H6("Navigation", className="mb-3"),
            dbc.Nav([
                dbc.NavItem(dbc.NavLink("Overview", href="#overview", className="text-decoration-none")),
                dbc.NavItem(dbc.NavLink("Trends", href="#trends", className="text-decoration-none")),
                dbc.NavItem(dbc.NavLink("Comparisons", href="#comparisons", className="text-decoration-none")),
                dbc.NavItem(dbc.NavLink("Benchmarks", href="#benchmarks", className="text-decoration-none")),
                dbc.NavItem(dbc.NavLink("Insights", href="#insights", className="text-decoration-none"))
            ], vertical=True, className="mb-4"),
            
            html.Hr(),
            
            # Quick Stats Section
            html.H6("Quick Facts", className="mb-3"),
            html.Div([
                html.P([
                    html.Strong("Data Period: "), 
                    "2008-2023"
                ], className="small mb-2"),
                html.P([
                    html.Strong("Provinces: "), 
                    "10 provinces + territories"
                ], className="small mb-2"),
                html.P([
                    html.Strong("Procedures: "), 
                    "15+ medical procedures"
                ], className="small mb-2"),
                html.P([
                    html.Strong("Metrics: "), 
                    "Percentiles, volumes, benchmarks"
                ], className="small mb-0")
            ], className="mb-4"),
            
            html.Hr(),
            
            # Help Section
            html.H6("Help & Info", className="mb-3"),
            dbc.Button("User Guide", color="outline-secondary", size="sm", className="mb-2 w-100"),
            dbc.Button("Export Data", color="outline-primary", size="sm", className="mb-2 w-100"),
            
            html.Hr(),
            
            # Data Source
            html.Small([
                html.Strong("Data Source: "),
                "Canadian Institute for Health Information (CIHI)"
            ], className="text-muted")
        ])
    ], className="h-100")