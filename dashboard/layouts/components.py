"""
Reusable Dashboard Components
"""

import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.graph_objects as go
from datetime import datetime

def create_metric_card(title, value, unit="", color="primary", icon=None):
    """Create a metric display card"""
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.H4(value, className=f"text-{color} mb-1 fw-bold"),
                html.P([title, html.Small(f" {unit}", className="text-muted")], 
                       className="mb-0 small")
            ])
        ])
    ], className="h-100 shadow-sm")

def create_filter_section(provinces, procedures, years):
    """Create filter controls section"""
    return dbc.Card([
        dbc.CardHeader([
            html.H5("Filters", className="mb-0"),
            html.Small("Select criteria to filter the data", className="text-muted")
        ]),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Province/Territory", className="form-label fw-bold"),
                    dcc.Dropdown(
                        id='province-dropdown',
                        options=[{'label': 'All Provinces', 'value': 'all'}] + 
                               [{'label': p, 'value': p} for p in provinces],
                        value='all',
                        className="mb-3",
                        placeholder="Select province..."
                    )
                ], width=12, md=4),
                dbc.Col([
                    html.Label("Medical Procedure", className="form-label fw-bold"),
                    dcc.Dropdown(
                        id='procedure-dropdown',
                        options=[{'label': 'All Procedures', 'value': 'all'}] + 
                               [{'label': p, 'value': p} for p in procedures],
                        value='all',
                        className="mb-3",
                        placeholder="Select procedure..."
                    )
                ], width=12, md=4),
                dbc.Col([
                    html.Label("Year Range", className="form-label fw-bold"),
                    dcc.RangeSlider(
                        id='year-range-slider',
                        min=min(years) if years else 2008,
                        max=max(years) if years else 2023,
                        step=1,
                        marks={year: str(year) for year in range(2008, 2024, 3)},
                        value=[2018, 2023],
                        className="mb-3"
                    )
                ], width=12, md=4)
            ])
        ])
    ], className="mb-4")

def create_loading_spinner():
    """Create loading spinner component"""
    return dbc.Spinner([
        html.Div("Loading data...", className="text-center p-4")
    ], size="lg", color="primary")

def create_error_alert(message):
    """Create error alert component"""
    return dbc.Alert([
        html.H6("Error Loading Data", className="alert-heading"),
        html.P(message, className="mb-0")
    ], color="danger", className="mb-4")

def create_info_alert(message):
    """Create info alert component"""
    return dbc.Alert([
        html.P(message, className="mb-0")
    ], color="info", className="mb-4")

def create_chart_container(chart_id, title, description=None):
    """Create standardized chart container"""
    return dbc.Card([
        dbc.CardHeader([
            html.H6(title, className="mb-0"),
            html.Small(description, className="text-muted") if description else None
        ]),
        dbc.CardBody([
            dcc.Graph(id=chart_id, className="chart-container")
        ])
    ], className="mb-4")

def create_data_table_container(table_id, title, description=None):
    """Create standardized data table container"""
    return dbc.Card([
        dbc.CardHeader([
            html.H6(title, className="mb-0"),
            html.Small(description, className="text-muted") if description else None
        ]),
        dbc.CardBody([
            html.Div(id=table_id)
        ])
    ], className="mb-4")

# ===================================



# ===================================



# ===================================



# ===================================

