"""
MiniLake UI - Interactive Data Explorer

A Dash application for exploring and visualizing data in MiniLake.
"""

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, State, dash_table, dcc, html

from minilake import Config
from minilake.query.execute import QueryExecutor


def create_app(config=None):
    """
    Create and configure the Dash application.

    Args:
        config: Optional Config instance. If not provided, a default one will be
        created.

    Returns:
        The configured Dash application.
    """
    if config is None:
        config = Config.from_env()

    executor = QueryExecutor()

    # Dash app
    app = dash.Dash(
        __name__,
        external_stylesheets=[
            "https://fonts.googleapis.com/css2?family=Poppins:wght@400;600&display=swap"
        ],
        meta_tags=[
            {"name": "viewport", "content": "width=device-width, initial-scale=1"}
        ],
    )

    app.layout = html.Div(
        [
            html.Div(
                [
                    html.H1("MiniLake Explorer", className="app-header"),
                    html.P(
                        "Explore and analyze your data with ease",
                        className="app-subheader",
                    ),
                ],
                className="header-container",
            ),
            # Main content
            html.Div(
                [
                    # Left panel for data selection and query
                    html.Div(
                        [
                            html.H3("Data Source"),
                            dcc.Dropdown(
                                id="table-dropdown",
                                placeholder="Select a table",
                                className="dropdown",
                            ),
                            html.Div(
                                [
                                    html.H3("SQL Query"),
                                    dcc.Textarea(
                                        id="query-input",
                                        placeholder="Enter your SQL query here...",
                                        className="query-textarea",
                                        style={"width": "100%", "height": 150},
                                    ),
                                    html.Button(
                                        "Run Query",
                                        id="run-query-button",
                                        className="query-button",
                                    ),
                                ],
                                className="query-container",
                            ),
                        ],
                        className="left-panel",
                    ),
                    # Right panel for results and visualization
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3("Results"),
                                    dcc.Loading(
                                        id="loading-results",
                                        type="circle",
                                        children=[
                                            html.Div(
                                                id="results-container",
                                                className="results-container",
                                            )
                                        ],
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.H3("Visualization"),
                                    html.Div(
                                        [
                                            html.Label("Chart Type"),
                                            dcc.Dropdown(
                                                id="chart-type",
                                                options=[
                                                    {
                                                        "label": "Bar Chart",
                                                        "value": "bar",
                                                    },
                                                    {
                                                        "label": "Line Chart",
                                                        "value": "line",
                                                    },
                                                    {
                                                        "label": "Scatter Plot",
                                                        "value": "scatter",
                                                    },
                                                    {
                                                        "label": "Box Plot",
                                                        "value": "box",
                                                    },
                                                ],
                                                value="bar",
                                                className="dropdown",
                                            ),
                                            html.Div(
                                                [
                                                    html.Div(
                                                        [
                                                            html.Label("X-Axis"),
                                                            dcc.Dropdown(
                                                                id="x-axis",
                                                                className="dropdown",
                                                            ),
                                                        ],
                                                        className="axis-selector",
                                                    ),
                                                    html.Div(
                                                        [
                                                            html.Label("Y-Axis"),
                                                            dcc.Dropdown(
                                                                id="y-axis",
                                                                className="dropdown",
                                                            ),
                                                        ],
                                                        className="axis-selector",
                                                    ),
                                                ],
                                                className="axis-container",
                                            ),
                                        ],
                                        className="chart-controls",
                                    ),
                                    dcc.Loading(
                                        id="loading-chart",
                                        type="circle",
                                        children=[
                                            html.Div(
                                                id="chart-container",
                                                className="chart-container",
                                            )
                                        ],
                                    ),
                                ],
                                className="visualization-container",
                            ),
                        ],
                        className="right-panel",
                    ),
                ],
                className="main-container",
            ),
            dcc.Store(id="query-results"),
        ],
        className="app-container",
    )

    # Load available tables
    @app.callback(
        Output("table-dropdown", "options"), Input("table-dropdown", "search_value")
    )
    def get_available_tables(search_value):
        try:
            # Try to get tables from storage
            # storage = create_storage(config)
            # Placeholder for actual tables
            tables = ["table1", "table2", "table3"]
            return [{"label": table, "value": table} for table in tables]
        except Exception as e:
            # Fallback to hardcoded tables if there's an error
            print(f"Error getting tables: {e}")
            return [{"label": "sample", "value": "sample"}]

    @app.callback(Output("query-input", "value"), Input("table-dropdown", "value"))
    def update_query_input(table_name):
        if table_name:
            return f"SELECT * FROM {table_name} LIMIT 100"
        return ""

    # Run query
    @app.callback(
        Output("query-results", "data"),
        Output("results-container", "children"),
        Input("run-query-button", "n_clicks"),
        State("query-input", "value"),
        prevent_initial_call=True,
    )
    def run_query(n_clicks, query):
        if not query:
            return None, html.Div("Please enter a query", className="error-message")

        try:
            df = executor.execute_query(query)

            # Create data table
            table = dash_table.DataTable(
                id="results-table",
                columns=[{"name": i, "id": i} for i in df.columns],
                data=df.to_dict("records"),
                filter_action="native",
                sort_action="native",
                page_size=10,
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#f4f6f9",
                    "fontWeight": "bold",
                    "border": "1px solid #ddd",
                },
                style_cell={"padding": "8px", "border": "1px solid #ddd"},
                style_data_conditional=[
                    {"if": {"row_index": "odd"}, "backgroundColor": "#f9f9f9"}
                ],
            )

            return df.to_json(date_format="iso", orient="split"), table
        except Exception as e:
            return None, html.Div(f"Error: {e!s}", className="error-message")

    @app.callback(
        [
            Output("x-axis", "options"),
            Output("y-axis", "options"),
            Output("x-axis", "value"),
            Output("y-axis", "value"),
        ],
        Input("query-results", "data"),
    )
    def update_axis_options(json_data):
        if not json_data:
            return [], [], None, None

        df = pd.read_json(json_data, orient="split")
        options = [{"label": col, "value": col} for col in df.columns]

        numeric_cols = df.select_dtypes(include=["number"]).columns
        x_default = df.columns[0] if len(df.columns) > 0 else None
        y_default = numeric_cols[0] if len(numeric_cols) > 0 else None

        return options, options, x_default, y_default

    @app.callback(
        Output("chart-container", "children"),
        [
            Input("chart-type", "value"),
            Input("x-axis", "value"),
            Input("y-axis", "value"),
            Input("query-results", "data"),
        ],
    )
    def update_chart(chart_type, x_axis, y_axis, json_data):
        if not json_data or not x_axis or not y_axis:
            return html.Div(
                "Please run a query and select axes", className="placeholder-message"
            )

        df = pd.read_json(json_data, orient="split")

        try:
            if chart_type == "bar":
                fig = px.bar(df, x=x_axis, y=y_axis)
            elif chart_type == "line":
                fig = px.line(df, x=x_axis, y=y_axis)
            elif chart_type == "scatter":
                fig = px.scatter(df, x=x_axis, y=y_axis)
            elif chart_type == "box":
                fig = px.box(df, x=x_axis, y=y_axis)

            return dcc.Graph(figure=fig)
        except Exception as e:
            return html.Div(f"Error creating chart: {e!s}", className="error-message")

    app.index_string = """
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>MiniLake Explorer</title>
            {%favicon%}
            {%css%}
            <style>
                :root {
                    --primary-color: #4a6fa5;
                    --secondary-color: #47b8e0;
                    --text-color: #333;
                    --light-bg: #f5f7fa;
                    --border-color: #e1e4e8;
                }

                body {
                    font-family: 'Poppins', sans-serif;
                    color: var(--text-color);
                    background-color: var(--light-bg);
                    margin: 0;
                    padding: 0;
                }

                .app-container {
                    max-width: 1400px;
                    margin: 0 auto;
                    padding: 20px;
                }

                .header-container {
                    text-align: center;
                    margin-bottom: 30px;
                    background: linear-gradient(135deg, var(--primary-color),
                    var(--secondary-color));
                    padding: 20px;
                    border-radius: 10px;
                    color: white;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }

                .app-header {
                    margin: 0;
                    font-weight: 600;
                    font-size: 2.5rem;
                }

                .app-subheader {
                    margin: 10px 0 0;
                    font-size: 1.2rem;
                    opacity: 0.9;
                }

                .main-container {
                    display: flex;
                    gap: 20px;
                    flex-wrap: wrap;
                }

                .left-panel {
                    flex: 1;
                    min-width: 300px;
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                }

                .right-panel {
                    flex: 2;
                    min-width: 500px;
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                }

                .dropdown {
                    margin-bottom: 15px;
                    border-radius: 5px;
                    border: 1px solid var(--border-color);
                }

                .query-textarea {
                    border-radius: 5px;
                    border: 1px solid var(--border-color);
                    padding: 10px;
                    font-family: monospace;
                    margin-bottom: 15px;
                    resize: vertical;
                }

                .query-button {
                    background-color: var(--primary-color);
                    color: white;
                    border: none;
                    padding: 10px 20px;
                    border-radius: 5px;
                    cursor: pointer;
                    font-weight: 600;
                    transition: background-color 0.2s;
                }

                .query-button:hover {
                    background-color: var(--secondary-color);
                }

                .results-container {
                    margin-bottom: 20px;
                    overflow-x: auto;
                }

                .visualization-container {
                    margin-top: 30px;
                }

                .chart-controls {
                    margin-bottom: 20px;
                }

                .axis-container {
                    display: flex;
                    gap: 15px;
                    flex-wrap: wrap;
                    margin-top: 15px;
                }

                .axis-selector {
                    flex: 1;
                    min-width: 200px;
                }

                .chart-container {
                    min-height: 400px;
                }

                .error-message {
                    color: #d32f2f;
                    padding: 10px;
                    background-color: #ffebee;
                    border-radius: 5px;
                    margin-top: 10px;
                }

                .placeholder-message {
                    color: #757575;
                    padding: 20px;
                    text-align: center;
                    background-color: #f5f5f5;
                    border-radius: 5px;
                    margin-top: 10px;
                }

                @media (max-width: 900px) {
                    .main-container {
                        flex-direction: column;
                    }

                    .left-panel, .right-panel {
                        min-width: 100%;
                    }
                }
            </style>
        </head>
        <body>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                {%renderer%}
            </footer>
        </body>
    </html>
    """

    return app
