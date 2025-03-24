"""
MiniLake UI - Interactive Data Explorer

A Streamlit application for exploring and visualizing data in MiniLake.
"""

import plotly.express as px
import streamlit as st

from minilake import Config
from minilake.query.execute import QueryExecutor


def get_available_tables(executor):
    """Fetch available tables from the storage."""
    try:
        # List available tables
        try:
            tables = executor.execute_query("SHOW TABLES").values.flatten().tolist()
            if tables:
                return tables
        except Exception:
            pass

        # Fallback to listing some example tables if available
        sample_tables = ["example_table", "delta_table"]
        return sample_tables
    except Exception as e:
        st.error(f"Error fetching tables: {e}")
        return ["No tables found"]


# Setup application
config = Config.from_env()

# Create storage manually by checking minio configuration
executor = QueryExecutor()

# Setup session state to store dataframe between interactions
if "query_result" not in st.session_state:
    st.session_state.query_result = None

# Initialize Streamlit app
st.title("MiniLake Explorer")
st.subheader("Explore and analyze your data with ease")

# Data Source Selection
table_names = get_available_tables(executor)
table_name = st.selectbox("Select a table", table_names)

# SQL Query Input
default_query = ""
if table_name != "No tables found":
    default_query = f"SELECT * FROM {table_name} LIMIT 100"
query = st.text_area("Enter your SQL query here...", value=default_query, height=150)

# Run Query Button
if st.button("Run Query"):
    if query:
        try:
            # Execute query
            df = executor.execute_query(query)
            st.session_state.query_result = df
            st.success("Query executed successfully!")
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.error("Please enter a query")

# Visualization (only if we have query results)
if st.session_state.query_result is not None:
    df = st.session_state.query_result
    st.subheader("Data Visualization")

    # Get column names from the dataframe for axes selection
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    all_cols = df.columns.tolist()

    # Visualization controls
    chart_types = ["Bar Chart", "Line Chart", "Scatter Plot", "Box Plot"]
    chart_type = st.selectbox("Chart Type", chart_types)
    col1, col2 = st.columns(2)

    with col1:
        x_axis = st.selectbox("X-Axis", all_cols, index=0 if all_cols else None)

    with col2:
        y_axis = st.selectbox("Y-Axis", numeric_cols, index=0 if numeric_cols else None)

    # Generate chart
    if st.button("Generate Chart"):
        if x_axis and y_axis:
            try:
                if chart_type == "Bar Chart":
                    fig = px.bar(df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}")
                elif chart_type == "Line Chart":
                    fig = px.line(
                        df, x=x_axis, y=y_axis, title=f"{y_axis} over {x_axis}"
                    )
                elif chart_type == "Scatter Plot":
                    fig = px.scatter(
                        df, x=x_axis, y=y_axis, title=f"{y_axis} vs {x_axis}"
                    )
                elif chart_type == "Box Plot":
                    title = f"Distribution of {y_axis} by {x_axis}"
                    fig = px.box(df, x=x_axis, y=y_axis, title=title)

                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating chart: {e}")
        else:
            st.warning("Please select both X and Y axes")

# Apply custom styling (simplified)
st.markdown(
    """
<style>
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
</style>
""",
    unsafe_allow_html=True,
)
