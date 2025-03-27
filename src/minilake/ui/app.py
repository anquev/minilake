"""
Minilake UI for data exploration

Based on Streamlit
"""

import plotly.express as px
import streamlit as st

from minilake import Config
from minilake.core.exceptions import ConfigurationError, StorageError
from minilake.query.execute import QueryExecutor
from minilake.storage.s3 import S3Manager


def verify_s3_connection(config):
    """Verify that S3/MinIO connection is working."""
    try:
        if not config.use_minio:
            return {
                "status": "disabled",
                "message": "S3/MinIO storage is not configured.",
            }

        s3_storage = S3Manager(
            conn=None,
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            bucket=config.minio_bucket,
        )

        # Test listing objects in bucket to verify connection
        s3_storage.s3_client.head_bucket(Bucket=config.minio_bucket)

        return {
            "status": "connected",
            "message": f"Connected to bucket '{config.minio_bucket}' at {
                config.minio_endpoint
            }",
        }
    except Exception as e:
        return {"status": "error", "message": f"S3/MinIO connection error: {e!s}"}


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

# Verify S3 connection status
connection_status = verify_s3_connection(config)

# Display connection status in the sidebar
st.sidebar.header("Storage Connection")
if connection_status["status"] == "connected":
    st.sidebar.success(connection_status["message"])
elif connection_status["status"] == "error":
    st.sidebar.error(connection_status["message"])
    st.sidebar.info("Some features may be limited without storage connection.")
else:
    st.sidebar.info(connection_status["message"])

# Create storage manually by checking minio configuration
try:
    executor = QueryExecutor()
except (ConfigurationError, StorageError) as e:
    st.error(f"Failed to initialize storage: {e}")
    st.info("Please check your S3/MinIO configuration in the .env file.")
    st.stop()

# Setup session state to store dataframe between interactions
if "query_result" not in st.session_state:
    st.session_state.query_result = None

# Init app
st.title("Minilake Explorer")

# Data Source Selection
table_names = get_available_tables(executor)
table_name = st.selectbox("Select a table", table_names)

# SQL Query
default_query = ""
if table_name != "No tables found":
    default_query = f"SELECT * FROM {table_name} LIMIT 100"
query = st.text_area("Enter your SQL query here...", value=default_query, height=150)

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

# Viz
if st.session_state.query_result is not None:
    df = st.session_state.query_result
    st.subheader("Data Visualization")

    # Get column names
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    all_cols = df.columns.tolist()

    chart_types = ["Bar Chart", "Line Chart", "Scatter Plot", "Box Plot"]
    chart_type = st.selectbox("Chart Type", chart_types)
    col1, col2 = st.columns(2)

    with col1:
        x_axis = st.selectbox("X-Axis", all_cols, index=0 if all_cols else None)

    with col2:
        y_axis = st.selectbox("Y-Axis", numeric_cols, index=0 if numeric_cols else None)

    if st.button("Chart"):
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
