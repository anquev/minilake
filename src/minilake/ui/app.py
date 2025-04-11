"""
Minilake UI for data exploration

Based on Streamlit
"""

import subprocess
import time

import streamlit as st

from minilake.core import MinilakeCore
from minilake.core.exceptions import MinilakeConnectionError
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


def start_docker_services():
    """Start Docker Compose services."""
    try:
        subprocess.run(
            ["docker-compose", "up", "-d"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(5)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def main():
    st.set_page_config(
        page_title="Minilake 🌊",
        layout="wide",
    )

    start_docker_services()

    try:
        core = MinilakeCore()

        folders = core.list_s3_folders()

        # Sidebar
        with st.sidebar:
            st.title("Minilake Explorer 🌊")
            st.markdown("---")

            if folders:
                # folder selection
                st.subheader("📂 S3 Folders")
                selected_folder = st.selectbox("Select a folder", folders)

            if selected_folder:
                st.subheader("📊 Tables")
                tables = core.list_tables(selected_folder)
                selected_table = st.selectbox("Select a table", tables)

        if selected_folder and selected_table:
            st.title(f"Table: {selected_table}")

            # Display sample data
            st.subheader("Preview (Top 10 rows)")
            df = core.get_table_preview(
                selected_folder, selected_table
            )  # TODO: Implement this method
            st.dataframe(df.head(10))

            # SQL Query interface
            st.markdown("---")
            st.subheader("🔍 SQL Query")

            query = st.text_area(
                "Write your SQL query:",
                height=150,
                placeholder="SELECT * FROM table_name LIMIT 10",
            )

            if st.button("Run Query ▶️"):
                if query.strip():
                    try:
                        result = core.execute_query(query)
                        st.success("Query executed successfully!")
                        st.dataframe(result)
                    except Exception as e:
                        st.error(f"Error executing query: {e!s}")
        else:
            st.title("Welcome to Minilake 🌊")
            st.write(
                "Please select a folder and table from the sidebar to begin "
                "exploring your data."
            )

    except MinilakeConnectionError as e:
        st.error(f"⚠️ Connection Error: {e!s}")
        st.info("Please check your credentials and try again.")
        return
    except Exception as e:
        st.error(f"⚠️ Unexpected Error: {e!s}")
        return


if __name__ == "__main__":
    main()

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
