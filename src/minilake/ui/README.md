# MiniLake Explorer

A modern web interface for exploring and querying your MiniLake data.

## Features

- Interactive SQL query interface
- Data visualization with charts (bar, line, scatter, box)
- Modern, responsive UI design
- Filter and sort query results
- Automatic chart suggestions based on data types

## Installation

### Using uv (Recommended)

1. Make sure you have uv installed
   ```bash
   # Check if uv is installed
   uv --version
   
   # Install uv if you don't have it
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Install MiniLake with UI dependencies
   ```bash
   # Navigate to the project root
   cd /path/to/minilake
   
   # Create and activate a virtual environment if needed
   uv venv
   source .venv/bin/activate
   
   # Install with UI dependencies
   uv pip install -e ".[ui]"
   ```

### Using pip

```bash
# Navigate to the project root
cd /path/to/minilake

# Create and activate a virtual environment if needed
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install with UI dependencies
pip install -e ".[ui]"
```

## Running the Explorer

After installation, you can run the Explorer:

```bash
# Run with default settings (host: 0.0.0.0, port: 8050)
minilake-explorer

# Run with custom host and port
minilake-explorer --host=localhost --port=8888

# Run in debug mode (useful during development)
minilake-explorer --debug
```

Alternatively, you can run the module directly:

```bash
python -m minilake.ui.run --host=localhost --port=8888 --debug
```

## Troubleshooting

### Connection Issues

If you encounter connection errors when running the Explorer, try the following:

1. Check that your `.env` file is properly set up with the required configuration:
   ```
   MINIO_ENDPOINT=localhost:9000
   MINIO_ROOT_USER=youraccesskey
   MINIO_ROOT_PASSWORD=yoursecretkey
   MINIO_DEFAULT_BUCKETS=yourbucket
   ```

2. If you're still having issues, you can try running a test query directly in Python:
   ```python
   from minilake.core.connection import get_connection
   from minilake.query.execute import QueryExecutor
   
   conn = get_connection()
   executor = QueryExecutor(conn=conn)
   
   # Test with a simple query
   result = executor.execute_query("SELECT 1 AS test")
   print(result)
   ```

## Usage

1. Select a data source from the dropdown
2. Enter a SQL query or use the auto-generated query
3. Click "Run Query" to execute
4. View results in the table
5. Configure visualization settings
6. Explore your data with interactive charts

## Development

### Project Structure

```
minilake/
└── ui/
    ├── __init__.py - Module exports
    ├── app.py - Main Dash application
    └── run.py - CLI entry point
```

### Adding new visualization types

To add a new chart type, update the `chart-type` dropdown options and the `update_chart` callback in `app.py`.

### Discovering Data Sources

The current implementation uses hardcoded table names. To implement automatic table discovery, you'll need to modify the `get_available_tables()` function in `app.py` to query your storage backend for available tables. 