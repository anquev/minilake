# Minilake

Minilake is a lightweight, Python-based data lake solution in early development. This project aims to provide simple components for data storage, ingestion, and querying with a focus on Delta Lake integration and S3 compatibility.

> **Note**: This project is in early development. The architecture, APIs, and features are subject to change as the project evolves.

## Overview

The project currently provides basic building blocks for:

- Storage with S3/MinIO and Delta Lake support
- Data ingestion for CSV and Parquet files
- Data querying via DuckDB 🦆

## Requirements

- Python 3.12 or higher
- Docker (optional, for containerized deployment with MinIO)

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/anquev/minilake.git
cd minilake
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e ".[dev]"
```

## Basic Configuration

Create a `.env` file in the project root with the following variables:

```env
# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ROOT_USER=your_access_key
MINIO_ROOT_PASSWORD=your_secret_key
MINIO_DEFAULT_BUCKETS=your_bucket
```

## Roadmap

The following features are planned for future development:

1. Unified client interface (probably with duckdb ui)
2. Additional ingestion formats (Excel, JSON)
3. Enhanced FastAPI endpoints for data retrieval
4. Enhanced query capabilities
5. Iceberg table support
...