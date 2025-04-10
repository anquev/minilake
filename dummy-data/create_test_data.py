#!/usr/bin/env python
def create_sample_data():
    """Create and upload sample data to MinIO for testing."""
    import numpy as np
    import pandas as pd

    from minilake.core import MinilakeCore

    np.random.seed(42)
    data = {
        "id": range(1, 101),
        "name": [f"User_{i}" for i in range(1, 101)],
        "age": np.random.randint(18, 80, 100),
        "salary": np.random.normal(50000, 10000, 100).round(2),
        "department": np.random.choice(["HR", "IT", "Sales", "Marketing"], 100),
        "join_date": pd.date_range(start="2020-01-01", periods=100),
    }

    df = pd.DataFrame(data)

    core = MinilakeCore()

    try:
        folder_name = "test-data"
        core.connection.s3_client.put_object(
            Bucket=core.connection.bucket, Key=f"{folder_name}/", Body=""
        )

        parquet_buffer = df.to_parquet()
        core.connection.s3_client.put_object(
            Bucket=core.connection.bucket,
            Key=f"{folder_name}/employees.parquet",
            Body=parquet_buffer,
        )

        print(f"Successfully created test data in {folder_name}/employees.parquet")

    except Exception as e:
        print(f"Error creating test data: {e}")


if __name__ == "__main__":
    create_sample_data()
