"""Unit tests for API endpoints."""

import pytest
from fastapi.testclient import TestClient

from minilake.api.endpoint.retriever import app


@pytest.fixture
def client():
    """Test for client."""
    return TestClient(app)


def test_retrieve_endpoint(client):
    """Test the retrieve endpoint."""
    response = client.get(
        "/retrieve",
        params={
            "delta_path": "test_table",
            "table_name": "test",
        }
    )
    assert response.status_code == 200
    assert response.json() == {"message": "Data retrieved successfully"}


def test_retrieve_endpoint_with_version(client):
    """Test the retrieve endpoint with version parameter."""
    response = client.get(
        "/retrieve",
        params={
            "delta_path": "test_table",
            "table_name": "test",
            "version": 1
        }
    )
    assert response.status_code == 200
    assert response.json() == {"message": "Data retrieved successfully"}


def test_retrieve_endpoint_with_timestamp(client):
    """Test the retrieve endpoint with timestamp parameter."""
    response = client.get(
        "/retrieve",
        params={
            "delta_path": "test_table",
            "table_name": "test",
            "timestamp": "2024-01-01T00:00:00"
        }
    )
    assert response.status_code == 200
    assert response.json() == {"message": "Data retrieved successfully"}
