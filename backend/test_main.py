import pytest
from fastapi.testclient import TestClient

try:
    from .main import app
except ImportError:
    from main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_version_endpoint(client):
    """Test the version endpoint."""
    response = client.get("/version")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "service" in data
    assert data["service"] == "astroedge-api"


def test_health_endpoint_basic(client):
    """Test the health endpoint returns a response."""
    response = client.get("/health")
    # Since we don't have real Supabase, it should return 503
    # but the endpoint should be reachable
    assert response.status_code in [200, 503]
    data = response.json()
    # Check that we get either healthy response or error
    assert "status" in data or "detail" in data
