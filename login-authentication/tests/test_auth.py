from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_login_success():
    response = client.post("/api/v1/login/", json={"username": "user", "password": "password"})
    assert response.status_code == 200
    assert response.json() == {"status": "success", "message": "Login successful"}

def test_login_failure():
    response = client.post("/api/v1/login/", json={"username": "user", "password": "wrongpassword"})
    assert response.status_code == 401
    assert response.json() == {"status": "error", "message": "Invalid credentials"}
