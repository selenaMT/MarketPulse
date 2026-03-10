from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.main import app, get_db_session
from app.models.user import User
from app.repositories.user_repository import UserRepository
from app.utils.auth import (
    get_password_hash,
    verify_password,
    create_access_token,
    verify_token,
    TokenData,
)


@pytest.fixture
def db_session():
    # use in-memory SQLite for tests
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    session = Session()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(db_session):
    def override_get_db_session():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db_session] = override_get_db_session
    return TestClient(app)


def test_password_hash_and_verify():
    raw = "mysecretpassword"
    hashed = get_password_hash(raw)
    assert hashed != raw
    assert verify_password(raw, hashed)
    assert not verify_password("wrong", hashed)


def test_token_creation_and_verification():
    token = create_access_token({"sub": "alice@example.com"})
    data = verify_token(token)
    assert isinstance(data, TokenData)
    assert data.email == "alice@example.com"


def test_user_repository_crud(db_session):
    repo = UserRepository(db_session)
    # initially no user
    assert repo.get_user_by_email("foo@bar.com") is None
    user = repo.create_user("foo@bar.com", get_password_hash("password"))
    assert user.email == "foo@bar.com"
    assert repo.get_user_by_email("foo@bar.com").id == user.id
    assert repo.get_user_by_id(user.id).email == user.email


def test_register_login_and_protected_routes(client):
    # register a new user
    resp = client.post("/auth/register", json={"email": "test@example.com", "password": "password123"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "test@example.com"

    # duplicate registration should fail
    resp2 = client.post("/auth/register", json={"email": "test@example.com", "password": "password123"})
    assert resp2.status_code == 400

    # login with wrong credentials
    resp = client.post("/auth/login", json={"email": "test@example.com", "password": "bad"})
    assert resp.status_code == 401

    # login with correct credentials
    resp = client.post("/auth/login", json={"email": "test@example.com", "password": "password123"})
    assert resp.status_code == 200
    token = resp.json()["access_token"]
    assert token

    # access protected route
    resp = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert resp.json()["email"] == "test@example.com"

    # invalid token
    resp = client.get("/auth/me", headers={"Authorization": "Bearer badtoken"})
    assert resp.status_code == 401
