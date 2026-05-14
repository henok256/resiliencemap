"""
Shared fixtures for integration tests.

Each test gets a real PostGIS session wrapped in a rolled-back transaction,
so tests are fully isolated without dropping/recreating tables between runs.
"""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.db.session import Base, get_db
from app.main import app

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://resiliencemap:resiliencemap@localhost:5432/resiliencemap_test",
)


@pytest.fixture(scope="session")
def engine():
    eng = create_engine(DATABASE_URL)
    with eng.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)


@pytest.fixture()
def db(engine):
    """
    Yields a session joined to an outer transaction that is always rolled back,
    giving each test a clean slate without truncating tables.
    """
    with engine.connect() as connection:
        with connection.begin() as transaction:
            session = Session(connection, join_transaction_mode="create_savepoint")
            yield session
            session.close()
        transaction.rollback()


@pytest.fixture()
def client(db):
    """TestClient with the real DB session injected via dependency override."""
    def _override_get_db():
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
