"""Pytest fixtures for fd-proxy-service: fresh isolated SQLite DB per test.

The management endpoints and the circuit code paths are exercised with Redis
unset (dark mode) — the same graceful-degradation property production relies
on; the cold ``source_proxy_health`` table still records resets.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from fd_proxy_service import db as dbmod
from fd_proxy_service.control.app import create_app
from fd_proxy_service.models import Base


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/proxy_test.db",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def session(engine):
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    s = factory()
    yield s
    s.close()


@pytest.fixture
def client(engine, monkeypatch):
    """Control-plane app wired to the test DB via dependency override."""
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    def _override():
        s = factory()
        try:
            yield s
        finally:
            s.close()

    app = create_app()
    app.dependency_overrides[dbmod.session_dep] = _override
    monkeypatch.delenv("MANAGEMENT_TOKEN", raising=False)
    with TestClient(app) as c:
        yield c
