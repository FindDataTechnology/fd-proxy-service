"""Pytest fixtures for fd-proxy-service: fresh isolated SQLite DB per test.

The management endpoints and the circuit code paths are exercised with Redis
unset (dark mode) — the same graceful-degradation property production relies
on; the cold ``source_proxy_health`` table still records resets. Tests that need
real circuit/limit state ask for the ``fake_redis`` fixture below.
"""
from __future__ import annotations

import fnmatch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from fd_proxy_service import db as dbmod
from fd_proxy_service.control.app import create_app
from fd_proxy_service.models import Base

TEST_MANAGEMENT_TOKEN = "test-management-token"


class _FakeRedis:
    """Minimal in-memory Redis for the circuit + rate-limit callsites.

    Stateful (unlike a ``MagicMock``) so a ``record_outcome`` write is read back
    by the next ``is_selectable`` / ``get_state`` — the property the grouped-
    circuit tests turn on. Implements only what ``circuit.py`` and
    ``rate_limit.py`` call. TTLs are not simulated: ``expire`` is a no-op, so
    the per-second QPS window never rolls over within a test — set
    ``max_qps=0`` (unthrottled) on tests that only care about the in-flight cap.
    """

    def __init__(self):
        self._h: dict[str, dict[str, str]] = {}
        self._kv: dict[str, int] = {}
        self.streams: dict[str, list] = {}

    def ping(self):
        return True

    # ── hashes (circuit state) ──
    def hgetall(self, key):
        return dict(self._h.get(key, {}))

    def hset(self, key, mapping=None, **kwargs):
        if mapping:
            self._h.setdefault(key, {}).update(
                {k: str(v) for k, v in mapping.items()})
        return 0

    def hdel(self, key, *fields):
        h = self._h.get(key, {})
        n = 0
        for f in fields:
            if f in h:
                del h[f]
                n += 1
        return n

    def scan_iter(self, match=None, count=None):
        for k in list(self._h.keys()):
            if match is None or fnmatch.fnmatch(k, match):
                yield k

    # ── counters (per-second QPS + INFLIGHT) ──
    def get(self, key):
        v = self._kv.get(key)
        return None if v is None else str(v)

    def incr(self, key):
        self._kv[key] = self._kv.get(key, 0) + 1
        return self._kv[key]

    def decr(self, key):
        self._kv[key] = self._kv.get(key, 0) - 1
        return self._kv[key]

    def expire(self, key, ttl):
        return True

    def exists(self, key):
        return int(key in self._kv)

    def delete(self, key):
        self._kv.pop(key, None)
        return 1

    # ── streams (outcomes) ──
    def xadd(self, name, fields, maxlen=None, approximate=None):
        self.streams.setdefault(name, []).append(fields)
        return b"0-0"

    def xrevrange(self, name, count=None):
        entries = list(reversed(self.streams.get(name, [])))
        return [(b"0-0", e) for e in (entries[:count] if count else entries)]


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
def fake_redis(monkeypatch):
    """Stateful in-memory Redis wired into circuit + rate_limit.

    Both modules are patched: ``rate_limit`` does ``from
    fd_proxy_service.circuit import _client``, binding its OWN name, so patching
    the circuit module alone would leave the limits hitting the real client.
    """
    from fd_proxy_service import circuit as circuit_mod
    from fd_proxy_service import rate_limit as rl_mod
    fr = _FakeRedis()
    monkeypatch.setattr(circuit_mod, "_client", lambda: fr)
    monkeypatch.setattr(rl_mod, "_client", lambda: fr)
    return fr


@pytest.fixture
def client(engine, monkeypatch):
    """Control-plane app wired to the test DB via dependency override.

    The shared-token guard is fail-closed, so the fixture configures a token and
    sends it by default; tests that exercise the gate override either side."""
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    def _override():
        s = factory()
        try:
            yield s
        finally:
            s.close()

    app = create_app()
    app.dependency_overrides[dbmod.session_dep] = _override
    monkeypatch.setenv("MANAGEMENT_TOKEN", TEST_MANAGEMENT_TOKEN)
    with TestClient(app, headers={"X-Management-Token": TEST_MANAGEMENT_TOKEN}) as c:
        yield c
