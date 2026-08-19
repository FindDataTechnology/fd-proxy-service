"""Database access for fd-proxy-service.

A single engine against the master DB (``PROXY_DATABASE_URL``, fallback
``DATABASE_URL``). The control plane, forwarder, and seed CLI all share this so
there is one connection pool. SQLAlchemy engines are thread-safe and pool
connections, so a module-level cached engine is correct for the uvicorn /
forwarder processes.

The service maps a *subset* of the master DB tables (``proxies``, ``ban_rules``,
``source_rate_limits``, ``source_probes``); ``fd-open-data-mcp`` keeps its own
ORM for the tables the crawler touches (``semantic_observations`` etc.). Two
mapped classes against one physical table do not collide — the crawler never
reads the ``provider``/``provider_meta`` columns added by migration 001, and
``fd-proxy-service`` never touches the crawler's tables.
"""
from __future__ import annotations

import contextlib
import os
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

_ENGINE: Optional[Engine] = None
_SESSION_FACTORY: Optional[sessionmaker] = None


def get_database_url() -> Optional[str]:
    """The master DB URL. ``PROXY_DATABASE_URL`` preferred, ``DATABASE_URL`` fallback."""
    return os.environ.get("PROXY_DATABASE_URL") or os.environ.get("DATABASE_URL")


def get_engine() -> Engine:
    """Lazily create + cache the SQLAlchemy engine against the master DB."""
    global _ENGINE
    if _ENGINE is None:
        url = get_database_url()
        if not url:
            raise RuntimeError(
                "set PROXY_DATABASE_URL (or DATABASE_URL) to the master DB"
            )
        # pool_pre_ping: drop stale conns left by a master DB restart (the
        # control plane is long-lived; a stale pool would 500 on first use).
        _ENGINE = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    return _ENGINE


def get_session_factory() -> sessionmaker:
    """Lazily create + cache the session factory bound to the engine."""
    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        # expire_on_commit=False: providers read Proxy rows after commit (health)
        # and must not get DetachedInstanceError on the cached address list.
        _SESSION_FACTORY = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _SESSION_FACTORY


@contextlib.contextmanager
def get_session() -> Iterator[Session]:
    """Context manager yielding a session (for scripts / the seed CLI)."""
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def session_dep() -> Iterator[Session]:
    """FastAPI dependency (generator) — one session per request, closed after.

    Use as ``session: Session = Depends(session_dep)``. Distinct from
    ``get_session`` (a ``@contextmanager``) because FastAPI's ``Depends`` needs a
    bare generator function, not a wrapped context-manager object.
    """
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()
