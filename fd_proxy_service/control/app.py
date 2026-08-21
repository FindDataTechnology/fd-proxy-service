"""Control-plane HTTP API for fd-proxy-service (the ``proxy-control`` server).

A small FastAPI app exposing the acquire/release contract + health/circuit
snapshots. Runs on the master cluster (NodePort 30090); the per-worker forwarder
reuses the same app (see ``forwarder/server.py``).

The acquire/release contract is **stateless across calls**: ``/acquire`` returns
the picked upstream URL + its ``(addr_id, provider)`` "token"; the caller
carries that token back on ``/release`` so the owning provider can drive its
circuit breaker. When no provider has a healthy upstream, ``/acquire`` returns
``upstream_url: null`` — the caller egresses direct (ships-dark, matching
``circuit.py``'s no-redis property).

Why acquire/release and not env-set ``HTTP_PROXY``: the crawler's HTTP client
terminates TLS, so only *it* can see the decrypted response needed to classify
a ban (status 403, body "too many requests"). A dumb TCP-relay forward proxy
on CONNECT cannot inspect HTTPS responses. The acquire/release contract puts
selection at the forwarder and classification at the crawler — both see what
they need. See ``design.md`` (Open Questions) for the forwarder-shape decision.
"""
from __future__ import annotations

from typing import Optional

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from fd_proxy_service import circuit
from fd_proxy_service.db import session_dep
from fd_proxy_service.providers import acquire_any, health_all, release


class AcquireRequest(BaseModel):
    source: str
    # addr_ids already tried in this caller's fetch loop — the forwarder skips
    # them so a dead proxy isn't re-acquired within one fetch (Bug 5). Absent
    # on old callers / empty = no exclusion (graceful with an old forwarder).
    exclude: list[int] = []


class AcquisitionResponse(BaseModel):
    upstream_url: Optional[str]
    addr_id: Optional[int]
    provider: Optional[str]


class ReleaseRequest(BaseModel):
    source: str
    addr_id: Optional[int] = None
    provider: Optional[str] = None
    outcome: str  # ok / transient / ban / blocked


def create_app() -> FastAPI:
    """Build the control-plane FastAPI app.

    A factory (not a module-level app) so the forwarder can construct a second
    instance with forwarder-scoped env without process-global state leaking.
    """
    app = FastAPI(title="fd-proxy-service control", version="0.1.0")

    @app.post("/acquire", response_model=AcquisitionResponse)
    def acquire(req: AcquireRequest, session: Session = Depends(session_dep)) -> AcquisitionResponse:
        acq = acquire_any(req.source, session, exclude=req.exclude)
        if acq is None:
            # Ships-dark: no healthy upstream. Caller egresses direct.
            return AcquisitionResponse(upstream_url=None, addr_id=None, provider=None)
        return AcquisitionResponse(
            upstream_url=acq.upstream_url,
            addr_id=acq.addr_id,
            provider=acq.provider,
        )

    @app.post("/release")
    def rel(req: ReleaseRequest, session: Session = Depends(session_dep)) -> dict:
        release(req.source, req.addr_id, req.outcome, req.provider, session)
        return {"ok": True}

    @app.get("/health")
    def health(session: Session = Depends(session_dep)) -> dict:
        db_ok = "ok"
        try:
            session.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001 - health check must never raise
            db_ok = "down"

        redis_state = "dark"
        try:
            rc = circuit._client()
            if rc is not None:
                rc.ping()
                redis_state = "ok"
        except Exception:  # noqa: BLE001
            redis_state = "down"

        providers = health_all(session)
        status = "ok" if (db_ok == "ok" and redis_state in ("ok", "dark")) else "degraded"
        return {
            "status": status,
            "db": db_ok,
            "redis": redis_state,
            "providers": len(providers),
        }

    @app.get("/providers")
    def providers(session: Session = Depends(session_dep)) -> list[dict]:
        return health_all(session)

    @app.get("/circuits")
    def circuits() -> list[dict]:
        return circuit.all_circuits()

    return app
