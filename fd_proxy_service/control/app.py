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

import os
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from fd_proxy_service import circuit, management
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


# ── management (panel-ops-console): panel-driven proxy administration ────────
class ImportRequest(BaseModel):
    provider: str = "paid-static"
    # pasted lines ("ip|port|user|pass") — structured endpoints may replace
    # this later; the paste format is the operator's actual workflow
    text: str


class ProxyStatusRequest(BaseModel):
    status: str  # active / retired
    actor: Optional[str] = None


class CircuitResetRequest(BaseModel):
    source: str
    proxy_id: int
    actor: Optional[str] = None


class RateLimitRequest(BaseModel):
    # max_qps 0/null = no forwarder-side throttling for the source
    max_qps: Optional[float] = None
    max_concurrent: Optional[int] = None


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

    # ── management endpoints (panel-ops-console) ─────────────────────────────
    # Mutating operations behind an optional MANAGEMENT_TOKEN (checked when
    # set) — /acquire stays tokenless like today, management is new surface
    # and the control API is exposed on a NodePort.
    def _mgmt_guard(management_token: Optional[str]) -> None:
        expected = os.environ.get("MANAGEMENT_TOKEN")
        if expected and management_token != expected:
            raise HTTPException(status_code=401, detail="invalid management token")

    @app.post("/management/proxies/import")
    def import_proxies(
        req: ImportRequest, session: Session = Depends(session_dep),
        x_management_token: Optional[str] = Header(default=None),
    ) -> dict:
        _mgmt_guard(x_management_token)
        try:
            endpoints = management.parse_endpoint_lines(req.text)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        return management.import_static_endpoints(session, req.provider, endpoints)

    @app.post("/management/proxies/{proxy_id}/status")
    def set_proxy_status(
        proxy_id: int, req: ProxyStatusRequest,
        session: Session = Depends(session_dep),
        x_management_token: Optional[str] = Header(default=None),
    ) -> dict:
        _mgmt_guard(x_management_token)
        try:
            return management.set_proxy_status(session, proxy_id, req.status, req.actor)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    @app.post("/management/circuits/reset")
    def reset_circuit(
        req: CircuitResetRequest, session: Session = Depends(session_dep),
        x_management_token: Optional[str] = Header(default=None),
    ) -> dict:
        _mgmt_guard(x_management_token)
        return management.manual_reset_circuit(session, req.source, req.proxy_id, req.actor)

    @app.get("/management/rate-limits")
    def get_rate_limits(
        session: Session = Depends(session_dep),
        x_management_token: Optional[str] = Header(default=None),
    ) -> list[dict]:
        _mgmt_guard(x_management_token)
        return management.list_rate_limits(session)

    @app.put("/management/rate-limits/{source}")
    def put_rate_limit(
        source: str, req: RateLimitRequest,
        session: Session = Depends(session_dep),
        x_management_token: Optional[str] = Header(default=None),
    ) -> dict:
        _mgmt_guard(x_management_token)
        try:
            return management.set_rate_limit(session, source, req.max_qps, req.max_concurrent)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    return app
