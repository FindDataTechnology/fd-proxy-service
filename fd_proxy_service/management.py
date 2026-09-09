"""Management operations behind the control-plane /management endpoints.

(panel-ops-console, spec proxy-service deltas.) The panel calls these via
HTTP; the write paths live HERE — next to the circuit state machine — rather
than in the panel process, so proxy-redis and the cold ``source_proxy_health``
table are updated by ONE writer (design D1: no second writer on the hottest
state we have).

All operations are idempotent or explicitly count no-ops:
- import: duplicate (scheme, ip, port) lines are skipped with a count.
- retire/reactivate: a row already in the target status is a no-op.
- circuit reset: HALF_OPEN is forced whatever the current state; failures
  afterwards re-open through the normal state machine.
- rate-limit edit: upsert; ``max_qps: 0`` means "no forwarder-side throttling"
  (``rate_limit.acquire`` always allows when ``max_qps <= 0``).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service import circuit
from fd_proxy_service.models import Proxy, SourceRateLimit, SourceProxyHealth
from fd_proxy_service.seed import register_provider_address

logger = logging.getLogger(__name__)


# ─── static paid-proxy import ────────────────────────────────────────────────
def parse_endpoint_lines(text: str) -> list[dict]:
    """Parse pasted ``ip|port|user|pass`` lines into endpoint dicts.

    Accepts 2, 3 or 4 pipe-separated fields per line:
      ``ip|port``                       — no auth
      ``ip|port|user|pass``             — basic auth
    Blank lines and ``#`` comments are ignored; surrounding whitespace is
    stripped. ``ip`` may be a hostname. Raises ValueError with the offending
    line number on malformed input so the caller sees which line broke.
    """
    out: list[dict] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) not in (2, 3, 4):
            raise ValueError(f"line {lineno}: expected ip|port|user|pass, got {line!r}")
        try:
            port = int(parts[1])
        except ValueError:
            raise ValueError(f"line {lineno}: port {parts[1]!r} is not an integer") from None
        if not (0 < port < 65536):
            raise ValueError(f"line {lineno}: port {port} out of range")
        auth = f"{parts[2]}:{parts[3]}" if len(parts) == 4 else (parts[2] if len(parts) == 3 else None)
        out.append({"scheme": "http", "ip": parts[0], "port": port, "auth": auth})
    return out


def import_static_endpoints(
    session: Session, provider: str, endpoints: list[dict],
) -> dict:
    """Batch-import static endpoints as provider-managed ``proxies`` rows.

    Idempotent per (scheme, ip, port): an already-known endpoint whose auth
    matches is SKIPPED (counted), one with changed credentials is UPDATED
    in place (also counted separately). Returns
    ``{imported, skipped, updated, rows: [labels]}``.
    """
    existing = {
        (p.scheme, p.ip, p.port): p
        for p in session.query(Proxy).filter(Proxy.provider == provider).all()
    }
    imported = skipped = updated = 0
    labels: list[str] = []
    for ep in endpoints:
        key = (ep["scheme"], ep["ip"], ep["port"])
        row = existing.get(key)
        if row is not None and (row.auth or None) == (ep.get("auth") or None):
            skipped += 1
            continue
        # label is the seed idempotency key; keep it stable per endpoint
        label = f"{provider}-{ep['ip']}:{ep['port']}"
        result = register_provider_address(
            session, provider=provider, scheme=ep["scheme"], ip=ep["ip"],
            port=ep["port"], auth=ep.get("auth"), label=label,
        )
        labels.append(label)
        if result == "created":
            imported += 1
        else:
            updated += 1
    session.commit()
    logger.info("import %s: imported=%d skipped=%d updated=%d",
                provider, imported, skipped, updated)
    return {"imported": imported, "skipped": skipped, "updated": updated, "rows": labels}


# ─── proxy lifecycle: retire / reactivate ────────────────────────────────────
def set_proxy_status(
    session: Session, proxy_id: int, status: str, actor: Optional[str] = None,
) -> dict:
    """Retire (``retired``) or reactivate (``active``) a proxy row.

    Soft state only: retired rows drop out of ``provider_addresses``
    immediately (next acquire), history is preserved, and the action is
    recorded (``retired_at`` + an ops note in ``provider_meta``). Hard DELETE
    is deliberately not offered while circuit-health rows may reference the id.
    """
    if status not in ("active", "retired"):
        raise ValueError(f"invalid status {status!r} (active|retired)")
    row = session.get(Proxy, proxy_id)
    if row is None:
        return {"status": "not_found"}
    if row.status == status:
        return {"status": "unchanged", "proxy_status": status}
    row.status = status
    if status == "retired":
        row.retired_at = datetime.now(timezone.utc)
    else:
        row.retired_at = None
    meta = dict(row.provider_meta or {})
    ops = list(meta.get("ops", []))
    ops.append({"action": status, "actor": actor, "at": datetime.now(timezone.utc).isoformat()})
    meta["ops"] = ops[-10:]  # keep the last 10 actions, actor + timestamp
    row.provider_meta = meta
    session.commit()
    logger.info("proxy %d -> %s (by %s)", proxy_id, status, actor or "(operator)")
    return {"status": "ok", "proxy_status": status}


# ─── manual circuit reset ────────────────────────────────────────────────────
def manual_reset_circuit(
    session: Session, source: str, proxy_id: int, actor: Optional[str] = None,
) -> dict:
    """Force a circuit OPEN/permanent -> HALF_OPEN, Redis hot state AND the
    cold ``source_proxy_health`` row (single writer: this code path, design D1).

    Failures after the reset re-open the circuit via the normal state machine;
    the reset itself is marked manual in the Redis hash.
    """
    st = circuit.manual_reset(source, proxy_id, actor=actor)
    cold = (
        session.query(SourceProxyHealth)
        .filter_by(source=source, proxy_id=proxy_id)
        .one_or_none()
    )
    if cold is None:
        cold = SourceProxyHealth(source=source, proxy_id=proxy_id)
        session.add(cold)
    cold.state = "half_open"
    cold.fail_streak = 0
    cold.cooldown_until = None
    cold.open_cycles = 0
    cold.permanent = False
    session.commit()
    return {"source": source, "proxy_id": proxy_id,
            "state": st["state"], "redis": circuit._client() is not None}


# ─── per-source rate limits ──────────────────────────────────────────────────
def list_rate_limits(session: Session) -> list[dict]:
    rows = session.query(SourceRateLimit).order_by(SourceRateLimit.source).all()
    return [
        {"source": r.source, "max_qps": r.max_qps,
         "max_concurrent": r.max_concurrent,
         "throttled": (r.max_qps or 0) > 0,
         "updated_at": r.updated_at.isoformat() if r.updated_at else None}
        for r in rows
    ]


def set_rate_limit(
    session: Session, source: str, max_qps: Optional[float],
    max_concurrent: Optional[int] = None,
) -> dict:
    """Upsert a source's politeness limits; effective on the next selector
    call (the selector reads the row per request — no redeploy).

    ``max_qps: 0`` / ``null`` disables forwarder-side throttling for the
    source (``rate_limit.acquire`` allows everything when ``max_qps <= 0``).
    """
    if max_qps is not None and max_qps < 0:
        raise ValueError("max_qps must be >= 0 (0 = no throttling)")
    if max_concurrent is not None and max_concurrent < 1:
        raise ValueError("max_concurrent must be >= 1")
    row = session.query(SourceRateLimit).filter_by(source=source).one_or_none()
    if row is None:
        row = SourceRateLimit(source=source, max_qps=max_qps or 0,
                              max_concurrent=max_concurrent or 4)
        session.add(row)
    else:
        if max_qps is not None:
            row.max_qps = max_qps
        if max_concurrent is not None:
            row.max_concurrent = max_concurrent
    session.commit()
    logger.info("rate limit %s: max_qps=%s max_concurrent=%s",
                source, row.max_qps, row.max_concurrent)
    return {"source": source, "max_qps": row.max_qps,
            "max_concurrent": row.max_concurrent,
            "throttled": (row.max_qps or 0) > 0}
