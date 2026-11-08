"""Provider-address enumeration + shared pool queries.

The proxy pool is global (not per-source); per-source health is in the Redis
circuit (``circuit:{source}:{unit}`` — see the circuit-unit helpers below for
what a "unit" is). ``scheme='direct'`` (the cluster's
own egress) is ranked first so upstream proxies are only used when direct is
banned - matching the eastmoney scenario where direct worked initially.

Lifted from ``fd_open_data_mcp/proxy/pool.py``. ``active_proxies`` was renamed
to ``provider_addresses`` to reflect that rows are now provider-owned (the
``provider`` column added by migration 001). A ``pool``-kind provider calls this
filtered by its own name to enumerate its addresses; the control plane / health
checks call it unfiltered.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service.models import Proxy, SourceRateLimit


def provider_addresses(session: Session, provider: Optional[str] = None) -> list[Proxy]:
    """Active proxy addresses, ``direct`` first then the rest by id.

    Args:
        provider: if given, only rows owned by this provider
            (``Proxy.provider == provider``). None = all active rows (used by the
            control plane / health checks).
    """
    q = session.query(Proxy).filter(Proxy.status == "active")
    if provider is not None:
        q = q.filter(Proxy.provider == provider)
    rows = q.all()
    direct = [p for p in rows if p.scheme == "direct"]
    others = [p for p in rows if p.scheme != "direct"]
    others.sort(key=lambda p: p.id)
    return direct + others


def get_rate_limit(session: Session, source: str) -> Optional[SourceRateLimit]:
    return (
        session.query(SourceRateLimit)
        .filter(SourceRateLimit.source == source)
        .one_or_none()
    )


# ─── circuit units ───────────────────────────────────────────────────────────
# A circuit is keyed by (source, unit). The unit is what a ban actually
# invalidates: for most providers one address = one egress IP, so the unit is
# the address itself. But a pool can front several addresses through ONE egress
# IP (the mihomo pool: measured 2026-11-08, its 24 live listeners leave through
# 7 IPs, the busiest carrying 6). Keying those
# per-address would make a single ban cost ~24 wasted attempts before the pool
# is exhausted; keying by the exit IP takes the group out on the first ban.
#
# Providers opt in by declaring ``provider_meta.exit_ip``. Everything else
# falls back to str(id) — deliberately NOT ``addr:{id}``, so the Redis keys of
# already-known addresses are byte-identical to the pre-group scheme and their
# live circuit state survives a deploy.

def circuit_unit(proxy) -> str:
    """The circuit unit for an address: its declared shared exit IP, else its id.

    ``str()`` on both branches — the unit is always a string, so it can be used
    directly as the ``circuit:{source}:{unit}`` key suffix.
    """
    meta = getattr(proxy, "provider_meta", None) or {}
    exit_ip = meta.get("exit_ip")
    if exit_ip:
        return str(exit_ip)
    return str(getattr(proxy, "id", proxy))


def unit_for_proxy_id(session: Session, proxy_id) -> str:
    """Resolve a ``proxies.id`` to its circuit unit.

    Falls back to ``str(proxy_id)`` for an unknown row (e.g. an operator
    resetting an id whose row was already deleted) — that keeps the circuit key
    the same one the address used while it existed.
    """
    row = session.get(Proxy, proxy_id)
    return circuit_unit(row) if row is not None else str(proxy_id)


def representative_proxy_id(session: Session, unit: str) -> Optional[int]:
    """Lowest ``proxies.id`` whose unit is ``unit`` — the row the cold
    ``source_proxy_health`` audit table records on behalf of a whole group.

    That table's ``proxy_id`` is NOT NULL under a per-(source, proxy_id) unique
    key, so a group needs one representative id. It is audit-only: selection
    never reads it. None when the unit has no address at all.
    """
    try:
        return int(unit)
    except (TypeError, ValueError):
        pass
    best: Optional[int] = None
    for p in provider_addresses(session):
        if circuit_unit(p) == unit and (best is None or p.id < best):
            best = p.id
    return best

