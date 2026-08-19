"""Provider-address enumeration + shared pool queries.

The proxy pool is global (not per-source); per-source health is in the Redis
circuit (``circuit:{source}:{addr_id}``). ``scheme='direct'`` (the cluster's
own egress) is ranked first so upstream proxies are only used when direct is
banned - matching the eastmoney scenario where direct worked initially.

Lifted from ``fd_open_data_mcp/proxy/pool.py``. ``active_proxies`` is renamed to
``provider_addresses`` to reflect that rows are now provider-owned (the
``provider`` column added by migration 001). A ``pool``-kind provider calls this
filtered by its own name to enumerate its addresses; the control plane / health
checks call it unfiltered. A backward-compatible ``active_proxies`` alias is
kept so any caller that hasn't switched still works during the overlap.
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
            control plane / ``all_proxies_unhealthy``).
    """
    q = session.query(Proxy).filter(Proxy.status == "active")
    if provider is not None:
        q = q.filter(Proxy.provider == provider)
    rows = q.all()
    direct = [p for p in rows if p.scheme == "direct"]
    others = [p for p in rows if p.scheme != "direct"]
    others.sort(key=lambda p: p.id)
    return direct + others


def active_proxies(session: Session) -> list[Proxy]:
    """Backward-compatible alias for ``provider_addresses(session)``."""
    return provider_addresses(session)


def get_rate_limit(session: Session, source: str) -> Optional[SourceRateLimit]:
    return (
        session.query(SourceRateLimit)
        .filter(SourceRateLimit.source == source)
        .one_or_none()
    )


def all_proxies_unhealthy(session: Session, source: str) -> bool:
    """True iff the source has registered proxies and every one is OPEN/HALF_OPEN
    or permanent (not selectable). When True, the source's accessibility score
    should be floored (source-ranking spec: all proxies banned -> floor). Returns
    False when no proxies are registered (ships-dark / direct)."""
    from fd_proxy_service import circuit  # local import to avoid cycle
    proxies = provider_addresses(session)
    if not proxies:
        return False
    return all(not circuit.is_selectable(source, p.id) for p in proxies)
