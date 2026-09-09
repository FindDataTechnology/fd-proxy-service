"""Provider registry — loads providers, picks one per fetch in priority order.

Provider *kind* and *priority* are code-level config here (not DB columns) —
this keeps ``proxies`` purely an address book: a row says *where* an upstream
is, the registry says *how* it's picked. Onboarding a new proxy source = a
``seed_provider`` call (addresses) + a registry entry here (kind + priority).

``acquire_any(source)`` tries providers in ascending ``priority``; the first
that returns a non-``None`` ``Acquisition`` wins. If every provider returns
``None`` (all circuits OPEN, or nothing seeded), it returns ``None`` and the
forwarder falls back to direct egress (ships-dark) — matching the
``circuit.py`` graceful-degradation property: with no healthy upstream,
behavior matches the pre-proxy baseline.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service.providers.base import Acquisition, ProxyProvider
from fd_proxy_service.providers.gost_own import GostOwnProvider
from fd_proxy_service.providers.pool_provider import PoolProvider

# Code-level provider config: (factory, priority). Lower priority = tried
# first. ``factory(session) -> ProxyProvider``. To add a provider, append a
# row here + seed its addresses (seed.seed_provider).
_REGISTRY: list[tuple] = [
    # gost-own: each worker's own cluster gost. Priority 0 — always try the
    # in-cluster forwarder first (zero firewall, zero mesh hops, lowest
    # latency, no 3 Mbps mesh cap).
    (lambda session: GostOwnProvider(session, priority=0), 0),
    # gost-mesh: sibling-server gosts over the private WireGuard mesh
    # (100.64.0.x:30080). Priority 1 — the per-IP-isolation fallback: when
    # gost-own's shared circuit is OPEN (own IP banned), rotate to a healthy
    # sibling-server gost, each with its OWN circuit key. Uses the generic
    # PoolProvider directly (no loopback-rewrite — mesh IPs are directly
    # reachable, unlike gost-own's 127.0.0.1 which needs node-IP rewrite).
    (lambda session: PoolProvider("gost-mesh", session, priority=1), 1),
    # paid-static: externally purchased static endpoints (panel-ops-console),
    # imported via the management API as provider-managed ``proxies`` rows.
    # Priority 2 — tried once both gost tiers are circuit-exhausted, so paid
    # bandwidth is only spent when the free tiers are banned.
    (lambda session: PoolProvider("paid-static", session, priority=2), 2),
    # External ``endpoint`` providers (paid rotating APIs, tor) are appended
    # here at a higher priority so gost-own/gost-mesh are preferred when
    # healthy. They are only tried once both are exhausted — the decoupled-IP
    # fallback the two-tier design exists to provide.
]


def all_providers(session: Session) -> list[ProxyProvider]:
    """Instantiate every registered provider against ``session``."""
    return [factory(session) for factory, _ in _REGISTRY]


def get_provider(name: str, session: Session) -> Optional[ProxyProvider]:
    """Look up a provider by name (None if not registered)."""
    for prov in all_providers(session):
        if prov.name == name:
            return prov
    return None


def acquire_any(source: str, session: Session,
                exclude: Optional[list[int]] = None) -> Optional[Acquisition]:
    """Try providers in priority order; return the first non-None Acquisition.

    Returns None when no provider has a healthy upstream — the forwarder then
    egresses direct (ships-dark).

    ``exclude`` is addr_ids already tried in this caller's fetch loop; passed
    to each provider so a dead proxy is not re-acquired within one fetch
    (Bug 5). None/empty = no exclusion (degrades to today's behavior against
    an old caller that omits the field).
    """
    providers = sorted(all_providers(session), key=lambda p: p.priority)
    for prov in providers:
        acq = prov.acquire(source, exclude=exclude)
        if acq is not None:
            return acq
    return None


def release(
    source: str, addr_id: Optional[int], outcome: str,
    provider: Optional[str], session: Session,
) -> None:
    """Dispatch a release to the owning provider (by name).

    ``provider`` is carried on the ``Acquisition``; if unknown or the provider
    is gone, fall back to the raw circuit update (the circuit key depends only
    on ``addr_id``, not the provider class).
    """
    from fd_proxy_service import circuit
    if addr_id is None:
        return
    prov = get_provider(provider, session) if provider else None
    if prov is not None:
        prov.release(source, addr_id, outcome)
    else:
        circuit.record_outcome(source, addr_id, outcome)


def health_all(session: Session) -> list[dict]:
    """Snapshot of every provider (for ``GET /providers``)."""
    return [prov.health() for prov in all_providers(session)]
