"""Provider registry — loads providers, picks one per fetch in priority order.

Provider *kind* and *priority* are code-level config here (not DB columns) —
this keeps ``proxies`` purely an address book: a row says *where* an upstream
is, the registry says *how* it's picked. Onboarding a new proxy source = a
``seed_provider`` call (addresses) + a registry entry here (kind + priority).

``acquire_any(source)`` picks the provider order two ways:

- **Per-source route** (``source_provider_routes``) — an explicit ordered list
  of provider names for that source, tried in order and *exclusively*. An
  international source routes straight to the international pool instead of
  burning attempts "warming up" through CN egresses that will be banned.
- **Global ladder** — ascending ``priority``, the default when no route row
  exists. The first provider returning a non-``None`` ``Acquisition`` wins.

If nothing yields, it returns ``None`` and the forwarder falls back to direct
egress (ships-dark) — matching the ``circuit.py`` graceful-degradation property:
with no healthy upstream, behavior matches the pre-proxy baseline.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service.providers.base import Acquisition, ProxyProvider
from fd_proxy_service.providers.gost_own import GostOwnProvider
from fd_proxy_service.providers.mihomo import MihomoProvider
from fd_proxy_service.providers.pool_provider import PoolProvider

logger = logging.getLogger(__name__)

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
    # mihomo: the fixed-exit international pool on xinru-server2 (27 ports over
    # 3 exit IPs), reached over Tailscale. Priority 2 — tried once both gost
    # tiers are circuit-exhausted, and the *first* choice for any source with a
    # route row naming it, since CN egresses are what international sources ban.
    (lambda session: MihomoProvider(session), 2),
    # paid-static: externally purchased static endpoints (panel-ops-console),
    # imported via the management API as provider-managed ``proxies`` rows.
    # Priority 3 — tried once every free tier is circuit-exhausted, so paid
    # bandwidth is only spent when nothing else works.
    (lambda session: PoolProvider("paid-static", session, priority=3), 3),
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


def route_for(session: Session, source: str) -> Optional[list[str]]:
    """The ordered provider names configured for ``source``, or None.

    A missing row *and* an empty list both mean "unconfigured" (spec: an empty
    route is treated as absent) — the global ladder then applies. Only a
    non-empty list pins selection.
    """
    from fd_proxy_service.models import SourceProviderRoute  # local: avoid cycle
    row = session.get(SourceProviderRoute, source)
    if row is None:
        return None
    names = [n for n in (row.providers or []) if n]
    return names or None


def acquire_any(source: str, session: Session,
                exclude: Optional[list[int]] = None) -> Optional[Acquisition]:
    """Try providers for ``source``; return the first non-None Acquisition.

    A configured route is exclusive: when every provider in it is exhausted the
    call returns ``None`` rather than spilling onto a provider outside the route.
    Falling back would spend the fetch — and a ban — on exactly the egress the
    route exists to avoid (an international source routed to mihomo must not
    quietly retry through a CN gost). Providers named in a route that are not
    registered are skipped with a warning, so a typo degrades to "fewer options"
    instead of an error on the fetch path.

    Returns None when no provider has a healthy upstream — the forwarder then
    egresses direct (ships-dark).

    ``exclude`` is addr_ids already tried in this caller's fetch loop; passed
    to each provider so a dead proxy is not re-acquired within one fetch
    (Bug 5). None/empty = no exclusion (degrades to today's behavior against
    an old caller that omits the field).
    """
    routed = route_for(session, source)
    if routed is not None:
        by_name = {p.name: p for p in all_providers(session)}
        for name in routed:
            prov = by_name.get(name)
            if prov is None:
                logger.warning("route for %s names unregistered provider %r — skipped",
                               source, name)
                continue
            acq = prov.acquire(source, exclude=exclude)
            if acq is not None:
                return acq
        return None  # route exhausted — deliberately no fallback outside it

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

    ``provider`` is carried on the ``Acquisition``; if it is unknown or the
    provider is gone, fall back to a raw circuit update. The circuit unit is
    resolved from the address row itself. The in-flight slot is deliberately NOT
    released on this fallback: we cannot tell whether the vanished provider had
    reserved one, and leaving it keeps the cap conservative (its TTL reclaims it)
    rather than freeing another holder's slot.
    """
    from fd_proxy_service import circuit, pool
    if addr_id is None:
        return
    prov = get_provider(provider, session) if provider else None
    if prov is not None:
        prov.release(source, addr_id, outcome)
    else:
        circuit.record_outcome(
            source, pool.unit_for_proxy_id(session, addr_id), outcome)


def health_all(session: Session) -> list[dict]:
    """Snapshot of every provider (for ``GET /providers``)."""
    return [prov.health() for prov in all_providers(session)]
