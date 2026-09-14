"""``pool``-kind provider: pick one address per request via circuit + rate-limit.

A pool provider owns N addresses (rows in ``proxies`` with
``proxies.provider == self.name``). For each fetch it delegates to the lifted
``ProxySelector`` to enumerate its addresses, skip any whose circuit is
OPEN/HALF_OPEN/permanent, skip any at its QPS cap, skip any at its address-wide
in-flight cap, and acquire a token on the first selectable one.

Circuits are keyed per *unit* (``circuit:{source}:{unit}``): a plain pool keys
one per address, so a ban on one IP opens only that IP's circuit and the next
fetch rotates away from it. A pool that declares ``provider_meta.exit_ip`` (the
mihomo pool: many ports, one egress) keys per exit IP instead, so a ban takes
the whole group out on the first hit.

This is the generalization of the old ``active_proxies`` loop: same selection
logic, now scoped to one provider's addresses and surfaced through the
``acquire``/``release`` interface so the forwarder is provider-agnostic.

``release`` drives the circuit state machine: a ``ban`` trips CLOSED→OPEN,
``ok`` resets the fail streak / closes HALF_OPEN, ``transient`` is a no-op.
The forwarder classifies the outcome via ``ban_rules`` before calling this.
It also returns the address's in-flight slot, whatever the outcome.
"""
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from fd_proxy_service import circuit, pool, rate_limit
from fd_proxy_service.providers.base import Acquisition, build_upstream_url
from fd_proxy_service.selector import ProxySelector


class PoolProvider:
    """A ``pool``-kind provider: per-request selection across N addresses.

    Subclass (e.g. ``GostOwnProvider``) or instantiate directly with a
    ``name`` matching a ``proxies.provider`` value.
    """

    kind = "pool"

    # Provider-level in-flight default, applied to addresses whose own
    # ``proxies.max_concurrency`` is NULL. None = uncapped. A provider fronting
    # a shared egress (MihomoProvider) sets this so the cap exists even before
    # any per-address column is written.
    default_max_concurrency: Optional[int] = None

    def __init__(self, name: str, session: Session, priority: int = 0):
        self.name = name
        self.session = session
        self.priority = priority
        self._selector = ProxySelector(session)

    def acquire(self, source: str, exclude: Optional[list[int]] = None):
        result = self._selector.select(
            source, provider=self.name, exclude=exclude,
            max_concurrency=self.default_max_concurrency)
        if result is None:
            return None
        addr_id, proxy = result
        return Acquisition(
            upstream_url=build_upstream_url(proxy),
            addr_id=addr_id,
            provider=self.name,
        )

    def release(self, source: str, addr_id, outcome: str) -> None:
        if addr_id is None:
            return  # direct sentinel — no circuit, no in-flight slot
        # The selector reserved a slot against the address; ok/ban/transient all
        # mean the request is over, so the slot always goes back (a crash is
        # covered by the slot's TTL).
        rate_limit.release_inflight(addr_id)
        circuit.record_outcome(
            source, pool.unit_for_proxy_id(self.session, addr_id), outcome)

    def health(self) -> dict:
        addrs = pool.provider_addresses(self.session, provider=self.name)
        return {
            "name": self.name,
            "kind": self.kind,
            "priority": self.priority,
            "address_count": len(addrs),
            "max_concurrency": self.default_max_concurrency,
            "addresses": [_addr_summary(a) for a in addrs],
        }


def _addr_summary(a: Any) -> dict:
    """One address for ``GET /providers``.

    Carries ``unit`` and the raw ``provider_meta`` so an operator can read the
    grouping actually in force (which exit IP this port belongs to, and the
    airport-node snapshot taken at seed time) without logging into the proxy
    host.
    """
    return {
        "id": a.id,
        "label": getattr(a, "label", None),
        "scheme": a.scheme,
        "ip": a.ip,
        "port": a.port,
        "status": a.status,
        "cluster_id": getattr(a, "cluster_id", None),
        "unit": pool.circuit_unit(a),
        "max_concurrency": getattr(a, "max_concurrency", None),
        "provider_meta": getattr(a, "provider_meta", None),
    }

