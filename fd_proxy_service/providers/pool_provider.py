"""``pool``-kind provider: pick one address per request via circuit + rate-limit.

A pool provider owns N addresses (rows in ``proxies`` with
``proxies.provider == self.name``). For each fetch it delegates to the lifted
``ProxySelector`` to enumerate its addresses, skip any whose circuit is
OPEN/HALF_OPEN/permanent, skip any at its QPS cap, and acquire a rate token on
the first selectable one. Each address has its own circuit
(``circuit:{source}:{addr_id}``), so a ban on one IP opens only that IP's
circuit — the rest stay healthy and the next fetch rotates to them.

This is the generalization of the old ``active_proxies`` loop: same selection
logic, now scoped to one provider's addresses and surfaced through the
``acquire``/``release`` interface so the forwarder is provider-agnostic.

``release`` drives the circuit state machine: a ``ban`` trips CLOSED→OPEN,
``ok`` resets the fail streak / closes HALF_OPEN, ``transient`` is a no-op.
The forwarder classifies the outcome via ``ban_rules`` before calling this.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from fd_proxy_service import circuit, pool
from fd_proxy_service.providers.base import Acquisition, build_upstream_url
from fd_proxy_service.selector import ProxySelector


class PoolProvider:
    """A ``pool``-kind provider: per-request selection across N addresses.

    Subclass (e.g. ``GostOwnProvider``) or instantiate directly with a
    ``name`` matching a ``proxies.provider`` value.
    """

    kind = "pool"

    def __init__(self, name: str, session: Session, priority: int = 0):
        self.name = name
        self.session = session
        self.priority = priority
        self._selector = ProxySelector(session)

    def acquire(self, source: str):
        result = self._selector.select(source, provider=self.name)
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
            return  # direct sentinel — no circuit to update
        circuit.record_outcome(source, addr_id, outcome)

    def health(self) -> dict:
        addrs = pool.provider_addresses(self.session, provider=self.name)
        return {
            "name": self.name,
            "kind": self.kind,
            "priority": self.priority,
            "address_count": len(addrs),
            "addresses": [_addr_summary(a) for a in addrs],
        }


def _addr_summary(a: Any) -> dict:
    return {
        "id": a.id,
        "label": getattr(a, "label", None),
        "scheme": a.scheme,
        "ip": a.ip,
        "port": a.port,
        "status": a.status,
        "cluster_id": getattr(a, "cluster_id", None),
    }
