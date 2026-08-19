"""``endpoint``-kind provider: one fixed upstream whose IP rotates upstream-side.

A paid rotating-proxy API (or tor) exposes ONE stable address
(``user:pass@rotate.example.com:8000``) that rotates the egress IP on its own
side, per request or per session. From our side there is a single address — so
a single circuit per ``(source, provider)`` is the correct semantics: the
circuit tracks the *provider as a whole*, not a specific IP (we never see the
underlying IP).

Implementation: the provider owns exactly one ``proxies`` row
(``proxies.provider == self.name``); ``acquire`` returns it iff its circuit is
selectable, ``release`` records the outcome on that single ``addr_id``. The
circuit key ``circuit:{source}:{addr_id}`` is therefore the provider-level
circuit — one address, one circuit. (Using the real ``proxies.id`` keeps the
``int(parts[2])`` parse in ``circuit.open_for_probe`` / ``all_circuits`` valid;
a string addr_id would break that scan.)

A provider whose API offers session-sticky IPs (same IP for N minutes) can be
modeled as an ``endpoint`` provider that, on a ``ban``, rotates the session
handle in ``provider_meta`` — out of scope for this change, but the shape fits.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from sqlalchemy.orm import Session

from fd_proxy_service import circuit, pool
from fd_proxy_service.providers.base import Acquisition, build_upstream_url


class EndpointProvider:
    """A ``endpoint``-kind provider: one rotating-upstream address.

    The address is read from the ``proxies`` row owned by this provider
    (``proxies.provider == self.name``); there should be exactly one active row.
    The address list is cached for ``_CACHE_TTL`` seconds — addresses change
    only on re-seed, so a per-request DB hit is wasteful.
    """

    kind = "endpoint"
    _CACHE_TTL = 60.0

    def __init__(self, name: str, session: Session, priority: int = 10):
        self.name = name
        self.session = session
        self.priority = priority
        self._cache: Optional[tuple[float, list]] = None

    def _addresses(self):
        now = time.time()
        if self._cache and now - self._cache[0] < self._CACHE_TTL:
            return self._cache[1]
        addrs = pool.provider_addresses(self.session, provider=self.name)
        self._cache = (now, addrs)
        return addrs

    def acquire(self, source: str) -> Optional[Acquisition]:
        addrs = self._addresses()
        if not addrs:
            return None  # not seeded — registry tries the next provider
        a = addrs[0]
        if not circuit.is_selectable(source, a.id):
            return None  # provider-level circuit OPEN — try next provider
        return Acquisition(
            upstream_url=build_upstream_url(a),
            addr_id=a.id,
            provider=self.name,
        )

    def release(self, source: str, addr_id, outcome: str) -> None:
        if addr_id is None:
            return
        circuit.record_outcome(source, addr_id, outcome)

    def health(self) -> dict:
        addrs = self._addresses()
        return {
            "name": self.name,
            "kind": self.kind,
            "priority": self.priority,
            "address_count": len(addrs),
            "addresses": [
                {
                    "id": a.id, "label": getattr(a, "label", None),
                    "scheme": a.scheme, "ip": a.ip, "port": a.port,
                    "status": a.status,
                }
                for a in addrs
            ],
        }
