"""ProxyProvider plugin abstraction.

A *provider* owns a set of upstream proxy addresses and the policy for picking
one per fetch. Two kinds:

- ``pool`` — a set of addresses (multiple gost instances, a fleet of
  self-hosted forwarders, a free-proxy list). The provider picks one per
  request via circuit + rate-limit; each address has its own circuit
  (``circuit:{source}:{addr_id}``). Adding a provider = adding a class.
- ``endpoint`` — one fixed upstream address whose IP rotates UPSTREAM-side
  (paid rotating-proxy API, tor). One circuit per ``(source, provider)`` —
  the single address IS the provider, so its circuit key is the natural
  per-provider circuit.

Onboarding a new *data source* = new ``ban_rules`` rows (data, no code).
Onboarding a new *proxy source* = a new provider class + seed rows. The
crawler (scraw) never changes — it only knows ``FD_PROXY_FORWARDER``.

``Acquisition`` is the unit handed to the forwarder: an ``upstream_url``
(or ``None`` for direct egress), the ``addr_id`` (``proxies.id``, or ``None``
for the synthetic direct sentinel), and the owning ``provider`` name. The
forwarder routes the request through ``upstream_url``; on the response it
classifies via ``ban_rules`` and calls ``release(source, addr_id, outcome)``,
which drives the circuit.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional, Protocol, runtime_checkable


@dataclass
class Acquisition:
    """A single acquired upstream for one fetch.

    Attributes:
        upstream_url: ``http(s)://[auth@]host:port`` to route through, or
            ``None`` for direct egress (the forwarder's own node IP, no
            upstream). The forwarder sets this as the request's proxy.
        addr_id: ``proxies.id`` of the chosen address, or ``None`` for the
            synthetic direct sentinel (ships-dark / no upstream). The circuit
            key is ``circuit:{source}:{addr_id}``; ``None`` means no circuit.
        provider: owning provider name (e.g. ``gost-own``). Carried through so
            ``release`` can dispatch to the right provider.
    """
    upstream_url: Optional[str]
    addr_id: Optional[int]
    provider: str


@runtime_checkable
class ProxyProvider(Protocol):
    """Interface every provider implements.

    ``acquire`` / ``release`` are the hot path (per fetch); ``health`` is the
    cold path (control API). ``priority`` orders providers in
    ``registry.acquire_any`` (lower = tried first).
    """
    kind: Literal["endpoint", "pool"]
    name: str
    priority: int

    def acquire(self, source: str) -> Optional[Acquisition]:
        """Pick one healthy upstream for ``source``. ``None`` = this provider
        has no healthy upstream (all circuits OPEN/saturated, or no addresses);
        ``registry.acquire_any`` then tries the next provider."""
        ...

    def release(self, source: str, addr_id: Optional[int], outcome: str) -> None:
        """Record a classified fetch outcome (``ok`` / ``transient`` / ``ban``
        / ``blocked``) so the circuit state machine advances. ``addr_id=None``
        (direct) is a no-op — direct egress has no circuit."""
        ...

    def health(self) -> dict:
        """Snapshot for ``GET /health`` / ``GET /providers``."""
        ...


def build_upstream_url(proxy: Any) -> Optional[str]:
    """Build an ``http(s)://[auth@]host:port`` URL from a Proxy row (or the
    selector's direct sentinel), or ``None`` for ``scheme='direct'``.

    Lifted from ``fd_open_data_mcp/proxy/injection.py::proxy_url``. The forwarder
    sets this as the request's proxy; ``None`` means 'no upstream, egress
    direct from the forwarder's own node IP'.
    """
    scheme = getattr(proxy, "scheme", "direct")
    if scheme == "direct" or not scheme:
        return None
    ip = getattr(proxy, "ip", None)
    port = getattr(proxy, "port", None)
    auth = getattr(proxy, "auth", None)
    if not ip:
        return None
    creds = f"{auth}@" if auth else ""
    if port:
        return f"{scheme}://{creds}{ip}:{port}"
    return f"{scheme}://{creds}{ip}"
