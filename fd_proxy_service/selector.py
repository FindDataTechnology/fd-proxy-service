"""Per-request upstream picker for the forwarder.

For a source: enumerate the active provider addresses (direct first), skip any
whose circuit is OPEN/HALF_OPEN/permanent, skip any at its QPS cap, and acquire
a rate token on the first selectable one. Returns the chosen
``(addr_id, Proxy)`` or ``None`` to signal source-level failure (every address
banned/saturated) so the forwarder can fail the request or fall back to a
lower-priority provider.

Ships-dark: when NO addresses are registered, returns a synthetic direct
sentinel (``addr_id=None``) — the forwarder egresses from its own node IP with
no upstream, matching the pre-proxy baseline.

Lifted from ``fd_open_data_mcp/proxy/selector.py`` and stripped to its hot
path. The forwarder is now the sole egress path, so the ``FD_EGRESS_MODE``
guard, the ``SCRAW_CLUSTER_ID`` per-cluster pinning branch, and the
``FD_PROXY_POOL=off`` local-dev branch are all dropped — only the
``provider_addresses`` loop + circuit + rate-limit remains. This is the
low-level picker that ``PoolProvider`` (section 4) wraps; ``provider`` kwarg
scopes it to one provider's addresses.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service import circuit, pool, rate_limit

logger = logging.getLogger(__name__)

_DEFAULT_QPS = 1.0


class _Direct:
    """Sentinel for 'no upstream proxy' (the forwarder's own node egress)."""
    id = None
    scheme = "direct"
    ip = "direct"
    port = None
    auth = None


_DIRECT = _Direct()


class ProxySelector:
    """Low-level per-request picker over registered provider addresses."""

    def __init__(self, session: Session):
        self.session = session

    def select(
        self, source: str, provider: Optional[str] = None
    ) -> Optional[tuple[Optional[int], object]]:
        """Pick one selectable address for ``source``.

        Args:
            source: real_source name (e.g. ``eastmoney``) or library fallback.
            provider: if given, restrict to one provider's addresses
                (``Proxy.provider == provider``); None = all active rows.
        """
        rl = pool.get_rate_limit(self.session, source)
        max_qps = rl.max_qps if rl else _DEFAULT_QPS
        proxies = pool.provider_addresses(self.session, provider=provider)
        if not proxies:
            if provider is None:
                # ships-dark: no pool registered at all -> direct, no circuit,
                # no rate limit (matches the pre-proxy baseline).
                return (None, _DIRECT)
            # This provider owns no addresses -> return None so the registry
            # tries the next provider (do NOT claim direct for a specific pool).
            return None
        for p in proxies:
            if not circuit.is_selectable(source, p.id):
                continue
            if not rate_limit.acquire(source, p.id, max_qps):
                continue
            return (p.id, p)
        return None  # addresses exist but all OPEN/saturated -> source-level failover
