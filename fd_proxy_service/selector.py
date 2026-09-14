"""Per-request upstream picker for the forwarder.

For a source: enumerate the active provider addresses (direct first), skip any
whose circuit is OPEN/HALF_OPEN/permanent, skip any at its QPS cap, skip any at
its address-wide in-flight cap, skip addr_ids the caller already tried this
fetch (Bug 5), and acquire a token on the next selectable one — rotating the
start across calls (Bug 10). Returns the chosen ``(addr_id, Proxy)`` or ``None``
to signal source-level failure (every address banned/saturated) so the
forwarder can fail the request or fall back to a lower-priority provider.

Circuits are keyed by *unit*, not by address: addresses sharing an egress IP
(the mihomo pool) share one circuit, so a ban on that exit takes the whole group
out at once. The loop therefore de-duplicates units — once a unit is known
unselectable, its remaining addresses are skipped without re-consulting the
circuit, so an exit-IP ban costs one check instead of one per port. Sibling
addresses of a *selectable* unit are still tried individually, so a single
port's own saturation does not shadow the rest of its group.

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


# Per-(source, provider) round-robin start offset (Bug 10). Advances by one
# past the chosen address on each successful pick so consecutive calls spread
# across addresses instead of always hammering the first-row id — no single IP
# is hammered until it gets banned. A lost increment under concurrency only
# costs fairness for one cycle, never correctness (every pick is still valid).
_CURSORS: dict[tuple[str, Optional[str]], int] = {}


class ProxySelector:
    """Low-level per-request picker over registered provider addresses."""

    def __init__(self, session: Session):
        self.session = session

    def select(
        self, source: str, provider: Optional[str] = None,
        exclude: Optional[list[int]] = None,
        max_concurrency: Optional[int] = None,
    ) -> Optional[tuple[Optional[int], object]]:
        """Pick one selectable address for ``source``.

        Args:
            source: real_source name (e.g. ``eastmoney``) or library fallback.
            provider: if given, restrict to one provider's addresses
                (``Proxy.provider == provider``); None = all active rows.
            exclude: addr_ids to skip — already tried in this caller's fetch
                loop, so a dead proxy is not re-acquired within one fetch
                (Bug 5). None/empty = no exclusion.
            max_concurrency: provider-level in-flight default, applied to any
                address whose own ``max_concurrency`` is NULL. None = uncapped.
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
        exclude_set = set(exclude) if exclude else set()
        # Round-robin start (Bug 10): begin at the per-(source,provider) cursor
        # instead of always row 0, so consecutive calls hit different addresses
        # and no single IP is hammered until it gets banned.
        key = (source, provider)
        start = _CURSORS.get(key, 0)
        n = len(proxies)
        # Units proven unselectable during THIS pass. Their sibling addresses
        # are skipped outright — one OPEN exit IP must not cost one circuit
        # check (and one caller attempt) per port behind it.
        dead_units: set[str] = set()
        for i in range(n):
            p = proxies[(start + i) % n]
            if p.id in exclude_set:
                continue  # tried this one already in this fetch (Bug 5)
            unit = pool.circuit_unit(p)
            if unit in dead_units:
                continue  # a sibling already proved this unit unselectable
            if not circuit.is_selectable(source, unit):
                dead_units.add(unit)
                continue
            # Address-wide in-flight cap first: it is the harder constraint (a
            # shared egress), and reserving before consuming a QPS token means a
            # saturated address does not leak one for the rest of the second.
            cap = (p.max_concurrency
                   if getattr(p, "max_concurrency", None) is not None
                   else max_concurrency)
            if not rate_limit.try_acquire_inflight(p.id, cap):
                continue
            if not rate_limit.acquire(source, p.id, max_qps):
                rate_limit.release_inflight(p.id)  # slot goes with the token
                continue
            # Advance the cursor past the chosen address so the next call
            # starts at the following row (round-robin distribution).
            _CURSORS[key] = (start + i + 1) % n
            return (p.id, p)
        return None  # all excluded/OPEN/saturated -> source-level failover
