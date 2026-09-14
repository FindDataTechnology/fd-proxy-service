"""Rate limiting for the forwarder: per-(source, address) QPS + per-address cap.

Two independent limits:
- **QPS** — ``rate:{source}:{proxy_id}:{unix_second}``, a fixed-window counter.
  Each fetch INCRs it; over ``max_qps`` the address is treated as unavailable
  for this fetch (the selector skips it, same as OPEN). This is the *politeness*
  limit, per source, distinct from refresh-frequency scheduling.
- **In-flight** — ``INFLIGHT:{proxy_id}``, aggregated across ALL sources, for
  addresses that must not be over-driven regardless of who is using them (see
  ``try_acquire_inflight``). This is the *cap*, per address.

Both degrade to always-allow when Redis is unavailable.

Lifted unchanged from ``fd_open_data_mcp/proxy/rate_limit.py``; the only edit is
the import path (circuit now lives in ``fd_proxy_service``), so it follows
``PROXY_REDIS_URL`` automatically.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fd_proxy_service.circuit import _client

logger = logging.getLogger(__name__)


def acquire(source: str, proxy_id: int, max_qps: float) -> bool:
    """Return True if a fetch through (source, proxy_id) is allowed under the
    QPS cap. Always True when max_qps <= 0 or Redis is unavailable."""
    r = _client()
    if r is None or max_qps is None or max_qps <= 0:
        return True
    sec = int(time.time())
    key = f"rate:{source}:{proxy_id}:{sec}"
    try:
        count = r.incr(key)
        if count == 1:
            r.expire(key, 2)
    except Exception as e:  # noqa: BLE001 - never block on redis failure
        logger.debug("rate_limit redis failed: %s", e)
        return True
    return count <= max_qps


def capacity_available(source: str, proxy_id: int, max_qps: float) -> bool:
    """Check without consuming (used by the selector to filter). Acquire then
    immediately refund is racy; instead peek the current-second counter."""
    r = _client()
    if r is None or max_qps is None or max_qps <= 0:
        return True
    sec = int(time.time())
    try:
        count = int(r.get(f"rate:{source}:{proxy_id}:{sec}") or 0)
    except Exception:
        return True
    return count < max_qps


# ─── per-address global in-flight cap ────────────────────────────────────────
# The QPS counter above is per (source, address): N sources each under their own
# quota can still pile N×max_qps in-flight requests onto ONE address. That is
# fine for an owned gost, but a shared egress (the mihomo pool, whose host also
# serves sub2api over a ~3 Mbps uplink) needs a cap on the address itself,
# aggregated across every source. Counter: INFLIGHT:{proxy_id}, one shared
# proxy-redis so the cap holds fleet-wide rather than per forwarder process.

INFLIGHT_TTL_SEC = 180  # reclaim slots from crashed holders (fetch timeout << this)


def try_acquire_inflight(proxy_id: int, cap: Optional[int]) -> bool:
    """Reserve an in-flight slot on the address. True if reserved (or uncapped).

    Peek-then-INCR: a saturated address must NOT consume a slot, or the counter
    would only ever climb and the address would be permanently saturated.
    ``cap`` None/<=0 = uncapped. The peek and the INCR are not atomic, so a
    burst can overshoot the cap by a request or two; erring toward permissive
    (and the TTL below reclaiming leaks) is the safe direction here.

    Always True when Redis is unavailable — same graceful degradation as the QPS
    limit (ships-dark: no Redis, no cap).
    """
    r = _client()
    if r is None or cap is None or cap <= 0:
        return True
    key = f"INFLIGHT:{proxy_id}"
    try:
        if int(r.get(key) or 0) >= cap:
            return False
        r.incr(key)
        r.expire(key, INFLIGHT_TTL_SEC)  # refreshed on every reserve
        return True
    except Exception as e:  # noqa: BLE001 - never block on redis failure
        logger.debug("inflight acquire redis failed: %s", e)
        return True


def release_inflight(proxy_id: int) -> None:
    """Release an in-flight slot. Call for EVERY outcome (ok/ban/transient) —
    all three mean the request ended.

    Only decrements a counter that exists. An uncapped address never reserves
    one, and a blind ``DECR`` would mint a ``-1`` key (then delete it) on every
    single release; a reserved slot always leaves the key present (TTL 180 s).
    The post-decrement floor covers the race where the TTL reclaims the key
    between the check and the decrement.
    """
    r = _client()
    if r is None:
        return
    key = f"INFLIGHT:{proxy_id}"
    try:
        if r.exists(key) and r.decr(key) < 0:
            r.delete(key)
    except Exception as e:  # noqa: BLE001
        logger.debug("inflight release failed: %s", e)
