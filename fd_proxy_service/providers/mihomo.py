"""``mihomo`` pool provider: the fixed-exit international pool on windows-laptop.

The pool is an existing external asset: 35 ``mixed`` (HTTP+SOCKS) listeners on
the Tailscale address <pool-host> (LAN <lan-host>), ports 31081-31115, one
Basic-auth credential per port, each port pinned to a fixed airport node. The
old the db host-server2 pool (<node>:3008x) is retired.

The listeners do NOT each own an exit IP: measured 2026-11-08, the 24 reachable
listeners leave through 7 IPs, the busiest carrying 6 of them. That is exactly
what ``provider_meta.exit_ip`` grouping exists for, so one ban costs one attempt
rather than six.

Why a class rather than a bare ``PoolProvider("mihomo", …)``: the cap. The
host's ~3 Mbps uplink is shared with sub2api's long-lived streaming connections,
so every port carries a hard in-flight limit (default 2). A plain pool provider
would impose none.

Nothing else is provider-specific. Addresses are ordinary ``proxies`` rows; the
grouping that makes a single ban retire every port behind one exit IP comes from
``provider_meta.exit_ip``, which the seed writes and ``pool.circuit_unit`` reads.
Priority 2 — after the in-cluster and CN gost tiers, before paid bandwidth.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from fd_proxy_service.providers.pool_provider import PoolProvider

NAME = "mihomo"
PRIORITY = 2
DEFAULT_MAX_CONCURRENCY = 2


class MihomoProvider(PoolProvider):
    """Fixed-exit international pool (24 live ports over 7 exits), capped per port."""

    default_max_concurrency = DEFAULT_MAX_CONCURRENCY

    def __init__(self, session: Session, priority: int = PRIORITY):
        super().__init__(NAME, session, priority=priority)
