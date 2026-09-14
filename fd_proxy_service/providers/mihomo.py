"""``mihomo`` pool provider: the fixed-exit international pool on xinru-server2.

The pool is an existing external asset: 28 ``mixed`` (HTTP+SOCKS) listeners on
the Tailscale address 100.64.0.7, one Basic-auth credential per port, each port
pinned to a fixed airport node. 27 are usable — fx02/30082 is dead and is left
alone on purpose, since repairing it means a config reload that would drop the
sub2api connections sharing that host.

Why a class rather than a bare ``PoolProvider("mihomo", …)``: the cap. The
host's ~3 Mbps uplink is shared with sub2api's long-lived streaming connections,
so every port carries a hard in-flight limit (default 2). A plain pool provider
would impose none.

Nothing else is provider-specific. Addresses are ordinary ``proxies`` rows; the
grouping that makes a single ban retire all ~24 ports behind one exit IP comes
from ``provider_meta.exit_ip``, which the seed writes and
``pool.circuit_unit`` reads. Priority 2 — after the in-cluster and CN gost
tiers, before paid bandwidth.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from fd_proxy_service.providers.pool_provider import PoolProvider

NAME = "mihomo"
PRIORITY = 2
DEFAULT_MAX_CONCURRENCY = 2


class MihomoProvider(PoolProvider):
    """Fixed-exit international pool (27 ports over 3 exits), capped per port."""

    default_max_concurrency = DEFAULT_MAX_CONCURRENCY

    def __init__(self, session: Session, priority: int = PRIORITY):
        super().__init__(NAME, session, priority=priority)
