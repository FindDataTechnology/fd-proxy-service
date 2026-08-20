"""Seed the proxy-pool, ban-rules, rate-limits, and providers with defaults.

Idempotent: re-running updates existing rows in place rather than duplicating.
Run via ``fd-proxy-service seed`` or ``python -m fd_proxy_service.seed``.

Lifted + reworked from ``fd_open_data_mcp/proxy/seed.py``. The rework: instead
of seeding flat proxy rows (``register_cluster_egress`` /
``register_egress_pod`` / ``seed_egress_pods``), we seed **providers** — a
provider name owns a set of addresses in the ``proxies`` table (the
``provider`` column added by migration 001). ``seed_all`` seeds the shared
ban-rule / rate-limit / probe tables AND the first provider (``gost-own``).
New providers register their addresses via ``seed_provider``; their
kind/priority is code-level config in the registry
(``fd_proxy_service.providers.registry``), not a DB column — so onboarding a
new source's ban signals is a row insert, and onboarding a new proxy provider
is a ``seed_provider`` call + a registry entry.

These defaults encode the ban signals observed in production (notably the
2026-08-02 eastmoney IP ban: ``RemoteDisconnected`` + ``000`` after high
concurrency). New sources declare their own ban signals here at onboarding.
"""
from __future__ import annotations

import json
import os
from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service.models import (
    Base,
    BanRule,
    Proxy,
    SourceProbe,
    SourceRateLimit,
)

# (rule_type, pattern, classification, streak_min, priority) per source.
BAN_RULES: dict[str, list[tuple]] = {
    "akshare": [
        ("status", "403", "ban", 0, 100),
        ("status", "429", "ban", 0, 100),
        ("status", "5xx", "transient", 0, 90),
        ("error", "RemoteDisconnected", "ban", 3, 80),   # ambiguous: only ban after streak
        ("error", "ConnectionError", "ban", 3, 80),
        ("error", "Connection aborted", "ban", 3, 80),
        ("error", "timeout", "transient", 0, 70),
        ("body", "访问频繁", "ban", 0, 80),
        ("body", "captcha", "blocked", 0, 85),
    ],
    "yfinance": [
        ("status", "429", "ban", 0, 100),
        ("status", "401", "blocked", 0, 95),   # needs auth
        ("status", "5xx", "transient", 0, 90),
    ],
    "edgar": [
        ("status", "429", "ban", 0, 100),
        ("status", "403", "blocked", 0, 95),   # SEC needs declared identity
        ("status", "5xx", "transient", 0, 90),
    ],
    "worldbank": [
        ("status", "429", "ban", 0, 100),
        ("status", "5xx", "transient", 0, 90),
    ],
    "wbgapi": [
        ("status", "429", "ban", 0, 100),
        ("status", "5xx", "transient", 0, 90),
    ],
    # nbs-gdp is the ONLY one of the 21 "industry" runners that actually hits a
    # network (data.stats.gov.cn via requests.post). The other 20 are mock stubs
    # returning hardcoded DataFrames - no network => no bans => no proxy-health
    # config needed until they're implemented for real.
    "nbs-gdp": [
        ("status", "429", "ban", 0, 100),
        ("status", "403", "ban", 0, 100),
        ("status", "404", "transient", 0, 95),   # endpoint moved, not an IP ban
        ("status", "5xx", "transient", 0, 90),
        ("error", "RemoteDisconnected", "ban", 3, 80),
        ("error", "ConnectionError", "ban", 3, 80),
        ("error", "timeout", "transient", 0, 70),
    ],
}

# source -> (max_qps, max_concurrent). Conservative for IP-rate-limited sources.
RATE_LIMITS: dict[str, tuple[float, int]] = {
    "akshare": (1.0, 4),
    "yfinance": (2.0, 4),
    "edgar": (0.5, 2),
    "worldbank": (2.0, 4),
    "wbgapi": (2.0, 4),
    "nbs-gdp": (0.5, 2),   # gov site (data.stats.gov.cn) - polite
}

# source -> (command, params) for the probe job's recovery probe. A cheap,
# known-good fetch; the result is unused (only whether it's classified as a ban).
PROBES: dict[str, tuple[str, dict]] = {
    # Use the Tencent endpoint (stock_zh_a_hist_tx) as the probe - it is the
    # reliable path on overseas IPs (eastmoney stock_zh_a_hist gets IP-banned).
    "akshare": ("stock_zh_a_hist_tx", {
        "symbol": "000001", "period": "daily",
        "start_date": "20260701", "end_date": "20260701", "adjust": "qfq",
    }),
    "yfinance": ("ticker_history", {"symbol": "AAPL", "period": "1d"}),
    "nbs-gdp": ("get_gdp_quarterly", {"start_year": 2024}),
}


# ─── shared table seeding (ban_rules / rate_limits / probes) ────────────────

def _seed_ban_rules(session: Session) -> int:
    n = 0
    for source, rules in BAN_RULES.items():
        for rule_type, pattern, cls, streak_min, priority in rules:
            row = (
                session.query(BanRule)
                .filter_by(source=source, rule_type=rule_type, pattern=pattern)
                .first()
            )
            if row is None:
                session.add(BanRule(
                    source=source, rule_type=rule_type, pattern=pattern,
                    classification=cls, streak_min=streak_min, priority=priority,
                ))
            else:
                row.classification = cls
                row.streak_min = streak_min
                row.priority = priority
                row.enabled = True
            n += 1
    return n


def _seed_rate_limits(session: Session) -> int:
    n = 0
    for source, (qps, conc) in RATE_LIMITS.items():
        row = session.query(SourceRateLimit).filter_by(source=source).first()
        if row is None:
            session.add(SourceRateLimit(source=source, max_qps=qps, max_concurrent=conc))
        else:
            row.max_qps = qps
            row.max_concurrent = conc
        n += 1
    return n


def _seed_probes(session: Session) -> int:
    n = 0
    for source, (command, params) in PROBES.items():
        row = session.query(SourceProbe).filter_by(source=source).first()
        if row is None:
            session.add(SourceProbe(source=source, command=command, params=params))
        else:
            row.command = command
            row.params = params
            row.enabled = True
        n += 1
    return n


# ─── provider address registration ───────────────────────────────────────────
# A provider owns one or more addresses (rows in ``proxies`` with
# ``proxies.provider = <name>``). ``label`` is the idempotency key within a
# provider so a re-seed updates host/port/auth in place instead of duplicating.
# Provider *kind* and *priority* are code-level config in the registry, not DB
# columns — this keeps ``proxies`` purely an address book.

def register_provider_address(
    session: Session,
    provider: str,
    scheme: str,
    ip: str,
    port: Optional[int] = None,
    auth: Optional[str] = None,
    label: Optional[str] = None,
    cluster_id: Optional[int] = None,
    provider_meta: Optional[dict] = None,
    status: str = "active",
) -> str:
    """Idempotent upsert of a provider-owned address into ``proxies``.

    Args:
        provider: owning provider name (``gost-own``, ``rotate-api``, …).
        scheme: ``direct`` (no upstream) / ``http`` / ``https`` / ``socks5``.
        ip: ``"direct"`` for scheme=direct, else the upstream host.
        port: upstream port (None for scheme=direct).
        auth: ``"user:pass"`` basic-auth for the upstream, or None.
        label: stable idempotency key within the provider (e.g.
            ``gost-own-local``). Defaults to ``"<provider>-<ip>:<port>"``.
        cluster_id: optional per-cluster tag (legacy direct-egress rows).
        provider_meta: provider-specific JSON (tor circuit id, paid-API session
            handle, region tag).
        status: ``active`` (default) or ``retired``.
    """
    label = label or f"{provider}-{ip}:{port}"
    row = (
        session.query(Proxy)
        .filter_by(provider=provider, label=label)
        .first()
    )
    if row is None:
        session.add(Proxy(
            scheme=scheme, ip=ip, port=port, auth=auth, status=status,
            label=label, cluster_id=cluster_id, provider=provider,
            provider_meta=provider_meta,
        ))
        return "created"
    row.status = status
    row.scheme = scheme
    row.ip = ip
    row.port = port
    row.auth = auth
    if cluster_id is not None:
        row.cluster_id = cluster_id
    if provider_meta is not None:
        row.provider_meta = provider_meta
    return "updated"


def seed_provider(
    session: Session, name: str, addresses: list[dict], auth: Optional[str] = None,
) -> dict[str, str]:
    """Register all addresses for a provider.

    ``addresses`` is a list of dicts with keys: ``scheme``, ``ip``, ``port``,
    ``label``, ``cluster_id``, ``provider_meta``, ``auth`` (per-address override
    of ``auth``). Returns ``{label: "created"/"updated"}``.
    """
    out: dict[str, str] = {}
    for addr in addresses:
        label = addr.get("label")
        key = label or f"{addr['ip']}:{addr.get('port')}"
        out[key] = register_provider_address(
            session, name, addr["scheme"], addr["ip"], addr.get("port"),
            auth=addr.get("auth", auth), label=label,
            cluster_id=addr.get("cluster_id"),
            provider_meta=addr.get("provider_meta"),
        )
    return out


# ─── gost-own provider (first provider; each worker's own cluster gost) ──────
# Default basic-auth shared with the gost pod manifest. Production MUST override
# via the ``FD_EGRESS_AUTH`` env (set it in a k8s Secret referenced by both the
# gost Job and the seed run) — the gost NodePort 30080 is public, so a known
# default password must not ship to prod as-is.
_DEFAULT_EGRESS_AUTH = "fdproxy:CHANGE_ME"

# gost-own is a pool-kind provider with ONE address: this cluster's own gost
# forward proxy at ``127.0.0.1:30080`` (in-cluster reach, no firewall). Each
# worker cluster's forwarder resolves ``127.0.0.1:30080`` to ITS OWN gost pod
# (hostNetwork), so one shared ``proxies`` row serves the whole fleet. The
# circuit key ``circuit:{source}:{gost-own-id}`` is therefore shared across
# clusters — a known limitation: a ban on one cluster's gost IP taints all.
# Genuinely decoupled per-IP circuits come from external ``endpoint`` providers
# (paid rotating APIs) added later, where the IP rotates upstream-side and the
# single shared circuit is the correct semantics.
GOST_OWN_ADDRESSES: list[dict] = [
    {"scheme": "http", "ip": "127.0.0.1", "port": 30080, "label": "gost-own-local"},
]


def seed_gost_own(session: Session) -> dict[str, str]:
    """Seed the gost-own provider's address.

    Reads the gost basic-auth from ``FD_EGRESS_AUTH`` (default fallback above)
    so the seeded ``Proxy.auth`` matches the gost pod's ``-L`` listener
    credential. Call after deploying the gost pod so the endpoint answers;
    re-run after an auth rotation to refresh the row in place.
    """
    auth = os.environ.get("FD_EGRESS_AUTH", _DEFAULT_EGRESS_AUTH)
    return seed_provider(session, "gost-own", GOST_OWN_ADDRESSES, auth=auth)


# ─── gost-mesh provider (sibling-server gosts over the private mesh) ───────
# gost-mesh is a pool-kind provider whose addresses are the gost forward-proxy
# on each cloud server, reached via the private WireGuard mesh (100.64.0.x:30080)
# — NOT public NodePorts (which are firewalled / Kong-hijacked). Each address is
# a separate ``proxies`` row with its OWN circuit key
# ``circuit:{source}:{addr_id}``, so a ban on one server's IP opens ONLY that
# address's circuit — the rest stay healthy and the next fetch rotates to a
# sibling. This is the per-IP isolation gost-own's single shared row cannot
# provide, achieved by seeding N rows + registering a second provider: NO
# selection-logic rewrite (the pool selector already iterates N addresses).
#
# Verified-egress mesh set (gost listening on :30080 AND a cross-verify fetch
# egressed the server's own public IP, 2026-08-20): chengsi (.1 -> 124.220.7.175),
# aliyun (.2 -> 47.99.94.85), zihan (.4 -> 106.55.20.23). america (.5) gost TCP-
# listens but the egress fetch FAILED (deferred — re-add via the env override or
# by uncommenting its row once fixed; the circuit self-heals on first failure).
# The DB/control hosts — guangzhou-xinru (.3, sole canonical PG+Redis) and the
# shards xinru2 (.6) / xinru3 (.7) — run NO gost by design (control-plane role,
# not egress). Onboard a new server by adding its mesh IP here (or via the env
# override) + re-running ``fd-proxy-service seed``; no
# selector/registry/crawler code change.
#
# Known minor redundancy: a worker on server X egresses the SAME IP whether it
# uses gost-own (its own cluster's gost) or the gost-mesh row for X — so when X's
# IP is banned, both circuits trip (in sequence, one wasted retry). Acceptable:
# 2 of 3 mesh rows are genuinely decoupled for any given worker. Excluding the
# self-IP per-worker would need per-worker filtering the fleet-wide registry
# doesn't do; deferred. The genuinely-decoupled IPs are the value here.
#
# Same ``FD_EGRESS_AUTH`` as gost-own: all gost instances share one basic-auth
# credential (per-row override available via ``auth`` in the address dict).
GOST_MESH_ADDRESSES: list[dict] = [
    {"scheme": "http", "ip": "100.64.0.1", "port": 30080, "label": "gost-mesh-chengsi"},
    {"scheme": "http", "ip": "100.64.0.2", "port": 30080, "label": "gost-mesh-aliyun"},
    {"scheme": "http", "ip": "100.64.0.4", "port": 30080, "label": "gost-mesh-zihan"},
]


def _parse_mesh_override(raw: str) -> list[dict]:
    """Parse the ``GOST_MESH_ADDRESSES`` env override as a JSON list.

    Each item is an address dict (``scheme`` defaults ``http``, ``port``
    30080) or a bare ``"host:port"`` string.
    """
    out: list[dict] = []
    for it in json.loads(raw):
        if isinstance(it, str):  # bare "host:port"
            host, _, port = it.partition(":")
            out.append({"scheme": "http", "ip": host,
                        "port": int(port) if port else 30080})
        else:
            d = dict(it)
            d.setdefault("scheme", "http")
            d.setdefault("port", 30080)
            out.append(d)
    return out


def seed_gost_mesh(session: Session) -> dict[str, str]:
    """Seed the gost-mesh provider's sibling-server addresses.

    Reads the gost basic-auth from ``FD_EGRESS_AUTH`` (same shared credential
    as gost-own — all gost instances share one basic-auth) and the address set
    from the ``GOST_MESH_ADDRESSES`` env (JSON list or ``host:port`` CSV)
    overriding the default ``GOST_MESH_ADDRESSES`` above. Idempotent upsert by
    ``provider='gost-mesh'``+``label``. Call after deploying gost on each target
    server so the endpoints answer; re-run after adding a server to refresh.
    """
    auth = os.environ.get("FD_EGRESS_AUTH", _DEFAULT_EGRESS_AUTH)
    override = os.environ.get("GOST_MESH_ADDRESSES")
    addresses = _parse_mesh_override(override) if override else GOST_MESH_ADDRESSES
    return seed_provider(session, "gost-mesh", addresses, auth=auth)


def seed_all(session: Session) -> dict:
    """Idempotently seed ban_rules, source_rate_limits, source_probes, and the
    gost-own + gost-mesh providers. Returns a summary of what was
    created/updated."""
    rules = _seed_ban_rules(session)
    limits = _seed_rate_limits(session)
    probes = _seed_probes(session)
    gost = seed_gost_own(session)
    mesh = seed_gost_mesh(session)
    session.commit()
    return {"ban_rules": rules, "rate_limits": limits, "probes": probes,
            "gost_own": gost, "gost_mesh": mesh}


if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    url = os.environ.get("PROXY_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("set PROXY_DATABASE_URL (or DATABASE_URL) to the master DB")
    engine = create_engine(url)
    # Idempotent: creates tables that don't exist yet. The provider/provider_meta
    # columns on an existing proxies table come from migration 001, not from
    # here (create_all does not ALTER existing tables).
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    try:
        print(seed_all(s))
    finally:
        s.close()
