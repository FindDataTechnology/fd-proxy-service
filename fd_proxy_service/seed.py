"""Seed the proxy-pool, ban-rules, rate-limits, and providers with defaults.

Idempotent: re-running updates existing rows in place rather than duplicating,
and an address whose stored fields already match is left untouched (reported as
``unchanged``). Run via ``fd-proxy-service seed`` or ``python -m fd_proxy_service.seed``.

``seed_mihomo`` is deliberately NOT part of ``seed_all``: it probes 27 live
listeners for their egress IP, so it belongs in its own Job (``fd-proxy-service
seed-mihomo``) rather than in the migration path.

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

import ipaddress
import json
import logging
import os
import pathlib
import urllib.request
from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service.models import (
    Base,
    BanRule,
    Proxy,
    SourceProbe,
    SourceProviderRoute,
    SourceRateLimit,
)

logger = logging.getLogger(__name__)

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
    # Compare before writing: re-seeding with identical inputs must leave the row
    # untouched AND say so, so "run the seed twice" is observably a no-op rather
    # than 27 silent writes. Callers can then distinguish a real change (a port
    # recovered, an exit IP moved) from a refresh.
    if (row.status == status and row.scheme == scheme and row.ip == ip
            and row.port == port and (row.auth or None) == (auth or None)
            and (provider_meta is None or row.provider_meta == provider_meta)
            and (cluster_id is None or row.cluster_id == cluster_id)):
        return "unchanged"
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
# egressed the server's own public IP, 2026-08-20): chengsi (.1 -> <origin-host>),
# aliyun (.2 -> 47.99.94.85), the node (.4 -> 106.55.20.23). america (.5) gost TCP-
# listens but the egress fetch FAILED (deferred — re-add via the env override or
# by uncommenting its row once fixed; the circuit self-heals on first failure).
# The DB/control hosts — guangzhou-the db host (.3, sole canonical PG+Redis) and the
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
#: Deployment-specific, supplied by the ``GOST_MESH_ADDRESSES`` env as a JSON list
#: (each item either ``"host:port"`` or an address object). Deliberately empty here.
#:
#: Why not a default: the address set *is* the fleet map — how many machines carry an
#: egress listener, and their private addresses. A hardcoded list in a public repo is a
#: description of the network, and it also silently hands a fresh deployment someone
#: else's egress endpoints.
#:
#: Behaviour change, stated plainly: seeding with this unset registers no gost-mesh
#: provider, so proxy routing skips that tier. Existing rows are untouched (seed is an
#: idempotent upsert), so a pool seeded before this change keeps working. A deployment
#: that wants the tier must set the env — which is the point: it should say so out loud
#: rather than inherit defaults.
GOST_MESH_ADDRESSES: list[dict] = []


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
    from the ``GOST_MESH_ADDRESSES`` env (JSON list; items may be ``"host:port"``)
    overriding the default ``GOST_MESH_ADDRESSES`` above. Idempotent upsert by
    ``provider='gost-mesh'``+``label``. Call after deploying gost on each target
    server so the endpoints answer; re-run after adding a server to refresh.
    """
    auth = os.environ.get("FD_EGRESS_AUTH", _DEFAULT_EGRESS_AUTH)
    override = os.environ.get("GOST_MESH_ADDRESSES")
    addresses = _parse_mesh_override(override) if override else GOST_MESH_ADDRESSES
    return seed_provider(session, "gost-mesh", addresses, auth=auth)


# ─── mihomo provider (fixed-exit international pool) ─────────────────────────
# The pool is an external asset on windows-laptop (tailnet <pool-host>, LAN
# <lan-host> — see docs/proxy-mihomo-windows-migration.md): 35 "mixed"
# (HTTP+SOCKS) listeners on 31081-31115, each pinned to one airport node, one
# Basic-auth credential per listener. This seed probes every listener for its
# real egress IP and registers it.
#
# The 2026-10 pool on the db host-server2 (<node>:3008x) is retired: its rows stay
# in the table as status='retired' for history and must not be re-seeded.
MIHOMO_NAME = "mihomo"
MIHOMO_HOST = os.environ.get("MIHOMO_HOST", "<pool-host>")
MIHOMO_CREDS_FILE = os.environ.get(
    "MIHOMO_CREDS_FILE", "~/.finddata/mihomo-win-creds.json")
MIHOMO_NODE_MAP_FILE = os.environ.get(
    "MIHOMO_NODE_MAP_FILE", "~/.finddata/mihomo-port-mapping.txt")
MIHOMO_EXIT_PROBE_URL = os.environ.get("MIHOMO_EXIT_PROBE_URL", "https://api.ipify.org")
MIHOMO_EXIT_PROBE_TIMEOUT = float(os.environ.get("MIHOMO_EXIT_PROBE_TIMEOUT", "8"))
# Attempts per listener before it is called unreachable. More than one because
# the FIRST dial through an airport node can exceed the timeout while the tunnel
# warms: measured on the real pool, fx23 timed out once at 8 s and then answered
# in 1.8-4.4 s on every subsequent attempt. One slow dial is not proof of death,
# and retiring a healthy port contradicts the pool's expected size.
MIHOMO_EXIT_PROBE_ATTEMPTS = int(os.environ.get("MIHOMO_EXIT_PROBE_ATTEMPTS", "2"))

# fx02@30082 is dead on the RETIRED the db host-server2 pool and is deliberately NOT
# repaired: fixing it means editing that host's config and reloading, which would
# drop the sub2api connections sharing it. The port does not exist on the current
# windows pool (31081-31115), so this skip is a no-op there — kept so a re-seed
# against the old host stays safe.
MIHOMO_SKIP_PORTS = {30082}

# Sources that must reach a foreign exit directly rather than warm up through CN
# egress. Only the design's example is seeded: a route is EXCLUSIVE (an
# exhausted route does not fall back to a provider outside it), so a source gets
# pinned here only once its international nature is confirmed. Add the rest as
# they onboard.
MIHOMO_SOURCE_ROUTES: dict[str, list[str]] = {
    "fred": [MIHOMO_NAME],
    # wbgapi (World Bank API) — 2026-11-10: three of its four circuits were OPEN
    # while the source itself was fine. The default ladder handed it domestic
    # gost-mesh exits (<node>/.7/.14); worldbank bans those, so every fetch
    # spent the egress and left the circuit closed to nobody. Measured the same
    # minute: direct egress from a cheap node = 200, via mihomo-kr01
    # (<pool-host>:31113) = 200 in 0.30s. International source → pin it to the
    # foreign pool like fred, instead of burning CN IPs on bans.
    "wbgapi": [MIHOMO_NAME],
}


def _is_ip(text: str) -> bool:
    """True for a bare IPv4/IPv6 literal — the only thing an exit probe may say."""
    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


def probe_exit_ip(host: str, port: int, auth: str, *,
                  url: Optional[str] = None,
                  timeout: Optional[float] = None,
                  attempts: Optional[int] = None) -> Optional[str]:
    """The egress IP observed through ``host:port``, or None if the probe failed.

    The measured value is what the grouped circuit keys on — most of the mihomo
    listeners exit through one IP — so it must be observed, not assumed. Any
    failure returns None (never raises), and a response that is not an IP is
    rejected so an error page can never be stored as an "exit IP".

    Retried ``attempts`` times (default 2): the first dial through an airport
    node can exceed the timeout while the tunnel warms, and a single slow dial
    must not retire a port that is in fact healthy.

    stdlib urllib with the credentials in the proxy URL. An explicit
    ProxyHandler also overrides ``HTTP_PROXY``/``HTTPS_PROXY``, so an operator's
    own proxy cannot silently sit between us and the listener being tested.
    """
    proxy = f"http://{auth}@{host}:{port}"
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    tries = MIHOMO_EXIT_PROBE_ATTEMPTS if attempts is None else attempts
    for attempt in range(1, tries + 1):
        try:
            with opener.open(url or MIHOMO_EXIT_PROBE_URL,
                             timeout=timeout or MIHOMO_EXIT_PROBE_TIMEOUT) as resp:
                text = resp.read(64).decode("utf-8", "replace").strip()
            if _is_ip(text):
                return text
            logger.debug("mihomo exit probe %s:%s attempt %d: non-IP %r",
                         host, port, attempt, text[:32])
        except Exception as e:  # noqa: BLE001 - any failure just means "not usable"
            logger.debug("mihomo exit probe %s:%s attempt %d failed (%s)",
                         host, port, attempt, e)
    return None


def _expand(path: str) -> pathlib.Path:
    return pathlib.Path(os.path.expanduser(path))


def load_mihomo_creds(path: Optional[str] = None) -> list[dict]:
    """Per-listener credentials from ``MIHOMO_CREDS_FILE``.

    The file is a JSON list holding BOTH the mihomo listeners and the gost
    egress; only the ``mixed`` entries are mihomo ports, so everything else is
    skipped. Credentials are never logged — the password travels only into the
    ``proxies.auth`` column.
    """
    raw = json.loads(_expand(path or MIHOMO_CREDS_FILE).read_text())
    out: list[dict] = []
    for e in raw:
        if str(e.get("type", "")).lower() not in ("mixed", "mix"):
            continue  # e.g. the gost-egress row
        try:
            port = int(e["port"])
        except (KeyError, TypeError, ValueError):
            continue
        user, pw = e.get("user"), e.get("pass")
        if not user or pw is None:
            continue
        out.append({
            "port": port,
            "auth": f"{user}:{pw}",
            "fx": user,
            "listener": e.get("name") or f"fixed-{port - 30080:02d}",
        })
    out.sort(key=lambda c: c["port"])
    return out


def load_node_map(path: Optional[str] = None) -> dict[int, str]:
    """``port -> airport node`` from the port-mapping snapshot.

    Mirrors the server's ``/root/mihomo/port-mapping.txt`` — lines of
    ``fixed-01 :30081 fx01 美国02|x3.5|Stream|AI``. A missing file yields ``{}``:
    the seed then records the measured exit IP without a node label, which is
    still everything the grouped circuit needs.
    """
    p = _expand(path or MIHOMO_NODE_MAP_FILE)
    if not p.is_file():
        return {}
    out: dict[int, str] = {}
    for line in p.read_text().splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        try:
            out[int(parts[1].lstrip(":"))] = parts[3].strip()
        except ValueError:
            continue
    return out


def _seed_source_routes(
    session: Session, routes: dict[str, list[str]],
) -> dict[str, str]:
    """Idempotent upsert of the per-source provider routes."""
    result: dict[str, str] = {}
    for source, providers in routes.items():
        row = session.get(SourceProviderRoute, source)
        if row is None:
            session.add(SourceProviderRoute(source=source, providers=list(providers)))
            result[source] = "created"
        elif list(row.providers or []) == list(providers):
            result[source] = "unchanged"
        else:
            row.providers = list(providers)
            result[source] = "updated"
    return result


def seed_mihomo(
    session: Session,
    *,
    creds: Optional[list[dict]] = None,
    node_map: Optional[dict[int, str]] = None,
    prober=None,
) -> dict:
    """Register the mihomo fixed-exit pool + its international source routes.

    Every listener is probed for its egress IP first: that measured value is
    what the grouped circuit keys on, and a listener that cannot be reached is
    registered ``retired`` rather than ``active`` — a dead port must never enter
    the pool.

    Idempotent: an address whose stored fields already match is left untouched
    and reported ``unchanged``, so a re-run is a no-op unless the pool actually
    moved (a port recovered, or the airport shifted an exit IP).

    ``creds`` / ``node_map`` / ``prober`` are injectable, so this is testable
    without the network or the credential file.
    """
    creds = load_mihomo_creds() if creds is None else creds
    nodes = load_node_map() if node_map is None else node_map
    prober = probe_exit_ip if prober is None else prober

    outcomes: list[str] = []
    retired: list[str] = []
    for c in creds:
        port = c["port"]
        if port in MIHOMO_SKIP_PORTS:
            continue  # fx02 — permanently skipped (see MIHOMO_SKIP_PORTS)
        exit_ip = prober(MIHOMO_HOST, port, c["auth"])
        label = f"{MIHOMO_NAME}-{c['fx']}"
        if exit_ip is None:
            retired.append(label)
            logger.warning("mihomo %s (%s:%s) unreachable — seeding retired",
                           c["fx"], MIHOMO_HOST, port)
        outcomes.append(register_provider_address(
            session,
            provider=MIHOMO_NAME, scheme="http", ip=MIHOMO_HOST, port=port,
            auth=c["auth"], label=label,
            status="active" if exit_ip else "retired",
            provider_meta={
                "exit_ip": exit_ip,
                "node": nodes.get(port),
                "fx": c["fx"],
                "listener": c.get("listener"),
            },
        ))
    routes = _seed_source_routes(session, MIHOMO_SOURCE_ROUTES)
    session.commit()

    summary = {k: outcomes.count(k) for k in ("created", "updated", "unchanged")}
    summary.update(retired=retired, skipped_ports=sorted(MIHOMO_SKIP_PORTS),
                   routes=routes)
    logger.info("seed_mihomo: %s", summary)
    return summary


def seed_all(session: Session) -> dict:
    """Idempotently seed ban_rules, source_rate_limits, source_probes, and the
    gost-own + gost-mesh providers. Returns a summary of what was
    created/updated.

    mihomo is NOT seeded here — it does live egress probes, so it runs as its
    own step (``fd-proxy-service seed-mihomo`` / ``seed.seed_mihomo``)."""
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
