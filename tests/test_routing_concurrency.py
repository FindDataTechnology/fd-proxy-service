"""Per-source provider routing + the address-wide in-flight cap.

Covers spec requirements "按源 provider 路由" and "每地址全局并发上限": a routed
source tries exactly its configured providers in order (and does NOT spill
outside the list), an unconfigured source keeps the global ladder, and the cap
aggregates across sources on one address.
"""
from __future__ import annotations

from fd_proxy_service import circuit, rate_limit
from fd_proxy_service.models import Proxy, SourceProviderRoute, SourceRateLimit
from fd_proxy_service.providers import acquire_any
from fd_proxy_service.providers.mihomo import MihomoProvider
from fd_proxy_service.providers.pool_provider import PoolProvider
from fd_proxy_service.selector import ProxySelector

EXIT_A = "38.76.150.159"
EXIT_B = "161.114.12.26"


def _addr(session, provider, port, exit_ip=None, cap=None, ip="100.64.0.7"):
    row = Proxy(
        scheme="http", ip=ip, port=port, status="active",
        label=f"{provider}-{port}", provider=provider,
        provider_meta=({"exit_ip": exit_ip} if exit_ip else None),
        max_concurrency=cap,
    )
    session.add(row)
    session.commit()
    return row


def _route(session, source, providers):
    session.add(SourceProviderRoute(source=source, providers=providers))
    session.commit()


def _unthrottled(session, source):
    """max_qps 0 = the politeness limit is inert (see conftest._FakeRedis)."""
    session.add(SourceRateLimit(source=source, max_qps=0, max_concurrent=4))
    session.commit()


def _open(source, unit):
    for _ in range(circuit.BAN_THRESHOLD):
        circuit.record_outcome(source, unit, "ban")


# ─── routing ────────────────────────────────────────────────────────────────

def test_route_sends_the_source_straight_to_the_configured_provider(session):
    """fred is international: routed to mihomo, it never touches the gost tier
    (which would only warm up through CN egress and collect bans)."""
    _addr(session, "gost-mesh", 30080, ip="100.64.0.1")
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _route(session, "fred", ["mihomo"])
    _unthrottled(session, "fred")

    acq = acquire_any("fred", session)

    assert acq is not None and acq.provider == "mihomo"


def test_route_fails_over_within_its_own_list(session, fake_redis):
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    paid = _addr(session, "paid-static", 8080)
    _route(session, "fred", ["mihomo", "paid-static"])
    _unthrottled(session, "fred")
    _open("fred", EXIT_A)  # mihomo exhausted

    acq = acquire_any("fred", session)

    assert acq is not None and acq.addr_id == paid.id


def test_exhausted_route_does_not_fall_back_outside_it(session, fake_redis):
    """Spilling onto a gost here would spend the fetch — and a ban — on exactly
    the CN egress the route exists to avoid."""
    _addr(session, "gost-mesh", 30080, ip="100.64.0.1")
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _route(session, "fred", ["mihomo"])
    _unthrottled(session, "fred")
    _open("fred", EXIT_A)

    assert acquire_any("fred", session) is None


def test_route_naming_an_unregistered_provider_is_skipped(session):
    mesh = _addr(session, "gost-mesh", 30080, ip="100.64.0.1")
    _route(session, "fred", ["does-not-exist", "gost-mesh"])
    _unthrottled(session, "fred")

    acq = acquire_any("fred", session)

    assert acq is not None and acq.addr_id == mesh.id


def test_unconfigured_source_uses_the_global_priority_ladder(session):
    mesh = _addr(session, "gost-mesh", 30080, ip="100.64.0.1")
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _unthrottled(session, "eastmoney")

    acq = acquire_any("eastmoney", session)

    # gost-mesh (priority 1) is tried before mihomo (priority 2)
    assert acq is not None and acq.addr_id == mesh.id


def test_empty_route_counts_as_unconfigured(session):
    mesh = _addr(session, "gost-mesh", 30080, ip="100.64.0.1")
    _route(session, "eastmoney", [])
    _unthrottled(session, "eastmoney")

    acq = acquire_any("eastmoney", session)

    assert acq is not None and acq.addr_id == mesh.id


# ─── the address-wide cap ───────────────────────────────────────────────────

def test_cap_saturates_across_sources(session, fake_redis):
    """The QPS limit is per (source, address); the cap is per ADDRESS — two
    sources must not be able to stack requests onto one port."""
    a = _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=2)

    assert rate_limit.try_acquire_inflight(a.id, 2) is True   # source A's fetch
    assert rate_limit.try_acquire_inflight(a.id, 2) is True   # source B's fetch
    assert rate_limit.try_acquire_inflight(a.id, 2) is False  # full for anyone


def test_saturated_attempt_does_not_consume_a_slot(session, fake_redis):
    a = _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=1)
    assert rate_limit.try_acquire_inflight(a.id, 1) is True
    assert rate_limit.try_acquire_inflight(a.id, 1) is False
    # the refused attempt must leave the counter at 1, or the address could
    # never drain
    assert fake_redis.get(f"INFLIGHT:{a.id}") == "1"


def test_cap_takes_precedence_over_source_qps(session, fake_redis):
    """Spec: a source still under its own QPS quota is skipped when the address
    is globally full."""
    a = _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=1)
    _unthrottled(session, "fred")          # QPS would allow the fetch
    assert rate_limit.try_acquire_inflight(a.id, 1) is True  # one already in flight

    assert ProxySelector(session).select("fred", provider="mihomo") is None


def test_saturated_address_is_skipped_in_favour_of_another(session, fake_redis):
    capped = _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=1)
    other = _addr(session, "mihomo", 30083, exit_ip=EXIT_B)
    _unthrottled(session, "fred")

    sel = ProxySelector(session)
    first = sel.select("fred", provider="mihomo")
    assert first is not None and first[0] == capped.id
    second = sel.select("fred", provider="mihomo")
    assert second is not None and second[0] == other.id


def test_cap_falls_back_to_the_provider_default(session, fake_redis):
    """mihomo ports carry no per-row cap; the provider default (2) must apply,
    so the shared host is protected before any column is written."""
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=None)
    _unthrottled(session, "fred")

    prov = MihomoProvider(session)
    assert prov.default_max_concurrency == 2
    assert prov.acquire("fred") is not None
    assert prov.acquire("fred") is not None
    assert prov.acquire("fred") is None   # one port, capped at 2


def test_release_returns_the_slot(session, fake_redis):
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=1)
    _unthrottled(session, "fred")

    prov = MihomoProvider(session)
    acq = prov.acquire("fred")
    assert acq is not None
    assert prov.acquire("fred") is None    # full

    prov.release("fred", acq.addr_id, "ok")

    assert prov.acquire("fred") is not None


def test_uncapped_address_never_mints_a_counter(session, fake_redis):
    """Uncapped tiers (gost/paid) must not touch the counter — including on
    release, which is why the decrement is guarded by EXISTS."""
    a = _addr(session, "paid-static", 8080)
    _unthrottled(session, "fred")

    prov = PoolProvider("paid-static", session, priority=3)
    acq = prov.acquire("fred")
    assert acq is not None
    assert fake_redis.get(f"INFLIGHT:{a.id}") is None

    prov.release("fred", acq.addr_id, "ok")

    assert fake_redis.get(f"INFLIGHT:{a.id}") is None
