"""Grouped-circuit (unit-key) behaviour.

Covers the unit contract (``pool.circuit_unit``) and the spec requirement
"共享出口 IP 的分组熔断": an exit-IP ban must take out every port behind it in
one trip, sibling exits must stay healthy, and recovery is per group too.

Everything runs against the stateful ``fake_redis`` fixture, so the real state
machine in ``circuit.py`` is what is under test (not a mock).
"""
from __future__ import annotations

from fd_proxy_service import circuit, management, pool
from fd_proxy_service.models import Proxy, SourceProxyHealth, SourceRateLimit
from fd_proxy_service.forwarder.server import CircuitSnapshot
from fd_proxy_service.selector import ProxySelector

EXIT_A = "38.76.150.159"   # ~24 mihomo ports share this exit
EXIT_B = "161.114.12.26"


def _addr(session, provider, port, exit_ip=None, cap=None):
    """Insert a proxy row, optionally declaring a shared exit IP."""
    row = Proxy(
        scheme="http", ip="100.64.0.7", port=port, auth=f"fx{port}:pw",
        status="active", label=f"{provider}-{port}", provider=provider,
        provider_meta=({"exit_ip": exit_ip} if exit_ip else None),
        max_concurrency=cap,
    )
    session.add(row)
    session.commit()
    return row


def _unthrottled(session, source):
    """max_qps 0 = the selector's politeness limit is inert, so a test that
    picks repeatedly in one (frozen) second is not blocked by the QPS window."""
    session.add(SourceRateLimit(source=source, max_qps=0, max_concurrent=4))
    session.commit()


def _open(session, source, unit):
    for _ in range(circuit.BAN_THRESHOLD):
        circuit.record_outcome(source, unit, "ban")


# ─── the unit contract ───────────────────────────────────────────────────────

def test_unit_is_own_id_when_no_exit_ip(session):
    """An address declaring no exit_ip keys on str(id) — byte-identical to the
    pre-group scheme, so its live circuit state survives the upgrade."""
    a = _addr(session, "gost-mesh", 30080)
    assert pool.circuit_unit(a) == str(a.id)
    assert f"circuit:eastmoney:{pool.circuit_unit(a)}" == f"circuit:eastmoney:{a.id}"


def test_unit_is_exit_ip_when_declared(session):
    a = _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    assert pool.circuit_unit(a) == EXIT_A


def test_unit_for_unknown_id_falls_back_to_str(session):
    """An operator may reset an id whose row is already gone; the key must stay
    whatever that address used."""
    assert pool.unit_for_proxy_id(session, 4242) == "4242"


# ─── group ban / sibling isolation ───────────────────────────────────────────

def test_ban_opens_whole_exit_group(session, fake_redis):
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _addr(session, "mihomo", 30083, exit_ip=EXIT_A)
    _addr(session, "mihomo", 30084, exit_ip=EXIT_B)

    _open(session, "fred", EXIT_A)

    assert circuit.is_selectable("fred", EXIT_A) is False
    assert circuit.is_selectable("fred", EXIT_B) is True


def test_selector_returns_only_the_healthy_exit(session, fake_redis):
    """A banned exit costs one circuit check for the whole group — no port on
    it is returned, and no per-port attempt is burned."""
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _addr(session, "mihomo", 30083, exit_ip=EXIT_A)
    good = _addr(session, "mihomo", 30084, exit_ip=EXIT_B)
    _unthrottled(session, "fred")
    _open(session, "fred", EXIT_A)

    sel = ProxySelector(session)
    picks = [sel.select("fred", provider="mihomo") for _ in range(4)]
    assert all(p is not None for p in picks)
    assert {p[0] for p in picks} == {good.id}


def test_selector_does_not_shadow_group_siblings(session, fake_redis):
    """A saturated port must not hide the rest of its exit: the group is still
    selectable, so a sibling port serves the fetch."""
    busy = _addr(session, "mihomo", 30081, exit_ip=EXIT_A, cap=1)
    free = _addr(session, "mihomo", 30083, exit_ip=EXIT_A)
    _unthrottled(session, "fred")

    sel = ProxySelector(session)
    first = sel.select("fred", provider="mihomo")
    assert first[0] == busy.id  # lowest id first
    # the in-flight slot on `busy` is now taken -> the sibling must be offered
    second = sel.select("fred", provider="mihomo")
    assert second is not None and second[0] == free.id


# ─── transient vs ban (spec: a port blip must not trip the group) ────────────

def test_single_transient_leaves_group_selectable(session, fake_redis):
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    circuit.record_outcome("fred", EXIT_A, "transient")
    assert circuit.get_state("fred", EXIT_A)["state"] == "closed"
    assert circuit.is_selectable("fred", EXIT_A) is True


def test_transient_threshold_opens_group(session, fake_redis):
    """A shared egress that keeps timing out IS flaky — same rule as before,
    now applied to the exit rather than one port."""
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    for _ in range(circuit.TRANSIENT_THRESHOLD):
        circuit.record_outcome("fred", EXIT_A, "transient")
    assert circuit.is_selectable("fred", EXIT_A) is False


# ─── group recovery ─────────────────────────────────────────────────────────

def test_group_half_open_recovers_all_ports(session, fake_redis):
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _addr(session, "mihomo", 30083, exit_ip=EXIT_A)
    _open(session, "fred", EXIT_A)
    assert circuit.is_selectable("fred", EXIT_A) is False

    circuit.probe_transition("fred", EXIT_A, probe_ok=True)

    assert circuit.get_state("fred", EXIT_A)["state"] == "closed"
    assert circuit.is_selectable("fred", EXIT_A) is True


# ─── scanners must accept a non-integer unit ────────────────────────────────

def test_scanners_handle_non_integer_units(session, fake_redis):
    """The unit can be an exit IP. Both scanners used to ``int(parts[2])`` and
    raised ValueError on one — which killed the probe loop and ``/circuits``."""
    _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    # an OPEN circuit whose cooldown has already elapsed -> probe-eligible
    fake_redis.hset(f"circuit:fred:{EXIT_A}", mapping={
        "state": "open", "cooldown_until": "1", "permanent": "0"})

    assert ("fred", EXIT_A) in circuit.open_for_probe()

    rows = circuit.all_circuits()
    assert {"source": "fred", "unit": EXIT_A} in [
        {"source": r["source"], "unit": r["unit"]} for r in rows]


# ─── operator reset follows the group ───────────────────────────────────────

def test_manual_reset_resolves_group_and_records_representative(session):
    r1 = _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    r2 = _addr(session, "mihomo", 30083, exit_ip=EXIT_A)
    lo, hi = sorted((r1, r2), key=lambda r: r.id)

    out = management.manual_reset_circuit(session, "fred", hi.id, actor="op")

    assert out["unit"] == EXIT_A            # reset addresses the group
    assert out["proxy_id"] == lo.id         # cold row lands on the lowest id
    assert out["state"] == "half_open"
    cold = session.query(SourceProxyHealth).filter_by(
        source="fred", proxy_id=lo.id).one()
    assert cold.state == "half_open"


def test_manual_reset_clears_the_group_key_not_the_port_key(session, fake_redis):
    r = _addr(session, "mihomo", 30081, exit_ip=EXIT_A)
    _open(session, "fred", EXIT_A)

    management.manual_reset_circuit(session, "fred", r.id)

    assert circuit.get_state("fred", EXIT_A)["state"] == "half_open"
    # nothing was written under the bare id key
    assert circuit.get_state("fred", str(r.id))["state"] == "closed"


# ─── the forwarder's hot-path cache ─────────────────────────────────────────

def test_snapshot_is_keyed_by_unit(monkeypatch):
    """The forwarder caches circuits by (source, unit); if it still keyed by id
    it would never see a grouped exit go OPEN."""
    snap = CircuitSnapshot(ttl_sec=99)
    monkeypatch.setattr(
        "fd_proxy_service.forwarder.server.circuit.all_circuits",
        lambda: [{"source": "fred", "unit": EXIT_A, "state": "open",
                  "fail_streak": 3, "open_cycles": 1, "permanent": False,
                  "cooldown_until": None}],
    )
    snap._refresh()

    assert snap.is_selectable("fred", EXIT_A) is False
    assert snap.is_selectable("fred", "123") is True  # unrelated id unaffected


def test_snapshot_record_outcome_writes_the_unit(monkeypatch):
    snap = CircuitSnapshot(ttl_sec=99)
    seen = {}

    def _fake(source, unit, outcome):
        seen["args"] = (source, unit, outcome)
        return {"state": "open", "fail_streak": 3, "open_cycles": 1,
                "permanent": False, "cooldown_until": None}

    snap._orig_record_outcome = _fake
    snap.record_outcome("fred", EXIT_A, "ban")

    assert seen["args"] == ("fred", EXIT_A, "ban")
    assert snap.is_selectable("fred", EXIT_A) is False
