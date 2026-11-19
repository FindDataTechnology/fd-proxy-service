"""mihomo pool seeding (spec: mihomo provider + its seed contract).

The seed probes every listener for its egress IP, retires unreachable ports, and
is idempotent. The prober and the credential/node inputs are injected, so none
of this touches the network or the real credential file.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from fd_proxy_service import pool, seed
from fd_proxy_service.models import Proxy, SourceProviderRoute

EXIT_SHARED = "38.76.150.159"
EXIT_OTHER = "161.114.12.26"

# fx02 is the known-dead listener; fx04 is made unreachable by the prober.
# CREDS is the NORMALIZED shape ``seed_mihomo(creds=...)`` takes; RAW is the
# on-disk credential-file shape ``load_mihomo_creds`` parses.
CREDS = [
    {"port": 30081, "auth": "fx01:pw1", "fx": "fx01", "listener": "fixed-01"},
    {"port": 30082, "auth": "fx02:pw2", "fx": "fx02", "listener": "fixed-02"},
    {"port": 30083, "auth": "fx03:pw3", "fx": "fx03", "listener": "fixed-03"},
    {"port": 30084, "auth": "fx04:pw4", "fx": "fx04", "listener": "fixed-04"},
]
RAW = [
    {"port": 30081, "user": "fx01", "pass": "pw1", "type": "mixed", "name": "fixed-01"},
    {"port": 30082, "user": "fx02", "pass": "pw2", "type": "mixed", "name": "fixed-02"},
    {"port": 30083, "user": "fx03", "pass": "pw3", "type": "mixed", "name": "fixed-03"},
    {"port": 30084, "user": "fx04", "pass": "pw4", "type": "mixed", "name": "fixed-04"},
    # the same file also carries the gost egress — not a mihomo listener
    {"port": 30080, "user": "fdproxy", "pass": "sekrit", "type": "http",
     "name": "gost-egress"},
]
NODES = {30081: "美国02|x3.5", 30083: "美国07|x1.5", 30084: "美国01|x5.0"}
EXITS = {30081: EXIT_SHARED, 30083: EXIT_OTHER}  # 30084 absent -> probe fails


def _prober(host, port, auth):
    return EXITS.get(port)


def _by_label(session):
    return {p.label: p for p in session.query(Proxy).filter_by(provider="mihomo")}


# ─── registration ───────────────────────────────────────────────────────────

def test_registers_reachable_listeners_and_skips_fx02(session):
    out = seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)

    rows = _by_label(session)
    assert set(rows) == {"mihomo-fx01", "mihomo-fx03", "mihomo-fx04"}
    assert "mihomo-fx02" not in rows  # dead port is never registered
    assert rows["mihomo-fx01"].status == "active"
    assert rows["mihomo-fx04"].status == "retired"  # unreachable -> not in the pool
    assert out["created"] == 3
    assert out["retired"] == ["mihomo-fx04"]
    assert out["skipped_ports"] == [30082]


def test_registers_the_tailscale_address_and_port(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    row = _by_label(session)["mihomo-fx01"]
    assert (row.scheme, row.ip, row.port) == ("http", seed.MIHOMO_HOST, 30081)
    assert seed.MIHOMO_HOST == "<pool-host>"  # windows-laptop; <node> is retired
    assert row.provider == "mihomo"


def test_retired_listener_is_not_in_the_pool(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    labels = [p.label for p in pool.provider_addresses(session, provider="mihomo")]
    assert labels == ["mihomo-fx01", "mihomo-fx03"]


# ─── provider metadata (spec: exit IP + port↔node snapshot) ─────────────────

def test_records_the_measured_exit_ip_and_node_snapshot(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    meta = _by_label(session)["mihomo-fx01"].provider_meta
    assert meta["exit_ip"] == EXIT_SHARED
    assert meta["node"] == "美国02|x3.5"
    assert meta["fx"] == "fx01"
    assert meta["listener"] == "fixed-01"


def test_credentials_land_only_in_the_auth_column(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    row = _by_label(session)["mihomo-fx01"]
    assert row.auth == "fx01:pw1"
    # nothing else on the row (or in the metadata) carries the password
    assert "pw1" not in json.dumps(row.provider_meta)


def test_seeded_ports_on_one_exit_share_a_circuit_unit(session):
    """Seed + unit mechanism compose: listeners that measured the SAME exit IP
    must collapse onto one circuit, which is the whole point of the grouping."""
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES,
                     prober=lambda h, p, a: EXIT_SHARED)  # every port reachable
    rows = session.query(Proxy).filter_by(provider="mihomo").all()
    assert {pool.circuit_unit(r) for r in rows} == {EXIT_SHARED}


def test_seed_tolerates_a_missing_node_map(session):
    out = seed.seed_mihomo(session, creds=CREDS, node_map={}, prober=_prober)
    assert out["created"] == 3
    assert _by_label(session)["mihomo-fx01"].provider_meta["node"] is None


# ─── idempotency (spec: re-running the seed is a no-op) ─────────────────────

def test_second_run_changes_nothing(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    before = {lbl: p.toDict() for lbl, p in _by_label(session).items()}

    out = seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)

    after = {lbl: p.toDict() for lbl, p in _by_label(session).items()}
    assert out["created"] == 0 and out["updated"] == 0
    assert out["unchanged"] == 3
    assert after == before  # not just no duplicates — nothing was written at all


def test_recovered_listener_is_reactivated(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    recovered = {30081: EXIT_SHARED, 30083: EXIT_OTHER, 30084: EXIT_SHARED}

    out = seed.seed_mihomo(session, creds=CREDS, node_map=NODES,
                           prober=lambda h, p, a: recovered.get(p))

    row = _by_label(session)["mihomo-fx04"]
    assert row.status == "active"
    assert row.provider_meta["exit_ip"] == EXIT_SHARED
    assert out["updated"] == 1 and out["unchanged"] == 2


def test_exit_ip_drift_is_picked_up(session):
    """The airport can move a port to a different exit; the stored value must
    follow, since the grouped circuit keys on it."""
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    drifted = {30081: "64.227.149.42", 30083: EXIT_OTHER}

    out = seed.seed_mihomo(session, creds=CREDS, node_map=NODES,
                           prober=lambda h, p, a: drifted.get(p))

    assert _by_label(session)["mihomo-fx01"].provider_meta["exit_ip"] == "64.227.149.42"
    assert out["updated"] == 1


# ─── source routes ─────────────────────────────────────────────────────────

def test_seeds_the_international_source_route(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    assert list(session.get(SourceProviderRoute, "fred").providers) == ["mihomo"]


def test_re_run_does_not_churn_the_route(session):
    seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    out = seed.seed_mihomo(session, creds=CREDS, node_map=NODES, prober=_prober)
    assert out["routes"]["fred"] == "unchanged"


# ─── input parsing ─────────────────────────────────────────────────────────

def test_creds_loader_maps_the_file_shape_and_drops_the_gost_row(tmp_path):
    f = tmp_path / "creds.json"
    f.write_text(json.dumps(RAW))
    # exact round-trip: on-disk shape -> the normalized shape the seed takes,
    # with the non-mixed (gost) entry dropped
    assert seed.load_mihomo_creds(str(f)) == CREDS


def test_creds_loader_skips_malformed_entries(tmp_path):
    f = tmp_path / "creds.json"
    f.write_text(json.dumps([
        {"port": 30081, "user": "fx01", "pass": "pw", "type": "mixed"},
        {"port": "not-a-port", "user": "fx02", "pass": "pw", "type": "mixed"},
        {"port": 30083, "pass": "pw", "type": "mixed"},   # no user
        {"port": 30084, "user": "fx04", "type": "mixed"},  # no pass
    ]))
    assert [c["port"] for c in seed.load_mihomo_creds(str(f))] == [30081]


def test_node_map_parser_ignores_junk(tmp_path):
    f = tmp_path / "map.txt"
    f.write_text("fixed-01 :30081 fx01 美国02|x3.5|Stream|AI\n"
                 "garbage line\n"
                 "fixed-03 :30083 fx03 美国07|x1.5\n")
    assert seed.load_node_map(str(f)) == {
        30081: "美国02|x3.5|Stream|AI", 30083: "美国07|x1.5"}


def test_exit_probe_rejects_non_ip_responses():
    """A captive portal or an error page must never be stored as an exit IP."""
    assert seed._is_ip("38.76.150.159") is True
    assert seed._is_ip("2001:db8::1") is True
    assert seed._is_ip("<html>502 Bad Gateway</html>") is False
    assert seed._is_ip("") is False


def _stub_opener(monkeypatch, *, fail_first=False, body=b"38.76.150.159"):
    """Patch urllib's opener with a counting stub."""
    calls = {"n": 0}

    class _Resp:
        def read(self, n):
            return body

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Opener:
        def open(self, url, timeout=None):
            calls["n"] += 1
            if fail_first and calls["n"] == 1:
                raise TimeoutError("cold dial")
            return _Resp()

    monkeypatch.setattr(seed.urllib.request, "build_opener",
                        lambda *a, **k: _Opener())
    return calls


def test_probe_retries_a_transient_failure(monkeypatch):
    """Measured on the real pool: fx23 timed out once at 8 s, then answered in
    1.8-4.4 s. A single cold dial must not retire a healthy listener."""
    calls = _stub_opener(monkeypatch, fail_first=True)
    assert seed.probe_exit_ip("<node>", 30103, "fx23:pw") == "38.76.150.159"
    assert calls["n"] == 2  # the retry did the work


def test_probe_gives_up_after_every_attempt(monkeypatch):
    calls = _stub_opener(monkeypatch, fail_first=False, body=b"<html>nope</html>")
    assert seed.probe_exit_ip("<node>", 30082, "fx02:pw", attempts=3) is None
    assert calls["n"] == 3


# ─── the real credential asset (skipped when absent, e.g. in CI) ───────────

def test_real_creds_asset_parses_to_the_35_windows_listeners():
    # The live pool moved to windows-laptop on 2026-11-08 (31081-31115); the old
    # the db host-server2 asset (~/.finddata/proxy-creds.json, 30081-30108) is retired.
    p = pathlib.Path.home() / ".finddata" / "mihomo-win-creds.json"
    if not p.is_file():
        pytest.skip("no local windows mihomo credential file")
    creds = seed.load_mihomo_creds(str(p))
    assert [c["port"] for c in creds] == list(range(31081, 31116))  # 35 listeners
    assert all(c["auth"].count(":") == 1 for c in creds)
