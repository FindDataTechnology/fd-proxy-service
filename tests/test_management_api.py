"""Management API tests (panel-ops-console, spec proxy-service deltas).

Covers: paid-static batch import (idempotent, pipe format, per-address
circuits via provider scoping), retire/reactivate lifecycle, manual circuit
reset (cold-table recording; Redis dark mode), and per-source rate-limit
editing effective without redeploy.
"""
from __future__ import annotations

import pytest

from fd_proxy_service import management
from fd_proxy_service.models import Proxy, SourceProxyHealth


THREE = (
    "122.246.5.153|20138|s7coink1|d1269o0\n"
    "122.246.5.134|20312|spzcf2r1|celx3b7\n"
    "122.246.5.153|20498|hasdoc71|jh3dqdz\n"
)


# ─── 2.1 batch import ────────────────────────────────────────────────────────
def test_import_three_lines_creates_rows(session):
    out = management.import_static_endpoints(
        session, "paid-static", management.parse_endpoint_lines(THREE))
    assert out["imported"] == 3 and out["skipped"] == 0
    rows = session.query(Proxy).filter_by(provider="paid-static").all()
    assert len(rows) == 3
    by_port = {r.port: r for r in rows}
    assert by_port[20138].auth == "s7coink1:d1269o0"
    assert all(r.status == "active" for r in rows)


def test_reimport_is_idempotent(session):
    eps = management.parse_endpoint_lines(THREE)
    management.import_static_endpoints(session, "paid-static", eps)
    out = management.import_static_endpoints(session, "paid-static", eps)
    # spec: re-importing the same three lines reports zero imported, three skipped
    assert out["imported"] == 0 and out["skipped"] == 3
    assert session.query(Proxy).filter_by(provider="paid-static").count() == 3


def test_import_bad_line_rejected(session):
    with pytest.raises(ValueError, match="line 2"):
        management.parse_endpoint_lines("1.2.3.4|8080\nnot-an-ip|port\n")


def test_retired_address_excluded_from_provider_selection(session):
    """Imported rows join the standard per-address selection machinery
    (provider-scoped ProxySelector + circuits); a retired address drops out
    immediately while the rest stay selectable."""
    management.import_static_endpoints(
        session, "paid-static", management.parse_endpoint_lines(THREE))
    rows = session.query(Proxy).filter_by(provider="paid-static").all()
    victim, keep = rows[0], rows[1]
    management.set_proxy_status(session, victim.id, "retired")
    from fd_proxy_service.pool import provider_addresses
    ids = [p.id for p in provider_addresses(session, provider="paid-static")]
    assert victim.id not in ids and keep.id in ids


def test_import_via_api(client):
    resp = client.post("/management/proxies/import",
                       json={"provider": "paid-static", "text": THREE})
    assert resp.status_code == 200
    body = resp.json()
    assert body["imported"] == 3


def test_import_api_rejects_malformed(client):
    resp = client.post("/management/proxies/import",
                       json={"provider": "paid-static", "text": "x|y|z|w|v\n"})
    assert resp.status_code == 422


# ─── 2.2 lifecycle ───────────────────────────────────────────────────────────
def test_retire_and_reactivate(session):
    management.import_static_endpoints(
        session, "paid-static", management.parse_endpoint_lines(THREE))
    row = session.query(Proxy).filter_by(provider="paid-static").first()
    assert management.set_proxy_status(session, row.id, "retired")["status"] == "ok"
    session.expire(row)
    assert row.status == "retired" and row.retired_at is not None
    # action recorded with actor
    assert row.provider_meta["ops"][-1]["action"] == "retired"
    # already retired -> unchanged no-op
    assert management.set_proxy_status(session, row.id, "retired")["status"] == "unchanged"
    assert management.set_proxy_status(session, row.id, "active")["status"] == "ok"
    session.expire(row)
    assert row.status == "active" and row.retired_at is None


def test_retire_api_validates_status(client, session):
    management.import_static_endpoints(
        session, "paid-static", management.parse_endpoint_lines(THREE))
    row = session.query(Proxy).filter_by(provider="paid-static").first()
    assert client.post(f"/management/proxies/{row.id}/status",
                       json={"status": "deleted"}).status_code == 422
    resp = client.post(f"/management/proxies/{row.id}/status",
                       json={"status": "retired", "actor": "op-9"})
    assert resp.status_code == 200 and resp.json()["proxy_status"] == "retired"


def test_retire_api_not_found(client):
    assert client.post("/management/proxies/4242/status",
                       json={"status": "retired"}).json()["status"] == "not_found"


# ─── 2.3 manual circuit reset ────────────────────────────────────────────────
def test_manual_reset_writes_cold_table(session, monkeypatch):
    # explicit dark-mode redis (no PROXY_REDIS_URL/REDIS_URL in tests)
    monkeypatch.delenv("PROXY_REDIS_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    out = management.manual_reset_circuit(session, "eastmoney", 7, actor="op")
    assert out["state"] == "half_open" and out["redis"] is False
    cold = session.query(SourceProxyHealth).filter_by(
        source="eastmoney", proxy_id=7).one()
    assert cold.state == "half_open" and cold.fail_streak == 0
    assert cold.cooldown_until is None and cold.permanent is False


def test_manual_reset_api(client, session):
    resp = client.post("/management/circuits/reset",
                       json={"source": "eastmoney", "proxy_id": 3, "actor": "op"})
    assert resp.status_code == 200
    assert resp.json()["state"] == "half_open"
    cold = session.query(SourceProxyHealth).filter_by(
        source="eastmoney", proxy_id=3).one()
    assert cold.state == "half_open"


# ─── 2.4 rate limits ─────────────────────────────────────────────────────────
def test_rate_limit_upsert_and_disable(session):
    out = management.set_rate_limit(session, "eastmoney", max_qps=0.5, max_concurrent=2)
    assert out["throttled"] is True
    # editing again updates in place — the selector reads the row per request,
    # so the new limit is effective on the next call with no redeploy
    out = management.set_rate_limit(session, "eastmoney", max_qps=0.1)
    assert out["max_qps"] == 0.1
    # disabled: max_qps 0 = no forwarder-side throttling
    out = management.set_rate_limit(session, "eastmoney", max_qps=0)
    assert out["throttled"] is False
    rows = management.list_rate_limits(session)
    assert rows == [{"source": "eastmoney", "max_qps": 0, "max_concurrent": 2,
                     "throttled": False, "updated_at": rows[0]["updated_at"]}]


def test_rate_limit_disable_means_acquire_always_allows(monkeypatch):
    from fd_proxy_service import rate_limit
    # acquire() short-circuits on max_qps <= 0 regardless of redis state
    assert rate_limit.acquire("src", 1, max_qps=0) is True
    assert rate_limit.acquire("src", 1, max_qps=-1) is True


def test_rate_limit_api_roundtrip(client):
    resp = client.put("/management/rate-limits/sina",
                      json={"max_qps": 2.0, "max_concurrent": 8})
    assert resp.status_code == 200 and resp.json()["throttled"] is True
    listing = client.get("/management/rate-limits").json()
    assert {"source": "sina", "max_qps": 2.0, "max_concurrent": 8,
            "throttled": True, "updated_at": listing[0]["updated_at"]} in listing


def test_rate_limit_api_validates(client):
    assert client.put("/management/rate-limits/sina",
                      json={"max_qps": -1}).status_code == 422


# ─── management token gate ───────────────────────────────────────────────────
def test_management_token_enforced(client, monkeypatch):
    monkeypatch.setenv("MANAGEMENT_TOKEN", "sekrit")
    assert client.get("/management/rate-limits").status_code == 401
    assert client.post("/management/proxies/import",
                       json={"text": THREE}).status_code == 401
    ok = client.get("/management/rate-limits",
                    headers={"X-Management-Token": "sekrit"})
    assert ok.status_code == 200
