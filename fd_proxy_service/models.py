"""SQLAlchemy models for fd-proxy-service.

This service owns a *subset* of the fd-open-data-mcp ontology tables — only the
proxy-pool / ban-rule / rate-limit / probe tables it reads and writes. They map
to the SAME physical tables in the master DB (``proxies``, ``ban_rules``,
``source_rate_limits``, ``source_probes``); ``fd-open-data-mcp`` keeps its own
ORM definitions for the tables the crawler still touches. SQLAlchemy tolerates
two mapped classes against one physical table, and the crawler never reads the
``provider``/``provider_meta`` columns added by migration 001, so the two
definitions do not collide.

Defining the models locally keeps ``fd-proxy-service`` a *standalone* package:
it does not depend on ``fd-open-data-mcp`` at install time (the service is
pip-installed on its own, not vendored into the crawler).
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    JSON,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB as _PG_JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# Postgres stores JSONB; SQLite (unit tests) can't compile that type, so the
# shared alias degrades to generic JSON there. One instance reused across
# columns is fine — Column copies the type on attach.
JSONB = _PG_JSONB().with_variant(JSON(), "sqlite")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Proxy(Base):
    """An upstream proxy address, owned by a provider.

    ``scheme='direct'`` means no upstream proxy (the cluster's own egress),
    ranked first so upstreams are only used when direct is banned. The
    ``provider`` column (migration 001) names which provider owns this row
    (``gost-own``, ``rotate-api``, …); ``provider_meta`` holds provider-specific
    metadata (tor circuit id, paid-API session handle, region tag).
    Permanently-banned proxies are retired; replacements start clean.
    """
    __tablename__ = "proxies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    scheme = Column(String(16), nullable=False)  # direct / http / https / socks5
    ip = Column(String(64), nullable=False)      # "direct" for scheme=direct, else the IP
    port = Column(Integer, nullable=True)
    auth = Column(String(255), nullable=True)   # "user:pass" or None
    status = Column(String(16), nullable=False, default="active")  # active / retired
    label = Column(String(64), nullable=True)
    cluster_id = Column(Integer, nullable=True, index=True)  # per-cluster direct egress (None = shared/legacy)
    # ── migration 001 (add-proxy-service): provider-managed rows ──────────────
    provider = Column(String(64), nullable=True)  # owning provider name (gost-own, rotate-api, …)
    provider_meta = Column(JSONB, nullable=True)  # provider-specific metadata
    # ── migration 002 (add-mihomo-proxy-provider) ─────────────────────────────
    # Per-address global in-flight cap (all sources aggregated). NULL = the
    # provider's own default (mihomo=2, others uncapped).
    max_concurrency = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False, default=_now)
    retired_at = Column(DateTime, nullable=True)

    def toDict(self) -> dict:
        return {
            "id": self.id, "scheme": self.scheme, "ip": self.ip, "port": self.port,
            "auth": self.auth, "status": self.status, "label": self.label,
            "cluster_id": self.cluster_id, "provider": self.provider,
            "provider_meta": self.provider_meta,
            "max_concurrency": self.max_concurrency,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "retired_at": self.retired_at.isoformat() if self.retired_at else None,
        }


class SourceProxyHealth(Base):
    """Cold aggregate of per-(source, proxy_id) circuit health (panel-ops-console).

    Maps the SAME physical master-DB table the crawler's fd-open-data-mcp ORM
    writes; added here so the management API can persist a manual circuit
    reset to the auditable record, not just the Redis hot state. Columns match
    the crawler-side definition; the crawler keeps writing the fetch-driven
    updates, this class only touches rows on explicit management actions.
    """
    __tablename__ = "source_proxy_health"
    __table_args__ = (
        UniqueConstraint("source", "proxy_id", name="uq_source_proxy"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False, index=True)
    proxy_id = Column(Integer, nullable=False, index=True)
    state = Column(String(16), nullable=False, default="closed")  # closed / open / half_open
    fail_streak = Column(Integer, nullable=False, default=0)
    success_streak = Column(Integer, nullable=False, default=0)
    last_fetch_at = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    banned_at = Column(DateTime, nullable=True)
    cooldown_until = Column(DateTime, nullable=True)
    open_cycles = Column(Integer, nullable=False, default=0)
    permanent = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, nullable=False, default=_now, onupdate=_now)


class BanRule(Base):
    """Per-source ban-classification rule. Matched in priority order (desc); first
    match wins. rule_type: status (http status code/pattern), error (exception
    message substring), body (response body regex). classification: ok /
    transient / ban / blocked. ``streak_min`` gates the rule (e.g.
    RemoteDisconnected -> ban only after streak >= 3)."""
    __tablename__ = "ban_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False, index=True)
    rule_type = Column(String(16), nullable=False)  # status / error / body
    pattern = Column(String(255), nullable=False)
    classification = Column(String(16), nullable=False)  # ok / transient / ban / blocked
    streak_min = Column(Integer, nullable=False, default=0)
    priority = Column(Integer, nullable=False, default=0)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=_now)


class SourceRateLimit(Base):
    """Per-source politeness rate limit (token bucket). Distinct from refresh
    frequency scheduling. Enforced at fetch time against Redis
    ``rate:{source}:{proxy_id}``."""
    __tablename__ = "source_rate_limits"
    __table_args__ = (
        UniqueConstraint("source", name="uq_source_rate_limit"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False, index=True)
    max_qps = Column(Float, nullable=False, default=1.0)
    max_concurrent = Column(Integer, nullable=False, default=4)
    updated_at = Column(DateTime, default=_now, onupdate=_now)


class SourceProbe(Base):
    """Per-source probe command used by the probe job to test whether a banned
    ``(source, proxy_id)`` has recovered. Data-driven (not code) so onboarding a
    new source is a row insert, not a code change. ``params`` is a JSON dict of
    the upstream call's kwargs (a cheap, known-good fetch - the result is not
    used, only whether the call is classified as a ban)."""
    __tablename__ = "source_probes"
    __table_args__ = (
        UniqueConstraint("source", name="uq_source_probe"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False, index=True)
    command = Column(String(128), nullable=False)
    params = Column(JSON, nullable=False, default=dict)
    enabled = Column(Boolean, nullable=False, default=True)
    updated_at = Column(DateTime, default=_now, onupdate=_now)


class SourceProviderRoute(Base):
    """Ordered per-source provider preference (migration 002).

    Absent row — or an empty ``providers`` list — means *unconfigured*: the
    global priority ladder applies. A non-empty list pins selection to exactly
    those providers, in order. Exhausting the list does NOT silently fall back
    to a provider outside it: for an international source whose route is
    ``["mihomo"]``, falling back to a gost CN exit would spend the fetch (and a
    ban) on an egress the route exists to avoid.
    """
    __tablename__ = "source_provider_routes"

    source = Column(String(64), primary_key=True)
    providers = Column(JSONB, nullable=False, default=list)  # ordered provider names
    updated_at = Column(DateTime, nullable=False, default=_now, onupdate=_now)

