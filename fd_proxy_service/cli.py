"""``fd-proxy-service`` admin CLI — seed providers, list proxies / circuits.

This is the operator-facing CLI (the control-plane HTTP API is ``proxy-control``).
Run on the master node (or anywhere with master DB + proxy-redis reach):

  fd-proxy-service seed        # seed gost-own + ban_rules/rate_limits/probes
  fd-proxy-service providers   # snapshot provider health + addresses
  fd-proxy-service circuits    # snapshot circuit states from proxy-redis
"""
from __future__ import annotations

import click

from fd_proxy_service import circuit, seed
from fd_proxy_service.db import get_engine, get_session
from fd_proxy_service.models import Base
from fd_proxy_service.providers import health_all


@click.group()
def cli() -> None:
    """fd-proxy-service admin CLI."""


@cli.command(name="seed")
def seed_cmd() -> None:
    """Seed gost-own + ban_rules / rate_limits / probes into the master DB.

    Idempotent: ``Base.metadata.create_all`` is a no-op on existing tables, and
    ``seed_all`` upserts. Safe to re-run after adding a provider / rule.
    """
    Base.metadata.create_all(get_engine())
    with get_session() as s:
        seed.seed_all(s)
        s.commit()
    click.echo("seeded gost-own + ban_rules/rate_limits/probes")


@cli.command(name="providers")
def providers_cmd() -> None:
    """Snapshot every registered provider + its addresses (from the master DB)."""
    with get_session() as s:
        for prov in health_all(s):
            click.echo(prov)


@cli.command(name="circuits")
def circuits_cmd() -> None:
    """Snapshot every circuit state from proxy-redis (empty when dark)."""
    rows = circuit.all_circuits()
    if not rows:
        click.echo("(no circuits — proxy-redis is dark or empty)")
        return
    for c in rows:
        click.echo(c)


if __name__ == "__main__":
    cli()
