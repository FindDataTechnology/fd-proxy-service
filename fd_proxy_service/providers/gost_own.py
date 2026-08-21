"""``gost-own``: the first concrete provider — each worker's own cluster gost.

``gost-own`` is a ``pool``-kind provider with ONE address: this cluster's own
gost forward proxy at ``127.0.0.1:30080`` (in-cluster reach, no firewall).
Each worker cluster's forwarder resolves ``127.0.0.1:30080`` to ITS OWN gost
pod (hostNetwork), so one shared ``proxies`` row (``provider='gost-own'``)
serves the whole fleet.

The gost basic-auth is read from ``FD_EGRESS_AUTH`` AT SEED TIME
(``seed.seed_gost_own``) and baked onto the ``proxies.auth`` column; this
provider reads the auth from the row at acquire time (via
``build_upstream_url``), not from the env per request. ``FD_EGRESS_AUTH`` is
surfaced in ``health()`` only so ops can see whether the default (insecure)
credential is still in effect — the gost NodePort 30080 is public, so a known
default password must not ship to prod; override via a k8s Secret.

Loopback rewrite: the seeded ``127.0.0.1`` names the forwarder's OWN node
(gost runs hostNetwork beside it), but the ``upstream_url`` is dialed by the
CRAWL pod on that node, whose loopback is its own netns — nothing listens
there. At acquire time the provider rewrites the loopback host to the
forwarder's node IP (``K8S_NODE_IP``, downward API ``status.hostIP``, set in
proxy-fw-job.yaml). Without the env (local dev, or the central proxy-control
which hosts no gost) the URL passes through unchanged.

Circuit semantics: the single shared ``proxies.id`` means the circuit key
``circuit:{source}:{gost-own-id}`` is SHARED across clusters — a ban on one
cluster's gost IP taints all clusters' gost-own. This is the known limitation
called out in the design: genuinely decoupled per-IP circuits come from
external ``endpoint`` providers (paid rotating APIs) added later, where the IP
rotates upstream-side and the single shared circuit is the *correct* semantic.
"""
from __future__ import annotations

import os
import urllib.parse
from typing import Optional

from sqlalchemy.orm import Session

from fd_proxy_service.providers.pool_provider import PoolProvider

_DEFAULT_AUTH = "fdproxy:CHANGE_ME"  # must override via FD_EGRESS_AUTH in prod

_LOOPBACKS = ("127.0.0.1", "localhost")


def _node_reachable(url: str) -> str:
    """Rewrite a loopback upstream host to the forwarder's own node IP.

    ``K8S_NODE_IP`` (downward API) is the node both the forwarder and its gost
    bind (hostNetwork); a crawl pod on that node reaches them at that IP. No
    env (local dev / the central proxy-control, which has no gost) leaves the
    URL untouched — correct when caller and forwarder share a netns.
    """
    node_ip = os.environ.get("K8S_NODE_IP", "").strip()
    if not node_ip:
        return url
    parts = urllib.parse.urlsplit(url)
    if parts.hostname not in _LOOPBACKS:
        return url
    userinfo, _, _ = parts.netloc.rpartition("@")
    netloc = f"{node_ip}:{parts.port}" if parts.port else node_ip
    if userinfo:
        netloc = f"{userinfo}@{netloc}"
    return urllib.parse.urlunsplit(
        (parts.scheme, netloc, parts.path, parts.query, parts.fragment))


class GostOwnProvider(PoolProvider):
    """The per-worker own-cluster gost forwarder (kind=pool, one address)."""

    def __init__(self, session: Session, priority: int = 0):
        super().__init__(name="gost-own", session=session, priority=priority)

    def acquire(self, source: str, exclude: Optional[list[int]] = None):
        acq = super().acquire(source, exclude=exclude)
        if acq is None or not acq.upstream_url:
            return acq
        acq.upstream_url = _node_reachable(acq.upstream_url)
        return acq

    def health(self) -> dict:
        h = super().health()
        h["auth_source"] = "FD_EGRESS_AUTH" if os.environ.get("FD_EGRESS_AUTH") else (
            "default (INSECURE — set FD_EGRESS_AUTH via k8s Secret)"
        )
        return h
