"""fd-proxy-service: standalone proxy service for the finddata crawl stack.

A standalone service (NOT a datasource) that owns proxy provider selection, the
per-(source, addr_id) circuit breaker, and per-worker forwarding. Two tiers:

- **Control plane** (``proxy-control``) — runs on the master cluster; owns the
  provider registry (master DB ``proxies`` table) + circuit state (dedicated
  ``proxy-redis``) + an HTTP control API (``/acquire``, ``/release``,
  ``/health``, ``/providers``, ``/circuits``).
- **Per-worker forwarder** (``proxy-fw``) — runs as a hostNetwork Job on each
  worker cluster; holds a local cached circuit snapshot, serves
  ``/acquire``/``/release`` to in-cluster crawl workers on :8080, and writes
  outcomes through to ``proxy-redis``.

Providers drop in as ``ProxyProvider`` classes (``endpoint`` or ``pool`` kind);
the first shipped provider is ``gost-own`` (this cluster's own gost). scraw
consumes the service by calling ``proxy-fw:8080/acquire`` + ``/release`` and
stays proxy-ignorant — it does no provider selection or circuit logic itself.

This package is infrastructure, not an ``fd-*`` datasource: it declares NO
``DatasourceManifest`` and NO entry-point under
``fd_open_data_mcp.datasources``, so ``fd-open-data-mcp register-discovered``
does not pick it up.
"""
__version__ = "0.1.0"
