"""Provider plugin abstraction for fd-proxy-service.

A *provider* owns upstream proxy addresses and the per-fetch selection policy.
The forwarder and control plane are provider-agnostic: they call
``registry.acquire_any(source)`` / ``registry.release(...)`` and never know
which provider served the request. Add a proxy source = add a provider class
+ seed its addresses; the crawler never changes.

Two kinds (see ``base.ProxyProvider``):
- ``pool``    — N addresses, per-addr circuit + rate-limit (``PoolProvider``).
- ``endpoint``— one rotating-upstream address, per-provider circuit
                (``EndpointProvider``).

First provider: ``GostOwnProvider`` (each worker's own cluster gost at
``127.0.0.1:30080``). External ``endpoint`` providers (paid rotating APIs) are
added later by appending to ``registry._REGISTRY`` + seeding addresses.
"""
from fd_proxy_service.providers.base import Acquisition, ProxyProvider, build_upstream_url
from fd_proxy_service.providers.endpoint_provider import EndpointProvider
from fd_proxy_service.providers.gost_own import GostOwnProvider
from fd_proxy_service.providers.pool_provider import PoolProvider
from fd_proxy_service.providers.registry import (
    acquire_any,
    all_providers,
    get_provider,
    health_all,
    release,
)

__all__ = [
    "Acquisition",
    "ProxyProvider",
    "PoolProvider",
    "EndpointProvider",
    "GostOwnProvider",
    "build_upstream_url",
    "acquire_any",
    "all_providers",
    "get_provider",
    "health_all",
    "release",
]
