"""Entry point for the ``proxy-fw`` console script.

Runs the forwarder FastAPI app (control app + circuit snapshot) under uvicorn,
bound to :8080 (hostNetwork node IP; ``PROXY_FW_HOST``/``PROXY_FW_PORT`` env).
The k8s Job ``command: [proxy-fw]`` selects this entrypoint (one image, two
entrypoints — ``proxy-control`` is the other).

Env:
  PROXY_REDIS_URL    → redis://:***@<proxy-redis-host>:30381/0 (cross-cluster, authed)
  PROXY_DATABASE_URL → master DB URL (from master-db-secret; reads the
                       proxies/provider rows — the forwarder is a read replica
                       of the provider registry for the hot path)
  PROXY_FW_HOST / PROXY_FW_PORT → bind (default 0.0.0.0:8080)
"""
from __future__ import annotations

import os


def main() -> None:
    import uvicorn

    from fd_proxy_service.forwarder.server import create_app

    host = os.environ.get("PROXY_FW_HOST", "0.0.0.0")
    port = int(os.environ.get("PROXY_FW_PORT", "8080"))
    uvicorn.run(create_app(), host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
