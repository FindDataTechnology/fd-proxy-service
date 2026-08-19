"""Entry point for the ``proxy-control`` console script.

Runs the control-plane FastAPI app under uvicorn. Bound to :8000 inside the
container; the k8s Service (NodePort 30090 on the master cluster) fronts it.

Env:
  PROXY_REDIS_URL   → redis://proxy-redis:6379  (dedicated circuit-state Redis)
  PROXY_DATABASE_URL → master DB URL (from master-db-secret)
  PROXY_CONTROL_HOST / PROXY_CONTROL_PORT → bind (default 0.0.0.0:8000)
"""
from __future__ import annotations

import os


def main() -> None:
    import uvicorn

    from fd_proxy_service.control.app import create_app

    host = os.environ.get("PROXY_CONTROL_HOST", "0.0.0.0")
    port = int(os.environ.get("PROXY_CONTROL_PORT", "8000"))
    uvicorn.run(create_app(), host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
