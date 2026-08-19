"""Control-plane package for fd-proxy-service (the `proxy-control` server).

Exposes :func:`fd_proxy_service.control.app.create_app` — a FastAPI app with
the acquire/release contract + health/circuit snapshots. The per-worker
forwarder reuses the same app (see ``forwarder/server.py``).
"""
