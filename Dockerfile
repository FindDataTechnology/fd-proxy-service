# One image, two entrypoints for fd-proxy-service.
#
# The same image runs either:
#   - `proxy-control` (the control plane; master cluster; serves the
#     acquire/release/health/providers/circuits HTTP API on :8000)
#   - `proxy-fw`      (the per-worker data-plane forwarder; serves the same
#     acquire/release contract on :8080, owns the local circuit snapshot +
#     ships-dark degradation)
# Selected by the k8s Deployment/Job `command:` (see k8s/proxy-control.yaml,
# k8s/proxy-fw-job.yaml, k8s/proxy-fw-deployment.yaml). The `fd-proxy-service`
# CLI is also available for ad-hoc `seed` / `providers` / `circuits` ops.
#
# Reuses the scraw-fd-open-data-mcp pattern: python:3.12-slim builder + runtime,
# /opt/venv. No scrapy/akshare here — this is infrastructure, not a crawler.
#
# fd-proxy-service is NOT on PyPI yet (no OIDC publish in this change), so the
# image vendors the local checkout (`pip install /build/fd-proxy-service`).
# Once it is published, swap to `pip install fd-proxy-service` like the scraw
# image does for fd-open-data-mcp.
# Overridable: the cheap-fleet build host cannot reach Docker Hub, so its
# Jenkins job passes a mirror (`--build-arg BASE_IMAGE=docker.m.daocloud.io/...`).
# Same convention as the platform repo. The default keeps GitHub Actions working.
ARG BASE_IMAGE=python:3.12-slim

FROM ${BASE_IMAGE} AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libssl-dev libffi-dev && rm -rf /var/lib/apt/lists/*
COPY . /build/fd-proxy-service
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install --no-cache-dir /build/fd-proxy-service

FROM ${BASE_IMAGE}
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
# k8s mounts nothing here at runtime; the app reads config from env
# (PROXY_REDIS_URL, PROXY_DATABASE_URL, FD_EGRESS_AUTH, etc.). Copy the source
# for inspection / `fd-proxy-service` CLI use, but the venv is the runtime.
COPY . /app
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
# No ENTRYPOINT: the k8s manifest selects the entrypoint via `command:`.
# A bare `docker run` prints help.
CMD ["fd-proxy-service", "--help"]
