# fd-proxy-service

> **Part of the [Base line (寻数·壹座)](https://www.finddatatech.cloud/products/base) of [FindData](https://www.finddatatech.cloud)** — the proxy pool: provider registry, circuit breaker, rate limits and per-source routing for outbound fetches.

**English** | [中文](README.zh-CN.md)

Standalone proxy service for the [finddata](https://github.com/FindDataTechnology/fd-proxy-service)
crawl stack. It owns proxy provider selection, the per-`(source, addr_id)`
circuit breaker, and a per-worker forwarder, so `scraw` crawlers stay
proxy-ignorant — they call one local in-cluster address and the service handles
the rest.

> **This is infrastructure, not a datasource.** Unlike `fd-*` datasource
> packages, `fd-proxy-service` declares no `DatasourceManifest` and no
> entry-point under `fd_open_data_mcp.datasources`, so
> `fd-open-data-mcp register-discovered` does not pick it up. It is deployed as
> its own pods, not installed into the crawler's datasource registry.

## Architecture (two-tier)

```
┌──────────── control plane (master cluster) ────────────────────────────┐
│  proxy-control  Deployment + Service (NodePort 30090)                  │
│   ├─ provider registry    (master DB: providers + proxies rows)        │
│   ├─ circuit state        (dedicated proxy-redis, NodePort 30381)       │
│   └─ HTTP API            POST /acquire {source} → {upstream_url, token}│
│                          POST /release  {token, outcome}              │
│                          GET  /health /providers /circuits            │
└─────────────────────────────────────────────────────────────────────────┘
        ▲                                       ▲
        │ control API (low-qps, batched)         │ circuit R/W (hot path,
        │  — workers reach master NodePort       │  direct to proxy-redis
        │    30090/30381, which work cross-cluster│  NodePort 30381)
        │                                         │
┌───────┴─────────────────────────────────────────┴──────────────────────┐
│  proxy-fw  hostNetwork Job (EACH worker cluster)                       │
│   ├─ listens :8080 (hostNetwork node IP; in-cluster for crawl workers)  │
│   ├─ per request: local circuit-aware upstream pick across all providers│
│   ├─ /acquire returns a selectable upstream + token                     │
│   └─ /release writes the outcome to circuit (proxy-redis) + echoes to   │
│      the control API async (batched, fire-and-forget)                   │
└─────────────────────────────────────────────────────────────────────────┘
        ▲
        │ POST proxy-fw:8080/acquire  → {upstream_url, token}
        │ POST proxy-fw:8080/release  → {token, outcome}
┌───────┴──────────────────────────────────────────────────────────────────┐
│  scraw crawl worker pod (any cluster)                                    │
│   akshare / yfinance / edgar / wbgapi / datacommons / polygon …         │
│   calls acquire, injects upstream_url via the injection shim, classifies │
│   the response locally (ban_rules), calls release. Stays ignorant of     │
│   provider selection + circuit logic.                                    │
└──────────────────────────────────────────────────────────────────────────┘
```

**Why two-tier over a single central broker.** A single central broker would
re-introduce the broken cross-server NodePort problem (worker NodePorts are
firewalled / Kong-hijacked). Two-tier keeps the hot path in-cluster: the worker
talks only to its own cluster's `proxy-fw` (localhost/ClusterIP, no firewall),
and `proxy-fw` talks to its own cluster's gost (in-cluster, no firewall). The
only cross-cluster hops are `proxy-fw → master control API / proxy-redis`,
which are **master** NodePorts — and master NodePorts demonstrably work from
workers.

## The `ProxyProvider` plugin abstraction

A new proxy source = a new class + a registry row. Zero crawler edits, zero
spec rewrite.

```python
class ProxyProvider(Protocol):
    kind: Literal["endpoint", "pool"]
    name: str
    def acquire(self, source: str) -> Acquisition | None: ...
    def release(self, source: str, addr_id: str, outcome: str) -> None: ...
    def health(self) -> dict: ...
```

- **`endpoint`** — one fixed upstream address whose IP rotates upstream-side
  (a paid rotating-proxy API, tor). The forwarder routes through the single
  addr; the circuit tracks the provider as a whole.
- **`pool`** — a set of addresses with per-request selection + per-addr circuit
  (free-proxy lists, multiple gost instances). The forwarder picks one addr per
  request via circuit + rate-limit.

### First provider: `gost-own`

Each worker cluster's already-deployed gost hostNetwork Job becomes a `pool`
provider named `gost-own` with one element: that cluster's own gost at the node
IP `:30080` (in-cluster — no firewall). The cross-server public-NodePort path
is **dropped as an egress mechanism**; a worker egresses through its *own*
cluster's gost only. Genuinely decoupled IPs come later from external
`endpoint` providers (paid rotating API / tor), which route to public domains
rather than sibling-server NodePorts.

### The `mihomo` fixed-exit pool

A pool of 35 mixed listeners on `<pool-host>:31081-31115` (windows-laptop over
Tailscale; LAN side `<lan-host>`), each pinned to one airport node, each with
its own Basic auth. It is the only reliable international egress for the fleet,
registered as `mihomo` (priority 2 — after both gost providers, before
paid-static). The old the db host-server2 pool (`<node>:3008x`, 27 rows) is
retired and kept in the table for history only — see
`docs/proxy-mihomo-windows-migration.md`.

The pool is **measured, not copied**: the airport reassigns ports to nodes, so a
snapshot from days ago is already wrong. `seed-mihomo` dials every listener
through and records the exit IP it actually reports:

```bash
# credentials come from a Secret, never from the repo
kubectl -n scraw create secret generic mihomo-creds \
  --from-file=proxy-creds.json="$HOME/.finddata/mihomo-win-creds.json"
kubectl -n scraw apply -f k8s/mihomo-seed-job.yaml     # runs `fd-proxy-service seed-mihomo`
kubectl -n scraw logs job/mihomo-seed
```

A listener that stays unreachable across the retries (`MIHOMO_EXIT_PROBE_ATTEMPTS`,
default 2 — a cold first connect can outlast the probe timeout) is registered
`retired` rather than active. Re-running is idempotent: an unchanged pool reports
zeros and writes nothing. Operationally: when the listener count drops far below
the 35 registered, re-run the seed to refresh exit IPs and retire dead ports.

Those measured exit IPs are what the grouped circuit keys on. The listeners do
**not** each own an exit IP — measured 2026-11-08, 24 live listeners leave
through 7 IPs, the busiest carrying 6 of them (and 11 ports point at nodes that
are currently dead). So one ban trips a group of up to 6 instead of being
re-discovered port by port. See `docs/SOURCE_PROXY_HEALTH.md` for the grouped
circuit, per-source routing, and address-wide concurrency cap semantics.

## Install

```bash
pip install fd-proxy-service
```

## Usage

### Control plane (master)

```bash
export PROXY_REDIS_URL=redis://...@<proxy-redis-host>:30381/0
export DATABASE_URL=postgresql://...@guangzhou-the db host:30432/fd_open_data
fd-proxy-service migrate     # additive: add provider + provider_meta cols
fd-proxy-service seed       # register gost-own + its localhost:30080 addr
proxy-control               # serve the control API on :8090 (NodePort 30090)
```

### Per-worker forwarder

```bash
export PROXY_REDIS_URL=redis://...@<proxy-redis-host>:30381/0
export FD_EGRESS_AUTH=fdproxy:...      # gost basic-auth (from k8s Secret in prod)
proxy-fw                                # serve /acquire /release on :8080
```

### Crawl worker (scraw) side

```bash
export FD_PROXY_FORWARDER=http://proxy-fw.scraw:8080
# the injection shim calls <forwarder>/acquire + /release per fetch; nothing else.
```

When `FD_PROXY_FORWARDER` is unset (local dev), the shim injects nothing and
behavior matches the pre-proxy direct-egress baseline (no regression).

## Ships-dark

If `proxy-redis` is unreachable, all circuits read CLOSED and the forwarder
routes through `gost-own` — fetches still succeed without IP rotation until
redis is restored. No fetch blocks on a control-API round-trip (the circuit read
is local; `/release` is fire-and-forget).

## License

MIT
