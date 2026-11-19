# fd-proxy-service

[English](README.md) | **中文**

finddata 爬虫栈的独立代理服务。它负责代理 Provider 选择、按 `(source, addr_id)`
维度的熔断器，以及每个 worker 的转发器（forwarder），让 `scraw` 爬虫本身对代理
无感——爬虫只需调用集群内的一个本地地址，其余由本服务处理。

> **这是基础设施，不是数据源。** 与 `fd-*` 数据源包不同，`fd-proxy-service` 不声明
> `DatasourceManifest`，也不注册 `fd_open_data_mcp.datasources` 入口，因此
> `fd-open-data-mcp register-discovered` 不会发现它。它以独立 Pod 部署，不安装进
> 爬虫的数据源注册表。

## 架构（两层）

```
┌──────────── 控制面（主控集群）────────────────────────────────────────────┐
│  proxy-control  Deployment + Service（NodePort 30090）                    │
│   ├─ Provider 注册表   （主控 DB：providers + proxies 行）              │
│   ├─ 熔断状态          （专用 proxy-redis，NodePort 30381）               │
│   └─ HTTP API         POST /acquire {source} → {upstream_url, token}     │
│                       POST /release  {token, outcome}                    │
│                       GET  /health /providers /circuits                  │
└──────────────────────────────────────────────────────────────────────────┘
        ▲                                       ▲
        │ 控制 API（低 QPS，批量）               │ 熔断读写（热路径，
        │  —— worker 走主控 NodePort             │  直连 proxy-redis
        │    30090/30381，跨集群可达）            │  NodePort 30381）
        │                                         │
┌───────┴─────────────────────────────────────────┴───────────────────────┐
│  proxy-fw  hostNetwork Job（每个 worker 集群）                             │
│   ├─ 监听 :8080（hostNetwork 节点 IP；对集群内爬虫 worker 可达）           │
│   ├─ 每请求：本地熔断感知地在所有 Provider 间挑选上游                      │
│   ├─ /acquire 返回可选上游 + token                                        │
│   └─ /release 把结果写回熔断器（proxy-redis）+ 异步回传                    │
│      控制面 API（批量，fire-and-forget）                                   │
└───────────────────────────────────────────────────────────────────────────┘
        ▲
        │ POST proxy-fw:8080/acquire  → {upstream_url, token}
        │ POST proxy-fw:8080/release  → {token, outcome}
┌───────┴───────────────────────────────────────────────────────────────────┐
│  scraw 爬虫 worker pod（任意集群）                                         │
│   akshare / yfinance / edgar / wbgapi / datacommons / polygon …           │
│   调 acquire，经注入 shim 注入 upstream_url，在本地分类 ban 响应            │
│   （ban_rules），再调 release。对 Provider 选择 + 熔断逻辑完全无感。       │
└───────────────────────────────────────────────────────────────────────────┘
```

**为何用两层而非单一中央代理。** 单一中央代理会重新引入跨服务器 NodePort 的
老问题（worker 侧 NodePort 被防火墙 / Kong 劫持）。两层架构把热路径留在集群内：
worker 只与本集群的 `proxy-fw` 通信（localhost / ClusterIP，无防火墙），`proxy-fw`
再与本集群的 gost 通信（集群内，无防火墙）。唯一的跨集群跳转是
`proxy-fw → 主控控制面 API / proxy-redis`，走的是**主控** NodePort——已验证
主控 NodePort 对 worker 可达。

## `ProxyProvider` 插件抽象

新增一个代理源 = 新增一个类 + 一行注册表记录。爬虫零改动，Spec 零重写。

```python
class ProxyProvider(Protocol):
    kind: Literal["endpoint", "pool"]
    name: str
    def acquire(self, source: str) -> Acquisition | None: ...
    def release(self, source: str, addr_id: str, outcome: str) -> None: ...
    def health(self) -> dict: ...
```

- **`endpoint`** —— 单一固定上游地址，其 IP 由上游侧轮换（付费轮换代理 API、tor）。
  转发器经此单一地址路由；熔断器以 Provider 整体为粒度跟踪。
- **`pool`** —— 一组地址，按请求选择 + 按地址熔断（免费代理列表、多个 gost 实例）。
  转发器通过熔断 + 限流每次挑一个地址。

### 首个 Provider：`gost-own`

每个 worker 集群已部署的 gost hostNetwork Job 成为一个 `pool` Provider，名为
`gost-own`，只有一个元素：本集群的 gost，地址为节点 IP `:30080`（集群内——无防火墙）。
跨服务器的公网 NodePort 路径**作为出口机制被废弃**；一个 worker 只经*本*集群的 gost
出口。真正解耦的 IP 日后由外部 `endpoint` Provider（付费轮换 API / tor）提供，它们
路由到公网域名，而非兄弟服务器的 NodePort。

## 安装

```bash
pip install fd-proxy-service
```

## 用法

### 控制面（主控）

```bash
export PROXY_REDIS_URL=redis://...@<proxy-redis-host>:30381/0
export DATABASE_URL=postgresql://...@guangzhou-the db host:30432/fd_open_data
fd-proxy-service migrate     # 增量：加 provider + provider_meta 列
fd-proxy-service seed       # 注册 gost-own + 其 localhost:30080 地址
proxy-control               # 在 :8000 提供控制 API（NodePort 30090）
```

### 每 worker 转发器

```bash
export PROXY_REDIS_URL=redis://...@<proxy-redis-host>:30381/0
export FD_EGRESS_AUTH=fdproxy:...      # gost basic-auth（生产环境从 k8s Secret 读取）
proxy-fw                                # 在 :8080 提供 /acquire /release
```

### 爬虫 worker（scraw）侧

```bash
export FD_PROXY_FORWARDER=http://proxy-fw.scraw:8080
# 注入 shim 每次抓取调用 <forwarder>/acquire + /release；其余什么都不用做。
```

当 `FD_PROXY_FORWARDER` 未设置时（本地开发），shim 不注入任何代理，行为与代理前的
直连出口基线一致（无回归）。

## Ships-dark（优雅降级）

若 `proxy-redis` 不可达，所有熔断器读为 CLOSED，转发器经 `gost-own` 路由——在 Redis
恢复前抓取仍会成功，只是没有 IP 轮换。任何抓取都不会因控制面 API 一次往返而阻塞
（熔断读取在本地；`/release` 是 fire-and-forget）。

## License

MIT
