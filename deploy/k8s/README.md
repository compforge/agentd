# Kubernetes 部署与 Worker 弹性

本文描述 agentd 在 Kubernetes 上的目标部署形态。Helm 安装一个可多副本的 agentd Deployment；
agentd 根据持久化的 Session 需求创建 Worker，并在 Worker 空闲后回收。一个 Worker 就是一个独立
Pod，Pod 内固定包含 Agentlet 与 Sandbox Engine 两个容器。

Helm Chart 位于 `deploy/k8s/agentd`，负责安装 agentd workload、namespace RBAC 和 Worker
PodTemplate。Worker Lifecycler 与 Connector 的运行时接线仍随 Control Plane 实现继续收敛。

## 部署拓扑

```text
                        ┌──────────────────────────────┐
Client ── Agent API ──►│ agentd Deployment (replicas=N)│
                        │                              │
                        │ Observer   Scheduler         │
                        │ Lifecycler Connector         │
                        └──────┬───────────┬───────────┘
                               │ K8s API   │ Agent API
                   Ensure/Destroy│         │ forward/stream
                               ▼           ▼
                         Worker Pod
                    ┌────────────────────────────┐
                    │ Pod                        │
                    │  Agentlet :8081            │
                    │      │ localhost           │
                    │  Sandbox Engine :8080      │
                    └────────────────────────────┘
```

每个 Worker 使用独立 Pod，不放进共享 Deployment。这样 agentd 可以精确 drain 和删除一个空闲
Worker，不会让 Kubernetes 在缩容时选中仍承载 Session 的 Pod。Worker 使用 agentd 生成的 UUID，
并写入 `agentd.compforge.dev/managed=true` 与 `agentd.compforge.dev/worker-id=<worker UUID>` labels；
Pod UID、endpoint 与 Ready 是 Observer facts。Kubernetes 可以重启 Pod 内异常
容器；Pod 消失则该 Worker 终止，Lifecycler 按 durable demand 创建具有新 identity 的 Worker。

当前 Sandbox Engine 实现使用 Hostel，但外部部署契约只称 Sandbox Engine。Agentlet 通过
`http://127.0.0.1:8080` 访问 sidecar；两个容器分别由自己的 PID 1 管理，Pod Ready 要求两者的
readiness probe 都通过。

## Control Plane 角色

| 角色 | 职责 | 不负责 |
|---|---|---|
| Observer | 周期观察 Worker Pod，持久化存在、Ready、endpoint 等事实 | 创建资源、分配 Session |
| Scheduler | 基于 Worker facts、容量与 Assignment 做纯 placement 决策 | 访问数据库/K8s、触发扩容 |
| Lifecycler | 根据持久化需求收敛 Worker 供给，推进 creating/active/draining/retired | 宣告 Ready、转发请求 |
| Provisioner | 幂等 `Ensure` / `Destroy` 一个 Worker Pod | 容量决策、调度 |
| Connector | 按 Assignment 和 Observer endpoint 转发 Agent API，并保持流式响应 | 选择 Worker、改 Assignment、管理生命周期 |

这组边界借鉴 sandctl 的 lifecycle / observer / connector 分工：动作、事实和数据面分别只有一个
owner。Lifecycler 调用 Provisioner 成功只表示 Kubernetes 接受了期望状态，只有 Observer 可以把
Worker 标为 Ready。Connector 每次从 Assignment 解析当前 Worker 和 endpoint，endpoint 是一次
路由结果，不是新的持久化 identity。

## 扩容

持久化但尚无有效 Assignment 的 Session 是 durable demand。一次调度无候选时只保留需求并唤醒
Lifecycler；Scheduler 本身不创建 Pod。

```text
pending Session
  → Scheduler: no capacity
  → Lifecycler recomputes demand gap
  → Provisioner.Ensure(worker)
  → Kubernetes creates Worker Pod
  → Observer persists Ready + endpoint
  → retry scheduling
  → persist Assignment
  → Connector forwards request to Agentlet
```

Lifecycler 每轮从数据库重新计算缺口：待分配需求减去 active Worker 的空闲 slot 和 creating Worker
即将提供的 slot。唤醒信号只是加速提示，不能代替数据库事实；agentd 重启后也能从 pending Session
恢复扩容。创建采用有上限的小批次；如果上一批 Pod 仍 Pending，则暂缓继续放大，给 Kubernetes
消化镜像、配额和节点容量压力。轻微超配允许由后续 idle 回收收敛。

## GC

### Pod GC

Worker 同时满足零绑定 Session、零执行中 Work 且超过 idle TTL 后才可回收。回收顺序固定为：

```text
active
  → live precheck
  → CAS draining
  → final safety check / checkpoint
  → CAS retired
  → Provisioner.Destroy
  → Observer confirms absent
```

`draining` 必须先持久化，使 Scheduler 不再分配新 Session。删除前再次检查 Assignment、运行状态及
安全恢复点；不能安全冻结的 Worker 留在 draining 或转为人工可诊断状态，而不是直接删除。
`retired` Worker 只允许幂等删除和缺失收敛，不能重新进入调度。带 managed label、但其 worker-id
不在 `workers` 表的 Pod 一律视为 zombie，由 Pod GC 分批幂等删除；Pod GC 不删除 Worker 数据库记录。

### DB Record GC

DB Record GC 只分批删除超过保留期的 `retired` Worker 行。删除时再次检查 phase、截止时间、Pod
已确认不存在且没有 Assignment。它不访问 Kubernetes；Pod 删除失败不能靠 Record GC 隐藏。

## 崩溃与漂移收敛

- `Ensure` 幂等：agentd 可能在创建成功、提交结果前崩溃；恢复后重复调用不能创建第二个 Worker。
- `Destroy` 幂等：资源 NotFound 视为成功。
- 数据库存在 creating Worker 而 Pod 缺失时，Provisioner 可以幂等完成本轮创建。
- active Worker 的 Pod 缺失时先退役该 Worker，再根据 durable demand 创建新 Worker。
- 存在带 agentd managed label 的 Pod 而数据库无对应 Worker 时，按批幂等删除。
- Observer 状态与 Lifecycler phase 正交：前者描述外部事实，后者描述 agentd 正在采取的动作。
- Connector 遇到过期 endpoint 或连接失败时重新解析 facts，不自行迁移 Session 或创建 Worker。

agentd 可以运行多个副本，每个副本都提供 API、Scheduler 和 Connector，也运行 Observer、Lifecycler
与 Record GC，不依赖全局 Leader。一次容量缺口计算和批量创建用短 DB lease 串行化；单个 Worker 的
回收权由 phase CAS 决定；Observer 通过 freshness fence 合并事实；外部 `Ensure` / `Destroy` 保持幂等。
任一副本退出后，其 lease 到期或下一轮 reconcile 即可由其它副本继续。

短 lease 只覆盖“重算缺口并插入 creating Worker”，不覆盖 Pod 启动等待。Worker 行必须先于 Pod
创建提交；随后任意副本都可以为 creating 行幂等补做 `Ensure`。这使多实例和进程崩溃共用同一条
恢复路径，也避免一个全局 controller leader 成为所有周期任务的串行点。

## Helm 职责

Helm Chart 应安装：

- 一个 agentd Deployment、Service 和 ServiceAccount；
- agentd 与 Agentlet 各自的可选 MySQL DSN，以及模型连接配置；
- Worker Pod 模板，包括两个容器、资源限制、探针、ServiceAccount、volume 和 placement；
- agentd 管理 Worker 所需的 namespace、managed labels 和 Worker PodTemplate；
- 最小 RBAC：观察、创建和删除受管 Worker Pod。

Helm 不预创建固定数量的匿名 Worker，也不直接参与 Session placement。SRE 通过 values 决定镜像、
资源、亲和性、污点容忍、网络策略、配额和集群容量；agentd 只在这些部署约束内调整 Worker 数量。
默认安装使用单副本 agentd 和容器本地 SQLite，不创建 PVC；这适合开发、试用和控制面联调，Pod
重建后数据会丢失。生产环境或多副本 agentd 通过 `agentd.database.dsn` 配置外部 MySQL；Chart
会拒绝“多副本但没有外置 DB”的配置。Agentlet 未配置 MySQL 时也使用 Worker Pod 内的 SQLite；
它能满足最小体验，但 Worker 被删除或重建后，Session、Checkpoint 与 Ledger 会一起丢失，无法实现
安全恢复。需要健壮恢复时应为 Agentlet 配置外部 MySQL。agentd 与 Agentlet 没有共享表契约；两者
可以使用同一 MySQL 实例中的不同 database，也可以使用完全独立的数据库和凭据。

```bash
helm upgrade --install agentd deploy/k8s/agentd \
  --namespace agentd --create-namespace
```

使用外部 MySQL 时在自定义 values 中分别配置两个 DSN：

```bash
helm upgrade --install agentd deploy/k8s/agentd \
  --namespace agentd --create-namespace \
  --values my-values.yaml
```

主要 values 契约为：

```yaml
replicaCount: 1

agentd:
  database:
    dsn: "" # 空值时 agentd 使用本地 SQLite
  extraEnv: [] # 仅在需要覆盖 config.go 默认值时添加

agentlet:
  database:
    dsn: "" # 空值时 Agentlet 使用本地 SQLite，Worker 删除后数据丢失

worker:
  agentlet:
    image:
      repository: ghcr.io/compforge/agentlet
  sandboxEngine:
    image:
      repository: ghcr.io/compforge/hostel
```

例如覆盖默认 Worker 容量时，只添加需要改变的环境变量：

```yaml
agentd:
  extraEnv:
    - name: AGENTD_WORKER_CAPACITY
      value: "4"
    - name: AGENTD_WORKER_MIN_IDLE
      value: "1"
```

Chart 把 Worker PodTemplate 挂载到 agentd，由 Provisioner materialize 为独立 Worker Pod；Helm
本身不创建或回收 Worker。
