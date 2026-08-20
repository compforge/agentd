# Control Plane 与 Agentlet

## 定位

agentd 通过 Worker 把 Managed Agent Session 分配给 Agentlet 执行：

- **agentd** 读取 Worker Observer 写入的 Pod 事实，保存 Worker 和 Assignment，选择有空闲容量的 Worker；
- **Agentlet** 运行在独立 Pod 中，在声明的并发上限内执行多个 Session；
- **Worker** 是 agentd 中的调度对象，一个 Worker 对应一个 Agentlet Pod；
- **Assignment** 是当前 `Session → Worker` 归属，不是 Session 的产品身份。

Sandbox Engine Adapter 位于 Agentlet 内部；Engine 服务可以是同一 Worker Pod 的 sidecar，也可以是
远端服务。agentd 不观察或调度具体 Sandbox Engine；更换实现不改变 Worker 调度模型。本地 sidecar
配置 readiness probe 后，Kubernetes 的 Pod Ready 同时反映 Agentlet 与 Engine 是否就绪。

## 拓扑

```text
Client
  │ Managed Agents API
  ▼
agentd
  │ schedule / lookup Assignment
  ├─────────────────┐
  ▼                 ▼
Worker A          Worker B
Agentlet Pod      Agentlet Pod
  │                 │
Harness runtime   Harness runtime
  │                 │
Sandbox Engine    Sandbox Engine
```

Agentlet 是普通 Kubernetes workload，不对应 Kubernetes Node，也不要求每个物理节点部署一个实例。
一个 Pod 对应一个 Worker，Worker ID 可使用 Pod UID。Pod 的创建、保活、重启和替换由 Kubernetes
控制器负责，agentd 不实现第二套 Worker 生命周期管理；替换出的新 Pod 使用新的 Worker ID。

## Worker

Worker 只保存调度所需的事实：

- `id`：Worker 唯一身份，Kubernetes 部署可使用 Pod UID；
- `name`：便于运维识别的 Pod 名称；
- `max_runs`：最大并发 Session 数；
- `observer_status`：最近一次外部观测，包含 `observed_at`、`exists`、`ready` 和可选的
  `endpoint`。

集群内 Agentlet 使用相同版本、Harness 和 Sandbox 配置。版本匹配、能力匹配、labels、亲和性及 Pod
扩缩容不属于 agentd 的 Worker 调度模型，由部署系统和 SRE 配置保证。

Worker Observer 通过 `agentd/internal/k8s` 周期性列出符合 namespace 和 label selector 的 Pod，并把
事实写入 Worker。Agentlet 不必主动注册或发送 heartbeat，也不存在外部 Worker 写入 API。Scheduler
只选择 observation 足够新、`exists=true`、`ready=true` 且具有 endpoint 的 Worker。

## Assignment 与容量

Assignment 只保存 `id`、`session_id`、`worker_id` 和时间戳。一个 Session 同时只有一个当前
Assignment；释放 Assignment 就释放对应 Worker 的一个并发名额。

Agent 是可复用定义，不能绑定 Worker；Harness 是执行实现，也不是 placement identity。Session 是
当前调度的逻辑运行需求，Assignment 单独表达临时 `Session → Worker` 归属。若未来一个 Session 支持
多个并发 Run，再让 Assignment 挂到 Run，而不是给 Agent 或 Harness 增加 `worker_id`。

Worker 的已用容量由当前 Assignment 数量计算，不在 Worker 表冗余保存计数：

```text
available = worker.max_runs - count(assignments where worker_id = worker.id)
```

Application 在数据库事务中锁定 Worker，读取 Observer facts 和当前 Assignment 数量，再把 typed
candidates 交给 Scheduler。Scheduler 是不访问数据库、Kubernetes 或网络的纯 placement 策略：保留
仍可调度的既有 Assignment，否则选择负载最低且仍有容量的 Worker；相同负载按 Worker ID 保持确定
顺序。这样容量真相只有 Assignment 一份，不需要在 observation 中维护容易漂移的 `active_runs`
计数。

## 调度流程

```text
Session 需要执行
  │
  ├─ 已有 Assignment 且 Worker 存活 ──► 继续使用原 Worker
  │
  └─ 无 Assignment 或 Worker observation 不可调度
       │
       ▼
     锁定可调度 Worker
       │
       ▼
     计算各 Worker 当前 Assignment 数
       │
       ├─ 无空闲容量 ──► 返回 no worker capacity
       │
       ▼
     保存新的 Assignment
       │
       ▼
     根据 Worker endpoint 路由到 Agentlet
```

Worker observation 过期、不存在或不 Ready 后，旧 Assignment 在该 Session 再次调度时被替换。
Harness State、Ledger 与安全恢复仍由 `state-ledger.md` 定义；调度器不根据日志猜测 Harness 是否
可以重放。

## 边界

1. agentd 决定 Session 当前属于哪个 Worker；Agentlet 不自行挑选或迁移 Session。
2. Worker 是否有容量只由 `max_runs` 和当前 Assignment 数判断。
3. Worker ID 对应一个 Pod 生命周期，不处理同一 Worker 原地重启。
4. Worker Pod 的健壮性以及 Kubernetes placement、故障域与扩缩容由 Kubernetes 和 SRE 的部署配置
   负责；agentd 只消费观测事实并做 Session placement。
5. Sandbox Engine 是 Agentlet 内部能力，不进入 agentd 的 Worker 调度表。
