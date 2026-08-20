# agentd Control Plane

## 定位

agentd 是 Managed Agent 的 Control Plane，负责全局资源、执行时机、Worker 供给、Session placement
和请求路由，不运行 Harness Loop。执行发生在 Agentlet，二者通过网络协议协作。

![agentd Managed Agent 架构](architecture.svg)

## 四个 Control Plane 角色

| 角色 | 回答的问题 | 唯一写权 |
|---|---|---|
| Observer | Worker 现在是否存在、Ready，endpoint 是什么 | `worker.observer_status` |
| Scheduler | 当前 Session 应分配给哪个可用 Worker | 无；只返回纯决策 |
| Lifecycler | 应创建、drain 或回收哪些 Worker | Worker lifecycle phase 和运行资源动作 |
| Connector | 已分配 Worker 的 Agent API 如何触达 | 无；只走数据面 |

动作、事实、决策和数据面不能合并成一个大 Driver：Lifecycler 创建成功不代表 Ready，Observer 不因
看到空闲容量而创建 Worker，Scheduler 不访问数据库或 Kubernetes，Connector 不修改 Assignment。

## Worker、Session 与 Assignment

Worker 是 Agentlet 在 agentd 调度域里的 workload 形态和运行载体称呼，也是容量与调度单位；当前
一个 Worker 对应一个 Agentlet Pod。它不是另一种独立服务，也不对应 Kubernetes Node，更不要求
每个物理节点运行一个实例。

Worker 只持久化稳定身份、`capacity`、lifecycle phase 和 Observer facts：

- `id`：agentd 生成的 Worker 唯一身份，同时写入 Pod 的
  `agentd.compforge.dev/worker-id` label；
- `name`：便于运维识别的 Pod 名称；
- `capacity`：最大并发 Work 数；
- lifecycle phase：`creating / active / draining / retired`；
- `observer_status`：最近观测的 `observed_at`、`exists`、`ready` 和可选 `endpoint`。

`sessions` 是持久化需求和当前绑定的唯一事实表：`rescheduling` 且 `worker_id` 为空表示等待容量；
`worker_id` 非空表示当前计算位置。`assignment_id` 随每次重新绑定生成，作为 Agentlet 请求的 fence。
Assignment 仍是 API 和运行时中的值对象，但不再单独建表。

Worker 已用容量由当前绑定的 Session 数计算，不在 Worker 或 observation 中冗余保存：

```text
available = worker.capacity - count(sessions where worker_id = worker.id)
```

控制面当前只需要三个持久化主体：`workers` 保存容量与观测事实，`sessions` 保存需求和绑定，
`resource_locks` 保存多实例短租约。表之间不声明数据库外键，归属一致性由 Service 事务保证。
Go 代码中，稳定对象定义在 `internal/model`，持久化契约定义在 `internal/repo/repository.go`，GORM
表映射和查询放在 `internal/repo/gorm`；`internal/service` 只编排业务事务，不暴露 GORM model。

## Observer

agentd 创建的 Worker Pod 必须同时带有 `agentd.compforge.dev/managed=true` 和
`agentd.compforge.dev/worker-id=<worker UUID>`。Observer 通过 `agentd/internal/k8s` 周期性列出这些
Pod，并把事实写入 Worker。Agentlet 不主动注册或发送 heartbeat，也不存在外部 Worker 写入 API。

Worker identity 不等于 Pod UID。Pod 名、UID、IP 和 Ready 都是 `observer_status` 中的外部事实。
Kubernetes 可以重启 Pod 内容器；Pod 消失则旧 Worker 退役，Lifecycler 创建新 Worker，执行中的
Session 按普通 checkpoint 恢复路径收敛。agentd 不实现 Worker 原地恢复协议。

Scheduler 只消费 observation 足够新、`exists=true`、`ready=true` 且具有 endpoint 的 Worker。
Observer status 与 lifecycle phase 正交：前者描述 Kubernetes 里的外部事实，后者描述 agentd 正在
采取的动作。Kubernetes substrate 只提供 `PodSnapshot`，不能成为第二个事实写入者。

## Scheduler

Service 在数据库事务中锁定 Worker，加载 Observer facts 和绑定 Session 数，再把 typed candidates
交给 Scheduler。Scheduler 是无 I/O 的纯 placement 策略：

1. 保留仍可调度的既有 Assignment；
2. 否则选择负载最低且仍有容量的 active Worker；
3. 相同负载按 Worker ID 保持确定顺序；
4. 没有候选时返回 `no capacity`，不直接创建 Pod。

```text
Session 需要执行
  ├─ live Assignment ───────────────► reuse
  └─ missing / stale Assignment
       → load and lock candidates
       → Scheduler decision
       ├─ candidate → update sessions.worker_id + assignment_id
       └─ no capacity → retain rescheduling Session and wake Lifecycler
```

## Worker Lifecycler

Lifecycler 是 Worker 资源动作的 owner。`rescheduling` 且尚未绑定 Worker 的 Session 是 durable demand；
每轮扩容都从数据库重新计算需求缺口，而不是依赖容易丢失的内存通知。通知只用于唤醒循环。

Lifecycler 与 Kubernetes 操作分两层：

| 层 | 职责 |
|---|---|
| Worker Lifecycler | 需求收敛、创建侧 phase 转换和重试 |
| Worker Provisioner | 幂等 `Ensure` / `Destroy` 一个 Worker Pod |

`Ensure` 返回只表示 Kubernetes 接受期望状态；Observer 写入 Ready 后 Scheduler 才能使用 Worker。
扩容采用有上限的小批次。如果上一批 Worker Pod 仍 Pending，则暂停继续创建，避免放大 Kubernetes
的镜像、配额或节点压力。轻微超配可以由 idle 回收最终收敛。

### 预热与按需扩容

预热不使用独立 controller，而是和 durable demand 共用一次容量计算：

1. active Worker 的空闲 slot 先满足尚未绑定 Worker 的 Session；
2. creating Worker 按 `capacity` 计入即将到来的容量，避免重复创建；
3. 再补足配置的 `minIdleWorkers`，它只统计零绑定 Session 的通用 Worker；
4. 创建数量同时受 `createBatchSize` 限制；
5. 任一受管 Worker Pod 仍 Pending 或 Unschedulable 时，本轮停止继续扩容。

`minIdleWorkers` 默认 0，此时只有 durable demand 才会创建 Worker。唤醒 channel 只合并通知，不携带
数量；周期 reconcile 和即时唤醒都从数据库重新计算相同目标。空闲 Worker 超过 idle TTL 后可以轮换，
若因此低于 idle floor，下一轮用当前镜像和模板补回。

creating Worker 的 Pod 缺失时可以幂等完成本轮创建；active Worker 的 Pod 缺失时先退役旧 Worker，
再根据 durable demand 创建新 Worker。Kubernetes 存在带 managed label 的 Pod 而数据库没有对应
Worker 时，超过 grace period 后按批删除。每个 agentd 副本都运行这些循环，不依赖全局 Leader：
容量缺口计算和创建用 `resource_locks` 中的短 DB lease 串行化，单 Worker 处置权由 phase CAS 决定，
Observer 写入带 freshness fence，外部动作保持幂等。持有者退出后，其 lease 到期或下一轮 reconcile
由其它副本继续。

### 多实例协调

agentd 借鉴 sandctl 的分布式收敛原则，但不照搬其每个 Sandbox 任务的长 lease。Worker Lifecycler
处理的是共享容量池，不执行一条 Session 的长任务，因此按动作选择最窄协调方式：

| 动作 | 协调方式 |
|---|---|
| 计算容量缺口、预热并插入 `creating` Worker | 短 DB lease；锁内重新读取 durable demand 和已有容量 |
| 为 `creating` Worker 创建 Pod | 先有 Worker 行，再幂等 `Ensure`；Pod AlreadyExists 视为成功 |
| `creating → active` | Observer Ready 后 phase CAS |
| `active → draining` | 条件更新同时确认零绑定 Session；CAS 胜者拥有回收权 |
| `draining → retired` 和删除 Pod | 再次复核后 CAS，`Destroy` 幂等 |
| Observer 写事实 | 按 Worker identity 合并，并用 freshness fence 拒绝旧 snapshot |
| Session binding | 数据库事务与 Worker 行锁，不能依赖容量循环的 resource lock |

短 DB lease 只保护一次“读取缺口并发布 creating demand”，不能覆盖 Pod 启动等待。这样持有者崩溃后
不会长时间阻塞扩容，任意副本都能看到 creating 行并补做幂等 `Ensure`。若以后出现真正需要长时间
独占的单 Worker 动作，再为该动作加入 holder、heartbeat 和过期 takeover，而不是预先把整套协议铺开。

## GC

GC 是独立维护面，分成运行资源与数据库记录两类。两者使用不同周期和失败重试，不能用删除数据库行
顺带代表 Pod 已经释放。

### Pod GC

Pod GC 负责回收 Worker Pod，同时保留 Worker 终态记录用于诊断。Worker 同时满足零绑定 Session、
没有执行中 Work 且超过 idle TTL 后，按以下顺序回收：

```text
active
  → zero-assignment/live precheck
  → CAS draining
  → final Work/checkpoint safety check
  → CAS retired
  → Provisioner.Destroy
  → Observer confirms absent
```

`draining` 必须先持久化，使 Scheduler 不再分配新 Session。CAS 胜者获得该 Worker 的处置权；删除前
再次复核运行状态，`Destroy` 保持幂等。Pod GC 同时收敛两类漂移：active Worker 的 Pod 已消失时退役
记录；带 managed label 但其 `worker-id` 不在 `workers` 表的 Pod 是 zombie，全部按批幂等删除。

Worker 行必须先于 Pod 创建提交，因此 zombie 不存在合法的“Pod 已创建、Worker 行稍后补写”窗口。
只有成功读取完整 Worker 集合后才允许删除 zombie；数据库读取失败时 fail closed，本轮不删任何 Pod。

### DB Record GC

DB Record GC 只处理已经 `retired`、Observer 已确认 Pod 不存在且超过记录保留期的 Worker 行。它按
`absent_at, id` 小批选择，并在删除时再次检查 phase、截止时间以及不存在绑定 Session。Record GC
不访问 Kubernetes，也不修改 active/draining Worker；它独立于 Worker source 常驻运行，失败后下一轮
重试即可。

## Connector

Connector 是 Agentlet 数据面的唯一入口：按 Session 读取 Assignment，从当前 Worker observation
解析 endpoint，把执行所需的 Agent、Environment 和 Session Control State 组成 `WorkSpec` 快照，再把
公开请求转换为 Agentlet 内部执行 API，并保持普通响应或 SSE stream。

```text
Session ID → Assignment → Worker → fresh endpoint → ensure WorkSpec → Agentlet internal API
```

endpoint 是一次解析结果，不是持久化 identity。Connector 不选择 Worker、不修改 Assignment、
不创建 Worker，也不因连接失败自行迁移 Session。连接失败或 observation 过期时，它重新解析 facts，
并把不可达结果交给上层恢复或重调度流程；是否可以安全重试仍由 Control State 和 Ledger 判断。

Agentlet 协议适配、显式超时、连接池、SSE 断连传播和 trace context 透传属于 Connector，不能散落
在各个 API handler。

## Control State 与恢复

Control State 保存 Session 当前状态、有效 Assignment、待处理输入和精确 `ResumeRef`，回答“现在应由
谁、从哪里继续”。Checkpoint、Ledger 与恢复提交顺序见 [agentlet.md](agentlet.md)。

Worker observation 过期、不存在或不 Ready 后，agentd 不能仅凭路由失败重放执行。它先释放或替换
Assignment，再根据 ResumeRef 和 Ledger 未决 Attempt 判断能否在新 Worker 恢复。工具副作用结果不明
时进入对账或人工处理，不自动重试。

## 部署边界

- agentd Lifecycler 管理 Worker 数量和生命周期；
- agentd Deployment 可以多副本运行；每个副本都运行 API、Scheduler、Connector 和周期控制器，
  通过 DB lease、phase CAS 与幂等动作协调；
- Kubernetes 管理已创建 Worker Pod 的健壮性和重建；
- SRE 通过 workload 模板管理镜像、资源、placement 约束、故障域和集群容量；
- Agentlet 管理已分配 Session 的 Harness runtime 和 Sandbox Engine 使用，不拥有全局 placement。

Helm 部署形态、Worker 双容器模板和弹性流程见
[`../../deploy/k8s/README.md`](../../deploy/k8s/README.md)。

启用 Kubernetes Worker source 后，每个 agentd 副本同时运行 Observer、Lifecycler 和 Pod GC；Record GC
作为独立数据库维护循环始终运行。
无容量的调度结果先持久化为待分配 Session，再唤醒 Lifecycler；Provisioner 根据缺口创建 Worker Pod，
Observer 确认 Ready 后，后续请求即可完成 Assignment 并由 Connector 转发到 Agentlet。Pod GC 同时收敛
空闲 Worker、已消失 Worker 和无数据库 owner 的受管 Pod。Assignment 主动释放与跨 Worker 恢复尚未
接通；它们属于 Control State 和 Ledger 恢复链路，不混入 Worker 供给控制器。
