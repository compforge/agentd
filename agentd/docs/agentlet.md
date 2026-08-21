# Agentlet 执行节点

## 定位

Agentlet 是 Managed Agent 的执行节点。它只接受分配到当前 Worker 的 Session，在容量边界内创建或
恢复 Harness runtime，并通过 Sandbox Engine 执行隔离工具。它不拥有全局资源、Worker 生命周期、
Session placement 或跨 Worker 恢复决策。

```text
agentd Connector
  │ internal execution API + Assignment
  ▼
Agentlet
  ├─ Internal API adapter
  ├─ Work Manager
  ├─ Harness Adapter ── AgentGo / future harness
  ├─ Persistence ────── Checkpoint / Agent Ledger
  └─ Sandbox Adapter ── remote engine / quick-start sidecar
```

## Assignment 门禁

Agentlet 执行请求必须携带 agentd 生成的 Assignment 身份和 fence。Agentlet 只接受目标 Worker 与自身
一致、尚未失效的 Assignment；它不接受 Session 自行注册，也不在多个 Worker 之间抢占任务。

Agentlet 的 HTTP 面只供 agentd 调用，统一位于 `/internal/v1`；Claude Managed Agents 的公开路径、
资源 envelope 和兼容性测试都由 agentd 拥有。内部协议只承载 WorkSpec、`wake`、`interrupt`、
Assignment 与 observed state，不成为第二套产品 API。

Assignment 是 Control Plane 的路由权威，Agentlet 的进程内 runtime 只是缓存。进程重启后，Agentlet
不得从本地内存猜测归属；它根据请求携带的 Assignment 和精确 `ResumeRef` 重建执行现场。

Connector 在转发执行动作前先幂等安装当前 Assignment 的完整 `WorkSpec`。相同
Assignment 的重复快照不能覆盖 Agentlet 已推进的 ResumePoint；执行状态读取也必须携带相同的 Worker
与 Assignment 身份。Agentlet 上报的状态只有在 agentd 仍持有该 Assignment 时才能提交，迟到响应不能
修改已经重调度的 Session。

Agentlet 不提供或代理公开 Event API，也不替 agentd 查询 Event。agentd 与所有 Agentlet 连接同一
数据库：agentd 写入用户 ingress 并提供 list/stream；Agentlet 只把当前 Assignment 内 Harness、模型、
工具和输出产生的执行事实写入同一个 Ledger。共享存储不改变双方职能边界。

## 执行流程

```text
validate Assignment
  → reserve Work capacity
  → persist input boundary
  → restore or create Harness runtime
  → reconcile unresolved Ledger attempts
  → run Harness Loop
  → call tools through Sandbox Adapter
  → persist output and safe checkpoint
  → report ResumePoint / terminal result
  → release local runtime
```

Harness Loop、模型调用、工具调用、compact 和原生上下文语义属于 Harness Adapter，详见
[harness.md](harness.md)。Agentlet Service 只编排 Work 生命周期，不复制某个 Harness 的
消息模型或 checkpoint 格式。

Work 是 Agentlet 中可冻结、迁移和恢复的长期逻辑对象；它不等于一次连续占用进程的调用。Worker
只是 Work 当前的执行载体，迁移前后 Work 身份保持不变。Harness 的 `Run` 是执行动作；Agent Ledger
中的 Run 是独立的记录层概念，agentd 不规定它与 Work 的映射关系。

## Checkpoint 与 Ledger

Agentlet 通过 Agent Ledger 提供的两个独立能力保存执行材料：

- **Checkpoint Store** 保存 Harness Adapter 生成的不透明状态。格式、版本和恢复语义属于具体
  Harness；Agentlet 与 agentd 只传递精确的 `ResumeRef`，不解释内容。
- **Ledger** 追加跨 Harness 的规范化执行事实。Harness recording adapter 负责把原生 hook 翻译为
  Run、Lane、Turn、Action 和 Attempt；模型与工具调用遵守 write-before-execute。

Ledger 保留 `Session → Run → Lane` 层级。Run 的最低契约是在 Session 下聚合一个或多个 Lane，
agentd 不规定其业务来源和生命周期。Harness 可以并行驱动多个 Lane，每个 Lane 表达一个串行的
Agent Loop。

Ledger 的对象模型、事件协议、append-only 约束、Checkpoint envelope 和存储实现由
[Agent Ledger](https://github.com/compforge/agent-ledger) 定义。agentd 仓只拥有使用顺序与恢复策略，
不复制其 schema。Checkpoint 可以独立使用；同时使用 Ledger 时，checkpoint 应锚定已吸收的 Ledger
位置，以便恢复器识别其后的已完成结果和未决 Attempt。

## 冻结与恢复

Agentlet 在 Harness 的安全边界请求 Adapter 保存 checkpoint，得到不透明 `ResumeRef` 后，再连同
Assignment fence 上报 agentd。只有 agentd 条件提交 ResumePoint 后，才能把该 Session 视为可在其它
Worker 恢复并释放当前资源。

```text
Adapter saves checkpoint
  → Agentlet reports ResumePoint
  → agentd commits Control State
  → Agentlet releases runtime
```

恢复时 Agentlet 校验 Harness/codec 兼容性，并结合 Ledger 未决 Attempt 判断是否可以继续。已经完成的
结果只幂等吸收到 Harness State，不重新执行。Action 在首次执行前固化 `Effect(kind, idempotency)`：
`none/read` 和已知幂等的 `write` 可以在原 Action 下递增 `attempt_no` 后重试；`write+none/unknown`、
`unknown` 或缺失声明都保持 fail-closed。Ledger 只保存 Effect 事实，是否重试由 Agentlet 的 Harness
恢复策略决定。

Fail-closed 不等于终止 Session。Agentlet 释放本轮执行资源，把未决 Attempt 投影成
`agent.tool_use`，随后写入带 `stop_reason.requires_action.event_ids` 的 `session.status_idle`。公开 API
只接受 Claude Managed Agents 已有的 `user.tool_confirmation` 与 `user.tool_result` 来解除阻塞：deny
或外部对账结果成为 Tool Result 后继续模型循环；allow 只允许原 Action 创建一个新 Attempt。确认
Event ID 会写入新 Attempt 的 requested payload；如果新 Attempt 的结果再次不明确，旧确认不能重放，
必须生成新的 `agent.tool_use` 再次询问。普通 user message 在 required action 解决前保持排队。

提交顺序必须保证：先保存可读取的 checkpoint，再把精确 `ResumeRef` 上报 agentd；先追加 Attempt 的
requested 事实，再发起模型或工具调用；拿到结果后再追加 completed / failed。跨 Control State、
Checkpoint 和 Ledger 不伪装成 exactly-once，而是通过稳定 ID、条件提交和恢复对账收敛。

## Sandbox Engine

Sandbox Engine Adapter 位于 Agentlet。Agentlet 根据稳定 Session 身份构造 `SandboxKey`，在 Harness
使用工具前调用 `Ensure`；这个 key 和 Engine 返回的临时连接信息都不进入 WorkSpec 或 Control State。
Engine 可以是远端服务，也可以是同一 Worker Pod 的 sidecar，具体实现名称不暴露给 agentd。

正式部署由独立 Sandbox Control Plane 保证执行环境可用，Sandbox 的粘滞、恢复和物理位置不参与
agentd 的 Worker placement。当前 Helm Chart 只为 Quick Start 在 Worker Pod 内共置 Agentlet 与
Hostel；双容器是最小部署实现，不是 Agentlet 或 Worker 的稳定定义。Sandbox 能力与资源边界见
[sandbox-engine.md](sandbox-engine.md)。

## 进程与容量

- Work Manager 是 Agentlet 内 Assignment fence、容量 reservation 和 active/pending 执行状态的唯一
  owner。容量在接受 `WorkSpec` 时按 Assignment 预留，而不是等 Harness goroutine 启动后才计算；这样
  Agentlet 的本地 admission 与控制面的 Session binding 使用同一口径。
- 同一 Assignment 的重复唤醒合并为后续执行 pass。新的 Assignment 只能替换 inactive Work，不能让
  迟到请求覆盖正在执行的 runtime。
- 一次执行 settle 后，Agentlet 释放进程内 Work reservation；Session、Event 和 ResumeRef 仍在持久层，
  因而状态观察不需要重新创建 Work，也不会让已完成的 Assignment 继续占用本地容量。
- 一个 Agentlet 可以在容量边界内执行多个 Work；容量归 Assignment 计数，不由 Agentlet
  heartbeat 上报。
- Kubernetes 负责容器重启和 Pod 替换；不设计 Agentlet 原地恢复同一个 Worker identity。
- 外部 HTTP、模型、Sandbox 和存储调用必须有显式超时与容量上限。
- 进程关闭先停止接收新 Work，再让执行到达安全 checkpoint；不能安全冻结的 Work 必须留下可诊断状态。

## 边界

1. Agentlet 不主动注册、发 heartbeat 或选择 Assignment。
2. Agentlet 不创建、drain 或删除 Worker。
3. Agentlet 不暴露、代理或查询公开 Event API，只写自身产生的执行事实。
4. Agentlet 不解释全局 Control State，只消费请求携带的执行切片。
5. Agentlet 不提供 Model API、不查询 Model 表，也不持有全局默认模型；它只把 WorkSpec 中已解析的
   Model 连接交给本次 Assignment 的 Harness。
6. Harness Adapter 拥有原生状态语义；Agent Ledger 拥有规范化执行事实。
7. Sandbox Engine 拥有隔离资源及其 placement；Agentlet 只构造 key 并通过 Adapter 使用它。
