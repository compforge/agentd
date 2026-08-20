# AGENTS.md

## 项目定位与边界

agentd 是一个 Go 实现的 Managed Agent Server。它不实现 Agent 智能、业务工作流或沙箱
技术，而是通过 Control Plane 与 Agentlet，把可替换的 Harness、Sandbox Engine 和 Agent Ledger
组织成长生命周期服务。稳定定位与边界见 `docs/kernel.md`。

- agentd 是 Control Plane 的代码与服务边界，拥有全局资源、Worker、Assignment、调度、转发和
  恢复决策。
- Agentlet 在单个 Worker Pod 内实现执行相关的 Managed Agents API，并管理多个 Harness
  runtime；它不拥有全局 Assignment。
- AgentGo 只拥有 Agent Loop 与原生会话状态，不感知 HTTP API 或具体 Sandbox Engine 实现。
- Agent Ledger 记录规范化执行事实并保存不透明 Checkpoint，不解释 Harness State，也不做调度或自动重放决策。

## 代码地图与核心模块

```text
├── go.mod                     # 仓库唯一 Go module
├── cmd/
│   ├── agentd/                # Control Plane 二进制入口
│   └── agentlet/              # 节点执行二进制入口
├── agentd/                    # Worker 观测、Assignment 与调度
│   ├── internal/
│   │   ├── api/               # Control Plane HTTP 适配
│   │   ├── app/               # 事务、数据加载与 Assignment 持久化
│   │   ├── k8s/               # Kubernetes Client 与 PodSnapshot substrate
│   │   ├── observer/          # 周期拉取 Worker Pod 事实并持久化 observation
│   │   ├── scheduler/         # 无 I/O 的 Session → Worker placement 策略
│   │   └── store/             # GORM Store
│   └── docs/
│       ├── kernel.md          # 稳定定位、核心模型、API 与组件主流程
│       ├── control-plane.md   # Control Plane、Agentlet、调度、容量与 Control State
│       ├── harness.md         # Harness 执行边界、适配契约与 AgentGo 实现
│       ├── sandbox-engine.md  # Sandbox Engine 能力、生命周期与隔离边界
│       └── state-ledger.md    # Harness State、Ledger、恢复、审计与轨迹边界
└── agentlet/
    ├── agentlet.go            # Agentlet 依赖组装与服务生命周期
    ├── internal/
    │   ├── api/               # 执行侧 Claude 兼容 HTTP/SSE 协议适配
    │   ├── app/               # Session Run 生命周期
    │   ├── execution/         # App 与 Harness 之间的执行契约
    │   ├── harness/           # Harness adapter；AgentGo 是首个实现，持有原生 Checkpoint codec
    │   ├── persistence/       # Agentlet MySQL/GORM Provider 与 Agent Ledger Store 组装
    │   ├── sandbox/           # Sandbox Engine 能力契约与默认 Adapter
    │   └── store/             # 当前执行侧资源 Store
    └── tests/e2e/             # 显式 e2e build tag；恢复契约与 live 组件联调
```

`cmd/<name>` 只负责对应二进制的启动；可测试实现分别归属 `agentd/` 和 `agentlet/`。两者通过网络
协议协作，不通过共享 Go interface 伪装进程边界。

## 关键约定

1. HTTP 路径、资源形状、Session 状态和 Event 命名以 Claude Managed Agents API 为准；
   未实现能力明确返回 `unsupported_feature`，不静默降级。
2. 用户 Event 先持久化再确认接收；模型和工具调用必须通过 Agent Ledger AgentGo
   Adapter 的 write-before-execute 边界。
3. agentd 把一个 Agentlet Pod 建模为一个 Worker，按最新 observation、`max_runs` 和当前
   Assignment 数调度；Agentlet 不主动注册或发心跳，Pod 健壮性与 placement 由 Kubernetes 和 SRE
   负责。
4. Worker Observer 是 Worker 运行事实的唯一写入者；Kubernetes substrate 只提供 PodSnapshot，
   Scheduler 只消费已持久化 facts，二者都不越权管理 Pod 生命周期。
5. 进程恢复由 Control State 中的精确 ResumeRef 定位 Agent Ledger Checkpoint，并结合 Ledger 未决 Attempt
   判断是否安全继续；同一 input 不重复注入，结果不明确的 Tool Attempt 不自动重放，Session
   转为 `terminated` 等待人工对账。
6. AgentGo 运行在 Agentlet 进程。Sandbox Engine 作为可替换的 sidecar 或远端服务运行，不与
   AgentGo 共享调用栈或本地文件路径。
7. 所有外部 HTTP、模型和存储调用显式配置超时；子进程退出和服务关闭必须收敛
   正在执行的 Session。

## References

- `docs/kernel.md` — agentd 稳定定位、核心模型、API 边界与组件主流程
- `docs/control-plane.md` — Control Plane / Agentlet 拓扑、调度容量和 Control State
- `docs/harness.md` — Harness 执行边界、适配契约、恢复语义与 AgentGo 实现
- `docs/sandbox-engine.md` — Sandbox Engine 能力契约、资源生命周期与隔离要求
- `docs/state-ledger.md` — Harness State、Ledger、恢复、审计和轨迹的数据所有权与一致性边界
- `https://platform.claude.com/docs/en/managed-agents/overview` — 上游 API 概念与行为基线
- `https://github.com/opensandbox-group/OpenSandbox/tree/main/specs` — Sandbox Lifecycle 与 Execd 设计参考
- `https://github.com/compforge/agent-ledger/tree/main/spec` — Ledger 事件、追加与 Adapter 契约
