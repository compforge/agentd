# AGENTS.md

## 项目定位与边界

agentd 是一个 Go 实现的 Managed Agent Server。它不实现 Agent 智能、业务工作流或沙箱
技术，而是通过 Control Plane 与 Agentlet，把可替换的 Harness、Sandbox Engine 和 Agent Ledger
组织成长生命周期服务。稳定定位与边界见 `docs/kernel.md`。

- agentd 是 Control Plane 的代码与服务边界，拥有全局资源、Agentlet 容量视图、Session Binding、
  调度、转发和恢复决策。
- Agentlet 接受 Assignment，在单个 Pod 内实现执行相关的 Managed Agents API，管理多个 Harness
  runtime；它上报 observed state，不拥有全局 Control State。
- AgentGo 只拥有 Agent Loop 与原生会话状态，不感知 HTTP API 或 Hostel。
- Hostel 只拥有 Bed、Executor 和 Execution；一个 Session 对应一个 Bed。
- Agent Ledger 只记录规范化执行事实，不拥有 Harness State，也不做调度或自动重放决策。

## 代码地图与核心模块

```text
├── go.mod                     # 仓库唯一 Go module
├── cmd/
│   └── agentlet/              # 薄二进制入口；未来新增 cmd/agentd
├── agentd/                    # Control Plane 边界；当前先承载稳定设计
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
    │   ├── harness/           # Harness adapter；AgentGo 是首个实现
    │   │   └── state/         # Harness-specific opaque state 与 GORM Store
    │   ├── ledger/store/      # Agent Ledger EventStore 的 GORM 实现
    │   ├── persistence/       # Agentlet MySQL/GORM Provider
    │   ├── sandbox/           # Harness 工具适配
    │   │   ├── engine/        # Sandbox Engine 能力契约
    │   │   └── hostel/        # Hostel Engine 实现
    │   └── store/             # 当前执行侧资源 Store
    └── tests/e2e/             # 显式 e2e build tag；恢复契约与 live 组件联调
```

`cmd/<name>` 只负责对应二进制的启动；可测试实现分别归属 `agentd/` 和 `agentlet/`。当前仓库已
落地 Agentlet，Control Plane 的 API、Control State、注册、调度和路由实现后续进入 `agentd/`，
不得反向依赖 `agentlet/internal`。两者通过网络协议协作，不通过共享 Go interface 伪装进程边界。

## 关键约定

1. HTTP 路径、资源形状、Session 状态和 Event 命名以 Claude Managed Agents API 为准；
   未实现能力明确返回 `unsupported_feature`，不静默降级。
2. 用户 Event 先持久化再确认接收；模型和工具调用必须通过 Agent Ledger AgentGo
   Adapter 的 write-before-execute 边界。
3. agentd 根据 Agentlet 能力及 `capacity / allocatable / reserved / active` 调度，并通过
   Assignment generation、lease 和 fencing 维护全局归属；Agentlet 只接受有效 Assignment，
   其 observed state 由 agentd 校验后提交为 Control State。
4. 进程恢复由 Control State 中的精确 ResumeRef 定位 Harness State，并结合 Ledger 未决 Attempt
   判断是否安全继续；同一 input 不重复注入，结果不明确的 Tool Attempt 不自动重放，Session
   转为 `terminated` 等待人工对账。
5. AgentGo 运行在 Agentlet 进程。Hostel 作为可替换的独立
   进程运行，不与 AgentGo 共享调用栈或本地文件路径。
6. 所有外部 HTTP、模型和存储调用显式配置超时；子进程退出和服务关闭必须收敛
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
