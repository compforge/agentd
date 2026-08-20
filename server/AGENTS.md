# AGENTS.md

## 项目定位与边界

agentd 是一个 Go 实现的 Managed Agent Server。它不实现 Agent 智能、业务工作流或沙箱
技术，而是把可替换的 Harness、Sandbox Engine 和 Agent Ledger 组织成长生命周期服务。
稳定定位与边界见 `docs/kernel.md`。

- agentd 拥有 Agent、Environment、Session 的生命周期、调度和恢复决策。
- AgentGo 只拥有 Agent Loop 与原生会话状态，不感知 HTTP API 或 Hostel。
- Hostel 只拥有 Bed、Executor 和 Execution；一个 Session 对应一个 Bed。
- Agent Ledger 只记录规范化执行事实，不拥有 Harness State，也不做调度或自动重放决策。

## 代码地图与核心模块

```text
server/
├── cmd/agentd/                # 进程入口与依赖组装
├── internal/
│   ├── api/                  # Claude 兼容 Hertz HTTP/SSE 协议适配
│   ├── app/                  # 资源生命周期与 Session Run Controller
│   ├── harness/              # Harness adapter；AgentGo 是首个实现
│   │   └── state/            # Harness-specific opaque state 与 GORM Store
│   ├── ledger/
│   │   └── store/            # Agent Ledger EventStore 的 GORM 实现
│   ├── persistence/          # MySQL/GORM Provider 与依赖组装
│   ├── sandbox/              # Sandbox Engine 契约与 Harness 工具适配
│   ├── hostel/               # Hostel Sandbox Engine 实现
│   └── store/                # Control State Repository 的 GORM 实现
├── tests/e2e/                 # 显式 e2e build tag；SQLite 恢复契约与 live 组件联调
└── docs/
    ├── kernel.md             # 稳定定位、核心模型、API 与组件主流程
    ├── sandbox-engine.md     # Sandbox Engine 能力、生命周期与隔离边界
    └── state-ledger.md       # 持久化、恢复、审计与轨迹边界
```

## 关键约定

1. HTTP 路径、资源形状、Session 状态和 Event 命名以 Claude Managed Agents API 为准；
   未实现能力明确返回 `unsupported_feature`，不静默降级。
2. 用户 Event 先持久化再确认接收；模型和工具调用必须通过 Agent Ledger AgentGo
   Adapter 的 write-before-execute 边界。
3. 进程恢复由 Control State 中的 ResumeRef 定位 Harness State，并结合 Ledger 未决 Attempt
   判断是否安全继续；同一 input 不重复注入，结果不明确的 Tool Attempt 不自动重放，Session
   转为 `terminated` 等待人工对账。
4. AgentGo 运行在 agentd 进程，Hostel 作为可替换的独立进程运行；两者不共享
   调用栈或本地文件路径。
5. 所有外部 HTTP、模型和存储调用显式配置超时；子进程退出和服务关闭必须收敛
   正在执行的 Session。

## References

- `docs/kernel.md` — agentd 稳定定位、核心模型、API 边界与组件主流程
- `docs/sandbox-engine.md` — Sandbox Engine 能力契约、资源生命周期与隔离要求
- `docs/state-ledger.md` — 持久化、恢复、审计和轨迹的数据所有权与一致性边界
- `https://platform.claude.com/docs/en/managed-agents/overview` — 上游 API 概念与行为基线
- `https://github.com/opensandbox-group/OpenSandbox/tree/main/specs` — Sandbox Lifecycle 与 Execd 设计参考
- `https://github.com/compforge/agent-ledger/tree/main/spec` — Ledger 事件、追加与 Adapter 契约
