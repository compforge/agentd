# AGENTS.md

## 项目定位与边界

agentd 是一个 Go 实现的 Managed Agent Server。它不实现 Agent 智能、业务工作流或沙箱
技术，而是把可替换的 Harness、Sandbox Engine 和 Agent Ledger 组织成长生命周期服务。
稳定定位与边界见 `docs/kernel.md`。

- agentd 拥有 Agent、Environment、Session 的生命周期、调度和恢复决策。
- AgentGo 只拥有 Agent Loop 与原生会话状态，不感知 HTTP API 或 Hostel。
- Hostel 只拥有 Bed、Executor 和 Execution；一个 Session 对应一个 Bed。
- Agent Ledger 只记录已发生的事实和原生恢复材料，不做调度或自动重放决策。

## 代码地图与核心模块

```text
server/
├── cmd/agentd/                # 进程入口与依赖组装
├── internal/
│   ├── api/                  # Claude 兼容 Hertz HTTP/SSE 协议适配
│   ├── app/                  # 资源生命周期与 Session Run Controller
│   ├── harness/              # Harness adapter；AgentGo 是首个实现
│   ├── sandbox/              # Sandbox Engine 契约与 Harness 工具适配
│   ├── hostel/               # Hostel Sandbox Engine 实现
│   └── store/                # Agent/Environment/Session 持久化
└── docs/
    ├── kernel.md             # 稳定定位、边界与演进原则
    └── managed-agents.md     # 系统模型、主流程与关键取舍
```

## 关键约定

1. HTTP 路径、资源形状、Session 状态和 Event 命名以 Claude Managed Agents API 为准；
   未实现能力明确返回 `unsupported_feature`，不静默降级。
2. 用户 Event 先持久化再确认接收；模型和工具调用必须通过 Agent Ledger AgentGo
   Adapter 的 write-before-execute 边界。
3. 进程恢复只从已持久化的 API Event 和 AgentGo native stream 恢复。未决的有副作用
   Tool Attempt 不自动重放。
4. AgentGo 运行在 agentd 进程，Hostel 作为可替换的独立进程运行；两者不共享
   调用栈或本地文件路径。
5. 所有外部 HTTP、模型和存储调用显式配置超时；子进程退出和服务关闭必须收敛
   正在执行的 Session。

## References

- `docs/kernel.md` — agentd 稳定定位、组件边界与冻结恢复原则
- `docs/managed-agents.md` — Managed Agent 核心模型、执行与恢复设计
- `https://platform.claude.com/docs/en/managed-agents/overview` — 上游 API 概念与行为基线
- `https://github.com/compforge/agent-ledger/tree/main/spec` — Ledger 事件、追加与 Adapter 契约
