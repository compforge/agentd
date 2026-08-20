# AGENTS.md

## 项目定位与边界

agentd 是一个 Go 实现的 Managed Agent Server。它不实现 Agent 智能、业务工作流或沙箱
技术，而是通过 Control Plane 与 Agentlet，把可替换的 Harness、Sandbox Engine 和 Agent Ledger
组织成长生命周期服务。稳定定位与边界见 `docs/kernel.md`。

- agentd 是 Control Plane 的代码与服务边界，拥有全局资源、Worker、Assignment、调度、转发和
  恢复决策。
- Agentlet 在单个 Worker Pod 内通过内部执行 API 管理多个 Harness runtime；它不实现面向客户端的
  Claude Managed Agents API，也不拥有全局 Assignment。
- AgentGo 只拥有 Agent Loop 与原生会话状态，不感知 HTTP API 或具体 Sandbox Engine 实现。
- Agent Ledger 记录规范化执行事实并保存不透明 Checkpoint，不解释 Harness State，也不做调度或自动重放决策。

## 代码地图与核心模块

```text
├── go.mod                     # 仓库唯一 Go module
├── cmd/
│   ├── agentd/                # Control Plane 二进制入口
│   └── agentlet/              # 节点执行二进制入口
├── deploy/
│   ├── docker/                 # agentd 与 agentlet 多阶段镜像构建
│   └── k8s/                    # Helm Chart、Worker 双容器模板与弹性设计
├── agentd/                    # Worker 观测、Session placement 与调度
│   ├── internal/
│   │   ├── api/               # Control Plane HTTP 适配
│   │   ├── service/           # 控制面事务与 Session placement 编排
│   │   ├── model/             # Worker、Session 与 Assignment 领域模型
│   │   ├── repo/              # Repository 契约及 GORM 实现、表映射与 resource lock
│   │   ├── k8s/               # Kubernetes Client 与 PodSnapshot substrate
│   │   ├── observer/          # 周期拉取 Worker Pod 事实并持久化 observation
│   │   ├── scheduler/         # 无 I/O 的 Session → Worker placement 策略
│   └── docs/
│       ├── kernel.md          # 稳定定位、核心模型、API 与组件主流程
│       ├── agentd.md          # Control Plane、Worker 弹性、调度、转发与 Control State
│       ├── agentlet.md        # 执行节点、Checkpoint、Ledger 与恢复编排
│       ├── harness.md         # Harness 执行边界、适配契约与 AgentGo 实现
│       └── sandbox-engine.md  # Sandbox Engine 能力、生命周期与隔离边界
└── agentlet/
    ├── agentlet.go            # Agentlet 依赖组装与服务生命周期
    ├── internal/
    │   ├── api/               # 仅供 agentd 调用的内部执行 HTTP/SSE 适配
    │   ├── service/           # Session Work 生命周期与进程内执行快照
    │   ├── harness/           # Harness 契约及 adapter；AgentGo 是首个实现
    │   ├── persistence/       # SQLite/MySQL 下的 Ledger 与 Checkpoint Store 组装
    │   ├── sandbox/           # Sandbox Engine 能力契约与默认 Adapter
    │   └── work/              # Assignment fence 下的进程内 Work 与唤醒合并
    └── tests/e2e/             # 显式 e2e build tag；恢复契约与 live 组件联调
```

`cmd/<name>` 只负责对应二进制的启动；可测试实现分别归属 `agentd/` 和 `agentlet/`。两者通过网络
协议协作，不通过共享 Go interface 伪装进程边界。

## 关键约定

1. agentd 对外 HTTP 路径、资源形状、Session 状态和 Event 命名以 Claude Managed Agents API
   为准；Agentlet 只暴露 `/internal/v1` 执行协议。
2. 用户 Event 先持久化再确认接收；模型和工具调用必须通过 Agent Ledger AgentGo
   Adapter 的 write-before-execute 边界。
3. agentd 把一个 Agentlet Pod 建模为一个 Worker，按最新 observation、容量和当前绑定
   Session 数调度；Agentlet 不主动注册或发心跳。
4. Worker Observer 是运行事实的唯一写入者，Scheduler 只做无 I/O 的 placement，Lifecycler 管理
   Worker 供给，Connector 只转发已分配流量；Kubernetes 管理已创建 Pod 的健壮性，SRE 管理 workload
   模板和集群容量。
5. 进程恢复由 Control State 中的精确 ResumeRef 定位 Agent Ledger Checkpoint，并结合 Ledger 未决 Attempt
   判断是否安全继续；同一 input 不重复注入，结果不明确的 Tool Attempt 不自动重放，Session
   转为 `terminated` 等待人工对账。
6. AgentGo 运行在 Agentlet 进程。Sandbox Engine 作为可替换的 sidecar 或远端服务运行，不与
   AgentGo 共享调用栈或本地文件路径。
7. 所有外部 HTTP、模型和存储调用显式配置超时；子进程退出和服务关闭必须收敛
   正在执行的 Session。

## References

- `docs/kernel.md` — agentd 稳定定位、核心模型、API 边界与组件主流程
- `docs/agentd.md` — Control Plane、Worker 弹性、调度、转发与 Control State
- `docs/agentlet.md` — Agentlet 执行、Checkpoint / Ledger 接入与恢复顺序
- `docs/harness.md` — Harness 执行边界、适配契约、恢复语义与 AgentGo 实现
- `docs/sandbox-engine.md` — Sandbox Engine 能力契约、资源生命周期与隔离要求
- `../deploy/k8s/README.md` — Helm 部署形态、Worker 双容器模板与扩缩容流程
- `https://platform.claude.com/docs/en/managed-agents/overview` — 上游 API 概念与行为基线
- `https://github.com/opensandbox-group/OpenSandbox/tree/main/specs` — Sandbox Lifecycle 与 Execd 设计参考
- `https://github.com/compforge/agent-ledger/tree/main/spec` — Ledger 事件、追加与 Adapter 契约
