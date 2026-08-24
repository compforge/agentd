# AGENTS.md

## 项目定位与边界

agentd 是一个 Go 实现的 Managed Agent Server。它不实现 Agent 智能、业务工作流或沙箱
技术，而是通过 Control Plane 与 Agentlet，把可替换的 Harness、Sandbox Engine 和 Agent Ledger
组织成长生命周期服务。稳定定位与边界见 `docs/kernel.md`。

- agentd 是 Control Plane 的代码与服务边界，拥有 Model 注册信息等全局资源、Worker、Session
  placement、调度、转发和恢复决策。
- Agentlet 在单个 Worker Pod 内通过内部执行 API 管理多个 Harness runtime；它不实现面向客户端的
  Claude Managed Agents API，也不拥有全局 placement。
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
│   └── k8s/                    # Quick Start Helm Chart、Worker 模板与弹性设计
├── internal/
│   ├── event/                  # 共享 Ledger 上的 Managed Event 投影与写入边界
│   ├── executionapi/           # agentd → Agentlet 内部执行协议
│   └── persistence/            # 两个进程共用的 SQLite/MySQL、Ledger 与 Checkpoint 组装
├── tests/
│   ├── e2e/                    # 仅通过公开 API 验收整个已部署系统
│   └── perf/                   # CaseSet、负载 profile、SLO 与系统观测
├── agentd/                    # Worker 观测、Session placement 与调度
│   ├── internal/
│   │   ├── api/               # Control Plane HTTP 适配；middleware/ 承载 Hertz 中间件
│   │   ├── model/             # Worker 与内嵌 placement 的 Session 领域模型
│   │   ├── repo/              # Repository 契约及 GORM 实现、表映射与 resource lock
│   │   ├── service/           # 跨 Session/Worker 的控制面事务与状态投影
│   │   ├── session/
│   │   │   ├── connector/     # 已分配 Session 的 Agentlet 数据面
│   │   │   ├── observer/      # 只读 Agentlet 状态并持久化 Session observation
│   │   │   ├── reconciler/    # 将 Ledger demand 收敛为 Session placement 与 wake
│   │   │   └── scheduler/     # 无 I/O 的 Session → Worker placement 策略
│   │   └── worker/
│   │       ├── pool.go        # Worker 容量创建/回收的唯一控制环与可续租 lease
│   │       ├── observer/      # 消费 Pod Informer cache 并持久化 Worker observation
│   │       ├── reconciler/    # Worker row → Pod 与预热容量计划
│   │       ├── gc/            # Pool 内 Pod 回收计划与独立终态记录回收
│   │       └── k8s/           # Kubernetes Client 与 PodSnapshot substrate
│   └── docs/
│       ├── kernel.md          # 稳定定位、核心模型、API 与组件主流程
│       ├── agentd.md          # Control Plane、Worker 弹性、调度、转发与 Control State
│       ├── agentlet.md        # 执行节点、Checkpoint、Ledger 与恢复编排
│       ├── harness.md         # Harness 执行边界、适配契约与 AgentGo 实现
│       └── sandbox-engine.md  # Sandbox Engine 能力、生命周期与隔离边界
└── agentlet/
    ├── agentlet.go            # Agentlet 依赖组装与服务生命周期
    ├── internal/
    │   ├── api/               # 仅供 agentd 调用的 WorkSpec/wake/interrupt/state HTTP 适配
    │   ├── service/           # Session Work 生命周期与进程内执行快照
    │   ├── harness/           # Harness 契约及 adapter；AgentGo 是首个实现
    │   ├── sandbox/           # Sandbox Engine 能力契约与默认 Adapter
    │   └── work/              # Assignment fence 下的进程内 Work 与唤醒合并
    └── tests/e2e/             # 显式 e2e build tag；恢复契约与 live 组件联调
```

`cmd/<name>` 只负责对应二进制的启动；可测试实现分别归属 `agentd/` 和 `agentlet/`。两者通过网络
协议协作，不通过共享 Go interface 伪装进程边界。

## 关键约定

1. agentd 拥有 Claude Managed Agents 兼容 API、全局资源和 Control State；Agentlet 只通过
   `/internal/v1` 执行当前 Assignment，不提供公开资源 API，也不拥有全局 placement。
2. durable input、Harness Checkpoint 与 Agent Ledger 共同构成恢复材料：外部调用遵守
   write-before-execute，恢复可以采用同一 Session key 上领先于 Control State 的已验证 checkpoint，
   但结果不明的非幂等副作用保持 fail-closed。具体顺序见 `docs/agentlet.md` 和 `docs/harness.md`。
3. Control Plane 的事实、决策、动作和数据面各有唯一 owner：Observer 写 observation，Scheduler 做
   纯 placement 决策，Session Reconciler 修改 placement，Worker Pool 管理 Worker 容量，Connector
   只按 Assignment 转发。详细收敛模型见 `docs/agentd.md`。
4. Harness 与 Sandbox Engine 都是可替换执行依赖，不得把原生状态或物理位置泄漏进公开 API 和
   Control Plane。Quick Start 的共置拓扑不改变生产边界；认证、凭据、超时和关闭约束见
   `docs/kernel.md`、`docs/sandbox-engine.md` 及部署文档。

## References

- `docs/kernel.md` — agentd 稳定定位、核心模型、API 边界与组件主流程
- `docs/agentd.md` — Control Plane、Worker 弹性、调度、转发与 Control State
- `docs/agentlet.md` — Agentlet 执行、Checkpoint / Ledger 接入与恢复顺序
- `docs/harness.md` — Harness 执行边界、适配契约、恢复语义与 AgentGo 实现
- `docs/sandbox-engine.md` — Sandbox Engine 能力契约、资源生命周期与隔离要求
- `../deploy/k8s/README.md` — Quick Start Helm 形态、Worker 模板与扩缩容流程
- `../tests/README.md` — 系统级 E2E / perf 边界、触发方式与子套件索引
- `https://platform.claude.com/docs/en/managed-agents/overview` — 上游 API 概念与行为基线
- `https://github.com/opensandbox-group/OpenSandbox/tree/main/specs` — Sandbox Lifecycle 与 Execd 设计参考
- `https://github.com/compforge/agent-ledger/tree/main/spec` — Ledger 事件、追加与 Adapter 契约
