# agentd

<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a>
</p>

`agentd` 是一个 Managed Agent 服务端，使用
[AgentGo](https://github.com/compforge/agentgo) 和
[Agent Ledger](https://github.com/compforge/agent-ledger) 构建，并支持可插拔的 Sandbox Engine。

它提供 Claude Managed Agents 的核心资源——Agent、Environment、Session 和 Event——同时把
Agent Harness 和 Sandbox 运行在用户控制的基础设施上。API 传输层使用 Hertz。整体架构将
`agentd` 控制面与每个 Pod 中的 Agentlet 实例分开：`agentd` 提供 Managed Agents API 并为
Session 调度执行位置；Agentlet 只暴露内部执行 API，负责管理分配给自己的 Harness 执行。

## 核心能力

- 可复用、可版本化的 Agent 定义；
- 可插拔的 Sandbox Engine，每个 Session 使用独立 Sandbox；
- 支持持久化事件历史和 SSE 输出的异步 Session 输入；
- 通过 Agent Ledger 保存 Harness State，并在模型或工具执行前写入记录；
- Agentlet 进程被替换后恢复 AgentGo Session；
- Worker 观测和基于容量的 Session Assignment。

## 架构

![agentd Managed Agent 架构](agentd/docs/architecture.svg)

`agentd` 负责公共 API、持久化的 Managed Agent 身份和控制状态。它把 Session 调度到弹性的
Worker，并通过 Connector 转发执行请求。Worker 是 Agentlet 的工作负载形态；在 Kubernetes
中，一个 Worker 对应一个包含 Agentlet 和 Sandbox Engine sidecar 的 Pod。

Agentlet 驱动选定的 Harness，AgentGo 是第一个实现；Sandbox Engine 提供隔离的工具执行环境。
Checkpoint 和 append-only 执行事实通过 Agent Ledger 持久化，因此 Session 可以释放 Worker，
随后在另一个 Worker 上恢复，而不要求控制面理解 Harness 的原生状态。

## 快速开始

```bash
export ANTHROPIC_API_KEY=your-key
export AGENTD_SANDBOX_ENDPOINT=http://127.0.0.1:8080
make run
```

当 `AGENTD_MYSQL_DSN` 为空时，agentd 和 Agentlet 使用本地 SQLite。这适合单进程试用，但进程
文件系统被删除后会丢失 Control State、Ledger 和 Checkpoint。生产部署应分别为两个服务配置
外部 MySQL DSN；它们不共享数据表。Controller、Harness 和 Ledger 集成都依赖存储接口，而不
直接依赖 GORM 类型。

`make run` 在 `127.0.0.1:8019` 直接启动 Agentlet。`make run-agentd` 在 `0.0.0.0:8020`
启动控制面；未设置 `AGENTD_MYSQL_DSN` 时使用本地 SQLite。在 Kubernetes Pod 内运行时，
agentd 会从 ServiceAccount 发现 Namespace，并默认启用 Kubernetes Worker source。在
Kubernetes 之外，可以通过 `AGENTD_WORKER_SOURCE=kubernetes` 显式启用。Worker Observer
周期性列出 Agentlet Pod，并保存观测结果，供基于容量的 Session Assignment 使用。
Agentlet 不向 agentd 注册或发送心跳；Kubernetes 负责 Worker Pod 的健康和替换，agentd 只用
新鲜的 Pod 观测结果做调度。兼容 Claude 的 API 只由 agentd 暴露；Agentlet 通过
`/internal/v1` 为 agentd 提供服务。

```bash
export AGENTD_WORKER_SOURCE=kubernetes
export AGENTD_WORKER_NAMESPACE=default
export AGENTD_WORKER_LABEL_SELECTOR='app.kubernetes.io/name=agentlet'
```

API 使用 Claude Managed Agents beta 路径，并接受
`anthropic-beta: managed-agents-2026-04-01`。

稳定产品边界、支持范围和组件流程见
[`agentd/docs/kernel.md`](agentd/docs/kernel.md)。控制面调度、Worker 生命周期、路由和 Control
State 见 [`agentd/docs/agentd.md`](agentd/docs/agentd.md)。Agentlet 执行、Checkpoint/Ledger
集成和恢复顺序见 [`agentd/docs/agentlet.md`](agentd/docs/agentlet.md)。Harness 执行和适配器
边界见 [`agentd/docs/harness.md`](agentd/docs/harness.md)。Sandbox 能力和隔离边界见
[`agentd/docs/sandbox-engine.md`](agentd/docs/sandbox-engine.md)。目标 Helm 拓扑和 Worker
弹性模型见 [`deploy/k8s/README.md`](deploy/k8s/README.md)。

## 开发

```bash
make fix
make lint
make test
make test-e2e
make build
```

`make test-e2e` 会显式启用 `tests/e2e` 测试套件。本地用例通过 Claude SDK 访问真实 Hertz
服务，并使用 SQLite 存储 Control State、Harness State 和 Ledger。用例通过确定性的本地
Anthropic API Server 覆盖进程替换、模型流成功和模型流中断，不依赖外部服务。详见
[`agentlet/tests/e2e`](agentlet/tests/e2e)。

可选的真实模型检查复用同一条 SQLite 服务链路，不要求 MySQL 或外部 Sandbox Engine：

```bash
export ANTHROPIC_API_KEY='your-key'
export ANTHROPIC_BASE_URL='https://your-anthropic-compatible-endpoint'
export AGENTD_TEST_MODEL='your-model-id'
make test-model-integration
```

可选的在线集成检查需要一个可丢弃的 MySQL 数据库和正在运行的 Sandbox Engine。它使用确定性
的本地 Anthropic API stub，因此不需要模型凭据：

```bash
export AGENTD_TEST_MYSQL_DSN='agentd:password@tcp(127.0.0.1:3306)/agentd_test'
export AGENTD_TEST_SANDBOX_ENDPOINT='http://127.0.0.1:8080'
make test-integration
```
