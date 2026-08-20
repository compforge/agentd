# agentd

<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a>
</p>

`agentd` is a managed agent server built with [AgentGo](https://github.com/compforge/agentgo) and
[Agent Ledger](https://github.com/compforge/agent-ledger), with a pluggable Sandbox Engine.

It exposes the core Claude Managed Agents resources—Agent, Environment, Session, and Event—while
running the agent harness and sandbox on infrastructure you control. The API transport uses Hertz.
The architecture separates the `agentd` control plane from per-Pod Agentlet instances. `agentd`
owns the Managed Agents API and schedules each Session; Agentlet exposes only an internal execution
API and manages assigned harness runtimes.

## Core capabilities

- Keep long-running work alive beyond a single process or model context window. A session and its
  history remain available when execution waits, stops, or moves elsewhere.
- Accept work asynchronously and stream progress and results as events, without tying the client's
  connection to the lifetime of an execution process.
- Provision execution capacity when it is needed, place sessions across available workers, and
  release idle compute without losing the session's identity.
- Run model-generated code in an isolated environment, separated from the control plane and its
  credentials.
- Preserve inputs, outputs, tool actions, checkpoints, and failures for recovery, audit, and
  trajectory analysis.
- Replace the agent harness or sandbox implementation behind stable interfaces as models and
  infrastructure evolve.

## Architecture

![agentd managed agent architecture](agentd/docs/architecture.svg)

`agentd` owns the public API, durable managed-agent identity, and control state. It schedules each
Session onto an elastic Worker and routes execution through the Connector. A Worker is Agentlet's
workload form; on Kubernetes it is a Pod containing Agentlet and a Sandbox Engine sidecar.

Agentlet drives the selected Harness—AgentGo is the first implementation—while the Sandbox Engine
provides isolated tool execution. Checkpoints and append-only execution facts are persisted through
Agent Ledger, so a Session can release its Worker and later resume on another one without making
the control plane understand Harness-native state.

## Quick start

```bash
export ANTHROPIC_API_KEY=your-key
export AGENTD_SANDBOX_ENDPOINT=http://127.0.0.1:8080
make run
```

agentd and Agentlet use local SQLite when `AGENTD_MYSQL_DSN` is empty. This is convenient for a
single-process trial, but deleting the process filesystem loses state, Ledger, and Checkpoints.
Set separate external MySQL DSNs for robust deployments; the two services do not share tables.
The controller, harness, and ledger integration use storage interfaces rather than GORM types.

`make run` starts the Agentlet directly on `127.0.0.1:8019`. `make run-agentd` starts the Control
Plane on `0.0.0.0:8020`; it uses local SQLite unless `AGENTD_MYSQL_DSN` is set. When running in a
Kubernetes Pod, agentd discovers the namespace from its ServiceAccount and enables the Kubernetes
Worker source by default. Outside Kubernetes, `AGENTD_WORKER_SOURCE=kubernetes` enables it explicitly.
The Worker Observer periodically
lists Agentlet Pods and persists observations for capacity-aware Session Assignment. Agentlet does
not register or heartbeat with agentd. Kubernetes owns Worker Pod health and replacement; agentd
only uses fresh Pod observations for placement. The Claude-compatible API is exposed only by agentd;
Agentlet serves agentd under `/internal/v1`.

```bash
export AGENTD_WORKER_SOURCE=kubernetes
export AGENTD_WORKER_NAMESPACE=default
export AGENTD_WORKER_LABEL_SELECTOR='app.kubernetes.io/name=agentlet'
```

The API uses the Claude Managed Agents beta paths and accepts
`anthropic-beta: managed-agents-2026-04-01`.

The stable product boundary, supported surface, and component flow are defined in
[`agentd/docs/kernel.md`](agentd/docs/kernel.md). Control Plane scheduling, Worker lifecycle, routing,
and Control State are defined in [`agentd/docs/agentd.md`](agentd/docs/agentd.md). Agentlet execution,
Checkpoint/Ledger integration, and recovery ordering are defined in
[`agentd/docs/agentlet.md`](agentd/docs/agentlet.md). Harness execution and adapter boundaries are
defined in [`agentd/docs/harness.md`](agentd/docs/harness.md). Sandbox capabilities and isolation
boundaries are defined in [`agentd/docs/sandbox-engine.md`](agentd/docs/sandbox-engine.md).
The target Helm topology and Worker elasticity model are described in
[`deploy/k8s/README.md`](deploy/k8s/README.md).
