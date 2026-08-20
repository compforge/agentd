# agentd

`agentd` is a managed agent server built with [AgentGo](https://github.com/compforge/agentgo) and
[Agent Ledger](https://github.com/compforge/agent-ledger), with a pluggable Sandbox Engine.

It exposes the core Claude Managed Agents resources—Agent, Environment, Session, and Event—while
running the agent harness and sandbox on infrastructure you control. The API transport uses Hertz.
The architecture separates the `agentd` control plane from per-Pod Agentlet instances. `agentd`
accepts the Managed Agents API, schedules or locates a Session, and forwards the same API request;
each Agentlet implements its execution semantics and manages multiple assigned harness runtimes.

## Core capabilities

- reusable, versioned agent definitions;
- a pluggable Sandbox Engine, with one isolated sandbox for each session;
- asynchronous session input with persisted event history and SSE output;
- durable Harness State plus write-before-execute model/tool records through Agent Ledger;
- recovery of an AgentGo session after the Agentlet process is replaced;
- Worker observation and capacity-aware Session Assignment.

## Quick start

```bash
export ANTHROPIC_API_KEY=your-key
export AGENTD_SANDBOX_ENDPOINT=http://127.0.0.1:8080
export AGENTD_MYSQL_DSN='agentd:password@tcp(127.0.0.1:3306)/agentd'
make run
```

MySQL/GORM is the initial persistence provider. The controller, harness, and ledger integration use
storage interfaces rather than GORM types, so another provider can be added without changing them.

`make run` starts the Agentlet directly on `127.0.0.1:8081`. `make run-agentd` starts the Control
Plane on `127.0.0.1:8082`. When `AGENTD_WORKER_SOURCE=kubernetes`, its Worker Observer periodically
lists Agentlet Pods and persists observations for capacity-aware Session Assignment. Agentlet does
not register or heartbeat with agentd. Kubernetes owns Worker Pod health and replacement; agentd
only uses fresh Pod observations for placement. The Claude-compatible resource API remains on
Agentlet while those global resources and routing are moved into agentd.

```bash
export AGENTD_WORKER_SOURCE=kubernetes
export AGENTD_WORKER_NAMESPACE=default
export AGENTD_WORKER_LABEL_SELECTOR='app.kubernetes.io/name=agentlet'
```

The API uses the Claude Managed Agents beta paths and accepts
`anthropic-beta: managed-agents-2026-04-01`.

The stable product boundary, supported surface, and component flow are defined in
[`agentd/docs/kernel.md`](agentd/docs/kernel.md). The Control Plane, Agentlet, scheduling, capacity,
and Control State model are defined in
[`agentd/docs/control-plane.md`](agentd/docs/control-plane.md). Harness execution and adapter
boundaries are defined in [`agentd/docs/harness.md`](agentd/docs/harness.md). Harness State, Ledger,
recovery, audit, and trajectory boundaries are defined in
[`agentd/docs/state-ledger.md`](agentd/docs/state-ledger.md). Sandbox capabilities and isolation
boundaries are defined in [`agentd/docs/sandbox-engine.md`](agentd/docs/sandbox-engine.md).

## Development

```bash
make fix
make lint
make test
make test-e2e
make build
```

`make test-e2e` explicitly enables the `tests/e2e` suite. The local cases run the Claude SDK against
a real Hertz server and SQLite-backed Control State, Harness State, and Ledger stores. They cover
process replacement plus successful and interrupted model streams through a deterministic local
Anthropic API server, so they need no external services. See
[`agentlet/tests/e2e`](agentlet/tests/e2e).

An opt-in real-model check uses the same SQLite-backed server path without requiring MySQL or an external Sandbox Engine:

```bash
export ANTHROPIC_API_KEY='your-key'
export ANTHROPIC_BASE_URL='https://your-anthropic-compatible-endpoint'
export AGENTD_TEST_MODEL='your-model-id'
make test-model-integration
```

The opt-in live integration check requires a disposable MySQL database and a running Sandbox Engine.
It uses a deterministic local Anthropic API stub, so no model credentials are required:

```bash
export AGENTD_TEST_MYSQL_DSN='agentd:password@tcp(127.0.0.1:3306)/agentd_test'
export AGENTD_TEST_SANDBOX_ENDPOINT='http://127.0.0.1:8080'
make test-integration
```
