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

## Kubernetes deployment (preview)

```bash
helm upgrade --install agentd deploy/k8s/agentd \
  --namespace agentd --create-namespace

kubectl -n agentd port-forward service/agentd 8020:8020
curl http://127.0.0.1:8020/healthz
```

The public Managed Agents API is exposed by agentd on port `8020`. Agentlet listens on port `8019`
inside each Worker Pod and serves only agentd under `/internal/v1`; clients should not connect to it
directly.

The default Helm values install one agentd replica backed by ephemeral SQLite. Production and
multi-replica deployments require external MySQL storage. The Helm topology is currently a preview
while Worker lifecycle runtime wiring is being completed. See
[`deploy/k8s/README.md`](deploy/k8s/README.md) for the topology, persistence options, image settings,
and current limitations.

The API uses the Claude Managed Agents beta paths and accepts
`anthropic-beta: managed-agents-2026-04-01`.

The stable product boundary, supported surface, and component flow are defined in
[`agentd/docs/kernel.md`](agentd/docs/kernel.md). Control Plane scheduling, Worker lifecycle, routing,
and Control State are defined in [`agentd/docs/agentd.md`](agentd/docs/agentd.md). Agentlet execution,
Checkpoint/Ledger integration, and recovery ordering are defined in
[`agentd/docs/agentlet.md`](agentd/docs/agentlet.md). Harness execution and adapter boundaries are
defined in [`agentd/docs/harness.md`](agentd/docs/harness.md). Sandbox capabilities and isolation
boundaries are defined in [`agentd/docs/sandbox-engine.md`](agentd/docs/sandbox-engine.md).
