# agentd

`agentd` is a managed agent server built with [AgentGo](https://github.com/compforge/agentgo),
[Hostel](https://github.com/qiankunli/hostel), and
[Agent Ledger](https://github.com/compforge/agent-ledger).

It exposes the core Claude Managed Agents resources—Agent, Environment, Session, and Event—while
running the agent harness and sandbox on infrastructure you control. The API transport uses Hertz.

## Core capabilities

- reusable, versioned agent definitions;
- a pluggable Sandbox Engine, with one isolated Hostel Bed for each session in the first adapter;
- asynchronous session input with persisted event history and SSE output;
- durable Harness State plus write-before-execute model/tool records through Agent Ledger;
- recovery of an AgentGo session after the `agentd` process is replaced.

## Quick start

```bash
export ANTHROPIC_API_KEY=your-key
export AGENTD_HOSTEL_URL=http://127.0.0.1:8080
export AGENTD_MYSQL_DSN='agentd:password@tcp(127.0.0.1:3306)/agentd'
make run
```

MySQL/GORM is the initial persistence provider. The controller, harness, and ledger integration use
storage interfaces rather than GORM types, so another provider can be added without changing them.

The first version binds to `127.0.0.1:8081` and is intended for a single agentd process. Multi-replica
ownership, durable wake-up, and fencing are part of the kernel evolution, not claimed by this version.

The API uses the Claude Managed Agents beta paths and accepts
`anthropic-beta: managed-agents-2026-04-01`.

The stable product boundary, supported surface, and component flow are defined in
[`server/docs/kernel.md`](server/docs/kernel.md). Persistence, recovery, audit, and trajectory
boundaries are defined in [`server/docs/state-ledger.md`](server/docs/state-ledger.md).

## Development

```bash
make fix
make lint
make test
make test-e2e
make build
```

`make test-e2e` explicitly enables the `tests/e2e` suite. Its default recovery case runs the Claude
SDK against a real Hertz server and SQLite-backed Control State, Harness State, and Ledger stores,
then replaces the application mid-turn. It needs no external services.

The opt-in live integration check still requires a disposable MySQL database, a running Hostel server,
and an Anthropic model:

```bash
export AGENTD_TEST_MYSQL_DSN='agentd:password@tcp(127.0.0.1:3306)/agentd_test'
export AGENTD_TEST_HOSTEL_URL='http://127.0.0.1:8080'
export AGENTD_TEST_MODEL='claude-sonnet-4-6'
export ANTHROPIC_API_KEY='your-key'
make test-integration
```
