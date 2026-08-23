# agentd system perf

This suite uses `perf_harness` from
[case-harness](https://github.com/compforge/case-harness) to assess agentd as one deployed Managed
Agent system. The harness owns load scheduling, Kubernetes observation, reports, and Verdicts;
this directory owns agentd's public API workload, stable stimuli, target profiles, and SLOs.

## Profiles

| Profile | Question |
|---|---|
| `managed-agent-turn.yaml` | Can a low-rate sandbox Turn stay healthy across agentd, Worker placement, Agentlet, AgentGo, the model, Ledger/Checkpoint, and Sandbox Engine? |
| `managed-agent-scale-out.yaml` | As concurrent plain Turns rise from 1 to 8, can agentd add Worker capacity without errors or restarts, then return to the configured Worker floor? |
| `managed-agent-mixed-soak.yaml` | Under a ten-minute 80/20 mix of plain and sandbox Turns, do latency, errors, drops, and Pod stability remain within the declared SLOs? |

Setup creates one Agent, one cloud Environment, and a bounded Session pool. Each measured request
leases one Session, persists a user Event, waits for the corresponding `agent.message` and final
`session.status_idle` Event, then returns that Session to the pool. Event History is authoritative:
safe reads are retried, while a lost send response is reconciled from persisted Events without
replaying the write. `accept_ms` isolates durable Event acceptance; `complete_ms` covers scheduling
and the complete Harness Turn. Transport retry, error, reconciliation latency, and reconciled-send
metrics remain visible even when the Turn eventually succeeds. `cases/turn.yaml` owns the plain and
sandbox stimuli and their diagnostic facet, while each profile only selects a mix and load shape.

## Run

Copy the closest profile, then set its target URL, Kubernetes reference, model resource ID, load,
and SLOs for the environment under test:

```bash
cd tests/perf
uv sync
make run PROFILE=managed-agent-turn.yaml
```

From the repository root, the equivalent command is:

```bash
make test-perf PERF_PROFILE=managed-agent-scale-out.yaml
```

These profiles invoke a real model and are deliberately user-triggered. Match `workload.pool_size`
to the profile's maximum concurrency, and confirm Worker and model capacity before increasing load.
The scale-out profile's 660-second cooldown and final Worker Pod SLO match the quick-start defaults
of a ten-minute Worker idle TTL and one minimum Worker; adjust both when the deployment policy
differs. Missing Kubernetes observations do not satisfy a strict SLO.

Agent, Environment, Session, and Event rows are durable audit records and have no public delete API.
Use a disposable performance environment or its record-retention policy. A trial creates only
`pool_size` Sessions and reuses them instead of creating one durable Session per request.

Run workload and profile tests without contacting a deployment:

```bash
uv run pytest -q
```
