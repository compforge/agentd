# agentd system perf

This directory is a standalone consumer of `perf_harness` from
[case-harness](https://github.com/compforge/case-harness). Like sandctl's perf suite, it separates:

- `cases/`: stable stimuli and diagnostic facets;
- `managed-agent-turn.yaml`: target, load stages, observation, and SLOs;
- `agentd_perf/`: the service-specific public API workload.

The initial profile measures a complete managed-agent turn. Setup creates one Agent, one cloud
Environment, and a bounded Session pool. Each measured request leases one Session, sends a user
Event, waits for the corresponding `agent.message` and final `idle` status, then returns that Session
to the pool. The Agent must call `bash`, so the measured path includes Worker placement, Agentlet,
AgentGo, model I/O, Ledger/Checkpoint, and Sandbox Engine.

Agent, Environment, Session, and Event rows are durable audit records and have no public delete API.
Use a disposable performance environment or its record-retention policy. One trial creates only
`pool_size` Sessions rather than one Session per request.

## Run

Copy `managed-agent-turn.yaml`, then set the target URL, Kubernetes reference, model, load, and SLOs
for the environment under test:

```bash
cd tests/perf
uv sync
uv run python -m perf_harness.cli run managed-agent-turn.yaml
```

The checked-in profile is deliberately low-rate because it invokes a real model. Increase the rate
only after matching `load.max_inflight` to `workload.pool_size` and confirming Worker/model capacity.
The report combines client latency/error/drop metrics with agentd and Worker Pod CPU, memory,
restarts, limits, and counts. A missing observation does not satisfy a strict SLO.

Run workload/config unit tests without contacting a deployment:

```bash
uv run pytest -q
```
