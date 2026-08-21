# System quality suites

This directory owns tests that assess agentd as one deployed system through its public Managed
Agents API. Component-local tests remain next to their implementation; the root suites do not
import `agentd/internal` or `agentlet/internal`.

| Suite | Question | Trigger |
|---|---|---|
| [`e2e`](e2e/README.md) | Can a durable Session execute and resume across agentd, Agentlet, Harness, Ledger, and Sandbox Engine? | `make test-e2e` |
| [`perf`](perf/README.md) | Under a declared load and resource profile, what latency, error, saturation, and restart behavior does the deployed system show? | `make test-perf` |

Both suites are explicit quality assessments. They are not part of `make test`, because they need a
real deployment, can consume model tokens, and may leave durable audit records in the target
database.

The root suites own cross-component orchestration and verdicts. Narrow deterministic contracts stay
in `agentd/tests/e2e` and `agentlet/tests/e2e`, where Go's `internal` import boundary permits direct
component setup.
