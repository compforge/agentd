# agentd system E2E

This suite drives a **running agentd deployment** through public APIs. Resilience cases inject and
observe controlled deployment faults through case-harness Kubernetes primitives; final acceptance
still uses the public Managed Agents API. Its canonical CaseSet is
[`cases/managed-agent.yaml`](cases/managed-agent.yaml):

- `api_key_authentication` calls a public endpoint without a key, with an invalid key, and with the
  configured deployment key, proving the Control Plane authentication boundary.
- `model_secret_redaction` registers and reads a Model through create, get, and list, proving that
  credentials remain write-only.
- `sandbox_resume` creates an Agent, cloud Environment, and Session, then executes two turns that
  require a Sandbox Engine tool. It proves that the request crosses the Control Plane, Worker
  placement, Agentlet, AgentGo, model, Ledger/Checkpoint, and Sandbox Engine, and that the second
  turn resumes the first Session.
- `worker_replacement_resume` completes one tool turn, deletes the explicitly selected managed
  Worker Pods, waits for a new logical Worker, and completes a second turn on the same Session. It
  proves that Worker loss does not lose durable Session input or checkpoint state.
- `mid_turn_worker_loss` force-deletes the selected Worker Pods after the Session reports `running`,
  then waits for a replacement to converge the same durable input. If an unknown-effect tool crossed
  its execution boundary, the case denies the exact required action and proves that agentd continues
  without silently replaying it or duplicating the user input.
- `agentlet_graceful_drain` normally deletes the selected Worker Pods after the Session reports
  `running`. It proves that SIGTERM drains accepted Work to a stable boundary, or leaves the durable
  input recoverable when the grace deadline expires, without converting shutdown into
  `retries_exhausted`.
- `control_plane_restart_before_dispatch` force-deletes the single Control Plane Pod immediately
  after durable ingress is accepted. It proves that the replacement process rediscovers demand
  from shared state and produces exactly one answer without a client retry.
- `control_plane_restart_during_turn` force-deletes the single Control Plane Pod after the Session
  reports `running`. It proves that Agentlet execution continues independently, the Worker set does
  not change, and the replacement Control Plane converges the final Session and Event projection.

The test uses unique resource names and intentionally keeps the resulting records. Agentd has no
public delete contract for these durable audit resources; use a disposable environment or the
deployment's record-retention policy.

## Run

Point the suite at a deployment whose agentd, shared MySQL, dynamically created Workers, and Sandbox
Engine are ready. The suite registers its external model connection through agentd before creating
the Agent:

```bash
AGENTD_E2E_BASE_URL=http://127.0.0.1:8020 \
AGENTD_E2E_MODEL_PROVIDER=anthropic \
AGENTD_E2E_MODEL=claude-sonnet-4-6 \
AGENTD_E2E_MODEL_API_KEY=... \
AGENTD_REQUIRE_E2E=1 \
go test -tags=e2e -v ./tests/e2e
```

| Variable | Required | Default | Meaning |
|---|---:|---|---|
| `AGENTD_E2E_BASE_URL` | yes to execute | — | Public agentd endpoint, without `/v1` |
| `AGENTD_E2E_API_KEY` | no | `test` | Client API key; must match the target agentd `AGENTD_API_KEY` |
| `AGENTD_E2E_MODEL` | no | `claude-sonnet-4-6` | Model configured on the created Agent |
| `AGENTD_E2E_MODEL_PROVIDER` | no | `anthropic` | AgentGo model provider registered through `/v1/models` |
| `AGENTD_E2E_MODEL_BASE_URL` | no | provider default | Optional external model endpoint |
| `AGENTD_E2E_MODEL_API_KEY` | yes to execute | `ANTHROPIC_API_KEY` | Write-only credential registered for the E2E Model |
| `AGENTD_E2E_TIMEOUT` | no | `10m` | Whole-case deadline, including Worker scale-out and two turns |
| `AGENTD_E2E_RUN_ID` | no | UTC timestamp | Stable identity for this E2E Run |
| `AGENTD_E2E_RUNS_DIR` | no | `runs` | Parent directory for Run artifacts |
| `AGENTD_REQUIRE_E2E` | no | `0` | Fail instead of skip when the target URL is absent |

`worker_replacement_resume` and `mid_turn_worker_loss` are intentionally disruptive and skip unless
explicitly enabled. Run them only against a disposable namespace or a dedicated Worker pool:

```bash
AGENTD_E2E_ALLOW_WORKER_DISRUPTION=1 \
AGENTD_E2E_KUBECONFIG="$KUBECONFIG" \
AGENTD_E2E_KUBE_CONTEXT=my-context \
AGENTD_E2E_KUBE_NAMESPACE=agentd-e2e \
AGENTD_E2E_WORKER_SELECTOR='app.kubernetes.io/instance=agentd-e2e,agentd.compforge.dev/managed=true' \
go test -tags=e2e -run TestManagedAgentResumesAfterWorkerReplacement -v ./tests/e2e
```

To exercise crash recovery during an active Turn, select the mid-turn case instead:

```bash
go test -tags=e2e -run TestManagedAgentRecoversAfterMidTurnWorkerLoss -v ./tests/e2e
```

To exercise normal Pod termination and Agentlet drain during an active Turn:

```bash
go test -tags=e2e -run TestManagedAgentDrainsOnWorkerTermination -v ./tests/e2e
```

Control Plane restart cases require a stable `AGENTD_E2E_BASE_URL` route that survives Pod
replacement; a port-forward bound to one physical Pod is not sufficient. Run them only in a
disposable namespace whose selector matches exactly one Ready Control Plane Pod:

```bash
AGENTD_E2E_ALLOW_CONTROL_PLANE_DISRUPTION=1 \
AGENTD_E2E_CONTROL_PLANE_SELECTOR='app.kubernetes.io/instance=agentd-e2e,app.kubernetes.io/component=control-plane' \
go test -tags=e2e -run 'TestManagedAgent(RecoversAfterControlPlaneRestartBeforeDispatch|ContinuesDuringControlPlaneRestart)' -v ./tests/e2e
```

| Variable | Required | Default | Meaning |
|---|---:|---|---|
| `AGENTD_E2E_ALLOW_WORKER_DISRUPTION` | yes | `0` | Must be `1` before the case can delete Worker Pods |
| `AGENTD_E2E_KUBE_NAMESPACE` | yes for disruption cases | — | Namespace-scoped boundary for Kubernetes operations |
| `AGENTD_E2E_WORKER_SELECTOR` | yes for Worker disruption and during-Turn Control Plane restart | — | Dedicated managed Worker pool to validate and observe |
| `AGENTD_E2E_ALLOW_CONTROL_PLANE_DISRUPTION` | yes for Control Plane restart | `0` | Must be `1` before a case can force-delete the Control Plane Pod |
| `AGENTD_E2E_CONTROL_PLANE_SELECTOR` | yes for Control Plane restart | — | Selector that must match exactly one Ready `component=control-plane` Pod |
| `AGENTD_E2E_KUBECONFIG` | yes on a client | — | Kubeconfig used by case-harness |
| `AGENTD_E2E_KUBE_CONTEXT` | no | current | Optional kubeconfig context override |
| `AGENTD_E2E_KUBE_IN_CLUSTER` | yes in a Job | `0` | Use in-cluster credentials instead of a kubeconfig |

`make test-e2e` also executes the deterministic component suites. When
`AGENTD_E2E_BASE_URL` is absent, these live black-box cases skip. Executed system CaseRuns are
aggregated into `runs/agentd-system/<run-id>/verdict.json`.
