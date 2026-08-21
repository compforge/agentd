# agentd system E2E

This suite drives a **running agentd deployment** exclusively through the Claude Managed Agents API.
It creates an Agent, cloud Environment, and Session, then executes two turns that require a Sandbox
Engine tool. A passing run proves the public request crossed the Control Plane, Worker placement,
Agentlet, AgentGo, model, Ledger/Checkpoint, and Sandbox Engine, and that the second turn resumed the
first Session.

The test uses unique resource names and intentionally keeps the resulting records. Agentd has no
public delete contract for these durable audit resources; use a disposable environment or the
deployment's record-retention policy.

## Run

Point the suite at a deployment whose agentd, shared MySQL, dynamically created Workers, model
credentials, and Sandbox Engine are all ready:

```bash
AGENTD_E2E_BASE_URL=http://127.0.0.1:8020 \
AGENTD_E2E_MODEL=claude-sonnet-4-6 \
AGENTD_REQUIRE_E2E=1 \
go test -tags=e2e -v ./tests/e2e
```

| Variable | Required | Default | Meaning |
|---|---:|---|---|
| `AGENTD_E2E_BASE_URL` | yes to execute | — | Public agentd endpoint, without `/v1` |
| `AGENTD_E2E_API_KEY` | no | `test` | Client API key; agentd currently does not authenticate it |
| `AGENTD_E2E_MODEL` | no | `claude-sonnet-4-6` | Model configured on the created Agent |
| `AGENTD_E2E_TIMEOUT` | no | `10m` | Whole-case deadline, including Worker scale-out and two turns |
| `AGENTD_REQUIRE_E2E` | no | `0` | Fail instead of skip when the target URL is absent |

`make test-e2e` also executes the deterministic component suites. When
`AGENTD_E2E_BASE_URL` is absent, only this live black-box case skips.
