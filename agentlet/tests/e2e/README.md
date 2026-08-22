# End-to-end cases

These component-local cases exercise Agentlet recovery, Harness, Ledger and model boundaries while
keeping every external dependency deterministic. Resource setup uses the Agentlet service fixture;
Event ingress and reads use the shared Ledger, then execution is triggered through `wake`. Public
Claude-compatible Event contracts belong to the agentd system cases. Run the unified suite with
`make test-e2e`.

E2E is a user-triggered, concentrated agentd system assessment. It is not part of routine
development checks such as `make test`, `make lint`, or `make build`.

| Case | Boundary | Dependency |
|---|---|---|
| assignment handoff fence | two live Agentlets share one Ledger; only the replacement consumes new input | in-memory Ledger |
| committed input recovery | process replacement across Control State, Harness State, and Ledger | SQLite |
| model question and answer | shared Ledger → wake → AgentGo → Anthropic streaming API → persisted event | SQLite + local model server |
| mid-stream model timeout | partial stream → audited failed turn → later input succeeds without leaked partial output | SQLite + local model server |
| unresolved write resolution | restored Checkpoint + unresolved Ledger Attempt → requires action → user deny → resumed answer | SQLite + local model server |
| real model answer | the same server path against an Anthropic-compatible provider | opt-in model credentials |
| live round trip and restart | MySQL, Sandbox Engine, local model stub, tool call, and restart | opt-in live services |

The recovery CaseSet is [`cases/recovery.yaml`](cases/recovery.yaml). The unresolved-write case
constructs the exact durable boundary left by a hard process loss: the assistant tool call is in the
Checkpoint and the non-idempotent write has only `attempt.requested` in the Ledger. A replacement
Agentlet must expose `requires_action`; denying that attempt must continue the Session without a
second Attempt or a Sandbox write.

Skills are not represented by a fake prompt in this suite. They require agentd to implement the
Managed Agents Skill and Skill Version contracts, durable skill content, and Sandbox injection first.
When that capability exists, cover version pinning, missing versions, workspace isolation, and use by
the model as separate cases.

Use `@clawhub_chindden/skill-creator@0.1.0` from
[SkillHub](https://skillhub.cloud.tencent.com/skills/clawhub_chindden/skill-creator) as the first
script-bearing case. The pinned bundle is Apache-2.0, requires no API key, and includes a
standard-library-only `scripts/init_skill.py`. The case should materialize the version into the
Sandbox, ask the model to run the script under `/workspace`, and assert both the generated files and
the Skill coordinate recorded in the Ledger. Keep the Hub download outside the test path: the checked
in fixture or fixture cache must be verified against the pinned file hashes so the default E2E suite
does not depend on a mutable third-party service.
