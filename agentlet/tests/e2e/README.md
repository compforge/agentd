# End-to-end cases

The local suite exercises the public Claude-compatible API through the official SDK and keeps every
external dependency deterministic. Run it with `make test-e2e`.

| Case | Boundary | Dependency |
|---|---|---|
| committed input recovery | process replacement across Control State, Harness State, and Ledger | SQLite |
| model question and answer | SDK → Hertz → AgentGo → Anthropic streaming API → persisted event | SQLite + local model server |
| mid-stream model timeout | partial stream → audited failed turn → later input succeeds without leaked partial output | SQLite + local model server |
| real model answer | the same server path against an Anthropic-compatible provider | opt-in model credentials |
| live round trip and restart | MySQL, Hostel, local model stub, tool call, and restart | opt-in live services |

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
