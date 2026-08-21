# Agentd system end-to-end cases

The E2E suite validates the whole agentd system. A case may enter through the Server and expose a
problem in the Agentlet, Ledger, Sandbox, or one of the system's lifecycle controllers. Tests live
in component-local packages only when Go's `internal` import boundary requires it; run the unified
suite with `make test-e2e`.

These deterministic cases cover system lifecycles that do not require a live Kubernetes cluster.
The E2E suite is triggered explicitly by a user for a concentrated system assessment. It is not
part of routine development checks such as `make test`, `make lint`, or `make build`.

| Case | Boundary | Dependency |
|---|---|---|
| retired Worker record retention | Pod GC → Observer → Record GC → SQLite | SQLite + deterministic Pod substrate |
| idle Session assignment release | Agentlet state API → Session Observer → Control State → Worker capacity | SQLite + deterministic Agentlet HTTP substrate |
| Event read after assignment release | Session Observer → Assignment release → healthy Worker → persisted Event API | SQLite + deterministic Agentlet HTTP substrate |

The Worker record case proves that Pod cleanup and metadata retention are separate lifecycles: an
idle Worker Pod is destroyed first, its terminal record remains available during the retention
window, and only Record GC deletes the aged, absent, unbound row.

The Session observation case proves that observation is read-only, the Assignment fence protects
the update, the safe ResumeRef advances, and an idle Session releases its Worker capacity. It also
proves that persisted Event reads remain available through a healthy Worker after that Assignment
has been released.
