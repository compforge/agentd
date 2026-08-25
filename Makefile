.PHONY: fix lint test test-e2e test-model-integration test-integration test-perf build version run run-agentd run-agentlet

PERF_PROFILE ?= managed-agent-turn.yaml
VERSION ?= $(shell tr -d '\r\n' < VERSION)
REVISION ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null)
REVISION := $(if $(strip $(REVISION)),$(REVISION),unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
BUILD_LDFLAGS := -X github.com/compforge/agentd/internal/buildinfo.Version=$(VERSION) -X github.com/compforge/agentd/internal/buildinfo.Revision=$(REVISION) -X github.com/compforge/agentd/internal/buildinfo.BuildTime=$(BUILD_TIME)

fix:
	gofmt -w $$(find agentd agentlet cmd internal tests -name '*.go' -not -path '*/vendor/*')

lint:
	go vet ./...

test:
	go test ./...

# E2E is a user-triggered system assessment, not part of the daily development checks.
test-e2e:
	go test -tags=e2e ./tests/e2e ./agentd/tests/e2e ./agentlet/tests/e2e -count=1 -v

test-model-integration:
	AGENTD_REQUIRE_INTEGRATION=1 go test -tags=e2e ./agentlet/tests/e2e -run TestManagedAgentAnswersThroughRealModel -count=1 -v

test-integration:
	AGENTD_REQUIRE_INTEGRATION=1 go test -tags=e2e ./agentlet/tests/e2e -run TestManagedAgentMySQLSandboxRoundTripAndRestart -count=1 -v

# Perf is intentionally explicit: edit/copy the target profile before running it.
test-perf:
	cd tests/perf && uv run python -m perf_harness.cli run $(PERF_PROFILE)

build:
	go build -ldflags "$(BUILD_LDFLAGS)" ./...

version:
	@printf '%s\n' "$(VERSION)"

run: run-agentlet

run-agentd:
	go run ./cmd/agentd

run-agentlet:
	go run ./cmd/agentlet
