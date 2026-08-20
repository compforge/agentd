.PHONY: fix lint test test-e2e test-model-integration test-integration build run run-agentd run-agentlet

fix:
	gofmt -w $$(find agentd agentlet cmd internal -name '*.go' -not -path '*/vendor/*')

lint:
	go vet ./...

test:
	go test ./...

# E2E is a user-triggered system assessment, not part of the daily development checks.
test-e2e:
	go test -tags=e2e ./agentd/tests/e2e ./agentlet/tests/e2e -count=1 -v

test-model-integration:
	AGENTD_REQUIRE_INTEGRATION=1 go test -tags=e2e ./agentlet/tests/e2e -run TestManagedAgentAnswersThroughRealModel -count=1 -v

test-integration:
	AGENTD_REQUIRE_INTEGRATION=1 go test -tags=e2e ./agentlet/tests/e2e -run TestManagedAgentMySQLSandboxRoundTripAndRestart -count=1 -v

build:
	go build ./...

run: run-agentlet

run-agentd:
	go run ./cmd/agentd

run-agentlet:
	go run ./cmd/agentlet
