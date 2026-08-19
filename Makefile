.PHONY: fix lint test test-e2e test-integration build run

fix:
	cd server && gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

lint:
	cd server && go vet ./...

test:
	cd server && go test ./...

test-e2e:
	cd server && go test -tags=e2e ./tests/e2e -run TestRecoverCommittedInputAfterRestart -count=1 -v

test-integration:
	cd server && AGENTD_REQUIRE_INTEGRATION=1 go test -tags=e2e ./tests/e2e -run TestManagedAgentMySQLHostelRoundTripAndRestart -count=1 -v

build:
	cd server && go build ./cmd/agentd

run:
	cd server && go run ./cmd/agentd
