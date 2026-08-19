.PHONY: fix lint test test-integration build run

fix:
	cd server && gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

lint:
	cd server && go vet ./...

test:
	cd server && go test ./...

test-integration:
	cd server && AGENTD_REQUIRE_INTEGRATION=1 go test ./internal/integration -run TestManagedAgentMySQLHostelRoundTripAndRestart -count=1 -v

build:
	cd server && go build ./cmd/agentd

run:
	cd server && go run ./cmd/agentd
