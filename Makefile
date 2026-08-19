.PHONY: fix lint test build run

fix:
	cd server && gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

lint:
	cd server && go vet ./...

test:
	cd server && go test ./...

build:
	cd server && go build ./cmd/agentd

run:
	cd server && go run ./cmd/agentd
