# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.0

FROM golang:${GO_VERSION}-bookworm AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/agentd ./cmd/agentd && \
    go build -trimpath -ldflags="-s -w" -o /out/agentlet ./cmd/agentlet

FROM gcr.io/distroless/cc-debian12:nonroot AS agentd
WORKDIR /tmp
COPY --from=builder /out/agentd /usr/local/bin/agentd
ENTRYPOINT ["/usr/local/bin/agentd"]

FROM gcr.io/distroless/cc-debian12:nonroot AS agentlet
WORKDIR /tmp
COPY --from=builder /out/agentlet /usr/local/bin/agentlet
ENTRYPOINT ["/usr/local/bin/agentlet"]
