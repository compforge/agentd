# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.0

FROM golang:${GO_VERSION}-bookworm AS builder
ARG REVISION=unknown
ARG BUILD_TIME=unknown
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN VERSION="$(tr -d '\r\n' < VERSION)" && \
    BUILD_LDFLAGS="-s -w -X github.com/compforge/agentd/internal/buildinfo.Version=${VERSION} -X github.com/compforge/agentd/internal/buildinfo.Revision=${REVISION} -X github.com/compforge/agentd/internal/buildinfo.BuildTime=${BUILD_TIME}" && \
    go build -trimpath -ldflags="${BUILD_LDFLAGS}" -o /out/agentd ./cmd/agentd && \
    go build -trimpath -ldflags="${BUILD_LDFLAGS}" -o /out/agentlet ./cmd/agentlet

FROM gcr.io/distroless/cc-debian12:nonroot AS agentd
WORKDIR /tmp
COPY --from=builder /out/agentd /usr/local/bin/agentd
ENTRYPOINT ["/usr/local/bin/agentd"]

FROM gcr.io/distroless/cc-debian12:nonroot AS agentlet
WORKDIR /tmp
COPY --from=builder /out/agentlet /usr/local/bin/agentlet
ENTRYPOINT ["/usr/local/bin/agentlet"]
