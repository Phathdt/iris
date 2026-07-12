# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.26-alpine AS builder

# UPX for binary compression; git for version stamping via `git describe`
RUN apk add --no-cache git upx

WORKDIR /src

# Cache module downloads
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Version info passed from the build args (populated by CI)
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

# Build a fully static, stripped binary (no CGO), then compress with UPX.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w \
      -X 'iris/pkg/config.version=${VERSION}' \
      -X 'iris/pkg/config.gitCommit=${GIT_COMMIT}' \
      -X 'iris/pkg/config.buildDate=${BUILD_DATE}'" \
    -o /out/iris ./cmd/iris \
 && upx --best --lzma /out/iris

# ---- Runtime stage ----
FROM alpine:3.21

# CA certs for TLS (Kafka/Postgres over TLS), tzdata for correct timestamps
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S iris && adduser -S -G iris iris

COPY --from=builder /out/iris /usr/local/bin/iris

USER iris

# Metrics/health server (observability.metrics.port default)
EXPOSE 9090

ENTRYPOINT ["iris"]
CMD ["--config", "/etc/iris/config.yaml"]
