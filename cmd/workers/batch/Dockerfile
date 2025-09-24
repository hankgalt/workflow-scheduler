# syntax=docker/dockerfile:1.7

# auto-injected BuildKit args
# ARG TARGETPLATFORM
# ARG TARGETOS
# ARG TARGETARCH
# ARG TARGETVARIANT
# ARG BUILDPLATFORM
# ARG BUILDOS
# ARG BUILDARCH
# ARG BUILDVARIANT
ARG GO_VERSION=1.24.7

############################
# Builder stage (Go + Alpine)
############################
FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

# Copy necessary go modules (using vendoring)
COPY go.mod go.sum ./
COPY vendor/ ./vendor/

# Copy required application code
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/

# Copy generated pb files
COPY api/ ./api/

# Force vendoring + static build
ENV CGO_ENABLED=0 GOFLAGS=-mod=vendor

# Build the server
RUN --mount=type=cache,target=/root/.cache/go-build \
GOOS=$TARGETOS GOARCH=$TARGETARCH \
go build \
-trimpath -buildvcs=false \
-tags netgo,osusergo \
-ldflags "-s -w" \
-o /out/batch-worker \
./cmd/workers/batch

############################
# Runtime stage (distroless, tiny)
############################
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app

COPY --from=builder /out/batch-worker /app/batch-worker

USER nonroot:nonroot
ENTRYPOINT ["/app/batch-worker"]
