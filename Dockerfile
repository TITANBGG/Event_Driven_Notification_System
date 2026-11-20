# WHAT: Multi-stage Dockerfile compiling all static Go binaries into a non-root distroless image.
# WHY: CGO_ENABLED=0 + -trimpath + -ldflags="-s -w" produces tiny static binaries (~8-12 MB),
#      while distroless:nonroot removes shells/package managers to minimize container CVE surface.
#      We use CMD (not ENTRYPOINT) so docker-compose can override `command:` cleanly per service.
# EXPECTED OUTPUT EXAMPLE (`docker images notification-system:local`):
#   REPOSITORY            TAG     SIZE
#   notification-system   local   ~45MB

FROM golang:1.24.7-alpine AS builder

WORKDIR /src

# Download module dependencies first so Docker layer caching avoids re-fetching on code edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN mkdir -p /out && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mockprovider ./cmd/mockprovider && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/ /app/

USER nonroot:nonroot
EXPOSE 8080 8090 9091

CMD ["/app/api"]

