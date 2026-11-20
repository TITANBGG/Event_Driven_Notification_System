# Multi-stage build compiling static Go binaries into a minimal non-root distroless image.
# Uses CMD (not ENTRYPOINT) so docker-compose can override `command:` cleanly per service.

FROM golang:1.27.4-alpine AS builder

WORKDIR /src

# Download dependencies before copying the source tree to preserve Docker layer caching.
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
