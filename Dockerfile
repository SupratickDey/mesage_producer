# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application (CGO enabled for parquet-go, native architecture)
RUN go build \
    -ldflags="-s -w -X main.version=1.0.0" \
    -o producer \
    ./cmd/producer

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create app user
RUN addgroup -g 1000 app && \
    adduser -D -u 1000 -G app app

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/producer /app/producer

# Copy data files
COPY --chown=app:app data/ /app/data/

# Create output directory
RUN mkdir -p /app/output && chown -R app:app /app/output

# Switch to app user
USER app

# Expose volume for output
VOLUME ["/app/output"]

# Set default command
ENTRYPOINT ["/app/producer"]
CMD ["-log-level", "info"]
