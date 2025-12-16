.PHONY: build run clean test deps help

# Default target
.DEFAULT_GOAL := help

# Build the application
build:
	@echo "Building..."
	@go build -o producer -ldflags="-s -w" ./cmd/producer
	@echo "Build complete: ./producer"

# Run the application
run: build
	@echo "Running producer..."
	@./producer

# Run with custom config
run-config: build
	@echo "Running producer with custom config..."
	@./producer -config $(CONFIG)

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f producer
	@rm -rf output/
	@echo "Clean complete"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies ready"

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Format complete"

# Lint code
lint:
	@echo "Linting code..."
	@golangci-lint run ./...

# Show help
help:
	@echo "Available targets:"
	@echo "  build       - Build the application"
	@echo "  run         - Build and run with default config"
	@echo "  run-config  - Build and run with custom config (CONFIG=path)"
	@echo "  clean       - Remove build artifacts and output"
	@echo "  deps        - Download and tidy dependencies"
	@echo "  test        - Run tests"
	@echo "  bench       - Run benchmarks"
	@echo "  fmt         - Format code"
	@echo "  lint        - Lint code (requires golangci-lint)"
	@echo "  help        - Show this help message"
