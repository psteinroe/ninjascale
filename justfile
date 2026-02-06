default:
    @just --list

# Build the binary
build:
    go build -o bin/ninjascale ./cmd/ninjascale

# Run all tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run linter
lint:
    golangci-lint run ./...

# Format code
fmt:
    go fmt ./...
    goimports -w .

# Run with example config in dry-run mode
run-dry:
    go run ./cmd/ninjascale -config example.yaml -dry-run

# Build Docker image
docker-build:
    docker build -t ninjascale .

# Run Docker image
docker-run: docker-build
    docker run --rm ninjascale --help

# Tidy dependencies
tidy:
    go mod tidy

# Run integration tests (requires Docker)
test-integration:
    go test -tags=integration -v ./...
