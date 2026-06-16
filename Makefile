# Makefile for RepliStore

.PHONY: all build test clean deb release lint fmt

BINARY_NAME=replistore
BUILD_DIR=build
VERSION=1.0.0
ARCH=amd64

all: lint test build

build:
	@echo "Building binary..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/replistore/...

test:
	@echo "Running tests..."
	go test -v -race ./...

clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)

deb: build
	@echo "Creating Debian package..."
	@chmod +x scripts/build_deb.sh
	./scripts/build_deb.sh

release: clean test deb
	@echo "Release packaging complete."

lint:
	@echo "Running go vet..."
	go vet ./...
	@echo "Running golangci-lint..."
	golangci-lint run ./...

fmt:
	@echo "Formatting source code..."
	gofmt -s -w .

