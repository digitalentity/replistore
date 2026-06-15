# Makefile for RepliStore

.PHONY: all build test clean deb release lint

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
	@echo "Running linters..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found, falling back to go vet and gofmt..."; \
		go vet ./...; \
		FMTOUT=$$(gofmt -s -l . 2>&1); \
		if [ -n "$$FMTOUT" ]; then \
			echo "gofmt found formatting issues in the following files:"; \
			echo "$$FMTOUT"; \
			exit 1; \
		fi; \
		echo "All fallback checks passed."; \
	fi

