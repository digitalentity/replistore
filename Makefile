# Makefile for RepliStore

.PHONY: all build test clean deb release lint fmt install-hook

BINARY_NAME=replistore
BUILD_DIR=build
VERSION ?= $(shell ./scripts/get_version.sh)
export VERSION
ARCH=amd64

all: install-hook lint test build

install-hook:
	@if [ -d .git ] && [ ! -L .git/hooks/pre-commit ] && [ ! -f .git/hooks/pre-commit ]; then \
		echo "Installing git pre-commit hook (symlink)..."; \
		ln -sf ../../scripts/git-pre-commit.sh .git/hooks/pre-commit; \
	fi

build: install-hook
	@echo "Building binary..."
	@mkdir -p $(BUILD_DIR)
	go build -ldflags "-X main.Version=$(VERSION)" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd

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

