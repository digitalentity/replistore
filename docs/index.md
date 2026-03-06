# RepliStore Documentation

Welcome to the RepliStore documentation. This project provides a distributed, FUSE-based replicated storage system written in Go.

## Contents

- [**Architecture Overview**](architecture.md) - High-level system design and component interaction.
- [**Components**](components/)
    - [FUSE Frontend](components/fuse.md) - Translating OS syscalls to VFS operations.
    - [VFS and Metadata Cache](components/vfs.md) - Unified namespace and metadata tracking.
    - [SMB Backend](components/backend.md) - Connecting to remote shares.
    - [Repair Manager](components/repair.md) - Ensuring data durability through background repair.
- [**Operational Flows**](flows/)
    - [Read Flow](flows/read.md) - How reads work with automatic failover.
    - [Write Flow](flows/write.md) - Parallel replication of data across backends.
    - [Startup & Warmup](flows/startup.md) - Building the initial metadata cache.
- [**Configuration Guide**](configuration.md) - Detailed description of `config.yaml` options.
- [**Testing Guide**](testing.md) - How to run and extend the test suite.

## Project Structure

```text
replistore/
├── cmd/
│   └── replistore/       # Application entry point
├── internal/
│   ├── backend/          # SMB connectivity and health monitoring
│   ├── config/           # Configuration loading and env expansion
│   ├── fuse/             # FUSE frontend implementation
│   ├── test/             # Mocks for testing
│   └── vfs/              # Virtual File System and Metadata Cache
└── docs/                 # Project documentation (you are here)
```

## Getting Started

To get started with RepliStore, please refer to the [README.md](../README.md) in the project root for installation and basic usage instructions.
