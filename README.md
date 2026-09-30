# Signalroom

Signalroom is a small, self-hosted observability and incident-response workbench written in Go. The initial baseline accepts structured events, keeps a deterministic in-memory timeline, and offers a product demo suitable for local verification.

## Requirements

- Go 1.26 or newer

## Run

```bash
go run ./cmd/signalroom demo
```

## Test

```bash
go test ./...
```
