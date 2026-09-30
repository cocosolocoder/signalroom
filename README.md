# Signalroom

Signalroom is a small, self-hosted observability and incident-response workbench written in Go. It accepts structured events, keeps a deterministic timeline, and offers a product demo suitable for local verification.

## Requirements

- Go 1.26 or newer

## Run

### Demo

The original in-memory demo is unchanged:

```bash
go run ./cmd/signalroom demo
```

### HTTP ingestion server

Start the persistent HTTP server with a listen address and a data directory:

```bash
go run ./cmd/signalroom serve --addr :8080 --data ./data
```

The server prints `signalroom listening on <addr>` and serves the API. Stop it with `SIGINT` or `SIGTERM` for a graceful shutdown; data already confirmed is durable. On restart the server recovers all confirmed events and continues.

Only one instance may use a given data directory at a time. A second instance started while the first is running fails immediately without modifying the data.

## HTTP API

All request and response bodies are JSON. Every error response is a JSON object containing a non-empty `error` string.

### POST /events

Accepts a batch of events. The body must be a JSON object containing only an `events` array:

```json
{
  "events": [
    {
      "id": "evt-001",
      "service": "gateway",
      "severity": "critical",
      "message": "错误率超过阈值",
      "at": "2026-10-01T10:00:00.123456789Z"
    }
  ]
}
```

Each event requires `id`, `service`, `severity`, `message`, and `at`. String fields are trimmed, `severity` is lower-cased, and `at` is an RFC3339Nano timestamp. Unknown fields, an empty array, missing fields, or invalid timestamps are rejected with `400` before any conflict check, and the whole batch is not written.

Response `200` with integer counts:

```json
{"created": 1, "replayed": 0}
```

- `created` — events newly stored.
- `replayed` — events identical to ones already stored (same normalized fields and the same absolute timestamp). Replays are idempotent and are not written again.

An event whose `id` matches an existing event but whose content differs is rejected with `409`; duplicate ids within a normalized batch also return `409`. No data changes.

The batch is accepted or rejected as a whole. Concurrent submissions of the same batch all succeed and store the event once; concurrent conflicting submissions allow only one version to win.

### GET /events

Returns stored events as a JSON array, sorted by absolute time ascending then `id` ascending. An empty result is `[]`.

Optional query parameters:

- `service` — exact service match.
- `severity` — exact severity match (case-insensitive).
- `since` — RFC3339Nano timestamp; inclusive lower bound.
- `until` — RFC3339Nano timestamp; inclusive upper bound.

An inverted range (`since` after `until`) or an unparsable timestamp returns `400`.

```bash
curl 'http://localhost:8080/events?service=gateway&severity=critical&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z'
```

Response:

```json
[
  {
    "id": "evt-001",
    "service": "gateway",
    "severity": "critical",
    "message": "错误率超过阈值",
    "at": "2026-10-01T10:00:00.123456789Z"
  }
]
```

## Persistence

Events are stored in an append-only framed log (`events.log`) in the data directory. Each frame carries a header CRC and a payload CRC.

- A successful response is returned only after the frame has been written and `fsync`ed.
- On restart, complete frames are recovered. A frame that was fully written but not yet confirmed is recovered and de-duplicated on retry.
- An incomplete frame at the end of the log (left by a crashed write) is ignored and truncated; it never pollutes subsequent writes.
- Corruption in the middle of the log, or a complete but illegal frame, causes startup to fail with a non-zero status without modifying the data.
- If a write or sync fails, the server marks itself broken and returns `503` for every request until restart.

## Test

```bash
go test ./...
```

The tests are deterministic and use in-process HTTP servers and temporary data directories; no external services or fixed waits are required.
