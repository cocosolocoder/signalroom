# Signalroom

Signalroom is a small, self-hosted observability and incident-response workbench written in Go. It accepts structured events, keeps a deterministic timeline, persists accepted batches to disk, and offers a product demo suitable for local verification. No external services are required.

## Requirements

- Go 1.26 or newer

## Demo

```bash
go run ./cmd/signalroom demo
```

## HTTP server

```bash
go run ./cmd/signalroom serve --addr 127.0.0.1:8080 --data ./data
```

- `--addr` is the listen address (default `:8080`).
- `--data` is the directory used for durable event data. It is created if
  missing. Only one running `serve` instance may use a given data directory;
  a second instance exits with a non-zero status and leaves the data
  untouched. Restarting after a normal exit or a killed process is allowed.

Accepted batches are fsynced before the success response, so every confirmed
event survives a restart or a process crash. Only an incomplete final write
(torn tail after a crash) is discarded; any other corruption makes startup
fail with a non-zero status without modifying the data.

### POST /events

Submit a batch. The body must be a JSON object containing exactly an `events`
array. Each event uses the fields `id`, `service`, `severity`, `message` and
`at`; `at` is an RFC3339Nano timestamp. Unknown fields are rejected. String
fields are trimmed and `severity` is lowercased; `id`, `service`, `message`
and `at` are required.

The whole batch is accepted or rejected together, with input validation
taking precedence over conflict checks:

- `200` — `{"created": <n>, "replayed": <m>}`, where `created` counts newly
  stored events and `replayed` counts retries identical to a stored event.
- `400` — invalid JSON, unknown fields, an empty array, or an event failing
  validation. Nothing from the batch is written.
- `409` — an id already stored with different content, or an id repeated
  within the batch. Nothing is changed.
- `503` — storage has failed; this and later requests fail until restart.

Time equality uses absolute instants, so the same moment in different
timezone offsets is treated as identical.

```bash
curl -sS -X POST localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"id":"evt-1","service":"gateway","severity":"CRITICAL","message":"error rate spike","at":"2026-10-01T10:00:00.123456789+08:00"}]}'
```

Re-sending the identical body returns `{"created":0,"replayed":1}`.

### GET /events

Return the stored events as a JSON array ordered by time then id; an empty
result is `[]`. Optional query parameters:

- `service` — exact match after trimming.
- `severity` — case-insensitive match after trimming.
- `since`, `until` — RFC3339Nano instants, both ends inclusive. A missing end
  is unbounded. An inverted range or an unparsable timestamp yields `400`.

```bash
curl -sS 'localhost:8080/events?service=gateway&severity=critical&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z'
```

All error responses are JSON objects with a non-empty `error` string, e.g.
`{"error":"event id already exists with different content"}`.

### GET /events/compare

Compare event counts across two equal-length windows. Required query
parameters:

- `baseline_since`, `baseline_until` — the baseline window.
- `since`, `until` — the observation window.
- `step` — segment width as a decimal integer of seconds from 1 to 86400.

All four instants are RFC3339Nano timestamps; each window must start before
it ends and both windows must have the same length. Windows and segments
contain their start and exclude their end, and segments are aligned from each
window's start, with the final partial segment ending at the window end. The
windows may overlap; an event in the overlap counts on both sides but only
once per side. Time equality uses absolute instants, and all output times are
UTC RFC3339Nano.

The `service` and `severity` filters follow the same rules as `GET /events`;
only filtered events are counted. A window requiring more than 10000 segments
is rejected with `400`.

```bash
curl -sS 'localhost:8080/events/compare?baseline_since=2026-10-01T10:00:00Z&baseline_until=2026-10-01T10:10:00Z&since=2026-10-01T10:10:00Z&until=2026-10-01T10:20:00Z&step=240'
```

```json
{
  "baseline_total": 2,
  "observation_total": 2,
  "difference": 0,
  "ratio": 0,
  "segments": [
    {"baseline_since": "2026-10-01T10:00:00Z", "baseline_until": "2026-10-01T10:04:00Z", "since": "2026-10-01T10:10:00Z", "until": "2026-10-01T10:14:00Z", "baseline_count": 2, "observation_count": 1, "difference": -1},
    {"baseline_since": "2026-10-01T10:04:00Z", "baseline_until": "2026-10-01T10:08:00Z", "since": "2026-10-01T10:14:00Z", "until": "2026-10-01T10:18:00Z", "baseline_count": 0, "observation_count": 0, "difference": 0},
    {"baseline_since": "2026-10-01T10:08:00Z", "baseline_until": "2026-10-01T10:10:00Z", "since": "2026-10-01T10:18:00Z", "until": "2026-10-01T10:20:00Z", "baseline_count": 0, "observation_count": 1, "difference": 1}
  ]
}
```

`ratio` is the observation-minus-baseline difference divided by the baseline
total, as a number (not a percentage string); it is `null` when the baseline
total is zero. Empty windows still return the full segment list with zero
counts.

## Test

```bash
go test ./...
go test -race ./...
```
