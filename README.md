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
`at`, plus an optional `labels` object; `at` is an RFC3339Nano timestamp.
Unknown fields are rejected. String fields are trimmed and `severity` is
lowercased; `id`, `service`, `message` and `at` are required.

`labels` is a JSON object of string to string (for example
`{"env":"prod","region":"cn-north-1","version":"v2"}`). Omitting it, `null`,
and `{}` all mean "no labels". Names and values are trimmed of surrounding
whitespace, keep their case, and must be non-empty afterwards; an event may
carry at most 32 labels, with names up to 64 and values up to 256 Unicode
code points. Non-string values, duplicate JSON keys, names that collide after
trimming, and anything past these limits are rejected with `400`.

Labels are part of the event's content: a retry with the same id must carry
the same labels (order and surrounding whitespace are irrelevant), otherwise
it is a `409` conflict.

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
  -d '{"events":[{"id":"evt-1","service":"gateway","severity":"CRITICAL","message":"error rate spike","at":"2026-10-01T10:00:00.123456789+08:00","labels":{"env":"prod","version":"v2"}}]}'
```

Re-sending the identical body returns `{"created":0,"replayed":1}`.

### GET /events

Return the stored events as a JSON array ordered by time then id; an empty
result is `[]`. Events with labels include the full stored `labels` object;
events without labels omit the field. Optional query parameters:

- `service` — exact match after trimming.
- `severity` — case-insensitive match after trimming.
- `since`, `until` — RFC3339Nano instants, both ends inclusive. A missing end
  is unbounded. An inverted range or an unparsable timestamp yields `400`.
- `label` — repeatable, up to 32 conditions, each `label=name=value`. The
  first `=` splits name from value (later equals signs belong to the value);
  both follow the ingestion trimming and limit rules. Every condition must
  match, events missing the label never match, and comparison is
  case-sensitive. Empty conditions, a missing `=`, invalid names or values,
  and conditions that collide after trimming all yield `400`.

```bash
curl -sS 'localhost:8080/events?service=gateway&severity=critical&label=env=prod&label=version=v2&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z'
```

### GET /events/page

Browse a large result set one page at a time. The first request captures the
committed set of events matching the filters; later requests continue from
that same set, so events written during paging never appear mid-query.

Filter parameters have the same meaning as `GET /events`: `service`,
`severity`, `since`, `until`, and `label`. A request without a cursor starts a
fresh query; a request with one continues the query it names.

- `limit` — page size. Omitted defaults to `100`; only decimal integers from
  `1` to `1000` are accepted. A continuation may change the limit and resumes
  from the same position.
- `cursor` — the opaque token from the previous response. Omit it to start a
  new query. Empty, repeated, unparseable, or tampered cursors yield `400`.

The response is `{"events": [...], "next_cursor": "..."}`. Events are ordered
by time then id and carry their full labels; `next_cursor` is `null` when the
set has no more records, and an empty result is `{"events":[],"next_cursor":null}`.

A continuation without filter parameters reuses the first page's filters. If
any filter parameter is present, the full filter set is checked against the
first page's; a different set yields `400`. Differences that normalize away
(whitespace, severity case, label order, timezone representation) are
considered the same. Cursors survive a normal restart or a process kill in
the same data directory and are rejected by any other directory.

```bash
curl -sS 'localhost:8080/events/page?limit=2'
# {"events":[...],"next_cursor":"eyJ..."}
curl -sS 'localhost:8080/events/page?limit=2&cursor=eyJ...'
```

### GET /events/compare

Compare event volume between a baseline window and an observation window,
broken into aligned segments. Both windows are half-open (the start is
included, the end is excluded), must each have a start strictly before their
end, and must have exactly equal lengths; they may overlap. All four
timestamps are required RFC3339Nano instants and `step` is a required decimal
integer from `1` to `86400` seconds:

- `baseline_since`, `baseline_until` — the baseline window.
- `since`, `until` — the observation window.
- `step` — segment size in seconds.
- `service`, `severity` — optional, using the same trimming and
  case-insensitive rules as `GET /events`; only matching events are counted.
- `label` — optional and repeatable (up to 32), same `name=value` rules as
  `GET /events`; totals and every segment count only events matching all
  conditions.

Segments start at each window's beginning and advance every `step` seconds;
the final segment ends at the window end even when it is shorter than a full
step. For example, a ten-minute window with `step=240` yields three segments
of 4, 4, and 2 minutes. A window needing more than 10000 segments is rejected
with `400` rather than truncated, and empty windows still return the complete
segment set with zero counts.

`200` returns the totals for both sides, their difference
(`observation_total - baseline_total`), the relative change
(`difference / baseline_total`, a JSON number or `null` when the baseline
total is zero), and the segments ordered from each window's start. Every
segment carries the actual UTC `baseline_since`/`baseline_until` and
`since`/`until` boundaries, both counts, and the per-segment difference.
Timestamps in the response always use UTC RFC3339Nano; comparing is by
absolute instant, so the same moment in another time zone gives identical
results, and nanosecond precision is preserved.

Events in an overlap count once per side but in at most one segment on each
side. The whole response is computed from one committed snapshot, so a batch
written during a request is either fully included or fully absent.

```bash
curl -sS 'localhost:8080/events/compare?baseline_since=2026-10-01T09:00:00Z&baseline_until=2026-10-01T09:10:00Z&since=2026-10-01T10:00:00Z&until=2026-10-01T10:10:00Z&step=240'
```

Missing, duplicated, or unparsable parameters, an out-of-range `step`,
inverted or unequal-length windows, and the 10000-segment limit all return
`400` with a non-empty `error`. As with the other endpoints, a poisoned store
returns `503`.

All error responses are JSON objects with a non-empty `error` string, e.g.
`{"error":"event id already exists with different content"}`.

## Test

```bash
go test ./...
go test -race ./...
```
