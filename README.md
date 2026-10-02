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

### POST /metrics

Submit a batch of metric samples. The body must be a JSON object containing
exactly a `samples` array. Each sample uses the fields `id`, `service`,
`name`, `at`, and `value`, plus an optional `labels` object; `at` is an
RFC3339Nano timestamp and `value` is a non-negative finite JSON number.
Unknown or duplicate JSON fields are rejected with `400`.

`id`, `service`, and `name` are trimmed of surrounding whitespace, keep their
case, and must be non-empty afterwards. `labels` follows the same trimming
and limit rules as events. The metric id space is independent of the event
id space.

A service, metric name, and the full label set determine one series. The
whole batch is accepted or rejected together, with input validation taking
precedence over conflict checks:

- `200` — `{"created": <n>, "replayed": <m>}`, where `created` counts newly
  stored samples and `replayed` counts retries identical to a stored sample.
  Numeric spelling is irrelevant: `1` and `1.0` are the same content, and the
  same instant in different timezones is identical.
- `400` — invalid JSON, unknown or duplicate fields, an empty array, or a
  sample failing validation. Nothing from the batch is written.
- `409` — an id already stored with different content, an id repeated within
  the batch, or the same series and timestamp presented under a different
  id. Nothing is changed.
- `503` — metric storage has failed; this and later metric requests fail
  until restart. Event and incident endpoints are unaffected.

```bash
curl -sS -X POST localhost:8080/metrics \
  -H 'Content-Type: application/json' \
  -d '{"samples":[{"id":"m1","service":"gateway","name":"cpu","at":"2026-10-01T10:00:00Z","value":42.5,"labels":{"env":"prod"}}]}'
```

Re-sending the identical body returns `{"created":0,"replayed":1}`.

### GET /metrics/aggregate

Return cumulative-counter increments for the series matching the required
`name` filter over a half-open window divided into aligned segments.

- `name` — required, exact match (case-sensitive, no trimming).
- `since`, `until` — required RFC3339Nano instants; `since` must be strictly
  before `until`. The range is half-open: the start is included and the end
  is excluded.
- `step` — required decimal integer from `1` to `86400` seconds.
- `service` — optional, exact match after trimming (same as `GET /events`).
- `label` — optional and repeatable (up to 32), same `name=value` rules as
  `GET /events`.

Segments start at `since` and advance every `step` seconds; the final segment
ends at `until` even when it is shorter than a full step. Only series with at
least one matching sample in the window are returned. Each series lists its
`service`, full `labels` (omitted when the series has none), and `segments`
ordered from the window start. Every segment carries the UTC `since`/`until`
boundaries, `count` of samples in the segment, and `increment`; empty segments
are zero. A query with no matching series returns `[]`.

For each series, adjacent samples in time order contribute their difference
when the value did not decrease, or the current value when it did (a reset).
A first sample with no predecessor contributes no increment; the latest
sample before `since` can serve as the predecessor, so the first in-window
sample still contributes, with its increment attributed to the segment
containing that later sample. Late arrivals are re-bucketed by their own
sample time, so an unchanged dataset always yields the same result. The whole
response is computed from one committed snapshot.

`200` returns the series array. Missing required parameters, duplicated
parameters (except `label`), invalid values, a window needing more than 10000
segments, or a result needing more than 100000 series-segments all return
`400` with a non-empty `error`. If an increment sum cannot be represented as
a finite number, the whole query returns `422`. A poisoned metric store
returns `503`.

```bash
curl -sS 'localhost:8080/metrics/aggregate?name=cpu&since=2026-10-01T10:00:00Z&until=2026-10-01T11:00:00Z&step=60&service=gateway&label=env=prod'
```

### POST /alerts/preview

Preview when each service would cross an event-count threshold and recover
again, straight from the already-ingested events. Nothing is sent, no
incident is created, and no event is modified; the request only reads one
committed snapshot, so a batch ingested concurrently is either counted in
full or not at all. Replaying identical events adds nothing, and a late
arrival is re-bucketed by its own event time on the next preview, so an
unchanged dataset always yields the same result.

The body is a JSON object with exactly these members (unknown or duplicate
members are rejected):

- `since`, `until` — required RFC3339Nano instants. The range is half-open
  (start included, end excluded) and `since` must be strictly before
  `until`.
- `window_seconds` — required integer from `1` to `86400`.
- `threshold` — required integer from `1` to `1000000`.
- `trigger_windows`, `recover_windows` — required integers from `1` to
  `100`: how many consecutive windows must be at or above the threshold to
  open an alert, and how many consecutive windows below it must follow to
  close one.
- `service`, `severity` — optional, using the same trimming and
  case-insensitive rules as `GET /events`.
- `labels` — optional object of string-to-string conditions, following the
  ingestion trimming and limit rules; every condition must match.
- `group_labels` — optional array of at most four label names. Names are
  trimmed with the label-name rules; duplicates after trimming and invalid
  or over-limit names are rejected. Present names are reordered
  lexicographically in the response.

The range is divided from `since` into windows of `window_seconds`; a final
remainder shorter than a full window still participates. Events are counted
per service, or per service plus the values of the `group_labels` (events
missing one of those labels form their own group with `null` for that
value). Groups exist only for combinations that have a matching event in the
range, and every group still lists the complete window set with zero counts
filled in; a range with no matching events returns an empty `groups` array.

Each group starts in the normal state without inheriting anything before
`since`: a run of `trigger_windows` windows at or above `threshold` opens an
alert at the last window's end, and one window below the threshold resets the
run. While alerting, windows at or above the threshold do not open another
alert, and `recover_windows` consecutive windows below it close the alert at
that window's end; a threshold-meeting window in between resets the recovery
run. An alert still open at the range end keeps `recovered_at` as `null` and
is never closed automatically; a later run can open a new alert after a
recovery.

`200` returns the sorted group-label names and groups ordered by service,
then by their label values with `null` before strings; each group carries
its complete window list (`since`, `until`, `count`, end-of-window
`status` of `normal` or `alerting`) and its alerts ordered by trigger time
(`triggered_at`, `recovered_at`). All timestamps are UTC RFC3339Nano.

```bash
curl -sS -X POST localhost:8080/alerts/preview \
  -H 'Content-Type: application/json' \
  -d '{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T11:00:00Z","window_seconds":60,"threshold":5,"trigger_windows":3,"recover_windows":2,"service":"gateway","severity":"critical","labels":{"env":"prod"},"group_labels":["region","version"]}'
```

Missing required members, wrong types or values, unknown/duplicate JSON
members, invalid filters or group-label names, a range needing more than
10000 windows, and a response whose groups would contain more than 100000
group windows in total all return `400` with a non-empty `error` and never a
partial result. A poisoned store returns `503`.

### POST /incidents

Open an incident. The body is a JSON object with exactly the members `id`,
`title`, `service`, and `operator`; unknown or duplicate members are
rejected. String values are trimmed of surrounding whitespace (their case is
kept) and each must be non-empty afterwards.

`200` returns `{"id","status","version"}` with the initial status `open` and
version `1`. Re-sending the same `id` with the same content after
normalization replays the first result (`open`, version `1`), even after the
incident has evolved; the same `id` with different content is `409`.

The `operator` also becomes the incident's sole participant and its owner.
No extra request fields are required; this default is derived from the
creation and is never stored as a separate version or history entry.

```bash
curl -sS -X POST localhost:8080/incidents \
  -H 'Content-Type: application/json' \
  -d '{"id":"INC-1","title":"Checkout outage","service":"checkout","operator":"alice"}'
```

### POST /incidents/{id}/actions

Advance one incident. Every body carries `action_id`, `operator`,
`expected_version` (a positive integer), and `action`, plus exactly the
payload member that action needs. `action_id` must be unique within the
incident. Actions are:

- `add_note` with `content`.
- `link_event` with `event_id`; the event must already be ingested, must
  belong to the incident's service, and must not already be linked. The same
  event may be linked to several incidents. Linking never modifies the event.
- `resolve` with a non-empty `reason`.
- `reopen` with a non-empty `reason`.
- `add_participant` with `participant`: joins a person to the incident.
- `remove_participant` with `participant`: removes a person. The current
  owner cannot be removed.
- `assign_owner` with `participant`: hands ownership to an existing
  participant.

Every `participant` value is trimmed of surrounding whitespace, keeps its
interior case, and must be non-empty afterwards; names differing only in
case are different people. While `open`, notes, links, resolve, and the
three personnel actions are accepted. While `resolved`, only reopen is;
anything else is `409` (including personnel changes). Duplicate joins,
removing someone who is not a participant, removing the current owner,
handing ownership to a non-participant or to the current owner, and any new
action against a resolved incident are `409` carrying the current version.
Resolving and reopening never changes the participants or owner. The
operator of a note or any other action is not added as a participant and is
not rejected for being outside the participant list.

`expected_version` is optimistic concurrency: it must equal the incident's
current committed version. Two different actions submitted at the same
version cannot both succeed; the loser gets `409` with the current version:

```json
{"error":"...","current_version":3}
```

On success `200` returns `{"incident_id","action_id","version"}`, where
`version` is the incident's version after the action (the previous version
plus one). Status, people, links, and history all advance together; a
personnel action adds a history entry whose `content` is the normalized
target name.

A request is idempotent on `(incident id, action id)`. Re-submitting a
successful action with the same normalized content — operator and submitted
`expected_version` included — returns the first result and adds no history or
version, even though that expected version is now stale and even if the
incident has since been handed over, resolved, or the target person removed;
it never restores an older people state. Concurrent identical submissions
produce exactly one record. The same `action_id` with different content is
`409`.

Status codes: `400` for a missing field, wrong type, unknown action,
unknown/extra payload field, or a body carrying more than one action
payload; `404` when the incident (or, for a link, the event) does not exist;
`409` for a stale version, a state-machine violation, a cross-service or
duplicate link, a rejected personnel change, or replayed-but-changed
content. A rejected request never changes the incident.

### GET /incidents/{id}

Return one incident:

```json
{
  "id": "INC-1",
  "title": "Checkout outage",
  "service": "checkout",
  "status": "resolved",
  "version": 4,
  "participants": ["alice", "bob"],
  "owner": "bob",
  "events": [ { "id": "evt-1", "...": "full stored event content" } ],
  "history": [
    {"operator":"alice","action":"create","content":"Checkout outage","version":1,"at":"2026-10-02T08:00:00.123456789Z"},
    {"action_id":"n1","operator":"bob","action":"add_note","content":"investigating","version":2,"at":"..."},
    {"action_id":"l1","operator":"bob","action":"link_event","content":"evt-1","version":3,"at":"..."},
    {"action_id":"r1","operator":"bob","action":"resolve","content":"rolled back","version":4,"at":"..."}
  ]
}
```

`participants` lists the participant names with duplicates removed and
sorted lexicographically by name (case kept); an empty list never occurs
because the owner is always a participant. `owner` is the current owner's
name. The creator is the initial sole participant and owner.

`events` lists every linked event with its full stored content in link order;
an empty list is `[]`. `history` covers the creation and every successful
action in commit order. The creation entry records the operator, action
`create`, the title as its `content`, version `1`, and the server time, and
has no `action_id`; later entries also carry their `action_id`. Personnel
actions appear like any other entry, with `content` set to the normalized
target name. Every `at` is UTC RFC3339Nano. The whole response is one
committed snapshot, so its people, status, version, and history always
belong to the same committed state. An unknown id is `404`.

Incidents are stored in their own append-only `incidents.log` in the same
data directory, under the same durability and recovery rules as
`events.log`: confirmed creates and actions survive a normal restart or a
kill, only an incomplete trailing record is discarded, and any other
corruption prevents startup without touching the existing data. A failed
incident write returns `503` and every later incident request in that process
returns `503` until restart; event endpoints are unaffected, and vice versa.
Personnel changes are ordinary action records, so they share the incident
version and the same fsync/503 semantics: a failed write leaves people,
version, and history exactly where they were.
Old data directories without `incidents.log` work unchanged, and
`POST /alerts/preview` never creates incidents. Logs written by an older
version read unchanged: the creator becomes each incident's initial
participant and owner in memory, and saved actions replay on top, without
adding versions or history, rewriting records, or promoting historical note
operators into participants.

All error responses are JSON objects with a non-empty `error` string, e.g.
`{"error":"event id already exists with different content"}`.

## Test

```bash
go test ./...
go test -race ./...
```
