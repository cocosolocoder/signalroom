package httpapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// bigMetricsNameLen returns how many '<' characters a sample needs in its
// metric name for its saved encoding to exceed the save capacity. The save
// format escapes '<' as a six-byte sequence, while a literal '<' is a single
// request-body byte, so the request stays comfortably under the 16 MiB body
// limit even though it saves to over 64 MiB.
func bigMetricsNameLen(t *testing.T, id string, at time.Time) int {
	t.Helper()
	base := metrics.Sample{ID: id, Service: "gw", At: at, Value: 1}
	lo, hi := 1, maxBodyBytes
	for lo < hi {
		mid := (lo + hi) / 2
		base.Name = strings.Repeat("<", mid)
		payload, err := metrics.MarshalBatch([]metrics.Sample{base})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if len(payload) > metrics.MaxBatchPayloadBytes {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	base.Name = strings.Repeat("<", lo)
	payload, err := metrics.MarshalBatch([]metrics.Sample{base})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(payload) <= metrics.MaxBatchPayloadBytes {
		t.Fatal("test setup failed to find an oversize name length")
	}
	return lo
}

// oversizeNameBody builds a one-sample body whose expanding '<' run rides in
// the metric name, keeping the id fixed and reusable later.
func oversizeNameBody(id, at string, n int) string {
	return `{"samples":[{"id":"` + id + `","service":"gw","name":"` +
		strings.Repeat("<", n) + `","at":"` + at + `","value":1}]}`
}

// bigMetricsIDLen returns an '<' run length whose one-sample saved encoding
// is about 62 MiB: large enough to dominate any batch it joins, yet small
// enough to fit the 64 MiB capacity itself and to arrive inside the 16 MiB
// request body limit on replay.
func bigMetricsIDLen(t *testing.T, at time.Time) int {
	t.Helper()
	n := (62 << 20) / 6
	id := "big-" + strings.Repeat("<", n)
	payload, err := metrics.MarshalBatch([]metrics.Sample{
		{ID: id, Service: "gw", Name: "requests", At: at, Value: 1},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(payload) >= metrics.MaxBatchPayloadBytes {
		t.Fatalf("test setup: seed payload %d must fit", len(payload))
	}
	return n
}

func oversizeIDBody(id, at string) string {
	return `{"samples":[{"id":"` + id + `","service":"gw","name":"requests","at":"` + at + `","value":1}]}`
}

// TestPostMetricsOversizeIs400NotStorageFailure exercises the reported bug:
// a legal, sub-16-MiB request whose saved content escapes past 64 MiB must be
// a 400 telling the client to send less, never a 503 demanding a restart.
// The batch is rejected wholesale, storage stays usable, and the rejected id
// remains free.
func TestPostMetricsOversizeIs400NotStorageFailure(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricsNameLen(t, "huge", at)
	body := oversizeNameBody("huge", "2026-10-01T09:00:00Z", n)
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: request body %d must be under the %d body limit", len(body), maxBodyBytes)
	}

	status, out := metricsDo(t, server, http.MethodPost, "/metrics", body)
	if status != http.StatusBadRequest {
		t.Fatalf("oversize batch must be 400, got %d: %v", status, out)
	}
	msg, _ := out["error"].(string)
	if msg == "" {
		t.Fatal("error must be non-empty")
	}
	lower := strings.ToLower(msg)
	if !strings.Contains(msg, "save capacity") && !strings.Contains(lower, "split") && !strings.Contains(lower, "reduce") {
		t.Fatalf("error must explain the save capacity and ask to split or reduce content, got %q", msg)
	}
	if strings.Contains(lower, "restart") || strings.Contains(lower, "unavailable") {
		t.Fatalf("oversize error must not suggest a storage failure or restart, got %q", msg)
	}
	if _, present := out["created"]; present {
		t.Fatalf("rejected response must not carry created counts: %v", out)
	}

	// Nothing durable was written, and storage was not poisoned.
	if len(mstore.batches) != 0 {
		t.Fatalf("oversize batch must not reach storage, appended %v", mstore.batches)
	}
	if mstore.Poisoned() {
		t.Fatal("oversize batch must not poison storage")
	}

	// Queries and submissions keep working without a restart.
	if status, series := aggregateGet(t, server,
		"?name=requests&service=gw&since=2026-10-01T09:00:00Z&until=2026-10-01T10:00:00Z&step=60"); status != http.StatusOK || len(series) != 0 {
		t.Fatalf("aggregate must stay available and empty, got %d %v", status, series)
	}
	small := `{"samples":[{"id":"ok","service":"gw","name":"requests","at":"2026-10-01T09:01:00Z","value":1}]}`
	if o := postSamples(t, server, small); int(number(o["created"])) != 1 {
		t.Fatalf("normal batch after rejection must commit: %v", o)
	}

	// The rejected id can be reused immediately by a smaller batch.
	reuse := `{"samples":[{"id":"huge","service":"gw","name":"requests","at":"2026-10-01T09:02:00Z","value":2}]}`
	if o := postSamples(t, server, reuse); int(number(o["created"])) != 1 {
		t.Fatalf("rejected id must remain usable: %v", o)
	}
	if count := windowCount(t, server,
		"?name=requests&service=gw&since=2026-10-01T09:00:00Z&until=2026-10-01T09:03:00Z&step=60"); count != 2 {
		t.Fatalf("only the two small samples should be present, got %d", count)
	}
}

// TestPostMetricsOversizeRejectedWholesale ensures no partial save: an
// oversize sample joined to a small sample in one request stores neither.
func TestPostMetricsOversizeRejectedWholesale(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricsNameLen(t, "huge", at)

	body := `{"samples":[` +
		`{"id":"tiny","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1},` +
		`{"id":"huge","service":"gw","name":"` + strings.Repeat("<", n) +
		`","at":"2026-10-01T09:00:01Z","value":2}]}`
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: body %d exceeds body limit", len(body))
	}
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", body); status != http.StatusBadRequest {
		t.Fatalf("want 400 for the whole batch, got %d %v", status, out)
	}
	if len(mstore.batches) != 0 {
		t.Fatalf("nothing must be appended, got %d batches", len(mstore.batches))
	}
	if status, series := aggregateGet(t, server,
		"?name=requests&service=gw&since=2026-10-01T09:00:00Z&until=2026-10-01T09:01:00Z&step=60"); status != http.StatusOK || len(series) != 0 {
		t.Fatalf("the small sample must not be saved without the oversize one, got %d %v", status, series)
	}

	// Both ids remain free in a later, smaller batch.
	follow := `{"samples":[
		{"id":"tiny","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1},
		{"id":"huge","service":"gw","name":"requests","at":"2026-10-01T09:00:01Z","value":2}
	]}`
	if o := postSamples(t, server, follow); int(number(o["created"])) != 2 {
		t.Fatalf("rejected ids must remain usable: %v", o)
	}
}

// TestPostMetricsReplayExcludedFromCapacity covers mixed batches: only
// brand-new samples count toward the save capacity. Replaying a sample whose
// saved form is already near the limit costs no capacity, and a batch made
// entirely of such retries returns the ordinary replay result without
// writing.
func TestPostMetricsReplayExcludedFromCapacity(t *testing.T) {
	store := metrics.NewStore()
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, store, mstore)
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricsIDLen(t, base)
	bigID := "big-" + strings.Repeat("<", n)
	big := metrics.Sample{ID: bigID, Service: "gw", Name: "requests", At: base, Value: 1}

	// Seed through recovery (Load performs no capacity check), as though the
	// big sample came from durable storage written by an older build.
	if err := store.Load(big); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A pure replay, however large its saved content, writes nothing and
	// reports the normal replay counts.
	body := oversizeIDBody(bigID, "2026-10-01T09:00:00Z")
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: replay body %d exceeds body limit", len(body))
	}
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", body)
	if status != http.StatusOK {
		t.Fatalf("pure replay must stay 200, got %d %v", status, out)
	}
	if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 1 {
		t.Fatalf("pure replay counts: %v", out)
	}
	if len(mstore.batches) != 0 {
		t.Fatalf("pure replay must not append a frame, got %d", len(mstore.batches))
	}

	// The same big replay next to a small new sample commits only the new
	// sample; the replay does not push it over the capacity.
	mixed := `{"samples":[` +
		`{"id":"` + bigID + `","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1},` +
		`{"id":"fresh","service":"gw","name":"requests","at":"2026-10-01T09:00:02Z","value":3}]}`
	if len(mixed) >= maxBodyBytes {
		t.Fatalf("test setup: mixed body %d exceeds body limit", len(mixed))
	}
	status, out = metricsDo(t, server, http.MethodPost, "/metrics", mixed)
	if status != http.StatusOK {
		t.Fatalf("big replay must not count against a small new sample, got %d %v", status, out)
	}
	if int(number(out["created"])) != 1 || int(number(out["replayed"])) != 1 {
		t.Fatalf("mixed counts: %v", out)
	}
	if len(mstore.batches) != 1 || len(mstore.batches[0]) != 1 || mstore.batches[0][0].ID != "fresh" {
		t.Fatalf("exactly one frame holding the one new sample, got %+v", mstore.batches)
	}
	if count := windowCount(t, server,
		"?name=requests&service=gw&since=2026-10-01T09:00:00Z&until=2026-10-01T09:00:03Z&step=60"); count != 2 {
		t.Fatalf("big seed plus one new sample, got %d", count)
	}
}

// TestPostMetricsCapacityPrecedence keeps validation and conflict checks
// ahead of the capacity check: an invalid sample or any conflict makes the
// batch 400/409 even when another sample is oversized.
func TestPostMetricsCapacityPrecedence(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricsNameLen(t, "huge", at)

	// Seed one stored sample.
	postSamples(t, server, `{"samples":[{"id":"dup","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1}]}`)

	// Invalid value plus an oversize sample: validation (400) wins.
	invalid := `{"samples":[` +
		`{"id":"bad","service":"gw","name":"requests","at":"2026-10-01T09:01:00Z","value":-1},` +
		`{"id":"huge1","service":"gw","name":"` + strings.Repeat("<", n) + `","at":"2026-10-01T09:02:00Z","value":1}]}`
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", invalid); status != http.StatusBadRequest {
		t.Fatalf("validation must precede capacity, got %d %v", status, out)
	}

	// Stored id with changed content plus an oversize sample: conflict (409).
	conflict := `{"samples":[` +
		`{"id":"dup","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":9},` +
		`{"id":"huge2","service":"gw","name":"` + strings.Repeat("<", n) + `","at":"2026-10-01T09:03:00Z","value":1}]}`
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", conflict); status != http.StatusConflict {
		t.Fatalf("conflict must precede capacity, got %d %v", status, out)
	}

	// A different id claiming the same series/instant as the stored sample,
	// joined to an oversize sample, is still a conflict.
	clash := `{"samples":[` +
		`{"id":"intruder","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":2},` +
		`{"id":"huge3","service":"gw","name":"` + strings.Repeat("<", n) + `","at":"2026-10-01T09:04:00Z","value":1}]}`
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", clash); status != http.StatusConflict {
		t.Fatalf("series/time clash must precede capacity, got %d %v", status, out)
	}

	// In-batch repeated id plus oversize content: conflict wins.
	inBatch := `{"samples":[` +
		`{"id":"twice","service":"gw","name":"requests","at":"2026-10-01T09:05:00Z","value":1},` +
		`{"id":"twice","service":"gw","name":"requests","at":"2026-10-01T09:06:00Z","value":2},` +
		`{"id":"huge4","service":"gw","name":"` + strings.Repeat("<", n) + `","at":"2026-10-01T09:07:00Z","value":1}]}`
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", inBatch); status != http.StatusConflict {
		t.Fatalf("in-batch duplicate id must precede capacity, got %d %v", status, out)
	}

	// Only the seeded batch was ever written.
	if len(mstore.batches) != 1 {
		t.Fatalf("precedence rejections must not persist, appended %d batches", len(mstore.batches))
	}
	if count := windowCount(t, server,
		"?name=requests&service=gw&since=2026-10-01T09:00:00Z&until=2026-10-01T09:08:00Z&step=60"); count != 1 {
		t.Fatalf("stored data must be unchanged, got %d", count)
	}
}

// TestPostMetricsRealStorageFailureStill503 keeps the two failure modes
// distinct: a genuine write failure still poisons metric storage and reports
// 503 with the restart guidance, even after an oversize rejection.
func TestPostMetricsRealStorageFailureStill503(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	// An oversize batch is a benign 400 first.
	n := bigMetricsNameLen(t, "huge", at)
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", oversizeNameBody("huge", "2026-10-01T09:00:00Z", n)); status != http.StatusBadRequest {
		t.Fatalf("oversize must be 400, got %d %v", status, out)
	}

	// A subsequent genuine write failure is still 503 and poisons storage.
	mstore.failNext = true
	status, out := metricsDo(t, server, http.MethodPost, "/metrics",
		`{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("real write failure must be 503, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); !strings.Contains(msg, "restart") {
		t.Fatalf("503 must keep the restart guidance, got %q", msg)
	}
	if status, _ = metricsDo(t, server, http.MethodPost, "/metrics",
		`{"samples":[{"id":"b","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":1}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("poisoned metric storage must keep returning 503, got %d", status)
	}
	if status, _ = metricsDo(t, server, http.MethodGet,
		"/metrics/aggregate?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T11:00:00Z&step=60", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("aggregate must stay 503 after a real failure, got %d", status)
	}
}
