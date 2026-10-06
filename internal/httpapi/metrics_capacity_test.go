package httpapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// bigMetricSampleLen returns how many '&' characters a sample id needs for
// its saved encoding to exceed the save capacity. The save format escapes
// '&' as the six-byte sequence 'u0026', while a literal '&' is a single
// request-body byte, so the request stays comfortably under the 16 MiB body
// limit even though it saves to over 64 MiB.
func bigMetricSampleLen(t *testing.T, id string, at time.Time) (n int) {
	t.Helper()
	base := metrics.Sample{ID: id, Service: "gw", Name: "requests", At: at, Value: 1}
	lo, hi := 1, maxBodyBytes
	for lo < hi {
		mid := (lo + hi) / 2
		base.ID = id + strings.Repeat("&", mid)
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
	base.ID = id + strings.Repeat("&", lo)
	payload, err := metrics.MarshalBatch([]metrics.Sample{base})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(payload) <= metrics.MaxBatchPayloadBytes {
		t.Fatal("test setup failed to find an oversize length")
	}
	return lo
}

func oversizeMetricsBody(id, at string, n int) string {
	return `{"samples":[{"id":"` + id + strings.Repeat("&", n) +
		`","service":"gw","name":"requests","at":"` + at + `","value":1}]}`
}

func metricWindow(since, until string) string {
	return "?name=requests&service=gw&since=" + since + "&until=" + until + "&step=60"
}

// TestPostMetricsOversizeBatchIs400NotStorageFailure exercises the reported
// bug: a legal, sub-16-MiB request whose saved content escapes past 64 MiB
// must be a 400 telling the client to split or shrink, never a 503 demanding
// a restart. The batch is rejected wholesale, the store stays usable, and the
// rejected id remains free.
func TestPostMetricsOversizeBatchIs400NotStorageFailure(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricSampleLen(t, "huge", at)
	body := oversizeMetricsBody("huge", "2026-10-01T09:00:00Z", n)
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
	if !strings.Contains(msg, "save capacity") ||
		(!strings.Contains(strings.ToLower(msg), "reduce") && !strings.Contains(strings.ToLower(msg), "split")) {
		t.Fatalf("error must name the save capacity and ask to split or reduce, got %q", msg)
	}
	low := strings.ToLower(msg)
	if strings.Contains(low, "restart") {
		t.Fatalf("oversize error must not suggest a restart, got %q", msg)
	}
	if strings.Contains(low, "unavailable") {
		t.Fatalf("oversize error must not claim storage is unavailable, got %q", msg)
	}
	if _, present := out["created"]; present {
		t.Fatalf("rejected batch must not report counts, got %v", out)
	}

	// Nothing durable was written, the store was not poisoned, and the
	// rejected sample is not visible.
	if len(mstore.batches) != 0 {
		t.Fatalf("oversize batch must not reach storage, appended %v", mstore.batches)
	}
	if mstore.Poisoned() {
		t.Fatal("oversize batch must not poison storage")
	}
	if code, series := aggregateGet(t, server, metricWindow("2026-10-01T09:00:00Z", "2026-10-01T09:01:00Z")); code != http.StatusOK || len(series) != 0 {
		t.Fatalf("store must stay queryable without a restart, got %d %v", code, series)
	}

	// The rejected id can be reused immediately by a smaller batch.
	small := `{"samples":[{"id":"huge","service":"gw","name":"requests","at":"2026-10-01T09:00:01Z","value":2}]}`
	if out := postSamples(t, server, small); int(number(out["created"])) != 1 {
		t.Fatalf("rejected id must remain usable: %v", out)
	}
	if count := windowCount(t, server, metricWindow("2026-10-01T09:00:00Z", "2026-10-01T09:02:00Z")); count != 1 {
		t.Fatalf("only the small sample should be present, got %d", count)
	}
}

// TestPostMetricsOversizeBatchRejectedWholesale ensures no partial save: a
// large new sample alongside a tiny new sample is not stored alone.
func TestPostMetricsOversizeBatchRejectedWholesale(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricSampleLen(t, "huge", at)

	body := `{"samples":[` +
		`{"id":"tiny","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1},` +
		`{"id":"huge` + strings.Repeat("&", n) + `","service":"gw","name":"requests","at":"2026-10-01T09:00:01Z","value":2}]}`
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: body %d exceeds body limit", len(body))
	}
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", body); status != http.StatusBadRequest {
		t.Fatalf("want 400 for the whole batch, got %d %v", status, out)
	}
	if len(mstore.batches) != 0 {
		t.Fatalf("nothing must be appended, got %d batches", len(mstore.batches))
	}
	if code, series := aggregateGet(t, server, metricWindow("2026-10-01T09:00:00Z", "2026-10-01T09:02:00Z")); code != http.StatusOK || len(series) != 0 {
		t.Fatalf("the small sample must not be saved without the large one, got %d %v", code, series)
	}
}

// TestPostMetricReplayExcludedFromCapacity covers mixed batches: only
// brand-new samples count toward the save capacity. Replaying a sample whose
// saved form is already past the limit costs no capacity, and a batch made
// entirely of such retries returns the ordinary created-zero result without
// writing.
func TestPostMetricReplayExcludedFromCapacity(t *testing.T) {
	store := metrics.NewStore()
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, store, mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMetricSampleLen(t, "big", at)
	big := metrics.Sample{
		ID: "big" + strings.Repeat("&", n), Service: "gw", Name: "requests", At: at, Value: 1,
	}

	// Seed the store through the recovery path (Load performs no capacity
	// check): the big sample is already present as though recovered from
	// durable storage written by an older build. No storage frame is
	// involved.
	if err := store.Load(big); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A pure replay, however large its saved content, writes nothing and
	// reports the normal replay counts.
	body := oversizeMetricsBody("big", "2026-10-01T09:00:00Z", n)
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
		t.Fatalf("pure replay must not append a frame, got %d batches", len(mstore.batches))
	}

	// The same big replay next to a small new sample commits only the new
	// sample; the replay does not push it over the capacity.
	mixed := `{"samples":[` +
		`{"id":"big` + strings.Repeat("&", n) + `","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1},` +
		`{"id":"fresh","service":"gw","name":"requests","at":"2026-10-01T09:00:02Z","value":2}]}`
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics", mixed); status != http.StatusOK {
		t.Fatalf("big replay must not count against a small new sample, got %d %v", status, out)
	} else if int(number(out["created"])) != 1 || int(number(out["replayed"])) != 1 {
		t.Fatalf("mixed counts: %v", out)
	}
	if len(mstore.batches) != 1 || len(mstore.batches[0]) != 1 || mstore.batches[0][0].ID != "fresh" {
		t.Fatalf("exactly one frame holding one new sample, got %v", mstore.batches)
	}
	if count := windowCount(t, server, metricWindow("2026-10-01T09:00:00Z", "2026-10-01T09:03:00Z")); count != 2 {
		t.Fatalf("seeded point plus one new point, got %d", count)
	}
}

// TestPostMetricsRealStorageFailureStill503 keeps the two failure modes
// distinct: a genuine metric write failure still poisons the store and
// reports 503 with the restart guidance, even after an oversize rejection.
func TestPostMetricsRealStorageFailureStill503(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, metrics.NewStore(), mstore)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	// An oversize batch is a benign 400 first.
	n := bigMetricSampleLen(t, "huge", at)
	if status, out := metricsDo(t, server, http.MethodPost, "/metrics",
		oversizeMetricsBody("huge", "2026-10-01T09:00:00Z", n)); status != http.StatusBadRequest {
		t.Fatalf("oversize must be 400, got %d %v", status, out)
	}

	// A subsequent genuine write failure is still 503 and poisons the store.
	mstore.failNext = true
	status, out := metricsDo(t, server, http.MethodPost, "/metrics",
		`{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T09:00:00Z","value":1}]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("real write failure must be 503, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); !strings.Contains(msg, "restart") {
		t.Fatalf("503 must keep the restart guidance, got %q", msg)
	}
	if status, _ = metricsDo(t, server, http.MethodPost, "/metrics",
		`{"samples":[{"id":"b","service":"gw","name":"requests","at":"2026-10-01T09:01:00Z","value":2}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("poisoned store must keep returning 503, got %d", status)
	}
}
