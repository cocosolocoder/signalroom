package httpapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// bigMessageLen returns how many '<' characters an event needs for its saved
// encoding to exceed the save capacity. The save format escapes '<' as the
// six-byte sequence '<', while a literal '<' is a single request-body
// byte, so the request stays comfortably under the 16 MiB body limit even
// though it saves to over 64 MiB.
func bigMessageLen(t *testing.T, id string, at time.Time) (n int) {
	t.Helper()
	base := events.Event{ID: id, Service: "svc", Severity: "info", At: at}
	lo, hi := 1, maxBodyBytes
	for lo < hi {
		mid := (lo + hi) / 2
		base.Message = strings.Repeat("<", mid)
		payload, err := events.MarshalBatch([]events.Event{base})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if len(payload) > events.MaxBatchPayloadBytes {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	base.Message = strings.Repeat("<", lo)
	payload, err := events.MarshalBatch([]events.Event{base})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(payload) <= events.MaxBatchPayloadBytes {
		t.Fatal("test setup failed to find an oversize length")
	}
	return lo
}

func oversizeBody(id string, at string, n int) string {
	return `{"events":[{"id":"` + id + `","service":"svc","severity":"info","message":"` +
		strings.Repeat("<", n) + `","at":"` + at + `"}]}`
}

// TestPostOversizeBatchIs400NotStorageFailure exercises the reported bug: a
// legal, sub-16-MiB request whose saved content escapes past 64 MiB must be a
// 400 telling the client to send less, never a 503 demanding a restart. The
// batch is rejected wholesale, the store stays usable, and the rejected ids
// remain free.
func TestPostOversizeBatchIs400NotStorageFailure(t *testing.T) {
	server, store, _ := newTestServer(t)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMessageLen(t, "huge", at)
	body := oversizeBody("huge", "2026-10-01T09:00:00Z", n)
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: request body %d must be under the %d body limit", len(body), maxBodyBytes)
	}

	status, out := post(t, server, body)
	if status != http.StatusBadRequest {
		t.Fatalf("oversize batch must be 400, got %d: %v", status, out)
	}
	msg, _ := out["error"].(string)
	if msg == "" {
		t.Fatal("error must be non-empty")
	}
	if !strings.Contains(msg, "save capacity") && !strings.Contains(strings.ToLower(msg), "reduce") {
		t.Fatalf("error must explain the save capacity and ask for less content, got %q", msg)
	}
	if strings.Contains(strings.ToLower(msg), "restart") {
		t.Fatalf("oversize error must not suggest a restart, got %q", msg)
	}

	// Nothing durable was written, the store was not poisoned, and the
	// rejected events are not visible.
	if len(store.appended) != 0 {
		t.Fatalf("oversize batch must not reach storage, appended %v", store.appended)
	}
	if store.Poisoned() {
		t.Fatal("oversize batch must not poison storage")
	}
	if status, _ := get(t, server, ""); status != http.StatusOK {
		t.Fatalf("store must stay available without a restart, got %d", status)
	}

	// The rejected id can be reused immediately by a smaller batch.
	small := `{"events":[{"id":"huge","service":"svc","severity":"info","message":"tiny","at":"2026-10-01T09:00:01Z"}]}`
	if status, out := post(t, server, small); status != http.StatusOK {
		t.Fatalf("rejected id must remain usable, got %d %v", status, out)
	}
	if status, found := get(t, server, ""); status != http.StatusOK || len(found.([]any)) != 1 {
		t.Fatalf("only the small event should be present, got %d %v", status, found)
	}
}

// TestPostOversizeBatchRejectedWholesale ensures no partial save: a large
// event that fits alongside a tiny event in one request is not stored alone.
func TestPostOversizeBatchRejectedWholesale(t *testing.T) {
	server, store, tl := newTestServer(t)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMessageLen(t, "huge", at)

	body := `{"events":[` +
		`{"id":"tiny","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},` +
		`{"id":"huge","service":"svc","severity":"info","message":"` + strings.Repeat("<", n) +
		`","at":"2026-10-01T09:00:01Z"}]}`
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: body %d exceeds body limit", len(body))
	}
	if status, out := post(t, server, body); status != http.StatusBadRequest {
		t.Fatalf("want 400 for the whole batch, got %d %v", status, out)
	}
	if len(store.appended) != 0 {
		t.Fatalf("nothing must be appended, got %v", store.appended)
	}
	if len(tl.Query(events.Query{})) != 0 {
		t.Fatal("the small event must not be saved without the large one")
	}
}

// TestPostReplayExcludedFromCapacity covers mixed batches: only brand-new
// events count toward the save capacity. Replaying an event whose saved form
// is already near the limit costs no capacity, and a batch made entirely of
// such retries returns the ordinary replay result without writing.
func TestPostReplayExcludedFromCapacity(t *testing.T) {
	server, store, tl := newTestServer(t)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	n := bigMessageLen(t, "big", at)
	big := events.Event{ID: "big", Service: "svc", Severity: "info", Message: strings.Repeat("<", n), At: at}

	// Seed the timeline through the recovery path (Load performs no capacity
	// check): the big event is already present as though recovered from
	// durable storage written by an older build. No storage frame is
	// involved.
	if err := tl.Load(big); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A pure replay, however large its saved content, writes nothing and
	// reports the normal replay counts.
	body := oversizeBody("big", "2026-10-01T09:00:00Z", n)
	if len(body) >= maxBodyBytes {
		t.Fatalf("test setup: replay body %d exceeds body limit", len(body))
	}
	status, out := post(t, server, body)
	if status != http.StatusOK {
		t.Fatalf("pure replay must stay 200, got %d %v", status, out)
	}
	if out["created"].(float64) != 0 || out["replayed"].(float64) != 1 {
		t.Fatalf("pure replay counts: %v", out)
	}
	if len(store.appended) != 0 {
		t.Fatalf("pure replay must not append a frame, got %v", store.appended)
	}

	// The same big replay next to a small new event commits only the new
	// event; the replay does not push it over the capacity.
	mixed := `{"events":[` +
		`{"id":"big","service":"svc","severity":"info","message":"` + strings.Repeat("<", n) +
		`","at":"2026-10-01T09:00:00Z"},` +
		`{"id":"fresh","service":"svc","severity":"info","message":"m","at":"2026-10-01T09:00:02Z"}]}`
	if status, out := post(t, server, mixed); status != http.StatusOK {
		t.Fatalf("big replay must not count against a small new event, got %d %v", status, out)
	} else if out["created"].(float64) != 1 || out["replayed"].(float64) != 1 {
		t.Fatalf("mixed counts: %v", out)
	}
	if len(store.appended) != 1 || store.appended[0] != 1 {
		t.Fatalf("exactly one frame holding one new event, got %v", store.appended)
	}
}

// TestPostRealStorageFailureStill503 keeps the two failure modes distinct: a
// genuine write failure still poisons the store and reports 503 with the
// restart guidance, even after an oversize rejection.
func TestPostRealStorageFailureStill503(t *testing.T) {
	server, store, _ := newTestServer(t)
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	// An oversize batch is a benign 400 first.
	n := bigMessageLen(t, "huge", at)
	if status, out := post(t, server, oversizeBody("huge", "2026-10-01T09:00:00Z", n)); status != http.StatusBadRequest {
		t.Fatalf("oversize must be 400, got %d %v", status, out)
	}

	// A subsequent genuine write failure is still 503 and poisons the store.
	store.failNext = true
	status, out := post(t, server, `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("real write failure must be 503, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); !strings.Contains(msg, "restart") {
		t.Fatalf("503 must keep the restart guidance, got %q", msg)
	}
	if status, _ = post(t, server, `{"events":[{"id":"b","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("poisoned store must keep returning 503, got %d", status)
	}
}
