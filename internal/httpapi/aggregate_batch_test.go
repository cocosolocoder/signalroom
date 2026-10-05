package httpapi

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/metrics"
)

// HTTP-level regression coverage for the cumulative-aggregation snapshot
// guarantee. These tests pin the same discriminating scenario as the
// metrics-package tests through the real POST /metrics and
// GET /metrics/aggregate endpoints, preserving the existing request and
// response formats:
//
//	one metric name "requests" in service "svc", window
//	[2026-10-01T10:00:00Z, 10:03:00Z) with three one-minute segments.
//	Series a=1 and a=2 exist with one in-window point each; one batch
//	supplements both, creates previously absent series a=3, and carries late
//	historical samples (one earlier than a=1's stored predecessor, one that
//	becomes a=2's first predecessor), plus a point per existing series exactly
//	at the query end, which is excluded. The batch is sent shuffled, so
//	predecessor and delta correctness must follow sample time, never body
//	order. Pre- and post-batch grand totals both equal 100 while every
//	per-series/per-segment vector changes, so only a whole-state comparison
//	can catch a splice.

const metricScenarioQuery = "?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:03:00Z&step=60"

const metricScenarioSeed = `{"samples":[
	{"id":"seed-pre-1","service":"svc","name":"requests","at":"2026-10-01T09:58:00Z","value":100,"labels":{"a":"1"}},
	{"id":"seed-a1","service":"svc","name":"requests","at":"2026-10-01T10:00:30Z","value":200,"labels":{"a":"1"}},
	{"id":"seed-a2","service":"svc","name":"requests","at":"2026-10-01T10:00:30Z","value":200,"labels":{"a":"2"}}
]}`

// metricScenarioBatch is the whole batch under test, deliberately shuffled.
const metricScenarioBatch = `{"samples":[
	{"id":"new-end-1","service":"svc","name":"requests","at":"2026-10-01T10:03:00Z","value":999,"labels":{"a":"1"}},
	{"id":"new-a3-1","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":50,"labels":{"a":"3"}},
	{"id":"new-nearpre-1","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":199,"labels":{"a":"1"}},
	{"id":"new-a1-2","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":201,"labels":{"a":"1"}},
	{"id":"new-farpre-2","service":"svc","name":"requests","at":"2026-10-01T09:58:00Z","value":20,"labels":{"a":"2"}},
	{"id":"new-a2-1","service":"svc","name":"requests","at":"2026-10-01T10:02:30Z","value":223,"labels":{"a":"2"}},
	{"id":"new-farpre-1","service":"svc","name":"requests","at":"2026-10-01T09:57:00Z","value":90,"labels":{"a":"1"}},
	{"id":"new-nearpre-2","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":180,"labels":{"a":"2"}},
	{"id":"new-a3-0","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":5,"labels":{"a":"3"}},
	{"id":"new-end-2","service":"svc","name":"requests","at":"2026-10-01T10:03:00Z","value":999,"labels":{"a":"2"}},
	{"id":"new-a3-2","service":"svc","name":"requests","at":"2026-10-01T10:02:30Z","value":60,"labels":{"a":"3"}}
]}`

// metricScenarioConflictBatch appends a brand-new id that claims the stored
// (series a=1, 10:00:30) point of seed-a1: the whole submission must be 409.
const metricScenarioConflictBatch = `{"samples":[
	{"id":"new-end-1","service":"svc","name":"requests","at":"2026-10-01T10:03:00Z","value":999,"labels":{"a":"1"}},
	{"id":"new-a3-1","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":50,"labels":{"a":"3"}},
	{"id":"new-nearpre-1","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":199,"labels":{"a":"1"}},
	{"id":"new-a1-2","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":201,"labels":{"a":"1"}},
	{"id":"new-farpre-2","service":"svc","name":"requests","at":"2026-10-01T09:58:00Z","value":20,"labels":{"a":"2"}},
	{"id":"new-a2-1","service":"svc","name":"requests","at":"2026-10-01T10:02:30Z","value":223,"labels":{"a":"2"}},
	{"id":"new-farpre-1","service":"svc","name":"requests","at":"2026-10-01T09:57:00Z","value":90,"labels":{"a":"1"}},
	{"id":"new-nearpre-2","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":180,"labels":{"a":"2"}},
	{"id":"new-a3-0","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":5,"labels":{"a":"3"}},
	{"id":"new-end-2","service":"svc","name":"requests","at":"2026-10-01T10:03:00Z","value":999,"labels":{"a":"2"}},
	{"id":"new-a3-2","service":"svc","name":"requests","at":"2026-10-01T10:02:30Z","value":60,"labels":{"a":"3"}},
	{"id":"clash-a1","service":"svc","name":"requests","at":"2026-10-01T10:00:30Z","value":201,"labels":{"a":"1"}}
]}`

// metricScenarioDuplicateBatch repeats one id inside the submission, which is
// also a whole-batch 409.
const metricScenarioDuplicateBatch = `{"samples":[
	{"id":"new-end-1","service":"svc","name":"requests","at":"2026-10-01T10:03:00Z","value":999,"labels":{"a":"1"}},
	{"id":"new-a3-1","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":50,"labels":{"a":"3"}},
	{"id":"new-nearpre-1","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":199,"labels":{"a":"1"}},
	{"id":"new-a1-2","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":201,"labels":{"a":"1"}},
	{"id":"new-farpre-2","service":"svc","name":"requests","at":"2026-10-01T09:58:00Z","value":20,"labels":{"a":"2"}},
	{"id":"new-a2-1","service":"svc","name":"requests","at":"2026-10-01T10:02:30Z","value":223,"labels":{"a":"2"}},
	{"id":"new-farpre-1","service":"svc","name":"requests","at":"2026-10-01T09:57:00Z","value":90,"labels":{"a":"1"}},
	{"id":"new-nearpre-2","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":180,"labels":{"a":"2"}},
	{"id":"new-a3-0","service":"svc","name":"requests","at":"2026-10-01T09:59:00Z","value":5,"labels":{"a":"3"}},
	{"id":"new-end-2","service":"svc","name":"requests","at":"2026-10-01T10:03:00Z","value":999,"labels":{"a":"2"}},
	{"id":"new-a3-2","service":"svc","name":"requests","at":"2026-10-01T10:02:30Z","value":60,"labels":{"a":"3"}},
	{"id":"new-a1-2","service":"svc","name":"requests","at":"2026-10-01T10:01:30Z","value":201,"labels":{"a":"1"}}
]}`

var metricSegBounds = [][2]string{
	{"2026-10-01T10:00:00Z", "2026-10-01T10:01:00Z"},
	{"2026-10-01T10:01:00Z", "2026-10-01T10:02:00Z"},
	{"2026-10-01T10:02:00Z", "2026-10-01T10:03:00Z"},
}

func findAggSeries(t *testing.T, out []map[string]any, label string) map[string]any {
	t.Helper()
	for _, series := range out {
		labels, ok := series["labels"].(map[string]any)
		if ok && labels["a"] == label {
			return series
		}
	}
	t.Fatalf("series a=%q missing from %d series", label, len(out))
	return nil
}

func assertAggSegments(t *testing.T, series map[string]any, wantCounts []int, wantDeltas []float64) {
	t.Helper()
	rawSegs, ok := series["segments"].([]any)
	if !ok || len(rawSegs) != len(wantCounts) {
		t.Fatalf("segments shape: %v", series["segments"])
	}
	for i, rawSeg := range rawSegs {
		seg := rawSeg.(map[string]any)
		if seg["since"] != metricSegBounds[i][0] || seg["until"] != metricSegBounds[i][1] {
			t.Fatalf("seg %d boundaries = %v..%v, want %s..%s",
				i, seg["since"], seg["until"], metricSegBounds[i][0], metricSegBounds[i][1])
		}
		if int(seg["count"].(float64)) != wantCounts[i] {
			t.Fatalf("seg %d count = %v, want %d", i, seg["count"], wantCounts[i])
		}
		if seg["delta"].(float64) != wantDeltas[i] {
			t.Fatalf("seg %d delta = %v, want %v", i, seg["delta"], wantDeltas[i])
		}
	}
}

// assertAggBefore pins the pre-submission response: exactly series a=1 and
// a=2, ordered by canonical label set, each with one in-window sample.
func assertAggBefore(t *testing.T, out []map[string]any) {
	t.Helper()
	if len(out) != 2 {
		t.Fatalf("before the batch exactly two series exist, got %d: %+v", len(out), out)
	}
	if out[0]["service"] != "svc" || out[0]["labels"].(map[string]any)["a"] != "1" {
		t.Fatalf("series order 0: %+v", out[0])
	}
	if out[1]["service"] != "svc" || out[1]["labels"].(map[string]any)["a"] != "2" {
		t.Fatalf("series order 1: %+v", out[1])
	}
	assertAggSegments(t, findAggSeries(t, out, "1"), []int{1, 0, 0}, []float64{100, 0, 0})
	assertAggSegments(t, findAggSeries(t, out, "2"), []int{1, 0, 0}, []float64{0, 0, 0})
}

// assertAggAfter pins the complete post-submission response: the new series
// a=3 appears, late predecessors moved every first-segment delta, and the
// query-end samples stay excluded.
func assertAggAfter(t *testing.T, out []map[string]any) {
	t.Helper()
	if len(out) != 3 {
		t.Fatalf("after the batch three series exist, got %d: %+v", len(out), out)
	}
	if out[0]["labels"].(map[string]any)["a"] != "1" ||
		out[1]["labels"].(map[string]any)["a"] != "2" ||
		out[2]["labels"].(map[string]any)["a"] != "3" {
		t.Fatalf("series order must be a=1,a=2,a=3: %+v", out)
	}
	assertAggSegments(t, findAggSeries(t, out, "1"), []int{1, 1, 0}, []float64{1, 1, 0})
	assertAggSegments(t, findAggSeries(t, out, "2"), []int{1, 0, 1}, []float64{20, 0, 23})
	assertAggSegments(t, findAggSeries(t, out, "3"), []int{0, 1, 1}, []float64{0, 45, 10})
}

func TestAggregateBeforeAfterBatchHTTP(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	if status, out := postSamplesRaw(t, server, metricScenarioSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	} else if int(number(out["created"])) != 3 {
		t.Fatalf("seed created=%v", out)
	}

	status, before := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate before: %d %v", status, before)
	}
	assertAggBefore(t, before)

	if status, out := postSamplesRaw(t, server, metricScenarioBatch); status != http.StatusOK {
		t.Fatalf("batch: %d %v", status, out)
	} else if int(number(out["created"])) != 11 || int(number(out["replayed"])) != 0 {
		t.Fatalf("batch counts: %v", out)
	}

	// Once the successful POST has returned, the same query must reflect the
	// complete post-submission dataset.
	status, after := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after: %d %v", status, after)
	}
	assertAggAfter(t, after)

	// Replaying the identical shuffled batch stores nothing and changes
	// nothing: timestamps, not body order, drive the result.
	if status, out := postSamplesRaw(t, server, metricScenarioBatch); status != http.StatusOK {
		t.Fatalf("replay: %d %v", status, out)
	} else if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 11 {
		t.Fatalf("replay counts: %v", out)
	}
	status, replayed := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after replay: %d", status)
	}
	if !reflect.DeepEqual(replayed, after) {
		t.Fatalf("replayed submission changed the aggregate:\n%+v\n%+v", replayed, after)
	}
}

// postSamplesRaw posts without failing the test on a 4xx, so conflict cases
// can assert the status themselves.
func postSamplesRaw(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	return metricsDo(t, server, http.MethodPost, "/metrics", body)
}

// newSlowMetricsServer builds a metrics store pre-seeded with many irrelevant
// series in another service (all outside the scenario window) so one
// aggregate round trip takes long enough to overlap deterministically with a
// concurrent submission. The fillers never appear in the scenario result.
func newSlowMetricsServer(t *testing.T) (*httptest.Server, *metrics.Store, *fakeMetricStorage) {
	t.Helper()
	store := metrics.NewStore()
	mstore := &fakeMetricStorage{}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	const fillers = 40000
	batch := make([]metrics.Sample, 0, fillers)
	for i := range fillers {
		id := strconv.Itoa(i)
		batch = append(batch, metrics.Sample{
			ID:      "filler-" + id,
			Service: "filler-svc",
			Name:    "requests",
			At:      base.Add(time.Duration(i) * time.Nanosecond),
			Value:   float64(i),
			Labels:  map[string]string{"i": id},
		})
	}
	if _, err := store.Ingest(batch, nil); err != nil {
		t.Fatalf("seed fillers: %v", err)
	}
	server := newMetricsServer(t, store, mstore)
	return server, store, mstore
}

// ensureSlowAggregateHTTP reports a round trip comfortably longer than the
// overlap sleep, seeding more filler series when the first chunk is too quick
// on a fast machine.
func ensureSlowAggregateHTTP(t *testing.T, server *httptest.Server, store *metrics.Store) time.Duration {
	t.Helper()
	roundTrip := func() time.Duration {
		start := time.Now()
		if status, _ := aggregateGet(t, server, metricScenarioQuery); status != http.StatusOK {
			t.Fatalf("warmup aggregate: %d", status)
		}
		return time.Since(start)
	}
	var slowest time.Duration
	for range 3 {
		if d := roundTrip(); d > slowest {
			slowest = d
		}
	}
	if slowest >= 20*time.Millisecond {
		return slowest
	}
	base := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	total := 40000
	for round := 0; round < 5; round++ {
		const chunk = 40000
		batch := make([]metrics.Sample, 0, chunk)
		for i := range chunk {
			id := strconv.Itoa(total + i)
			batch = append(batch, metrics.Sample{
				ID:      "filler2-" + id,
				Service: "filler-svc",
				Name:    "requests",
				At:      base.Add(time.Duration(total+i) * time.Nanosecond),
				Value:   float64(total + i),
				Labels:  map[string]string{"j": id},
			})
		}
		if _, err := store.Ingest(batch, nil); err != nil {
			t.Fatalf("seed more fillers: %v", err)
		}
		total += chunk
		for range 3 {
			if d := roundTrip(); d > slowest {
				slowest = d
			}
		}
		if slowest >= 20*time.Millisecond {
			return slowest
		}
	}
	return slowest
}

type aggCall struct {
	status int
	body   []map[string]any
}

// TestAggregateOverlappingBatchCommitHTTP overlaps real aggregate requests
// with a batch held mid durable commit:
//
//   - An aggregate already scanning when the submission reaches the metric
//     store lock finishes with the complete pre-submission state.
//   - Every aggregate whose read lock is queued while the durable commit is
//     pending is allowed to wait (that wait is not an error) and then returns
//     the complete post-submission state.
//
// In no response may one series already count the batch while another still
// shows its pre-batch series set or segment vector.
func TestAggregateOverlappingBatchCommitHTTP(t *testing.T) {
	server, store, mstore := newSlowMetricsServer(t)
	if status, out := postSamplesRaw(t, server, metricScenarioSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
	status, before := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate before: %d", status)
	}
	assertAggBefore(t, before)
	scanTime := ensureSlowAggregateHTTP(t, server, store)

	earlyCh := make(chan aggCall, 1)
	go func() {
		st, body := aggregateGet(t, server, metricScenarioQuery)
		earlyCh <- aggCall{st, body}
	}()
	time.Sleep(scanTime / 2)

	release, entered := mstore.armStall()
	postCh := make(chan aggCall, 1)
	go func() {
		st, out := postSamplesRaw(t, server, metricScenarioBatch)
		postCh <- aggCall{st, []map[string]any{out}}
	}()
	<-entered

	// The commit hook is holding the metric store write lock. Aggregates
	// launched here queue behind it, wait out the commit, and scan afterwards,
	// deterministically pinning the post-submission state.
	const readers = 8
	var wg sync.WaitGroup
	calls := make([]aggCall, readers)
	for i := range readers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, body := aggregateGet(t, server, metricScenarioQuery)
			calls[i] = aggCall{st, body}
		}(i)
	}
	time.Sleep(30 * time.Millisecond)
	close(release)

	posted := <-postCh
	if posted.status != http.StatusOK {
		t.Fatalf("batch status=%d body=%v", posted.status, posted.body)
	}
	if created := int(posted.body[0]["created"].(float64)); created != 11 {
		t.Fatalf("batch created=%d, want 11", created)
	}
	wg.Wait()

	status, after := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after: %d", status)
	}
	assertAggAfter(t, after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("pre- and post-batch aggregates must differ")
	}
	for i, call := range calls {
		if call.status != http.StatusOK {
			t.Fatalf("queued aggregate %d status=%d: waiting out a commit must not be an error", i, call.status)
		}
		if !reflect.DeepEqual(call.body, after) {
			t.Fatalf("queued aggregate %d must show the complete post-batch state:\n%+v\n%+v",
				i, call.body, after)
		}
	}

	// The aggregate in flight when the submission started must be exactly one
	// whole state: fully pre-batch or fully post-batch, never a splice.
	early := <-earlyCh
	if early.status != http.StatusOK {
		t.Fatalf("overlapping aggregate status=%d", early.status)
	}
	pre := reflect.DeepEqual(early.body, before)
	post := reflect.DeepEqual(early.body, after)
	if !pre && !post {
		t.Fatalf("overlapping aggregate must be the complete pre- or post-batch state, got a splice:\n%+v", early.body)
	}
}

// TestAggregateUnchangedAfterConflictBatchesHTTP covers the 409 contract:
// one different-id sample claiming a stored (series, instant) point rejects
// the whole submission; none of its other samples may appear, and the segment
// counts and deltas stay byte-for-byte at their pre-submission values. A
// repeated id inside the batch behaves identically. Only a later clean batch
// moves the aggregate to its full post state.
func TestAggregateUnchangedAfterConflictBatchesHTTP(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, nil, mstore)
	if status, out := postSamplesRaw(t, server, metricScenarioSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	} else if int(number(out["created"])) != 3 {
		t.Fatalf("seed counts: %v", out)
	}
	status, before := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate before: %d", status)
	}
	assertAggBefore(t, before)

	if status, out := postSamplesRaw(t, server, metricScenarioConflictBatch); status != http.StatusConflict {
		t.Fatalf("series/time clash must be 409, got %d %v", status, out)
	} else if out["error"] == "" {
		t.Fatalf("409 needs a non-empty error: %v", out)
	}
	status, afterConflict := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after conflict: %d", status)
	}
	if !reflect.DeepEqual(afterConflict, before) {
		t.Fatalf("409 batch must leave the aggregate untouched:\n%+v\n%+v", afterConflict, before)
	}

	if status, out := postSamplesRaw(t, server, metricScenarioDuplicateBatch); status != http.StatusConflict {
		t.Fatalf("in-batch duplicate id must be 409, got %d %v", status, out)
	}
	status, afterDuplicate := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after duplicate: %d", status)
	}
	if !reflect.DeepEqual(afterDuplicate, before) {
		t.Fatalf("duplicate-id batch must leave the aggregate untouched:\n%+v", afterDuplicate)
	}

	// Rejected submissions never reach durable storage: only the seed batch
	// was persisted.
	if len(mstore.batches) != 1 || len(mstore.batches[0]) != 3 {
		t.Fatalf("rejected batches must persist nothing, got %d batches", len(mstore.batches))
	}

	// A clean submission still moves the aggregate to the full post state.
	if status, out := postSamplesRaw(t, server, metricScenarioBatch); status != http.StatusOK {
		t.Fatalf("valid batch: %d %v", status, out)
	} else if int(number(out["created"])) != 11 {
		t.Fatalf("valid batch counts: %v", out)
	}
	status, after := aggregateGet(t, server, metricScenarioQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after valid batch: %d", status)
	}
	assertAggAfter(t, after)
}
