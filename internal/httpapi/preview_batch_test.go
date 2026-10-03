package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// HTTP-level regression coverage for the preview/ingestion snapshot
// guarantee. These tests pin the same discriminating scenario as the
// events-package tests through the real POST /events and POST
// /alerts/preview endpoints, keeping the existing request and response
// formats unchanged:
//
//	one-minute windows, threshold 2, trigger on two consecutive windows,
//	recover on one quiet window. The seeded group old has [2,1,0] events and
//	no alert; one batch tops its middle window to 2 and fills group new with
//	[2,2,0]. Afterwards both groups alert at 10:02 and recover at 10:03, and
//	the empty window stays present with count zero.

const scenarioPreviewBody = `{
	"since":"2026-10-01T10:00:00Z",
	"until":"2026-10-01T10:03:00Z",
	"window_seconds":60,
	"threshold":2,
	"trigger_windows":2,
	"recover_windows":1,
	"service":"svc",
	"group_labels":["grp"]
}`

const scenarioOldSeed = `{"events":[
	{"id":"o0a","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:05Z","labels":{"grp":"old"}},
	{"id":"o0b","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:30Z","labels":{"grp":"old"}},
	{"id":"o10","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:05Z","labels":{"grp":"old"}}
]}`

// scenarioBatch tops group old's middle window up and fills group new in a
// single submission.
const scenarioBatch = `{"events":[
	{"id":"o11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:40Z","labels":{"grp":"old"}},
	{"id":"n0a","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:10Z","labels":{"grp":"new"}},
	{"id":"n0b","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:20Z","labels":{"grp":"new"}},
	{"id":"n10","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:10Z","labels":{"grp":"new"}},
	{"id":"n11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:50Z","labels":{"grp":"new"}}
]}`

// scenarioConflictBatch contains the five valid batch events plus one event
// that reuses a stored id with different content. The conflict must reject
// the whole submission with 409.
const scenarioConflictBatch = `{"events":[
	{"id":"o11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:40Z","labels":{"grp":"old"}},
	{"id":"n0a","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:10Z","labels":{"grp":"new"}},
	{"id":"n0b","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:20Z","labels":{"grp":"new"}},
	{"id":"n10","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:10Z","labels":{"grp":"new"}},
	{"id":"n11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:50Z","labels":{"grp":"new"}},
	{"id":"o10","service":"svc","severity":"info","message":"changed content","at":"2026-10-01T10:01:05Z","labels":{"grp":"old"}}
]}`

// scenarioDuplicateBatch repeats one id inside the submission, which is also
// a whole-batch 409.
const scenarioDuplicateBatch = `{"events":[
	{"id":"o11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:40Z","labels":{"grp":"old"}},
	{"id":"n0a","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:10Z","labels":{"grp":"new"}},
	{"id":"n0b","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:00:20Z","labels":{"grp":"new"}},
	{"id":"n10","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:10Z","labels":{"grp":"new"}},
	{"id":"n11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:50Z","labels":{"grp":"new"}},
	{"id":"o11","service":"svc","severity":"info","message":"m","at":"2026-10-01T10:01:40Z","labels":{"grp":"old"}}
]}`

type scenarioPreview struct {
	GroupLabels []string               `json:"group_labels"`
	Groups      []scenarioPreviewGroup `json:"groups"`
}

type scenarioPreviewGroup struct {
	Service     string                  `json:"service"`
	LabelValues []*string               `json:"label_values"`
	Windows     []scenarioPreviewWindow `json:"windows"`
	Alerts      []scenarioPreviewAlert  `json:"alerts"`
}

type scenarioPreviewWindow struct {
	Since  string `json:"since"`
	Until  string `json:"until"`
	Count  int    `json:"count"`
	Status string `json:"status"`
}

type scenarioPreviewAlert struct {
	TriggeredAt string  `json:"triggered_at"`
	RecoveredAt *string `json:"recovered_at"`
}

func getScenarioPreview(t *testing.T, server *httptest.Server, body string) (int, scenarioPreview) {
	t.Helper()
	resp, err := http.Post(server.URL+"/alerts/preview", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post preview: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read preview: %v", err)
	}
	var got scenarioPreview
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode preview %q: %v", raw, err)
	}
	return resp.StatusCode, got
}

func assertScenarioPreviewBefore(t *testing.T, got scenarioPreview) {
	t.Helper()
	if !reflect.DeepEqual(got.GroupLabels, []string{"grp"}) {
		t.Fatalf("group_labels = %v", got.GroupLabels)
	}
	if len(got.Groups) != 1 {
		t.Fatalf("before the batch only group old exists, got %d groups: %+v", len(got.Groups), got.Groups)
	}
	old := findScenarioHTTPGroup(t, got, "old")
	assertScenarioWindowBounds(t, old)
	wantCounts := []int{2, 1, 0}
	for i, window := range old.Windows {
		if window.Count != wantCounts[i] {
			t.Fatalf("old group w%d count=%d want %d", i, window.Count, wantCounts[i])
		}
		if window.Status != "normal" {
			t.Fatalf("old group must stay normal before the batch, w%d=%s", i, window.Status)
		}
	}
	if len(old.Alerts) != 0 {
		t.Fatalf("old group must have no alerts before the batch, got %+v", old.Alerts)
	}
}

func assertScenarioPreviewAfter(t *testing.T, got scenarioPreview) {
	t.Helper()
	if len(got.Groups) != 2 {
		t.Fatalf("after the batch both groups exist, got %d: %+v", len(got.Groups), got.Groups)
	}
	// Groups order is service then label value: new before old.
	if got.Groups[0].Service != "svc" || got.Groups[1].Service != "svc" ||
		*got.Groups[0].LabelValues[0] != "new" || *got.Groups[1].LabelValues[0] != "old" {
		t.Fatalf("unexpected group ordering: %+v", got.Groups)
	}
	for _, name := range []string{"new", "old"} {
		group := findScenarioHTTPGroup(t, got, name)
		assertScenarioWindowBounds(t, group)
		wantCounts := []int{2, 2, 0}
		wantStatus := []string{"normal", "alerting", "normal"}
		for i, window := range group.Windows {
			if window.Count != wantCounts[i] {
				t.Fatalf("group %s w%d count=%d want %d", name, i, window.Count, wantCounts[i])
			}
			if window.Status != wantStatus[i] {
				t.Fatalf("group %s w%d status=%s want %s", name, i, window.Status, wantStatus[i])
			}
		}
		if len(group.Alerts) != 1 {
			t.Fatalf("group %s must have one alert cycle, got %+v", name, group.Alerts)
		}
		alert := group.Alerts[0]
		if alert.TriggeredAt != "2026-10-01T10:02:00Z" {
			t.Fatalf("group %s triggers at 10:02, got %s", name, alert.TriggeredAt)
		}
		if alert.RecoveredAt == nil || *alert.RecoveredAt != "2026-10-01T10:03:00Z" {
			t.Fatalf("group %s recovers at 10:03, got %v", name, alert.RecoveredAt)
		}
	}
}

func assertScenarioWindowBounds(t *testing.T, group scenarioPreviewGroup) {
	t.Helper()
	if len(group.Windows) != 3 {
		t.Fatalf("three windows including the empty one, got %d", len(group.Windows))
	}
	bounds := [][2]string{
		{"2026-10-01T10:00:00Z", "2026-10-01T10:01:00Z"},
		{"2026-10-01T10:01:00Z", "2026-10-01T10:02:00Z"},
		{"2026-10-01T10:02:00Z", "2026-10-01T10:03:00Z"},
	}
	for i, window := range group.Windows {
		if window.Since != bounds[i][0] || window.Until != bounds[i][1] {
			t.Fatalf("group %v w%d boundaries = %s..%s, want %s..%s",
				group.LabelValues, i, window.Since, window.Until, bounds[i][0], bounds[i][1])
		}
	}
}

func findScenarioHTTPGroup(t *testing.T, got scenarioPreview, value string) scenarioPreviewGroup {
	t.Helper()
	for _, group := range got.Groups {
		if len(group.LabelValues) == 1 && group.LabelValues[0] != nil && *group.LabelValues[0] == value {
			return group
		}
	}
	t.Fatalf("group grp=%q missing from %+v", value, got.Groups)
	return scenarioPreviewGroup{}
}

// assertScenarioPreviewCoherent re-derives statuses and alert records from
// the reported window counts, so one response can never mix a post-batch
// count in one window with a pre-batch state elsewhere: a completed trigger
// run must carry an alert, a recovery must agree with the alerting status,
// and no alert may read recovered while its window still says alerting.
func assertScenarioPreviewCoherent(t *testing.T, got scenarioPreview) {
	t.Helper()
	const (
		threshold      = 2
		triggerWindows = 2
		recoverWindows = 1
	)
	for _, group := range got.Groups {
		alerting := false
		triggerRun, recoverRun := 0, 0
		type wantAlert struct {
			triggeredAt string
			recoveredAt *string
		}
		var wantAlerts []wantAlert
		wantStatus := make([]string, len(group.Windows))
		for i, window := range group.Windows {
			if !alerting {
				if window.Count >= threshold {
					triggerRun++
					if triggerRun >= triggerWindows {
						alerting = true
						triggerRun = 0
						wantAlerts = append(wantAlerts, wantAlert{triggeredAt: window.Until})
					}
				} else {
					triggerRun = 0
				}
			} else if window.Count < threshold {
				recoverRun++
				if recoverRun >= recoverWindows {
					alerting = false
					recoverRun = 0
					recovered := window.Until
					wantAlerts[len(wantAlerts)-1].recoveredAt = &recovered
				}
			} else {
				recoverRun = 0
			}
			wantStatus[i] = "normal"
			if alerting {
				wantStatus[i] = "alerting"
			}
		}
		for i, window := range group.Windows {
			if window.Status != wantStatus[i] {
				t.Fatalf("group %v w%d status=%s does not follow count %d (want %s)",
					group.LabelValues, i, window.Status, window.Count, wantStatus[i])
			}
		}
		if len(wantAlerts) != len(group.Alerts) {
			t.Fatalf("group %v has %d alerts but its counts imply %d: %+v vs %+v",
				group.LabelValues, len(group.Alerts), len(wantAlerts), group.Alerts, wantAlerts)
		}
		for i, want := range wantAlerts {
			alert := group.Alerts[i]
			if alert.TriggeredAt != want.triggeredAt {
				t.Fatalf("group %v alert %d triggered_at=%s want %s",
					group.LabelValues, i, alert.TriggeredAt, want.triggeredAt)
			}
			switch {
			case want.recoveredAt == nil && alert.RecoveredAt != nil:
				t.Fatalf("group %v alert %d must remain open, recovered at %s",
					group.LabelValues, i, *alert.RecoveredAt)
			case want.recoveredAt != nil && (alert.RecoveredAt == nil || *alert.RecoveredAt != *want.recoveredAt):
				t.Fatalf("group %v alert %d recovered_at=%v want %s",
					group.LabelValues, i, alert.RecoveredAt, *want.recoveredAt)
			}
		}
	}
}

func TestPreviewBeforeAfterBatchHTTP(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, out := post(t, server, scenarioOldSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}

	status, before := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview before: %d", status)
	}
	assertScenarioPreviewBefore(t, before)
	assertScenarioPreviewCoherent(t, before)

	if status, out := post(t, server, scenarioBatch); status != http.StatusOK {
		t.Fatalf("batch: %d %v", status, out)
	} else if out["created"].(float64) != 5 || out["replayed"].(float64) != 0 {
		t.Fatalf("batch counts: %v", out)
	}

	// Once the successful POST /events has returned, the same preview must
	// reflect the complete post-submission dataset.
	status, after := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview after: %d", status)
	}
	assertScenarioPreviewAfter(t, after)
	assertScenarioPreviewCoherent(t, after)

	// Repeating the identical submission replays everything and changes
	// nothing.
	if status, out := post(t, server, scenarioBatch); status != http.StatusOK {
		t.Fatalf("replay: %d %v", status, out)
	} else if out["created"].(float64) != 0 || out["replayed"].(float64) != 5 {
		t.Fatalf("replay counts: %v", out)
	}
	_, replayed := getScenarioPreview(t, server, scenarioPreviewBody)
	if !reflect.DeepEqual(replayed, after) {
		t.Fatalf("replayed submission changed the preview:\n%+v\n%+v", replayed, after)
	}
}

// ensureSlowPreview seeds irrelevant filler events (another service, outside
// the scenario range) until one preview round trip takes long enough to
// overlap deterministically with a concurrent submission. The fillers never
// change the scenario preview.
func ensureSlowPreview(t *testing.T, server *httptest.Server) time.Duration {
	t.Helper()
	roundTrip := func() time.Duration {
		start := time.Now()
		if status, _ := getScenarioPreview(t, server, scenarioPreviewBody); status != http.StatusOK {
			t.Fatalf("warmup preview: %d", status)
		}
		return time.Since(start)
	}
	total := 0
	for round := 0; round < 6; round++ {
		const chunk = 50000
		var sb strings.Builder
		sb.WriteString(`{"events":[`)
		for i := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			id := total + i
			fmt.Fprintf(&sb, `{"id":"filler-%d","service":"filler-svc","severity":"info","message":"m","at":"2026-09-01T00:00:00Z"}`, id)
		}
		sb.WriteString(`]}`)
		if status, out := post(t, server, sb.String()); status != http.StatusOK {
			t.Fatalf("seed fillers: %d %v", status, out)
		}
		total += chunk
		var slowest time.Duration
		for range 3 {
			if d := roundTrip(); d > slowest {
				slowest = d
			}
		}
		if slowest >= 20*time.Millisecond {
			return slowest
		}
	}
	return roundTrip()
}

type previewCall struct {
	status int
	body   scenarioPreview
}

// TestPreviewOverlappingBatchCommitHTTP overlaps real preview requests with
// a batch held mid commit:
//
//   - A preview already scanning when the submission reaches the timeline
//     lock must finish with the complete pre-submission state.
//   - Every preview whose read lock is queued while the durable commit is
//     pending must return with the complete post-submission state.
//
// Either way a single response may count only the full batch or none of it:
// no partial counts, no mix of groups across states, and no statuses/alerts
// that its own counts do not imply.
func TestPreviewOverlappingBatchCommitHTTP(t *testing.T) {
	server, store, _ := newTestServer(t)
	if status, out := post(t, server, scenarioOldSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
	status, before := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview before: %d", status)
	}
	scanTime := ensureSlowPreview(t, server)

	earlyCh := make(chan previewCall, 1)
	go func() {
		st, body := getScenarioPreview(t, server, scenarioPreviewBody)
		earlyCh <- previewCall{st, body}
	}()
	// Start the submission while the slow preview is (almost certainly)
	// mid-scan; the assertions below are correct for either scheduling
	// outcome.
	time.Sleep(scanTime / 2)

	release, entered := store.armStall()
	postCh := make(chan map[string]any, 1)
	postStatus := make(chan int, 1)
	go func() {
		st, out := post(t, server, scenarioBatch)
		postStatus <- st
		postCh <- out
	}()
	<-entered

	// The commit hook is now holding the timeline write lock. Previews
	// launched here queue behind it and, by RWMutex writer preference, scan
	// only after the batch commits, so they deterministically pin the
	// post-submission side of the overlap.
	const readers = 8
	var wg sync.WaitGroup
	calls := make([]previewCall, readers)
	for i := range readers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, body := getScenarioPreview(t, server, scenarioPreviewBody)
			calls[i] = previewCall{st, body}
		}(i)
	}
	time.Sleep(30 * time.Millisecond)
	close(release)

	if st := <-postStatus; st != http.StatusOK {
		t.Fatalf("batch submit status=%d body=%v", st, <-postCh)
	}
	if out := <-postCh; out["created"].(float64) != 5 {
		t.Fatalf("batch counts after release: %v", out)
	}
	wg.Wait()

	status, after := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview after: %d", status)
	}
	assertScenarioPreviewAfter(t, after)
	assertScenarioPreviewCoherent(t, after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("pre- and post-batch previews must differ")
	}
	for i, call := range calls {
		if call.status != http.StatusOK {
			t.Fatalf("queued preview %d status=%d", i, call.status)
		}
		assertScenarioPreviewCoherent(t, call.body)
		if !reflect.DeepEqual(call.body, after) {
			t.Fatalf("queued preview %d must show the complete post-batch state:\n%+v\n%+v",
				i, call.body, after)
		}
	}

	// The preview in flight when the submission started must be one whole
	// state or the other, never a splice: it scanned either before the
	// writer's commit (pre) or after the lock passed to queued readers (post).
	early := <-earlyCh
	if early.status != http.StatusOK {
		t.Fatalf("overlapping preview status=%d", early.status)
	}
	assertScenarioPreviewCoherent(t, early.body)
	pre := reflect.DeepEqual(early.body, before)
	post := reflect.DeepEqual(early.body, after)
	if !pre && !post {
		t.Fatalf("overlapping preview must be the complete pre- or post-batch state, got a splice:\n%+v", early.body)
	}
}

func TestPreviewUnchangedAfterConflictBatchesHTTP(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, out := post(t, server, scenarioOldSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
	status, before := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview before: %d", status)
	}
	assertScenarioPreviewBefore(t, before)

	// One event clashes with stored content: the whole batch is rejected 409
	// and none of its other events may move the preview.
	if status, out := post(t, server, scenarioConflictBatch); status != http.StatusConflict {
		t.Fatalf("conflict batch must be 409, got %d %v", status, out)
	}
	status, afterConflict := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview after conflict: %d", status)
	}
	if !reflect.DeepEqual(afterConflict, before) {
		t.Fatalf("409 batch must leave the preview untouched:\n%+v\n%+v", afterConflict, before)
	}

	// A repeated id inside the batch is rejected just as wholesale.
	if status, out := post(t, server, scenarioDuplicateBatch); status != http.StatusConflict {
		t.Fatalf("duplicate-id batch must be 409, got %d %v", status, out)
	}
	_, afterDuplicate := getScenarioPreview(t, server, scenarioPreviewBody)
	if !reflect.DeepEqual(afterDuplicate, before) {
		t.Fatalf("duplicate-id batch must leave the preview untouched:\n%+v", afterDuplicate)
	}

	// The rejected submissions created no incidents and changed no events:
	// the stored timeline still holds exactly the three seeded events.
	if status, got := get(t, server, "?service=svc"); status != http.StatusOK {
		t.Fatalf("list events: %d", status)
	} else if len(got.([]any)) != 3 {
		t.Fatalf("rejected batches must store nothing, timeline has %v events", got)
	}

	// A clean submission still moves the preview to the full post state.
	if status, out := post(t, server, scenarioBatch); status != http.StatusOK {
		t.Fatalf("valid batch: %d %v", status, out)
	} else if out["created"].(float64) != 5 {
		t.Fatalf("valid batch counts: %v", out)
	}
	status, after := getScenarioPreview(t, server, scenarioPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("preview after valid batch: %d", status)
	}
	assertScenarioPreviewAfter(t, after)
}
