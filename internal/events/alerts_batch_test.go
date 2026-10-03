package events

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Regression coverage for the relationship between whole-batch ingestion
// (POST /events) and alert previews (POST /alerts/preview): every preview
// response must be computed from one committed snapshot. The groups, window
// counts, end-of-window statuses, and alert trigger/recovery times must all
// belong to the same observation, never a mix of two batches.
//
// The discriminating scenario: one-minute windows, threshold 2, trigger on
// two consecutive windows at/over the threshold, recover on one window below
// it. The seeded "old" label group has counts [2,1,0] (no alert); one batch
// tops its middle window up to 2 while filling a second label group "new"
// with [2,2,0]. After the commit both groups alert at the end of window 1 and
// recover at the end of the empty window 2. A partial application of the
// batch would surface as a group or a state machine that matches neither the
// committed-before nor the committed-after result.

func scenarioAnchor() time.Time {
	return time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
}

func scenarioQuery() AlertPreviewQuery {
	t0 := scenarioAnchor()
	return AlertPreviewQuery{
		Service:        "svc",
		Since:          t0,
		Until:          t0.Add(3 * time.Minute),
		Window:         time.Minute,
		Threshold:      2,
		TriggerWindows: 2,
		RecoverWindows: 1,
		GroupLabels:    []string{"grp"},
	}
}

// seedScenarioOldData commits only the old group: two events in window 0,
// one in window 1, window 2 empty.
func seedScenarioOldData(t *testing.T, tl *Timeline) {
	t.Helper()
	t0 := scenarioAnchor()
	old := []Event{
		scenarioEvent("o0a", "old", t0.Add(5*time.Second)),
		scenarioEvent("o0b", "old", t0.Add(30*time.Second)),
		scenarioEvent("o10", "old", t0.Add(65*time.Second)),
	}
	if _, err := tl.Ingest(old, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// scenarioBatch tops the old group's middle window up and fills the new
// group's first two windows, all in one submission.
func scenarioBatch() []Event {
	t0 := scenarioAnchor()
	return []Event{
		scenarioEvent("o11", "old", t0.Add(100*time.Second)),
		scenarioEvent("n0a", "new", t0.Add(10*time.Second)),
		scenarioEvent("n0b", "new", t0.Add(20*time.Second)),
		scenarioEvent("n10", "new", t0.Add(70*time.Second)),
		scenarioEvent("n11", "new", t0.Add(110*time.Second)),
	}
}

func scenarioEvent(id, group string, at time.Time) Event {
	return Event{
		ID:       id,
		Service:  "svc",
		Severity: "info",
		Message:  "m",
		At:       at,
		Labels:   map[string]string{"grp": group},
	}
}

func findScenarioGroup(t *testing.T, result AlertPreviewResult, value string) *AlertGroup {
	t.Helper()
	for i := range result.Groups {
		g := &result.Groups[i]
		if len(g.LabelValues) == 1 && g.LabelValues[0] != nil && *g.LabelValues[0] == value {
			return g
		}
	}
	t.Fatalf("group grp=%q not found in %+v", value, result.Groups)
	return nil
}

// assertScenarioBefore pins the pre-submission observation: only the old
// group exists, its counts never reach two consecutive threshold hits, the
// empty window is retained, and no alert exists.
func assertScenarioBefore(t *testing.T, result AlertPreviewResult) {
	t.Helper()
	if !reflect.DeepEqual(result.GroupLabelNames, []string{"grp"}) {
		t.Fatalf("group label names: %v", result.GroupLabelNames)
	}
	if len(result.Groups) != 1 {
		t.Fatalf("before the batch only the old group exists, got %d groups", len(result.Groups))
	}
	old := findScenarioGroup(t, result, "old")
	assertWindowCounts(t, old, []int{2, 1, 0})
	for i, window := range old.Windows {
		if window.Status != AlertStatusNormal {
			t.Fatalf("old group must never alert before the batch, w%d=%s", i, window.Status)
		}
	}
	if len(old.Alerts) != 0 {
		t.Fatalf("old group must have no alerts before the batch, got %+v", old.Alerts)
	}
}

// assertScenarioAfter pins the post-submission observation: both groups
// carry [2,2,0], alert at the end of window 1 and recover at the end of the
// empty window 2.
func assertScenarioAfter(t *testing.T, result AlertPreviewResult) {
	t.Helper()
	if len(result.Groups) != 2 {
		t.Fatalf("after the batch both groups exist, got %d groups", len(result.Groups))
	}
	t0 := scenarioAnchor()
	for _, value := range []string{"old", "new"} {
		group := findScenarioGroup(t, result, value)
		assertWindowCounts(t, group, []int{2, 2, 0})
		wantStatus := []string{AlertStatusNormal, AlertStatusAlerting, AlertStatusNormal}
		for i, window := range group.Windows {
			if window.Status != wantStatus[i] {
				t.Fatalf("group %s w%d status=%s want %s", value, i, window.Status, wantStatus[i])
			}
		}
		if len(group.Alerts) != 1 {
			t.Fatalf("group %s must have one alert cycle, got %+v", value, group.Alerts)
		}
		alert := group.Alerts[0]
		if !alert.OpenedAt.Equal(t0.Add(2 * time.Minute)) {
			t.Fatalf("group %s triggers at end of window 1, got %s", value, alert.OpenedAt)
		}
		if alert.RecoveredAt == nil || !alert.RecoveredAt.Equal(t0.Add(3*time.Minute)) {
			t.Fatalf("group %s recovers at end of the empty window 2, got %v", value, alert.RecoveredAt)
		}
	}
}

func assertWindowCounts(t *testing.T, group *AlertGroup, want []int) {
	t.Helper()
	if len(group.Windows) != len(want) {
		t.Fatalf("group %v: got %d windows, want %d", group.LabelValues, len(group.Windows), len(want))
	}
	for i, window := range group.Windows {
		if window.Count != want[i] {
			t.Fatalf("group %v w%d count=%d, want %d", group.LabelValues, i, window.Count, want[i])
		}
	}
}

// assertPreviewCoherent re-derives every end-of-window status and every
// alert trigger/recovery record from the reported counts, so a response can
// never pass with threshold runs lacking an alert or with an alert that
// recovered while a window still reports alerting.
func assertPreviewCoherent(t *testing.T, query AlertPreviewQuery, result AlertPreviewResult) {
	t.Helper()
	for _, group := range result.Groups {
		alerting := false
		triggerRun, recoverRun := 0, 0
		var wantAlerts []AlertRecord
		wantStatus := make([]string, len(group.Windows))
		for i, window := range group.Windows {
			if !alerting {
				if window.Count >= query.Threshold {
					triggerRun++
					if triggerRun >= query.TriggerWindows {
						alerting = true
						triggerRun = 0
						wantAlerts = append(wantAlerts, AlertRecord{OpenedAt: window.End})
					}
				} else {
					triggerRun = 0
				}
			} else {
				if window.Count < query.Threshold {
					recoverRun++
					if recoverRun >= query.RecoverWindows {
						alerting = false
						recoverRun = 0
						end := window.End
						wantAlerts[len(wantAlerts)-1].RecoveredAt = &end
					}
				} else {
					recoverRun = 0
				}
			}
			wantStatus[i] = AlertStatusNormal
			if alerting {
				wantStatus[i] = AlertStatusAlerting
			}
		}
		for i, window := range group.Windows {
			if window.Status != wantStatus[i] {
				t.Fatalf("group %v w%d status %s does not follow its counts: want %s (count=%d)",
					group.LabelValues, i, window.Status, wantStatus[i], window.Count)
			}
		}
		if len(wantAlerts) != len(group.Alerts) {
			t.Fatalf("group %v has %d alerts but its counts imply %d: %+v vs %+v",
				group.LabelValues, len(group.Alerts), len(wantAlerts), group.Alerts, wantAlerts)
		}
		for i := range wantAlerts {
			got, want := group.Alerts[i], wantAlerts[i]
			if !got.OpenedAt.Equal(want.OpenedAt) {
				t.Fatalf("group %v alert %d triggered_at=%s want %s",
					group.LabelValues, i, got.OpenedAt, want.OpenedAt)
			}
			switch {
			case want.RecoveredAt == nil && got.RecoveredAt != nil:
				t.Fatalf("group %v alert %d must stay open, recovered at %s",
					group.LabelValues, i, *got.RecoveredAt)
			case want.RecoveredAt != nil && (got.RecoveredAt == nil || !got.RecoveredAt.Equal(*want.RecoveredAt)):
				t.Fatalf("group %v alert %d recovered_at=%v want %s",
					group.LabelValues, i, got.RecoveredAt, *want.RecoveredAt)
			}
		}
	}
}

func TestPreviewScenarioBeforeAndAfterWholeBatch(t *testing.T) {
	tl := NewTimeline()
	query := scenarioQuery()
	seedScenarioOldData(t, tl)

	before, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview before: %v", err)
	}
	assertScenarioBefore(t, before)
	assertPreviewCoherent(t, query, before)

	result, err := tl.Ingest(scenarioBatch(), nil)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Created != 5 || result.Replayed != 0 {
		t.Fatalf("batch result = %+v, want created=5", result)
	}

	// A preview issued once the successful ingestion has returned must show
	// the complete post-submission observation, never a partial count.
	after, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview after: %v", err)
	}
	assertScenarioAfter(t, after)
	assertPreviewCoherent(t, query, after)
}

// ensureSlowScan grows the timeline with irrelevant events until one preview
// scan takes long enough to overlap deterministically with another caller.
// The filler events belong to another service and fall outside the scenario
// range, so they never change a preview result; they only widen the scan.
func ensureSlowScan(t *testing.T, tl *Timeline, query AlertPreviewQuery) time.Duration {
	t.Helper()
	base := scenarioAnchor().AddDate(0, -1, 0)
	scan := func() time.Duration {
		start := time.Now()
		if _, err := tl.PreviewAlerts(query); err != nil {
			t.Fatalf("warmup preview: %v", err)
		}
		return time.Since(start)
	}
	total := 0
	for round := 0; round < 6; round++ {
		const chunk = 100000
		fillers := make([]Event, 0, chunk)
		for i := range chunk {
			fillers = append(fillers, Event{
				ID:       "filler-" + strconv.Itoa(total+i),
				Service:  "filler-svc",
				Severity: "info",
				Message:  "m",
				At:       base.Add(time.Duration(total+i) * time.Nanosecond),
			})
		}
		if _, err := tl.Ingest(fillers, nil); err != nil {
			t.Fatalf("seed fillers: %v", err)
		}
		total += chunk
		var slowest time.Duration
		for range 3 {
			if d := scan(); d > slowest {
				slowest = d
			}
		}
		if slowest >= 5*time.Millisecond {
			return slowest
		}
	}
	return scan()
}

// TestPreviewOverlappingBatchCommitIsOneSnapshot overlaps previews with a
// whole-batch submission:
//
//   - A preview already scanning when the submission reaches the timeline
//     lock finishes with the complete pre-submission state.
//   - Every preview whose read lock is queued while the commit hook is held
//     runs after the commit and returns the complete post-submission state.
//
// A single response may therefore contain the full batch or none of it; the
// test also asserts the general contract on the in-flight preview: whichever
// side scheduling chose, the result must be one whole observation, never a
// splice with a partial batch, mixed group snapshots, or statuses/alerts that
// the reported counts do not imply.
func TestPreviewOverlappingBatchCommitIsOneSnapshot(t *testing.T) {
	tl := NewTimeline()
	query := scenarioQuery()
	seedScenarioOldData(t, tl)

	before, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview before: %v", err)
	}
	scanTime := ensureSlowScan(t, tl, query)

	earlyDone := make(chan AlertPreviewResult, 1)
	go func() {
		result, err := tl.PreviewAlerts(query)
		if err != nil {
			t.Errorf("early preview: %v", err)
			return
		}
		earlyDone <- result
	}()
	// Start the submission while the slow preview is (almost certainly)
	// mid-scan; the assertions below hold for either scheduling outcome.
	time.Sleep(scanTime / 2)

	entered := make(chan struct{})
	release := make(chan struct{})
	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		result, err := tl.Ingest(scenarioBatch(), func(persisted []Event) error {
			close(entered)
			<-release
			return nil
		})
		if err != nil {
			t.Errorf("blocked ingest: %v", err)
			return
		}
		if result.Created != 5 {
			t.Errorf("batch created=%d, want 5", result.Created)
		}
	}()
	<-entered

	// The commit hook holds the write lock now; these previews queue behind
	// it and scan only after the batch commits.
	const readers = 24
	observed := make(chan AlertPreviewResult, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := tl.PreviewAlerts(query)
			if err != nil {
				t.Errorf("queued preview: %v", err)
				return
			}
			observed <- result
		}()
	}
	// Give the previews time to pile onto the pending write lock; the
	// guarantee does not depend on this delay, it only widens coverage.
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-ingestDone
	wg.Wait()
	close(observed)

	after, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview after: %v", err)
	}
	assertScenarioAfter(t, after)
	assertPreviewCoherent(t, query, after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("pre- and post-batch observations must differ")
	}

	count := 0
	for result := range observed {
		count++
		assertPreviewCoherent(t, query, result)
		if !reflect.DeepEqual(result, after) {
			t.Fatalf("queued preview must show the whole post-batch state:\n%+v", summarizePreview(result))
		}
	}
	if count != readers {
		t.Fatalf("collected %d queued previews, want %d", count, readers)
	}

	// The preview in flight when the submission started must be exactly one
	// committed observation: fully pre-batch or fully post-batch.
	early := <-earlyDone
	assertPreviewCoherent(t, query, early)
	pre := reflect.DeepEqual(early, before)
	post := reflect.DeepEqual(early, after)
	if !pre && !post {
		t.Fatalf("overlapping preview must be the complete pre- or post-batch state, got a splice:\n%+v",
			summarizePreview(early))
	}
}

// TestPreviewUnchangedAfterConflictBatch covers the 409 contract: when one
// event conflicts with stored content (or an id repeats inside the batch),
// none of the batch's other events may become visible and the preview stays
// identical to the observation taken before the rejected submission.
func TestPreviewUnchangedAfterConflictBatch(t *testing.T) {
	tl := NewTimeline()
	query := scenarioQuery()
	seedScenarioOldData(t, tl)

	before, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview before: %v", err)
	}

	// A batch whose valid events would create the new group and top up the
	// old one, but whose last event reuses a stored id with new content.
	conflict := append(scenarioBatch(), Event{
		ID:       "o10",
		Service:  "svc",
		Severity: "info",
		Message:  "changed content",
		At:       scenarioAnchor().Add(65 * time.Second),
		Labels:   map[string]string{"grp": "old"},
	})
	if _, err := tl.Ingest(conflict, nil); !IsConflictError(err) {
		t.Fatalf("changed content for a stored id must conflict, got %v", err)
	}
	after, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview after conflict: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("409 batch changed the preview:\nbefore=%+v\nafter =%+v", summarizePreview(before), summarizePreview(after))
	}
	if got := tl.Query(Query{}); len(got) != 3 {
		t.Fatalf("rejected batch must store nothing, timeline has %d events", len(got))
	}

	// An id repeated inside the batch is rejected wholesale as well.
	batch := scenarioBatch()
	dup := append(append([]Event{}, batch...), batch[0])
	if _, err := tl.Ingest(dup, nil); !IsConflictError(err) {
		t.Fatalf("repeated id within batch must conflict, got %v", err)
	}
	again, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview after duplicate batch: %v", err)
	}
	if !reflect.DeepEqual(before, again) {
		t.Fatalf("duplicate-id batch changed the preview: %+v", summarizePreview(again))
	}

	// The untouched dataset still moves to the full post state once a valid
	// batch commits, proving the equality checks above were discriminating.
	if _, err := tl.Ingest(scenarioBatch(), nil); err != nil {
		t.Fatalf("valid batch after conflicts: %v", err)
	}
	committed, err := tl.PreviewAlerts(query)
	if err != nil {
		t.Fatalf("preview after valid batch: %v", err)
	}
	assertScenarioAfter(t, committed)
}

func summarizePreview(result AlertPreviewResult) []struct {
	group  string
	counts []int
	alerts []AlertRecord
} {
	summary := make([]struct {
		group  string
		counts []int
		alerts []AlertRecord
	}, 0, len(result.Groups))
	for _, group := range result.Groups {
		name := ""
		if len(group.LabelValues) == 1 && group.LabelValues[0] != nil {
			name = *group.LabelValues[0]
		}
		counts := make([]int, len(group.Windows))
		for i, window := range group.Windows {
			counts[i] = window.Count
		}
		summary = append(summary, struct {
			group  string
			counts []int
			alerts []AlertRecord
		}{name, counts, group.Alerts})
	}
	return summary
}
