package events

import (
	"testing"
	"time"
)

func previewTimeline(t *testing.T, events ...Event) *Timeline {
	t.Helper()
	tl := NewTimeline()
	for _, event := range events {
		if _, err := tl.Ingest([]Event{event}, nil); err != nil {
			t.Fatalf("ingest %s: %v", event.ID, err)
		}
	}
	return tl
}

func at(minute, second int) time.Time {
	return time.Date(2026, 10, 1, 10, minute, second, 0, time.UTC)
}

func TestPreviewStateMachine(t *testing.T) {
	// Counts per 60s window: [3,3,0,0,3,3]. Threshold 3, trigger 2, recover 2.
	// Trigger at the end of w1 (10:02), recover at the end of w3 (10:04),
	// trigger again at the end of w5 (10:06), ending still alerting.
	tl := previewTimeline(t,
		Event{ID: "a1", Service: "svc", Severity: "critical", Message: "m", At: at(0, 10)},
		Event{ID: "a2", Service: "svc", Severity: "critical", Message: "m", At: at(0, 20)},
		Event{ID: "a3", Service: "svc", Severity: "critical", Message: "m", At: at(0, 30)},
		Event{ID: "b1", Service: "svc", Severity: "critical", Message: "m", At: at(1, 10)},
		Event{ID: "b2", Service: "svc", Severity: "critical", Message: "m", At: at(1, 20)},
		Event{ID: "b3", Service: "svc", Severity: "critical", Message: "m", At: at(1, 30)},
		Event{ID: "c1", Service: "svc", Severity: "critical", Message: "m", At: at(4, 10)},
		Event{ID: "c2", Service: "svc", Severity: "critical", Message: "m", At: at(4, 20)},
		Event{ID: "c3", Service: "svc", Severity: "critical", Message: "m", At: at(4, 30)},
		Event{ID: "d1", Service: "svc", Severity: "critical", Message: "m", At: at(5, 10)},
		Event{ID: "d2", Service: "svc", Severity: "critical", Message: "m", At: at(5, 20)},
		Event{ID: "d3", Service: "svc", Severity: "critical", Message: "m", At: at(5, 30)},
	)

	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(6, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(preview.Groups) != 1 {
		t.Fatalf("expected one group, got %d", len(preview.Groups))
	}
	group := preview.Groups[0]
	wantCounts := []int{3, 3, 0, 0, 3, 3}
	wantStates := []string{"normal", "alerting", "alerting", "normal", "normal", "alerting"}
	if len(group.Windows) != len(wantCounts) {
		t.Fatalf("windows: got %d, want %d", len(group.Windows), len(wantCounts))
	}
	for i := range wantCounts {
		w := group.Windows[i]
		if w.Count != wantCounts[i] {
			t.Errorf("window %d count: got %d, want %d", i, w.Count, wantCounts[i])
		}
		if w.State != wantStates[i] {
			t.Errorf("window %d state: got %q, want %q", i, w.State, wantStates[i])
		}
		if w.Since != at(i, 0) || w.Until != at(i+1, 0) {
			t.Errorf("window %d boundaries wrong: %v..%v", i, w.Since, w.Until)
		}
	}

	if len(preview.Alerts) != 2 {
		t.Fatalf("expected 2 alerts, got %d", len(preview.Alerts))
	}
	first := preview.Alerts[0]
	if first.TriggeredAt != at(2, 0) || first.RecoveredAt == nil || *first.RecoveredAt != at(4, 0) {
		t.Errorf("first alert: trigger=%v recover=%v", first.TriggeredAt, first.RecoveredAt)
	}
	second := preview.Alerts[1]
	if second.TriggeredAt != at(6, 0) || second.RecoveredAt != nil {
		t.Errorf("second alert: trigger=%v recover=%v (want nil)", second.TriggeredAt, second.RecoveredAt)
	}
}

func TestPreviewTriggerStreakResetsBelowThreshold(t *testing.T) {
	// [3,0,3,3], trigger 2: the single high window in w0 must not trigger;
	// w2/w3 then trigger at end of w3.
	tl := previewTimeline(t,
		Event{ID: "a1", Service: "svc", Severity: "critical", Message: "m", At: at(0, 10)},
		Event{ID: "a2", Service: "svc", Severity: "critical", Message: "m", At: at(0, 20)},
		Event{ID: "a3", Service: "svc", Severity: "critical", Message: "m", At: at(0, 30)},
		Event{ID: "b1", Service: "svc", Severity: "critical", Message: "m", At: at(2, 10)},
		Event{ID: "b2", Service: "svc", Severity: "critical", Message: "m", At: at(2, 20)},
		Event{ID: "b3", Service: "svc", Severity: "critical", Message: "m", At: at(2, 30)},
		Event{ID: "c1", Service: "svc", Severity: "critical", Message: "m", At: at(3, 10)},
		Event{ID: "c2", Service: "svc", Severity: "critical", Message: "m", At: at(3, 20)},
		Event{ID: "c3", Service: "svc", Severity: "critical", Message: "m", At: at(3, 30)},
	)
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(4, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	states := make([]string, len(preview.Groups[0].Windows))
	for i, w := range preview.Groups[0].Windows {
		states[i] = w.State
	}
	want := []string{"normal", "normal", "normal", "alerting"}
	for i := range want {
		if states[i] != want[i] {
			t.Errorf("window %d state: got %q, want %q", i, states[i], want[i])
		}
	}
	if len(preview.Alerts) != 1 || preview.Alerts[0].TriggeredAt != at(4, 0) {
		t.Errorf("alerts: %v", preview.Alerts)
	}
}

func TestPreviewRecoveryStreakResetsAtThreshold(t *testing.T) {
	// [3,3,0,3,0,0], recover 2: trigger at w1; w2 low, w3 high restarts the
	// recovery streak; w4/w5 then recover at end of w5.
	tl := previewTimeline(t,
		Event{ID: "a1", Service: "svc", Severity: "critical", Message: "m", At: at(0, 10)},
		Event{ID: "a2", Service: "svc", Severity: "critical", Message: "m", At: at(0, 20)},
		Event{ID: "a3", Service: "svc", Severity: "critical", Message: "m", At: at(0, 30)},
		Event{ID: "b1", Service: "svc", Severity: "critical", Message: "m", At: at(1, 10)},
		Event{ID: "b2", Service: "svc", Severity: "critical", Message: "m", At: at(1, 20)},
		Event{ID: "b3", Service: "svc", Severity: "critical", Message: "m", At: at(1, 30)},
		Event{ID: "c1", Service: "svc", Severity: "critical", Message: "m", At: at(3, 10)},
		Event{ID: "c2", Service: "svc", Severity: "critical", Message: "m", At: at(3, 20)},
		Event{ID: "c3", Service: "svc", Severity: "critical", Message: "m", At: at(3, 30)},
	)
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(6, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantStates := []string{"normal", "alerting", "alerting", "alerting", "alerting", "normal"}
	for i, w := range preview.Groups[0].Windows {
		if w.State != wantStates[i] {
			t.Errorf("window %d state: got %q, want %q", i, w.State, wantStates[i])
		}
	}
	if len(preview.Alerts) != 1 {
		t.Fatalf("alerts: %v", preview.Alerts)
	}
	if preview.Alerts[0].TriggeredAt != at(2, 0) {
		t.Errorf("trigger: %v", preview.Alerts[0].TriggeredAt)
	}
	if preview.Alerts[0].RecoveredAt == nil || *preview.Alerts[0].RecoveredAt != at(6, 0) {
		t.Errorf("recover: %v", preview.Alerts[0].RecoveredAt)
	}
}

func TestPreviewNoHistoryOutsideRange(t *testing.T) {
	// Events before since must not trigger; events at/after until are excluded.
	tl := previewTimeline(t,
		Event{ID: "before", Service: "svc", Severity: "critical", Message: "m", At: at(0, 30)},
		Event{ID: "on-start", Service: "svc", Severity: "critical", Message: "m", At: at(1, 0)},
		Event{ID: "in-1", Service: "svc", Severity: "critical", Message: "m", At: at(1, 10)},
		Event{ID: "in-2", Service: "svc", Severity: "critical", Message: "m", At: at(1, 20)},
		Event{ID: "on-end", Service: "svc", Severity: "critical", Message: "m", At: at(3, 0)},
	)
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(1, 0), Until: at(3, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Groups) != 1 {
		t.Fatalf("groups: %v", preview.Groups)
	}
	counts := []int{}
	for _, w := range preview.Groups[0].Windows {
		counts = append(counts, w.Count)
	}
	// [on-start + in-1 + in-2 = 3, 0]; the before-range event and the
	// at-end event are excluded.
	want := []int{3, 0}
	for i := range want {
		if counts[i] != want[i] {
			t.Errorf("window %d count: got %v, want %v", i, counts, want)
		}
	}
	if len(preview.Alerts) != 0 {
		t.Errorf("no alert should trigger from a single high window: %v", preview.Alerts)
	}
}

func TestPreviewGroupingAndSorting(t *testing.T) {
	// Two services; label values exercise nulls-first and lexicographic order.
	tl := previewTimeline(t,
		Event{ID: "z1", Service: "zeta", Severity: "critical", Message: "m", At: at(0, 10), Labels: map[string]string{"env": "prod", "region": "us"}},
		Event{ID: "z2", Service: "zeta", Severity: "critical", Message: "m", At: at(0, 20), Labels: map[string]string{"env": "prod", "region": "us"}},
		Event{ID: "z3", Service: "zeta", Severity: "critical", Message: "m", At: at(0, 30), Labels: map[string]string{"env": "prod", "region": "us"}},
		Event{ID: "a1", Service: "alpha", Severity: "critical", Message: "m", At: at(0, 10), Labels: map[string]string{"env": "prod"}},
		Event{ID: "a2", Service: "alpha", Severity: "critical", Message: "m", At: at(0, 20), Labels: map[string]string{"env": "prod"}},
		Event{ID: "a3", Service: "alpha", Severity: "critical", Message: "m", At: at(0, 30), Labels: map[string]string{"env": "prod"}},
		Event{ID: "a4", Service: "alpha", Severity: "critical", Message: "m", At: at(1, 10), Labels: map[string]string{"env": "prod", "region": "eu"}},
		Event{ID: "a5", Service: "alpha", Severity: "critical", Message: "m", At: at(1, 20), Labels: map[string]string{"env": "prod", "region": "eu"}},
		Event{ID: "a6", Service: "alpha", Severity: "critical", Message: "m", At: at(1, 30), Labels: map[string]string{"env": "prod", "region": "eu"}},
	)
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(2, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 1, RecoverWindows: 1,
		GroupBy: []string{"env", "region"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Groups) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(preview.Groups))
	}
	// Order: alpha/prod/missing, alpha/prod/eu, zeta/prod/us.
	got := [][3]string{}
	for _, g := range preview.Groups {
		got = append(got, [3]string{g.Service, *g.Labels["env"], labelValue(g.Labels, "region")})
	}
	want := [][3]string{
		{"alpha", "prod", ""},
		{"alpha", "prod", "eu"},
		{"zeta", "prod", "us"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("group %d: got %v, want %v", i, got[i], want[i])
		}
	}
	// The alpha/prod group has a null region.
	if preview.Groups[0].Labels["region"] != nil {
		t.Errorf("missing region should be nil, got %v", *preview.Groups[0].Labels["region"])
	}
	// Every group has both windows, zero-filled where no events matched.
	for _, g := range preview.Groups {
		if len(g.Windows) != 2 {
			t.Errorf("group %s has %d windows", g.Service, len(g.Windows))
		}
	}
	// Alerts sorted by trigger time: all three trigger at 10:01; the flat
	// list keeps group order for equal instants.
	if len(preview.Alerts) != 3 {
		t.Fatalf("alerts: %v", preview.Alerts)
	}
	for i := 1; i < len(preview.Alerts); i++ {
		if preview.Alerts[i].TriggeredAt.Before(preview.Alerts[i-1].TriggeredAt) {
			t.Errorf("alerts not sorted by trigger time: %v", preview.Alerts)
		}
	}
}

func labelValue(labels map[string]*string, name string) string {
	if v := labels[name]; v != nil {
		return *v
	}
	return ""
}

func TestPreviewEmptyResult(t *testing.T) {
	tl := NewTimeline()
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(2, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Groups) != 0 || len(preview.Alerts) != 0 {
		t.Errorf("empty timeline should give empty groups/alerts, got %v / %v", preview.Groups, preview.Alerts)
	}
}

func TestPreviewShortFinalWindow(t *testing.T) {
	tl := previewTimeline(t,
		Event{ID: "a1", Service: "svc", Severity: "critical", Message: "m", At: at(0, 10)},
		Event{ID: "a2", Service: "svc", Severity: "critical", Message: "m", At: at(0, 20)},
		Event{ID: "a3", Service: "svc", Severity: "critical", Message: "m", At: at(0, 30)},
		Event{ID: "b1", Service: "svc", Severity: "critical", Message: "m", At: at(1, 10)},
	)
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(1, 30), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	windows := preview.Groups[0].Windows
	if len(windows) != 2 {
		t.Fatalf("windows: %v", windows)
	}
	if windows[1].Since != at(1, 0) || windows[1].Until != at(1, 30) {
		t.Errorf("short final window boundaries: %v..%v", windows[1].Since, windows[1].Until)
	}
	if windows[1].Count != 1 {
		t.Errorf("short final window count: %d", windows[1].Count)
	}
}

func TestPreviewFilters(t *testing.T) {
	tl := previewTimeline(t,
		Event{ID: "p1", Service: "payments", Severity: "critical", Message: "m", At: at(0, 10), Labels: map[string]string{"env": "prod"}},
		Event{ID: "p2", Service: "payments", Severity: "critical", Message: "m", At: at(0, 20), Labels: map[string]string{"env": "prod"}},
		Event{ID: "p3", Service: "payments", Severity: "critical", Message: "m", At: at(0, 30), Labels: map[string]string{"env": "prod"}},
		Event{ID: "s1", Service: "payments", Severity: "info", Message: "m", At: at(0, 10), Labels: map[string]string{"env": "staging"}},
		Event{ID: "o1", Service: "orders", Severity: "critical", Message: "m", At: at(0, 10), Labels: map[string]string{"env": "prod"}},
	)
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Service: "payments", Severity: "critical", Labels: map[string]string{"env": "prod"},
		Since: at(0, 0), Until: at(1, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Groups) != 1 {
		t.Fatalf("groups: %v", preview.Groups)
	}
	if preview.Groups[0].Service != "payments" || preview.Groups[0].Windows[0].Count != 3 {
		t.Errorf("filtering wrong: %v", preview.Groups[0])
	}
}

func TestPreviewValidation(t *testing.T) {
	tl := NewTimeline()
	base := AlertPreviewQuery{
		Since: at(0, 0), Until: at(1, 0), Window: time.Minute,
		Threshold: 3, TriggerWindows: 2, RecoverWindows: 2,
	}
	cases := []struct {
		name string
		edit func(*AlertPreviewQuery)
		want error
	}{
		{"zero window", func(q *AlertPreviewQuery) { q.Window = 0 }, ErrPreviewWindow},
		{"inverted range", func(q *AlertPreviewQuery) { q.Until = q.Since }, ErrPreviewWindow},
		{"threshold zero", func(q *AlertPreviewQuery) { q.Threshold = 0 }, ErrPreviewThreshold},
		{"threshold too large", func(q *AlertPreviewQuery) { q.Threshold = 1000001 }, ErrPreviewThreshold},
		{"trigger zero", func(q *AlertPreviewQuery) { q.TriggerWindows = 0 }, ErrPreviewStreak},
		{"trigger too large", func(q *AlertPreviewQuery) { q.TriggerWindows = 101 }, ErrPreviewStreak},
		{"recover zero", func(q *AlertPreviewQuery) { q.RecoverWindows = 0 }, ErrPreviewStreak},
		{"recover too large", func(q *AlertPreviewQuery) { q.RecoverWindows = 101 }, ErrPreviewStreak},
		{"too many group by", func(q *AlertPreviewQuery) { q.GroupBy = []string{"a", "b", "c", "d", "e"} }, ErrPreviewGroupBy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := base
			tc.edit(&q)
			_, err := tl.PreviewAlerts(q)
			if err != tc.want {
				t.Errorf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPreviewTooManyWindows(t *testing.T) {
	tl := NewTimeline()
	_, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(0, 0).Add(10001 * time.Second), Window: time.Second,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != ErrPreviewTooManyWindows {
		t.Errorf("got %v, want ErrPreviewTooManyWindows", err)
	}
}

func TestPreviewTooManyGroups(t *testing.T) {
	tl := NewTimeline()
	// 10000 windows with 11 groups = 110000 cells.
	events := make([]Event, 0, 11)
	for i := 0; i < 11; i++ {
		events = append(events, Event{
			ID: "g" + string(rune('a'+i)), Service: "svc" + string(rune('a'+i)),
			Severity: "critical", Message: "m", At: at(0, 5),
		})
	}
	for _, event := range events {
		if _, err := tl.Ingest([]Event{event}, nil); err != nil {
			t.Fatal(err)
		}
	}
	_, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(0, 0).Add(10000 * time.Second), Window: time.Second,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != ErrPreviewTooManyGroups {
		t.Errorf("got %v, want ErrPreviewTooManyGroups", err)
	}
}

func TestPreviewReplayDoesNotDoubleCount(t *testing.T) {
	tl := NewTimeline()
	event := Event{ID: "dup", Service: "svc", Severity: "critical", Message: "m", At: at(0, 10)}
	for i := 0; i < 3; i++ {
		if _, err := tl.Ingest([]Event{event}, nil); err != nil {
			t.Fatal(err)
		}
	}
	preview, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: at(0, 0), Until: at(1, 0), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if preview.Groups[0].Windows[0].Count != 1 {
		t.Errorf("replayed event counted %d times", preview.Groups[0].Windows[0].Count)
	}
}
