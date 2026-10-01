package events

import (
	"errors"
	"testing"
	"time"
)

func alertTime(sec float64) time.Time {
	return time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC).Add(
		time.Duration(sec * float64(time.Second)))
}

func seedAlerts(t *testing.T, tl *Timeline, events []Event) {
	t.Helper()
	if _, err := tl.Ingest(events, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func mustEvent(id, service string, at time.Time, labels map[string]string) Event {
	return Event{ID: id, Service: service, Severity: "info", Message: "m", At: at, Labels: labels}
}

func TestPreviewTriggerRecoveryAndReTrigger(t *testing.T) {
	tl := NewTimeline()
	// Five 60s windows; threshold 2, trigger 2, recover 1.
	at := func(w, offset int) time.Time { return alertTime(float64(w*60 + offset)) }
	seedAlerts(t, tl, []Event{
		mustEvent("a1", "s", at(0, 5), nil), mustEvent("a2", "s", at(0, 30), nil),
		mustEvent("b1", "s", at(1, 5), nil), mustEvent("b2", "s", at(1, 40), nil),
		// w2 empty: recovery.
		mustEvent("d1", "s", at(3, 10), nil), mustEvent("d2", "s", at(3, 20), nil),
		mustEvent("e1", "s", at(4, 10), nil), mustEvent("e2", "s", at(4, 59), nil),
	})

	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(300), Window: time.Minute,
		Threshold: 2, TriggerWindows: 2, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(result.Groups) != 1 {
		t.Fatalf("one service => one group, got %d", len(result.Groups))
	}
	group := result.Groups[0]
	if len(group.Windows) != 5 {
		t.Fatalf("five windows, got %d", len(group.Windows))
	}
	wantStatus := []string{
		AlertStatusNormal,   // w0: trigger run 1
		AlertStatusAlerting, // w1: opens at 10:02
		AlertStatusNormal,   // w2: one quiet window recovers at 10:03
		AlertStatusNormal,   // w3: trigger run restarts
		AlertStatusAlerting, // w4: reopens at range end, unresolved
	}
	for i, window := range group.Windows {
		if window.Status != wantStatus[i] {
			t.Fatalf("w%d status = %s, want %s", i, window.Status, wantStatus[i])
		}
	}
	if group.Windows[2].Count != 0 {
		t.Fatalf("empty window must be zero-filled, got %d", group.Windows[2].Count)
	}
	// The 10:04:59 event stays inside the range; the last window is short of
	// nothing here (range is exactly five minutes), so verify counts.
	if group.Windows[4].Count != 2 {
		t.Fatalf("w4 count = %d", group.Windows[4].Count)
	}

	if len(group.Alerts) != 2 {
		t.Fatalf("two alert cycles, got %d: %+v", len(group.Alerts), group.Alerts)
	}
	first := group.Alerts[0]
	if !first.OpenedAt.Equal(alertTime(120)) {
		t.Fatalf("first opens at end of w1 (10:02), got %s", first.OpenedAt)
	}
	if first.RecoveredAt == nil || !first.RecoveredAt.Equal(alertTime(180)) {
		t.Fatalf("first recovers at end of w2 (10:03), got %v", first.RecoveredAt)
	}
	second := group.Alerts[1]
	if !second.OpenedAt.Equal(alertTime(300)) || second.RecoveredAt != nil {
		t.Fatalf("second opens at range end and stays open, got %+v", second)
	}
}

func TestPreviewConsecutiveRunsReset(t *testing.T) {
	tl := NewTimeline()
	at := func(w int) time.Time { return alertTime(float64(w * 60)) }
	// threshold 1, trigger 2: w0 met, w1 quiet (reset), w2 met, w3 met opens.
	seedAlerts(t, tl, []Event{
		mustEvent("a", "s", at(0).Add(5*time.Second), nil),
		mustEvent("c", "s", at(2).Add(5*time.Second), nil),
		mustEvent("d", "s", at(3).Add(5*time.Second), nil),
	})
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(300), Window: time.Minute,
		Threshold: 1, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	statuses := result.Groups[0].Windows
	for i, want := range []string{
		AlertStatusNormal, AlertStatusNormal, AlertStatusNormal, AlertStatusAlerting, AlertStatusAlerting,
	} {
		if statuses[i].Status != want {
			t.Fatalf("w%d = %s, want %s", i, statuses[i].Status, want)
		}
	}

	// Recovery run resets when a threshold-meeting window interrupts it:
	// trigger w0,w1 opens; w2 quiet (run 1), w3 met again (reset), w4 quiet
	// (run 1 only) => still alerting at the end, never recovered.
	tl = NewTimeline()
	seedAlerts(t, tl, []Event{
		mustEvent("a", "s", at(0).Add(5*time.Second), nil),
		mustEvent("b", "s", at(1).Add(5*time.Second), nil),
		mustEvent("d", "s", at(3).Add(5*time.Second), nil),
	})
	result, err = tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(300), Window: time.Minute,
		Threshold: 1, TriggerWindows: 2, RecoverWindows: 2,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	group := result.Groups[0]
	statuses = group.Windows
	want := []string{
		AlertStatusNormal, AlertStatusAlerting, AlertStatusAlerting, AlertStatusAlerting, AlertStatusAlerting,
	}
	for i := range want {
		if statuses[i].Status != want[i] {
			t.Fatalf("w%d = %s, want %s", i, statuses[i].Status, want[i])
		}
	}
	if len(group.Alerts) != 1 || group.Alerts[0].RecoveredAt != nil {
		t.Fatalf("interrupted recovery must not close the alert: %+v", group.Alerts)
	}
}

func TestPreviewDoesNotReopenWhileAlerting(t *testing.T) {
	tl := NewTimeline()
	at := func(w int) time.Time { return alertTime(float64(w*60 + 5)) }
	seedAlerts(t, tl, []Event{
		mustEvent("a", "s", at(0), nil), mustEvent("b", "s", at(1), nil),
		mustEvent("c", "s", at(2), nil), mustEvent("d", "s", at(3), nil),
		mustEvent("e", "s", at(4), nil),
	})
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(300), Window: time.Minute,
		Threshold: 1, TriggerWindows: 2, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	alerts := result.Groups[0].Alerts
	if len(alerts) != 1 || alerts[0].RecoveredAt != nil {
		t.Fatalf("continued threshold hits must not reopen: %+v", alerts)
	}
}

func TestPreviewHalfOpenRangeAndShortLastWindow(t *testing.T) {
	tl := NewTimeline()
	// Range 10:00:00..10:02:30 with 60s windows: three windows (60, 60, 30).
	// An event at exactly until is excluded; an event at since lands in w0.
	seedAlerts(t, tl, []Event{
		mustEvent("start", "s", alertTime(0), nil),
		mustEvent("end", "s", alertTime(150), nil),
		mustEvent("tail", "s", alertTime(149.5), nil),
	})
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(150), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	windows := result.Groups[0].Windows
	if len(windows) != 3 {
		t.Fatalf("three windows including the short tail, got %d", len(windows))
	}
	if !windows[2].End.Equal(alertTime(150)) {
		t.Fatalf("last window ends at range end, got %s", windows[2].End)
	}
	if windows[0].Count != 1 || windows[1].Count != 0 || windows[2].Count != 1 {
		t.Fatalf("half-open counts wrong: %d %d %d", windows[0].Count, windows[1].Count, windows[2].Count)
	}
}

func TestPreviewGroupingByLabelsWithNullFirst(t *testing.T) {
	tl := NewTimeline()
	seed := func(id, service string, w int, labels map[string]string) {
		seedAlerts(t, tl, []Event{mustEvent(id, service, alertTime(float64(w*60+5)), labels)})
	}
	seed("g1", "gateway", 0, map[string]string{"env": "prod"})
	seed("g2", "gateway", 0, map[string]string{"env": "dev"})
	seed("g3", "gateway", 0, nil) // missing env
	seed("g4", "api", 0, map[string]string{"env": "prod"})
	seed("g5", "api", 0, map[string]string{"env": "prod", "region": "cn"}) // group env only: still prod

	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(120), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
		GroupLabels: []string{"env"},
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(result.GroupLabelNames) != 1 || result.GroupLabelNames[0] != "env" {
		t.Fatalf("group names: %v", result.GroupLabelNames)
	}
	// api/prod, gateway/null, gateway/dev, gateway/prod — service first, then
	// null before strings, strings ascending.
	want := []struct {
		service string
		value   *string
	}{
		{"api", strPtr("prod")},
		{"gateway", nil},
		{"gateway", strPtr("dev")},
		{"gateway", strPtr("prod")},
	}
	if len(result.Groups) != len(want) {
		t.Fatalf("got %d groups, want %d", len(result.Groups), len(want))
	}
	for i, g := range result.Groups {
		if g.Service != want[i].service {
			t.Fatalf("group %d service = %s, want %s", i, g.Service, want[i].service)
		}
		if (g.LabelValues[0] == nil) != (want[i].value == nil) ||
			(g.LabelValues[0] != nil && *g.LabelValues[0] != *want[i].value) {
			t.Fatalf("group %d value = %v, want %v", i, g.LabelValues[0], want[i].value)
		}
		// api/prod collects both the plain prod event and the prod+region
		// one because only env is used for grouping.
		wantCount := 1
		if i == 0 {
			wantCount = 2
		}
		if len(g.Windows) != 2 || g.Windows[0].Count != wantCount || g.Windows[1].Count != 0 {
			t.Fatalf("group %d must have zero-filled windows: %+v", i, g.Windows)
		}
	}
}

func TestPreviewMultipleGroupLabelsSorted(t *testing.T) {
	tl := NewTimeline()
	seedAlerts(t, tl, []Event{
		mustEvent("a", "s", alertTime(5), map[string]string{"env": "prod", "region": "east"}),
		mustEvent("b", "s", alertTime(5), map[string]string{"env": "prod"}), // region missing
		mustEvent("c", "s", alertTime(5), map[string]string{"env": "prod", "region": "east", "zone": "z"}),
	})
	// Request names out of lexicographic order; response sorts them.
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(60), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
		GroupLabels: []string{"zone", "region", "env"},
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(result.GroupLabelNames) != 3 ||
		result.GroupLabelNames[0] != "env" || result.GroupLabelNames[1] != "region" || result.GroupLabelNames[2] != "zone" {
		t.Fatalf("names must be sorted: %v", result.GroupLabelNames)
	}
	if len(result.Groups) != 3 {
		t.Fatalf("three combos, got %d", len(result.Groups))
	}
	// All share env=prod; null region (and null zone) sorts first, then
	// region=east/null zone, then region=east/zone=z.
	first := result.Groups[0].LabelValues
	if first[0] == nil || *first[0] != "prod" || first[1] != nil || first[2] != nil {
		t.Fatalf("missing region/zone group first: %v", first)
	}
}

func TestPreviewEmptyReturnsEmptyGroups(t *testing.T) {
	tl := NewTimeline()
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(60), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(result.Groups) != 0 {
		t.Fatalf("no matching events => no groups, got %d", len(result.Groups))
	}
}

func TestPreviewFiltersUseQueryRules(t *testing.T) {
	tl := NewTimeline()
	seedAlerts(t, tl, []Event{
		mustEvent("a", "gateway", alertTime(5), map[string]string{"env": "prod"}),
		mustEvent("b", "api", alertTime(5), map[string]string{"env": "prod"}),
		mustEvent("c", "gateway", alertTime(5), map[string]string{"env": "dev"}),
	})
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(60), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
		Service: " gateway ", Severity: " INFO ", Labels: map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(result.Groups) != 1 || result.Groups[0].Service != "gateway" {
		t.Fatalf("trimmed service + case-insensitive severity + labels must leave one event: %+v", result.Groups)
	}
}

func TestPreviewValidation(t *testing.T) {
	base := func() AlertPreviewQuery {
		return AlertPreviewQuery{
			Since: alertTime(0), Until: alertTime(60), Window: time.Minute,
			Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
		}
	}
	cases := map[string]func(AlertPreviewQuery) AlertPreviewQuery{
		"inverted range": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Since, q.Until = q.Until, q.Since
			return q
		},
		"zero length range": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Until = q.Since
			return q
		},
		"window zero": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Window = 0
			return q
		},
		"window fractional": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Window = time.Second + time.Nanosecond
			return q
		},
		"window too large": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Window = 86401 * time.Second
			return q
		},
		"threshold zero": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Threshold = 0
			return q
		},
		"threshold too large": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.Threshold = 1000001
			return q
		},
		"trigger zero": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.TriggerWindows = 0
			return q
		},
		"recover too large": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.RecoverWindows = 101
			return q
		},
		"too many group labels": func(q AlertPreviewQuery) AlertPreviewQuery {
			q.GroupLabels = []string{"a", "b", "c", "d", "e"}
			return q
		},
	}
	wantErr := map[string]error{
		"inverted range":        ErrAlertRange,
		"zero length range":     ErrAlertRange,
		"window zero":           ErrAlertWindow,
		"window fractional":     ErrAlertWindow,
		"window too large":      ErrAlertWindow,
		"threshold zero":        ErrAlertThreshold,
		"threshold too large":   ErrAlertThreshold,
		"trigger zero":          ErrAlertConsecutive,
		"recover too large":     ErrAlertConsecutive,
		"too many group labels": ErrAlertGroupLabels,
	}
	tl := NewTimeline()
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := tl.PreviewAlerts(mutate(base()))
			if !errors.Is(err, wantErr[name]) {
				t.Fatalf("got %v, want %v", err, wantErr[name])
			}
		})
	}

	// Window-count limit: exactly 10000 one-second windows is allowed; one
	// nanosecond more needs 10001 windows and is rejected.
	ok := base()
	ok.Since = alertTime(0)
	ok.Until = alertTime(10000)
	ok.Window = time.Second
	if _, err := tl.PreviewAlerts(ok); err != nil {
		t.Fatalf("10000 windows must be allowed: %v", err)
	}
	over := ok
	over.Until = alertTime(10000).Add(time.Nanosecond)
	if _, err := tl.PreviewAlerts(over); !errors.Is(err, ErrAlertTooManyWindows) {
		t.Fatalf("10001 windows: got %v, want ErrAlertTooManyWindows", err)
	}
}

func TestPreviewRejectsTooManyGroupWindows(t *testing.T) {
	tl := NewTimeline()
	// 10000 one-second windows with eleven distinct services => 110000 group
	// windows, over the 100000 cap.
	events := make([]Event, 0, 11)
	for i := 0; i < 11; i++ {
		events = append(events, mustEvent(
			"e"+string(rune('a'+i)),
			"s"+string(rune('a'+i)),
			alertTime(5), nil))
	}
	seedAlerts(t, tl, events)
	q := AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(10000), Window: time.Second,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	}
	if _, err := tl.PreviewAlerts(q); !errors.Is(err, ErrAlertTooManyGroups) {
		t.Fatalf("got %v, want ErrAlertTooManyGroups", err)
	}

	// Ten services hit exactly 100000 and must succeed.
	tl2 := NewTimeline()
	ten := make([]Event, 0, 10)
	for i := 0; i < 10; i++ {
		ten = append(ten, mustEvent(
			"e"+string(rune('a'+i)),
			"s"+string(rune('a'+i)),
			alertTime(5), nil))
	}
	seedAlerts(t, tl2, ten)
	result, err := tl2.PreviewAlerts(q)
	if err != nil {
		t.Fatalf("exactly 100000 group windows must pass: %v", err)
	}
	if len(result.Groups) != 10 {
		t.Fatalf("ten groups, got %d", len(result.Groups))
	}
}

func TestPreviewLateEventsReevaluatedByEventTime(t *testing.T) {
	tl := NewTimeline()
	baseQ := AlertPreviewQuery{
		Since: alertTime(0), Until: alertTime(180), Window: time.Minute,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	}
	first, err := tl.PreviewAlerts(baseQ)
	if err != nil || len(first.Groups) != 0 {
		t.Fatalf("empty preview: %v %v", first, err)
	}
	// A late-arriving event belongs to w0 by event time and shows up there on
	// the next preview without changing the window skeleton.
	seedAlerts(t, tl, []Event{mustEvent("late", "s", alertTime(5), nil)})
	second, err := tl.PreviewAlerts(baseQ)
	if err != nil {
		t.Fatalf("second preview: %v", err)
	}
	if second.Groups[0].Windows[0].Count != 1 || second.Groups[0].Windows[1].Count != 0 {
		t.Fatalf("late event must land in its event-time window: %+v", second.Groups[0].Windows)
	}

	// Replaying the same event changes nothing.
	seedAlerts(t, tl, []Event{mustEvent("late", "s", alertTime(5), nil)})
	third, err := tl.PreviewAlerts(baseQ)
	if err != nil {
		t.Fatalf("replay preview: %v", err)
	}
	if third.Groups[0].Windows[0].Count != 1 {
		t.Fatalf("replayed event must not double-count: %d", third.Groups[0].Windows[0].Count)
	}
}

func TestPreviewBoundaryTimestampsPreserveNanoseconds(t *testing.T) {
	tl := NewTimeline()
	start := time.Date(2026, 10, 1, 10, 0, 0, 123456789, time.UTC)
	seedAlerts(t, tl, []Event{mustEvent("a", "s", start.Add(5*time.Nanosecond), nil)})
	result, err := tl.PreviewAlerts(AlertPreviewQuery{
		Since: start, Until: start.Add(2 * time.Second), Window: time.Second,
		Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	windows := result.Groups[0].Windows
	if !windows[0].Start.Equal(start) || windows[0].Start.Nanosecond() != 123456789 {
		t.Fatalf("nanosecond start lost: %s", windows[0].Start)
	}
}

func strPtr(s string) *string { return &s }
