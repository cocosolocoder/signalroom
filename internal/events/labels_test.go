package events

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeLabelsAbsenceSpellings(t *testing.T) {
	for name, labels := range map[string]map[string]string{
		"nil":   nil,
		"empty": {},
	} {
		got, err := NormalizeLabels(labels)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != nil {
			t.Fatalf("%s: want nil, got %v", name, got)
		}
	}
}

func TestNormalizeLabelsTrimsAndKeepsCase(t *testing.T) {
	got, err := NormalizeLabels(map[string]string{"  Env ": " Prod ", "region": "cn-north 1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["Env"] != "Prod" || got["region"] != "cn-north 1" {
		t.Fatalf("trimming/case wrong: %v", got)
	}
}

func TestNormalizeLabelsRejectsInvalid(t *testing.T) {
	tooMany := make(map[string]string, MaxLabelsPerEvent+1)
	for i := 0; i < MaxLabelsPerEvent+1; i++ {
		tooMany[string(rune('a'+i))] = "v"
	}
	cases := map[string]map[string]string{
		"empty name after trim":  {"   ": "v"},
		"empty value after trim": {"env": "  "},
		"name too long":          {strings.Repeat("n", MaxLabelNameRunes+1): "v"},
		"value too long":         {"env": strings.Repeat("v", MaxLabelValueRunes+1)},
		"too many labels":        tooMany,
		"trim collision":         {"env": "prod", " env ": "prod"},
		"trim collision differ":  {"env": "prod", "env\t": "stage"},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeLabels(labels); !IsValidationError(err) {
				t.Fatalf("want ValidationError, got %v", err)
			}
		})
	}
}

func TestNormalizeLabelsAllowsLimitsAndUnicode(t *testing.T) {
	exact := make(map[string]string, MaxLabelsPerEvent)
	for i := 0; i < MaxLabelsPerEvent-2; i++ {
		exact[string(rune('a'+i))] = "v"
	}
	exact["名字"] = strings.Repeat("值", MaxLabelValueRunes)
	exact[strings.Repeat("名", MaxLabelNameRunes-1)+"x"] = "v"
	got, err := NormalizeLabels(exact)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != MaxLabelsPerEvent {
		t.Fatalf("unexpected count: %d", len(got))
	}
}

func TestParseLabelConditions(t *testing.T) {
	got, err := ParseLabelConditions([]string{" env = prod ", "version=v2=rc1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["env"] != "prod" || got["version"] != "v2=rc1" {
		t.Fatalf("conditions wrong: %v", got)
	}
	if none, err := ParseLabelConditions(nil); err != nil || none != nil {
		t.Fatalf("no params must give nil, got %v, %v", none, err)
	}
}

func TestParseLabelConditionsRejectsInvalid(t *testing.T) {
	tooMany := make([]string, MaxLabelFilters+1)
	for i := range tooMany {
		tooMany[i] = string(rune('a'+i)) + "=v"
	}
	cases := map[string][]string{
		"too many":          tooMany,
		"empty condition":   {""},
		"no equals":         {"env"},
		"empty name":        {"=prod"},
		"blank name":        {"   =prod"},
		"empty value":       {"env="},
		"blank value":       {"env=  "},
		"name too long":     {strings.Repeat("n", MaxLabelNameRunes+1) + "=v"},
		"value too long":    {"env=" + strings.Repeat("v", MaxLabelValueRunes+1)},
		"exact duplicate":   {"env=prod", "env=prod"},
		"trimmed duplicate": {"env=prod", " env =stage"},
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseLabelConditions(raw); !IsValidationError(err) {
				t.Fatalf("want ValidationError, got %v", err)
			}
		})
	}
}

func ingestOne(t *testing.T, tl *Timeline, event Event) IngestResult {
	t.Helper()
	result, err := tl.Ingest([]Event{event}, nil)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return result
}

func TestIngestLabelReplayEquivalence(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	base := Event{ID: "e1", Service: "svc", Severity: "info", Message: "m", At: at,
		Labels: map[string]string{"env": "prod", "version": "v2"}}
	if got := ingestOne(t, tl, base); got.Created != 1 {
		t.Fatalf("created: %+v", got)
	}

	// Reordered keys with surrounding whitespace are the same labels.
	retry := base
	retry.Labels = map[string]string{" version ": " v2", "env": "prod "}
	if got := ingestOne(t, tl, retry); got.Replayed != 1 || got.Created != 0 {
		t.Fatalf("whitespace/order retry must replay: %+v", got)
	}

	// Any added, removed, or changed label conflicts.
	for name, labels := range map[string]map[string]string{
		"added":   {"env": "prod", "version": "v2", "region": "cn1"},
		"removed": {"env": "prod"},
		"changed": {"env": "prod", "version": "v3"},
		"case":    {"env": "Prod", "version": "v2"},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.Labels = labels
			if _, err := tl.Ingest([]Event{changed}, nil); !IsConflictError(err) {
				t.Fatalf("want ConflictError, got %v", err)
			}
		})
	}
}

func TestIngestNoLabelSpellingsEquivalent(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	plain := Event{ID: "e1", Service: "svc", Severity: "info", Message: "m", At: at}
	if got := ingestOne(t, tl, plain); got.Created != 1 {
		t.Fatalf("created: %+v", got)
	}
	withEmpty := plain
	withEmpty.Labels = map[string]string{}
	if got := ingestOne(t, tl, withEmpty); got.Replayed != 1 {
		t.Fatalf("empty labels must replay a label-less event: %+v", got)
	}
	// And the reverse order: stored with empty set, retried without.
	tl2 := NewTimeline()
	if got := ingestOne(t, tl2, withEmpty); got.Created != 1 {
		t.Fatalf("created: %+v", got)
	}
	if got := ingestOne(t, tl2, plain); got.Replayed != 1 {
		t.Fatalf("missing labels must replay an empty-labels event: %+v", got)
	}
}

func TestQueryLabelFilters(t *testing.T) {
	tl := NewTimeline()
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	events := []Event{
		{ID: "e1", Service: "svc", Severity: "info", Message: "m", At: at, Labels: map[string]string{"env": "prod", "version": "v2"}},
		{ID: "e2", Service: "svc", Severity: "info", Message: "m", At: at, Labels: map[string]string{"env": "prod"}},
		{ID: "e3", Service: "svc", Severity: "info", Message: "m", At: at},
	}
	for _, event := range events {
		ingestOne(t, tl, event)
	}

	cases := []struct {
		name  string
		query Query
		want  []string
	}{
		{"no filter", Query{}, []string{"e1", "e2", "e3"}},
		{"single", Query{Labels: map[string]string{"env": "prod"}}, []string{"e1", "e2"}},
		{"multiple AND", Query{Labels: map[string]string{"env": "prod", "version": "v2"}}, []string{"e1"}},
		{"missing label", Query{Labels: map[string]string{"region": "cn1"}}, nil},
		{"case sensitive", Query{Labels: map[string]string{"env": "Prod"}}, nil},
		{"combined", Query{Service: "svc", Labels: map[string]string{"version": "v2"}}, []string{"e1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tl.Query(tc.query)
			if len(got) != len(tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
			for i, event := range got {
				if event.ID != tc.want[i] {
					t.Fatalf("want %v, got %v", tc.want, got)
				}
			}
		})
	}
}

func TestCompareLabelFilters(t *testing.T) {
	tl := NewTimeline()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	events := []Event{
		{ID: "b1", Service: "svc", Severity: "info", Message: "m", At: base, Labels: map[string]string{"env": "prod"}},
		{ID: "b2", Service: "svc", Severity: "info", Message: "m", At: base, Labels: map[string]string{"env": "stage"}},
		{ID: "o1", Service: "svc", Severity: "info", Message: "m", At: base.Add(time.Hour), Labels: map[string]string{"env": "prod"}},
		{ID: "o2", Service: "svc", Severity: "info", Message: "m", At: base.Add(time.Hour)},
	}
	for _, event := range events {
		ingestOne(t, tl, event)
	}
	query := CompareQuery{
		Labels:        map[string]string{"env": "prod"},
		BaselineSince: base, BaselineUntil: base.Add(10 * time.Minute),
		Since: base.Add(time.Hour), Until: base.Add(time.Hour + 10*time.Minute),
		Step: 5 * time.Minute,
	}
	result, err := tl.Compare(query)
	if err != nil {
		t.Fatal(err)
	}
	if result.BaselineTotal != 1 || result.ObservationTotal != 1 {
		t.Fatalf("label filter must restrict both sides: %+v", result)
	}
	if result.Segments[0].BaselineCount != 1 || result.Segments[0].ObservationCount != 1 {
		t.Fatalf("segment counts: %+v", result.Segments[0])
	}
}
