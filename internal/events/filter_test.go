package events

import (
	"testing"
	"time"
)

func filterEvent(id, service, severity string, labels map[string]string) Event {
	return Event{
		ID:       id,
		Service:  service,
		Severity: severity,
		Message:  "m",
		At:       time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC),
		Labels:   labels,
	}
}

func TestFilterNormalization(t *testing.T) {
	f := NewFilter("  gateway ", " CRITICAL ", map[string]string{"env": "prod"})
	if f.Service() != "gateway" {
		t.Fatalf("service must be trimmed and keep case, got %q", f.Service())
	}
	if f.Severity() != "critical" {
		t.Fatalf("severity must be trimmed and lowercased, got %q", f.Severity())
	}
	if got := f.Labels(); len(got) != 1 || got["env"] != "prod" {
		t.Fatalf("labels must be retained, got %v", got)
	}

	// Missing or blank service/severity impose no limit; an empty label set is
	// stored as nil so "no label conditions" has one spelling internally.
	empty := NewFilter("   ", "\t", map[string]string{})
	if empty.Service() != "" || empty.Severity() != "" || empty.Labels() != nil {
		t.Fatalf("blank conditions must normalize to unrestricted, got %+v", empty)
	}

	var zero Filter
	if zero.Service() != "" || zero.Severity() != "" || zero.Labels() != nil {
		t.Fatalf("zero Filter must be unrestricted, got %+v", zero)
	}
}

func TestFilterMatches(t *testing.T) {
	gwProd := filterEvent("e1", "gateway", "critical", map[string]string{"env": "prod", "region": "cn1"})
	gwProdNoRegion := filterEvent("e2", "gateway", "critical", map[string]string{"env": "prod"})
	gwStage := filterEvent("e3", "gateway", "warning", map[string]string{"env": "stage"})
	otherProd := filterEvent("e4", "checkout", "critical", map[string]string{"env": "prod"})
	noLabels := filterEvent("e5", "gateway", "info", nil)
	gatewayUpper := filterEvent("e6", "Gateway", "critical", map[string]string{"env": "prod"})

	all := []Event{gwProd, gwProdNoRegion, gwStage, otherProd, noLabels, gatewayUpper}

	matchedIDs := func(f Filter) []string {
		var ids []string
		for _, event := range all {
			if f.Matches(event) {
				ids = append(ids, event.ID)
			}
		}
		return ids
	}

	cases := []struct {
		name   string
		filter Filter
		want   []string
	}{
		{
			name:   "zero filter matches all",
			filter: Filter{},
			want:   []string{"e1", "e2", "e3", "e4", "e5", "e6"},
		},
		{
			name:   "service exact and case sensitive after trim",
			filter: NewFilter(" gateway ", "", nil),
			want:   []string{"e1", "e2", "e3", "e5"},
		},
		{
			name:   "blank service unrestricted",
			filter: NewFilter("   ", "", nil),
			want:   []string{"e1", "e2", "e3", "e4", "e5", "e6"},
		},
		{
			name:   "severity case insensitive after trim",
			filter: NewFilter("", "  CrItIcAl ", nil),
			want:   []string{"e1", "e2", "e4", "e6"},
		},
		{
			name:   "blank severity unrestricted",
			filter: NewFilter("", "  ", nil),
			want:   []string{"e1", "e2", "e3", "e4", "e5", "e6"},
		},
		{
			name:   "single label condition",
			filter: NewFilter("", "", map[string]string{"env": "prod"}),
			want:   []string{"e1", "e2", "e4", "e6"},
		},
		{
			name:   "labels are ANDed",
			filter: NewFilter("", "", map[string]string{"env": "prod", "region": "cn1"}),
			want:   []string{"e1"},
		},
		{
			name:   "missing label never matches",
			filter: NewFilter("", "", map[string]string{"zone": "us"}),
			want:   nil,
		},
		{
			name:   "label name and value are case sensitive",
			filter: NewFilter("", "", map[string]string{"env": "Prod"}),
			want:   nil,
		},
		{
			name:   "service + severity + labels all required",
			filter: NewFilter("gateway", "CRITICAL", map[string]string{"env": "prod"}),
			want:   []string{"e1", "e2"},
		},
		{
			name:   "combined filter excludes events missing the label",
			filter: NewFilter("gateway", "critical", map[string]string{"region": "cn1"}),
			want:   []string{"e1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchedIDs(tc.filter)
			if !equalStringSlices(got, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestFilterLabelOrderIrrelevant(t *testing.T) {
	events := []Event{
		filterEvent("a", "gateway", "critical", map[string]string{"env": "prod", "region": "cn1"}),
		filterEvent("b", "gateway", "critical", map[string]string{"region": "cn1", "env": "prod"}),
		filterEvent("c", "gateway", "critical", map[string]string{"env": "prod"}),
	}

	first := NewFilter("gateway", "critical", map[string]string{"env": "prod", "region": "cn1"})
	second := NewFilter("gateway", "critical", map[string]string{"region": "cn1", "env": "prod"})

	var firstIDs, secondIDs []string
	for _, event := range events {
		if first.Matches(event) {
			firstIDs = append(firstIDs, event.ID)
		}
		if second.Matches(event) {
			secondIDs = append(secondIDs, event.ID)
		}
	}
	if !equalStringSlices(firstIDs, secondIDs) || !equalStringSlices(firstIDs, []string{"a", "b"}) {
		t.Fatalf("label condition order must not change the match set: %v vs %v", firstIDs, secondIDs)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
