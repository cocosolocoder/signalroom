package events

import "strings"

// Filter is the one normalized form of the optional service, severity, and
// label conditions shared by every event selection: the plain list, paged
// snapshot capture, window comparison, and the alert preview. Build it once
// with NewFilter and use Matches in every scan so a change to the rules is
// made in exactly one place.
//
// The zero Filter matches every event. A Filter value is immutable after
// construction; the Labels map must be treated as read-only.
type Filter struct {
	service  string
	severity string
	labels   map[string]string
}

// NewFilter normalizes the three optional conditions the same way every
// feature applies them:
//
//   - Service is trimmed of surrounding whitespace and then matched exactly,
//     keeping its case. A missing or blank service imposes no limit.
//   - Severity is trimmed of surrounding whitespace and matched
//     case-insensitively (stored lowercased). A missing or blank severity
//     imposes no limit.
//   - Labels keep the rules from ParseLabelConditions /
//     NormalizeLabelConditions: names and values are case-sensitive, an event
//     missing a conditioned label never matches, and every condition must
//     hold (AND). A nil or empty condition set imposes no label limit.
//
// Conditions are conjunctive: only events satisfying all of service,
// severity, and every label condition match.
func NewFilter(service, severity string, labels map[string]string) Filter {
	f := Filter{
		service:  strings.TrimSpace(service),
		severity: strings.ToLower(strings.TrimSpace(severity)),
	}
	if len(labels) > 0 {
		f.labels = labels
	}
	return f
}

// Matches reports whether event satisfies all of the filter's service,
// severity, and label conditions. Time-window membership is intentionally
// not part of the filter: each feature keeps its own boundary rule
// (inclusive at both ends for the list and paging, half-open for comparison
// and the alert preview).
func (f Filter) Matches(event Event) bool {
	if f.service != "" && event.Service != f.service {
		return false
	}
	if f.severity != "" && event.Severity != f.severity {
		return false
	}
	return labelsMatch(event.Labels, f.labels)
}

// Service returns the normalized (trimmed, case-preserving) service
// condition, or "" when service is not limited.
func (f Filter) Service() string { return f.service }

// Severity returns the normalized (trimmed and lowercased) severity
// condition, or "" when severity is not limited.
func (f Filter) Severity() string { return f.severity }

// Labels returns the label condition set as a read-only map, or nil when
// labels are not limited. Callers must not modify the returned map.
func (f Filter) Labels() map[string]string { return f.labels }
