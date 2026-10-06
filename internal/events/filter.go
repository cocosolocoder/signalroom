package events

import "strings"

// Filter is the normalized form of the optional service, severity, and
// label conditions every event-selecting feature shares: the plain list,
// pagination snapshots, window comparison, and the alert preview all match
// events against exactly one rule set, so the rules live here once.
//
// The service is trimmed of surrounding whitespace and matched exactly,
// keeping case; the severity is trimmed and lowercased, so matching ignores
// case. An empty service or severity after trimming means the condition does
// not restrict the result. Labels carry the already parsed conditions (from
// ParseLabelConditions or NormalizeLabelConditions); a nil set imposes no
// label restriction. An event participates only when it satisfies every
// condition at once.
type Filter struct {
	Service  string
	Severity string
	Labels   map[string]string
}

// NewFilter normalizes raw service and severity strings into the shared
// filter form. labels must already follow the condition parsing rules.
func NewFilter(service, severity string, labels map[string]string) Filter {
	return Filter{
		Service:  strings.TrimSpace(service),
		Severity: strings.ToLower(strings.TrimSpace(severity)),
		Labels:   labels,
	}
}

// Matches reports whether event satisfies every condition of the filter.
// Label names and values compare case-sensitively, and an event missing a
// conditioned label never matches.
func (f Filter) Matches(event Event) bool {
	if f.Service != "" && event.Service != f.Service {
		return false
	}
	if f.Severity != "" && event.Severity != f.Severity {
		return false
	}
	return labelsMatch(event.Labels, f.Labels)
}

// Filter returns the shared service, severity, and label conditions of the
// query. The time bounds stay with the caller: each feature applies its own
// inclusion rules on top of this filter.
func (q Query) Filter() Filter {
	return NewFilter(q.Service, q.Severity, q.Labels)
}

// Filter returns the shared service, severity, and label conditions of the
// comparison. Window membership stays with Compare.
func (q CompareQuery) Filter() Filter {
	return NewFilter(q.Service, q.Severity, q.Labels)
}

// Filter returns the shared service, severity, and label conditions of the
// preview. Window membership stays with PreviewAlerts.
func (q AlertPreviewQuery) Filter() Filter {
	return NewFilter(q.Service, q.Severity, q.Labels)
}
