package events

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Label limits shared by ingestion and query filters. Names and values are
// trimmed of surrounding whitespace, keep their case, and must be non-empty
// afterwards; lengths are counted in Unicode code points.
const (
	// MaxLabelsPerEvent caps how many labels a single event may carry.
	MaxLabelsPerEvent = 32
	// MaxLabelNameRunes bounds a label name in Unicode code points.
	MaxLabelNameRunes = 64
	// MaxLabelValueRunes bounds a label value in Unicode code points.
	MaxLabelValueRunes = 256
	// MaxLabelFilters caps how many label conditions one query may carry.
	MaxLabelFilters = 32
)

// NormalizeLabels trims and validates a label set. A nil or empty set
// normalizes to nil, so the three "no labels" spellings (absent, null, {})
// are indistinguishable once stored. Names that differ only in surrounding
// whitespace collide and are rejected, as are over-long or empty names and
// values.
func NormalizeLabels(labels map[string]string) (map[string]string, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	if len(labels) > MaxLabelsPerEvent {
		return nil, &ValidationError{Reason: fmt.Sprintf("event must not have more than %d labels", MaxLabelsPerEvent)}
	}
	normalized := make(map[string]string, len(labels))
	for name, value := range labels {
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if err := checkLabel(name, value); err != nil {
			return nil, err
		}
		if _, dup := normalized[name]; dup {
			return nil, &ValidationError{Reason: fmt.Sprintf("label name %q repeats after trimming", name)}
		}
		normalized[name] = value
	}
	return normalized, nil
}

// checkLabel enforces the name/value rules every label and every label
// filter condition follows.
func checkLabel(name, value string) error {
	if name == "" {
		return &ValidationError{Reason: "label name must not be empty"}
	}
	if value == "" {
		return &ValidationError{Reason: "label value must not be empty"}
	}
	if utf8.RuneCountInString(name) > MaxLabelNameRunes {
		return &ValidationError{Reason: fmt.Sprintf("label name must not exceed %d characters", MaxLabelNameRunes)}
	}
	if utf8.RuneCountInString(value) > MaxLabelValueRunes {
		return &ValidationError{Reason: fmt.Sprintf("label value must not exceed %d characters", MaxLabelValueRunes)}
	}
	return nil
}

// ParseLabelConditions turns repeated "name=value" query parameters into a
// normalized condition set. The first '=' splits name from value; later
// equals signs belong to the value. Names and values follow the ingestion
// trimming and limit rules, and conditions whose names collide after
// trimming are rejected.
func ParseLabelConditions(raw []string) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > MaxLabelFilters {
		return nil, &ValidationError{Reason: fmt.Sprintf("at most %d label filters are allowed", MaxLabelFilters)}
	}
	conditions := make(map[string]string, len(raw))
	for _, item := range raw {
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			return nil, &ValidationError{Reason: "label filter must use the form name=value"}
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if err := checkLabel(name, value); err != nil {
			return nil, err
		}
		if _, dup := conditions[name]; dup {
			return nil, &ValidationError{Reason: fmt.Sprintf("label filter %q repeats", name)}
		}
		conditions[name] = value
	}
	return conditions, nil
}

// labelsMatch reports whether a stored label set satisfies every condition.
// An event missing a conditioned label never matches, and comparison is
// case-sensitive on both names and values.
func labelsMatch(labels, conditions map[string]string) bool {
	for name, want := range conditions {
		got, ok := labels[name]
		if !ok || got != want {
			return false
		}
	}
	return true
}
