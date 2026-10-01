package events

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// Event is a normalized operational signal recorded on the incident
// timeline. Labels carries optional trimmed metadata (environment, region,
// release version, ...); it is nil when the event has none.
type Event struct {
	ID       string
	Service  string
	Severity string
	Message  string
	At       time.Time
	Labels   map[string]string
}

// Query selects events by optional service, severity, and label filters.
type Query struct {
	Service  string
	Severity string
	Since    time.Time
	Until    time.Time
	Labels   map[string]string
}

// ValidationError reports that an event failed normalization.
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// IsValidationError reports whether err is an event validation failure.
func IsValidationError(err error) bool {
	var target *ValidationError
	return errors.As(err, &target)
}

// ConflictError reports that an event id clashes with a different event.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return e.Reason }

// IsConflictError reports whether err is an id-content conflict.
func IsConflictError(err error) bool {
	var target *ConflictError
	return errors.As(err, &target)
}

// Normalize applies the same field rules every ingestion path uses: trim
// surrounding whitespace from strings, lowercase severity, and require a
// non-empty id, service, and message together with a non-zero instant.
// Labels are trimmed and validated by NormalizeLabels.
func Normalize(event Event) (Event, error) {
	event.ID = strings.TrimSpace(event.ID)
	event.Service = strings.TrimSpace(event.Service)
	event.Severity = strings.ToLower(strings.TrimSpace(event.Severity))
	event.Message = strings.TrimSpace(event.Message)
	if event.ID == "" || event.Service == "" || event.Message == "" || event.At.IsZero() {
		return Event{}, &ValidationError{Reason: "event id, service, message, and timestamp are required"}
	}
	labels, err := NormalizeLabels(event.Labels)
	if err != nil {
		return Event{}, err
	}
	event.Labels = labels
	return event, nil
}

func byTimeThenID(a, b Event) bool {
	if a.At.Equal(b.At) {
		return a.ID < b.ID
	}
	return a.At.Before(b.At)
}

// Store keeps a concurrency-safe, de-duplicated event timeline.
type Store struct {
	mu     sync.RWMutex
	byID   map[string]Event
	events []Event
}

func NewStore() *Store {
	return &Store{byID: make(map[string]Event)}
}

func (s *Store) Add(event Event) error {
	normalized, err := Normalize(event)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byID[normalized.ID]; exists {
		return errors.New("event id already exists")
	}
	s.byID[normalized.ID] = normalized
	s.events = append(s.events, normalized)
	sort.SliceStable(s.events, func(i, j int) bool {
		return byTimeThenID(s.events[i], s.events[j])
	})
	return nil
}

func (s *Store) Query(query Query) []Event {
	service := strings.TrimSpace(query.Service)
	severity := strings.ToLower(strings.TrimSpace(query.Severity))

	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Event, 0, len(s.events))
	for _, event := range s.events {
		if service != "" && event.Service != service {
			continue
		}
		if severity != "" && event.Severity != severity {
			continue
		}
		if !query.Since.IsZero() && event.At.Before(query.Since) {
			continue
		}
		if !query.Until.IsZero() && event.At.After(query.Until) {
			continue
		}
		if !labelsMatch(event.Labels, query.Labels) {
			continue
		}
		result = append(result, event)
	}
	return result
}
