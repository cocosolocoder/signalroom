package events

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// Event is a normalized operational signal recorded on the incident timeline.
type Event struct {
	ID       string
	Service  string
	Severity string
	Message  string
	At       time.Time
}

// Query selects events by optional service and severity filters.
type Query struct {
	Service  string
	Severity string
	Since    time.Time
	Until    time.Time
}

// ErrInvalidEvent is returned when an event fails required-field validation.
var ErrInvalidEvent = errors.New("event id, service, message, and timestamp are required")

// NormalizeEvent trims free-form string fields, lower-cases severity, and
// enforces the required-field rules shared by every ingestion path.
func NormalizeEvent(event Event) (Event, error) {
	event.ID = strings.TrimSpace(event.ID)
	event.Service = strings.TrimSpace(event.Service)
	event.Severity = strings.ToLower(strings.TrimSpace(event.Severity))
	event.Message = strings.TrimSpace(event.Message)
	if event.ID == "" || event.Service == "" || event.Message == "" || event.At.IsZero() {
		return Event{}, ErrInvalidEvent
	}
	return event, nil
}

// EventsEqual reports whether two events are identical after normalization.
// Timestamps are compared as absolute instants so equivalent RFC3339 offsets
// compare equal.
func EventsEqual(a, b Event) bool {
	return a.ID == b.ID &&
		a.Service == b.Service &&
		a.Severity == b.Severity &&
		a.Message == b.Message &&
		a.At.Equal(b.At)
}

// less orders events by absolute time ascending, then by ID ascending.
func less(a, b Event) bool {
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
	event, err := NormalizeEvent(event)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byID[event.ID]; exists {
		return errors.New("event id already exists")
	}
	s.byID[event.ID] = event
	s.events = append(s.events, event)
	sort.SliceStable(s.events, func(i, j int) bool {
		return less(s.events[i], s.events[j])
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
		result = append(result, event)
	}
	return result
}
