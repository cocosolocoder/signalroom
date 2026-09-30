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
	event.ID = strings.TrimSpace(event.ID)
	event.Service = strings.TrimSpace(event.Service)
	event.Severity = strings.ToLower(strings.TrimSpace(event.Severity))
	event.Message = strings.TrimSpace(event.Message)
	if event.ID == "" || event.Service == "" || event.Message == "" || event.At.IsZero() {
		return errors.New("event id, service, message, and timestamp are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byID[event.ID]; exists {
		return errors.New("event id already exists")
	}
	s.byID[event.ID] = event
	s.events = append(s.events, event)
	sort.SliceStable(s.events, func(i, j int) bool {
		if s.events[i].At.Equal(s.events[j].At) {
			return s.events[i].ID < s.events[j].ID
		}
		return s.events[i].At.Before(s.events[j].At)
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
