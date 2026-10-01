package incidents

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

// EventLookup resolves an event id to its normalized event. The timeline
// implements this; the store calls it while holding the incident lock, so a
// link check and the state change it guards are atomic.
type EventLookup interface {
	EventByID(id string) (events.Event, bool)
}

// ApplyResult is the outcome of a successful action. Incident is the
// post-action state; Entry is the applied history entry (the original entry
// for an idempotent replay); Replay reports whether the action was a
// duplicate of an earlier submission.
type ApplyResult struct {
	Incident *Incident
	Entry    HistoryEntry
	Replay   bool
}

// RecordLogger is the durable sink for incident records. *Log implements it;
// tests may provide a fake to simulate write failures.
type RecordLogger interface {
	Append(rec record) error
	Poisoned() bool
}

// Store keeps incidents in memory and durably appends changes to a log.
// Every mutation is persisted before it becomes visible, so a confirmed
// change survives a restart or a process kill.
type Store struct {
	mu        sync.Mutex
	incidents map[string]*Incident
	log       RecordLogger
	events    EventLookup
	poisoned  bool
}

// NewStore returns an empty store backed by log. events is used for link
// existence and service checks; it may be nil for tests that never link.
func NewStore(log RecordLogger, events EventLookup) *Store {
	return &Store{
		incidents: make(map[string]*Incident),
		log:       log,
		events:    events,
	}
}

// Poisoned reports whether the store has suffered an unrecoverable write
// failure; all further incident requests return 503 until restart.
func (s *Store) Poisoned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.poisoned
}

func (s *Store) fail(err error) error {
	s.poisoned = true
	return err
}

// Create idempotently creates an incident. A resubmission with the same
// normalized title, service, and operator returns the existing incident; a
// different content returns a ConflictError.
func (s *Store) Create(id, title, service, operator string) (*Incident, error) {
	var err error
	if id, err = normalizeText("incident id", id); err != nil {
		return nil, err
	}
	if title, err = normalizeText("title", title); err != nil {
		return nil, err
	}
	if service, err = normalizeText("service", service); err != nil {
		return nil, err
	}
	if operator, err = normalizeText("operator", operator); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poisoned {
		return nil, ErrPoisoned
	}

	if existing, ok := s.incidents[id]; ok {
		if existing.Title == title && existing.Service == service &&
			len(existing.History) > 0 && existing.History[0].Operator == operator {
			return existing.clone(), nil
		}
		return nil, &ConflictError{Reason: "incident id already exists with different content"}
	}

	now := time.Now().UTC()
	inc := &Incident{
		ID:      id,
		Title:   title,
		Service: service,
		Status:  StatusOpen,
		Version: 1,
		History: []HistoryEntry{{
			Action:   ActionCreate,
			Operator: operator,
			Version:  1,
			At:       now,
		}},
	}

	if s.log != nil {
		rec := record{
			Kind:     kindCreate,
			ID:       id,
			Title:    title,
			Service:  service,
			Operator: operator,
			At:       now,
		}
		if err := s.log.Append(rec); err != nil {
			return nil, s.fail(err)
		}
	}

	s.incidents[id] = inc
	return inc.clone(), nil
}

// ApplyAction validates and applies one action. content is the note text for
// add_note, reason is the resolve/reopen reason, and eventID is the linked
// event for link; callers pass the fields relevant to the action.
//
// Idempotency: an action id already recorded with the same normalized
// content (including operator and expected_version) returns the first
// success result without incrementing the version or appending history.
func (s *Store) ApplyAction(id, actionID, action, operator string, expectedVersion int, content, reason, eventID string) (*ApplyResult, error) {
	var err error
	if id, err = normalizeText("incident id", id); err != nil {
		return nil, err
	}
	if actionID, err = normalizeText("action id", actionID); err != nil {
		return nil, err
	}
	if operator, err = normalizeText("operator", operator); err != nil {
		return nil, err
	}
	if expectedVersion < 1 {
		return nil, &ValidationError{Reason: "expected_version must be a positive integer"}
	}
	switch action {
	case ActionAddNote, ActionLink, ActionResolve, ActionReopen:
	default:
		return nil, &ValidationError{Reason: "unknown action " + action}
	}

	// Normalize the action-specific content.
	var display string
	switch action {
	case ActionAddNote:
		if content, err = normalizeText("content", content); err != nil {
			return nil, err
		}
		display = content
	case ActionLink:
		if eventID, err = normalizeText("event_id", eventID); err != nil {
			return nil, err
		}
		display = eventID
	case ActionResolve, ActionReopen:
		if reason, err = normalizeText("reason", reason); err != nil {
			return nil, err
		}
		display = reason
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poisoned {
		return nil, ErrPoisoned
	}

	inc, ok := s.incidents[id]
	if !ok {
		return nil, &NotFoundError{Reason: "incident not found"}
	}

	// Idempotent replay must be detected before the version check: a resubmission
	// of an already-applied action returns its first result even when the
	// incident has since moved on.
	if existing, found := inc.findEntry(actionID); found {
		if existing.Action == action && existing.Operator == operator &&
			existing.ExpectedVersion == expectedVersion && existing.Content == display {
			return &ApplyResult{Incident: inc.clone(), Entry: existing, Replay: true}, nil
		}
		return nil, &ConflictError{Reason: "action id already exists with different content"}
	}

	if inc.Version != expectedVersion {
		return nil, &VersionError{Reason: "version mismatch", CurrentVersion: inc.Version}
	}

	if !actionAllowed(inc.Status, action) {
		return nil, &ConflictError{Reason: "action " + action + " is not allowed in status " + inc.Status}
	}

	if action == ActionLink {
		if s.events == nil {
			return nil, &NotFoundError{Reason: "event not found"}
		}
		event, ok := s.events.EventByID(eventID)
		if !ok {
			return nil, &NotFoundError{Reason: "event not found"}
		}
		if event.Service != inc.Service {
			return nil, &ConflictError{Reason: "event service does not match incident service"}
		}
		if containsString(inc.Links, eventID) {
			return nil, &ConflictError{Reason: "event already linked to this incident"}
		}
	}

	now := time.Now().UTC()
	newVersion := inc.Version + 1
	entry := HistoryEntry{
		Action:          action,
		ActionID:        actionID,
		Operator:        operator,
		Content:         display,
		Version:         newVersion,
		ExpectedVersion: expectedVersion,
		At:              now,
	}

	if s.log != nil {
		rec := record{
			Kind:            kindAction,
			ID:              id,
			ActionID:        actionID,
			Action:          action,
			Operator:        operator,
			ExpectedVersion: expectedVersion,
			Content:         display,
			At:              now,
		}
		if err := s.log.Append(rec); err != nil {
			return nil, s.fail(err)
		}
	}

	switch action {
	case ActionLink:
		inc.Links = append(inc.Links, eventID)
	case ActionResolve:
		inc.Status = StatusResolved
	case ActionReopen:
		inc.Status = StatusOpen
	}
	inc.Version = newVersion
	inc.History = append(inc.History, entry)

	return &ApplyResult{Incident: inc.clone(), Entry: entry}, nil
}

// Get returns a deep copy of the incident with the given id.
func (s *Store) Get(id string) (*Incident, error) {
	if strings.TrimSpace(id) == "" {
		return nil, &ValidationError{Reason: "incident id must not be empty"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return nil, ErrPoisoned
	}
	inc, ok := s.incidents[id]
	if !ok {
		return nil, &NotFoundError{Reason: "incident not found"}
	}
	return inc.clone(), nil
}

// Load rebuilds incidents from durable records during startup. Records must
// be in write order; a record for an unknown incident is fatal.
func (s *Store) Load(records []record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range records {
		switch rec.Kind {
		case kindCreate:
			if _, exists := s.incidents[rec.ID]; exists {
				return fmt.Errorf("duplicate create record for incident %q", rec.ID)
			}
			s.incidents[rec.ID] = &Incident{
				ID:      rec.ID,
				Title:   rec.Title,
				Service: rec.Service,
				Status:  StatusOpen,
				Version: 1,
				History: []HistoryEntry{{
					Action:   ActionCreate,
					Operator: rec.Operator,
					Version:  1,
					At:       rec.At,
				}},
			}
		case kindAction:
			inc, ok := s.incidents[rec.ID]
			if !ok {
				return fmt.Errorf("action record for unknown incident %q", rec.ID)
			}
			entry := HistoryEntry{
				Action:          rec.Action,
				ActionID:        rec.ActionID,
				Operator:        rec.Operator,
				Content:         rec.Content,
				Version:         inc.Version + 1,
				ExpectedVersion: rec.ExpectedVersion,
				At:              rec.At,
			}
			switch rec.Action {
			case ActionLink:
				inc.Links = append(inc.Links, rec.Content)
			case ActionResolve:
				inc.Status = StatusResolved
			case ActionReopen:
				inc.Status = StatusOpen
			}
			inc.Version = entry.Version
			inc.History = append(inc.History, entry)
		default:
			return fmt.Errorf("unknown record kind %q", rec.Kind)
		}
	}
	return nil
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
