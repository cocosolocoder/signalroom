// Package incidents implements the incident-response workbench: durable
// incident creation, state transitions, event links, and a full action
// history. All changes are appended to a frame log before they become
// visible, so confirmed operations survive a restart or a process kill.
package incidents

import (
	"errors"
	"strings"
	"time"
)

// Incident statuses.
const (
	// StatusOpen is the initial and active state.
	StatusOpen = "open"
	// StatusResolved is the terminal state; only reopen leaves it.
	StatusResolved = "resolved"
)

// Action types recorded in an incident's history. ActionCreate is the
// implicit first entry; the others are submitted through the actions
// endpoint.
const (
	ActionCreate  = "create"
	ActionAddNote = "add_note"
	ActionLink    = "link"
	ActionResolve = "resolve"
	ActionReopen  = "reopen"
)

// HistoryEntry is one recorded incident change. ActionID is empty for the
// create entry. Content holds the note text, the linked event id, or the
// resolve/reopen reason, depending on the action. Version is the incident
// version after the change; ExpectedVersion is the version the change was
// submitted against (used for idempotency comparison).
type HistoryEntry struct {
	Action          string
	ActionID        string
	Operator        string
	Content         string
	Version         int
	ExpectedVersion int
	At              time.Time
}

// Incident is the current state of one incident.
type Incident struct {
	ID      string
	Title   string
	Service string
	Status  string
	Version int
	// Links holds linked event ids in link order.
	Links []string
	// History holds every recorded change in submission order.
	History []HistoryEntry
}

// ValidationError reports a field that failed normalization or validation.
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// IsValidationError reports whether err is an incident validation failure.
func IsValidationError(err error) bool {
	var target *ValidationError
	return errors.As(err, &target)
}

// ConflictError reports a state, content, or idempotency conflict.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return e.Reason }

// IsConflictError reports whether err is an incident conflict.
func IsConflictError(err error) bool {
	var target *ConflictError
	return errors.As(err, &target)
}

// NotFoundError reports a missing incident or event.
type NotFoundError struct{ Reason string }

func (e *NotFoundError) Error() string { return e.Reason }

// IsNotFoundError reports whether err is a not-found error.
func IsNotFoundError(err error) bool {
	var target *NotFoundError
	return errors.As(err, &target)
}

// VersionError reports an expected_version mismatch. CurrentVersion is the
// incident's current version, included in the 409 response.
type VersionError struct {
	Reason         string
	CurrentVersion int
}

func (e *VersionError) Error() string { return e.Reason }

// normalizeText trims surrounding whitespace and requires a non-empty result,
// keeping the original case.
func normalizeText(field, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", &ValidationError{Reason: field + " must not be empty"}
	}
	return value, nil
}

// clone returns a deep copy of the incident so a read under the store lock
// yields a consistent snapshot safe to use after the lock is released.
func (inc *Incident) clone() *Incident {
	out := *inc
	out.Links = append([]string(nil), inc.Links...)
	out.History = make([]HistoryEntry, len(inc.History))
	copy(out.History, inc.History)
	return &out
}

// findEntry returns the history entry with the given action id.
func (inc *Incident) findEntry(actionID string) (HistoryEntry, bool) {
	for _, entry := range inc.History {
		if entry.ActionID == actionID {
			return entry, true
		}
	}
	return HistoryEntry{}, false
}

// actionAllowed reports whether action is permitted in the given status.
func actionAllowed(status, action string) bool {
	switch status {
	case StatusOpen:
		return action == ActionAddNote || action == ActionLink || action == ActionResolve
	case StatusResolved:
		return action == ActionReopen
	}
	return false
}
