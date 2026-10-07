// Package incidents holds the incident-handling register: incidents are
// created once, evolve through append-only actions under optimistic
// concurrency, and link back to events without modifying them.
package incidents

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// Incident statuses.
const (
	StatusOpen     = "open"
	StatusResolved = "resolved"
)

// Actions accepted by POST /incidents/{id}/actions. ActionCreate marks the
// creation entry in an incident's history.
const (
	ActionCreate            = "create"
	ActionNote              = "add_note"
	ActionLinkEvent         = "link_event"
	ActionResolve           = "resolve"
	ActionReopen            = "reopen"
	ActionAddParticipant    = "add_participant"
	ActionRemoveParticipant = "remove_participant"
	ActionAssignOwner       = "assign_owner"
)

// ValidationError reports missing fields, wrong types, or unknown actions.
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// NotFoundError reports a missing incident or a missing linked event.
type NotFoundError struct{ Reason string }

func (e *NotFoundError) Error() string { return e.Reason }

// ConflictError carries the current incident version alongside the reason,
// so a stale-version response can report it. CurrentVersion is zero when the
// conflict is not version-related.
type ConflictError struct {
	Reason         string
	CurrentVersion int
}

func (e *ConflictError) Error() string { return e.Reason }

// IsValidationError reports whether err is an incident validation failure.
func IsValidationError(err error) bool {
	var target *ValidationError
	return errors.As(err, &target)
}

// IsNotFoundError reports whether err is a missing-incident/event failure.
func IsNotFoundError(err error) bool {
	var target *NotFoundError
	return errors.As(err, &target)
}

// IsConflictError reports whether err is an incident conflict failure.
func IsConflictError(err error) bool {
	var target *ConflictError
	return errors.As(err, &target)
}

// CurrentVersionOf returns the version reported with a version conflict, or
// zero when err is not a version conflict.
func CurrentVersionOf(err error) int {
	var target *ConflictError
	if errors.As(err, &target) {
		return target.CurrentVersion
	}
	return 0
}

func validationErr(format string, args ...any) error {
	return &ValidationError{Reason: fmt.Sprintf(format, args...)}
}

func notFoundErr(format string, args ...any) error {
	return &NotFoundError{Reason: fmt.Sprintf(format, args...)}
}

func conflictErr(currentVersion int, format string, args ...any) error {
	return &ConflictError{Reason: fmt.Sprintf(format, args...), CurrentVersion: currentVersion}
}

// Creation is the normalized content of POST /incidents. Strings are trimmed
// of surrounding whitespace but keep their interior case.
type Creation struct {
	ID       string
	Title    string
	Service  string
	Operator string
}

// Request is the normalized content of POST /incidents/{id}/actions. Content
// carries the action payload: the note text, the linked event id, or the
// resolve/reopen reason.
type Request struct {
	IncidentID      string
	ActionID        string
	Operator        string
	ExpectedVersion int
	Type            string
	Content         string
}

// CreateResult is the result of a successful create, including an idempotent
// replay.
type CreateResult struct {
	ID      string
	Status  string
	Version int
}

// ActionResult is the result of a successful action, including an idempotent
// replay.
type ActionResult struct {
	IncidentID string
	ActionID   string
	Version    int
}

// Entry is one committed item in an incident's history: the creation itself
// or a later action.
type Entry struct {
	ActionID string
	Operator string
	Action   string
	Content  string
	Version  int
	At       time.Time
}

// Incident is one committed snapshot, suitable for a single-version read.
type Incident struct {
	ID           string
	Title        string
	Service      string
	Status       string
	Version      int
	Participants []string
	Owner        string
	Links        []string
	History      []Entry
}

// EventLookup resolves an event id without exposing the whole timeline, so
// incident handling never modifies events.
type EventLookup interface {
	// LookupEvent returns (service, true) for a stored event, or ("", false).
	LookupEvent(id string) (service string, ok bool)
}

// BeforeCommit persists one new record while the registry lock is held and
// before the in-memory state advances. Returning an error rejects the request
// without touching the incident.
type BeforeCommit func(record Record) error

// Registry is the concurrency-safe set of incidents.
type Registry struct {
	mu        sync.Mutex
	incidents map[string]*incidentState

	events       EventLookup
	beforeCommit BeforeCommit
	now          func() time.Time
}

// incidentState is the mutable state of one incident.
type incidentState struct {
	id      string
	title   string
	service string
	status  string
	version int

	// participants holds every participant name in membership order; the
	// membership is tracked separately by participantSet. The owner is always
	// among the participants. A newly created incident starts with only its
	// operator as both participant and owner; note/link operators never join
	// automatically.
	participants   []string
	participantSet map[string]struct{}
	owner          string

	links   []string
	linkSet map[string]struct{}

	history []Entry
	// actions remembers each successful action's normalized request and the
	// version reached afterwards, so an identical retry replays the first
	// result even once the version has moved on.
	actions map[string]committedAction

	creation Creation
}

type committedAction struct {
	request       Request
	resultVersion int
}

// Option configures a Registry at construction.
type Option func(*Registry)

// WithClock overrides the server clock, mainly for tests.
func WithClock(now func() time.Time) Option {
	return func(r *Registry) {
		if now != nil {
			r.now = now
		}
	}
}

// NewRegistry returns an empty registry. events resolves linked event ids and
// beforeCommit durably stores each accepted record.
func NewRegistry(events EventLookup, beforeCommit BeforeCommit, opts ...Option) *Registry {
	r := &Registry{
		incidents:    make(map[string]*incidentState),
		events:       events,
		beforeCommit: beforeCommit,
		now:          func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NormalizeCreation trims the four creation fields, keeping their interior
// case, and requires each to be non-empty afterwards.
func NormalizeCreation(input Creation) (Creation, error) {
	var err error
	if input.ID, err = trimRequired("id", input.ID); err != nil {
		return Creation{}, err
	}
	if input.Title, err = trimRequired("title", input.Title); err != nil {
		return Creation{}, err
	}
	if input.Service, err = trimRequired("service", input.Service); err != nil {
		return Creation{}, err
	}
	if input.Operator, err = trimRequired("operator", input.Operator); err != nil {
		return Creation{}, err
	}
	return input, nil
}

// NormalizeRequest trims and validates one action request. incidentID comes
// from the URL path and is only checked for emptiness.
func NormalizeRequest(input Request) (Request, error) {
	var err error
	if input.IncidentID, err = trimRequired("incident id", input.IncidentID); err != nil {
		return Request{}, err
	}
	if input.ActionID, err = trimRequired("action_id", input.ActionID); err != nil {
		return Request{}, err
	}
	if input.Operator, err = trimRequired("operator", input.Operator); err != nil {
		return Request{}, err
	}
	if input.ExpectedVersion <= 0 {
		return Request{}, validationErr("expected_version must be a positive integer")
	}
	switch input.Type {
	case ActionNote, ActionLinkEvent, ActionResolve, ActionReopen,
		ActionAddParticipant, ActionRemoveParticipant, ActionAssignOwner:
	default:
		return Request{}, validationErr("unknown action %q", input.Type)
	}
	if input.Content, err = trimRequired(actionContentField(input.Type), input.Content); err != nil {
		return Request{}, err
	}
	return input, nil
}

// actionContentField names the request member that carries each action's
// payload, used in validation messages.
func actionContentField(action string) string {
	switch action {
	case ActionLinkEvent:
		return "event_id"
	case ActionResolve, ActionReopen:
		return "reason"
	case ActionAddParticipant, ActionRemoveParticipant, ActionAssignOwner:
		return "participant"
	default:
		return "content"
	}
}

func trimRequired(field, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", validationErr("%s must not be empty", field)
	}
	return value, nil
}

// Create accepts a new incident or replays an identical creation. A stored id
// presented with different normalized content conflicts.
func (r *Registry) Create(input Creation) (CreateResult, error) {
	creation, err := NormalizeCreation(input)
	if err != nil {
		return CreateResult{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.incidents[creation.ID]; ok {
		if existing.creation != creation {
			return CreateResult{}, conflictErr(existing.version,
				"incident %q already exists with different content", creation.ID)
		}
		// A creation replay returns the first result verbatim: open at
		// version 1, even if the incident has moved on since.
		return CreateResult{ID: creation.ID, Status: StatusOpen, Version: 1}, nil
	}

	at := r.now()
	record := NewCreationRecord(creation, at)
	if r.beforeCommit != nil {
		if err := r.beforeCommit(record); err != nil {
			return CreateResult{}, err
		}
	}
	r.incidents[creation.ID] = newIncidentState(creation, at)
	return CreateResult{ID: creation.ID, Status: StatusOpen, Version: 1}, nil
}

// Apply accepts one action or replays an identical one. Validation and every
// state check happen under the registry lock before persistence, so a
// rejected request leaves the incident untouched.
func (r *Registry) Apply(input Request) (ActionResult, error) {
	req, err := NormalizeRequest(input)
	if err != nil {
		return ActionResult{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.incidents[req.IncidentID]
	if !ok {
		return ActionResult{}, notFoundErr("incident %q not found", req.IncidentID)
	}

	// Idempotent replay is checked first: a retry of the first successful
	// request still returns that result even though its expected_version is
	// stale by now.
	if prior, repeated := state.actions[req.ActionID]; repeated {
		if prior.request != req {
			return ActionResult{}, conflictErr(state.version,
				"action %q already exists with different content", req.ActionID)
		}
		return ActionResult{IncidentID: state.id, ActionID: req.ActionID, Version: prior.resultVersion}, nil
	}

	if err := r.checkAction(state, req); err != nil {
		return ActionResult{}, err
	}

	at := r.now()
	record := NewActionRecord(req, at)
	// Only a brand-new legal note is measured against the per-record save
	// capacity; replays were handled above and every other rejection order is
	// unchanged. Measure the exact bytes the record would save, including the
	// JSON escaping the save format applies (so a note full of '&', '<', or
	// '>' is counted by its escaped length) and the server timestamp, but
	// excluding the storage frame header. The check precedes persistence, so
	// an oversize note neither touches durable storage nor advances the
	// incident; the log stays usable without a restart.
	if req.Type == ActionNote {
		payload, err := MarshalRecord(record)
		if err != nil {
			return ActionResult{}, err
		}
		if len(payload) > MaxRecordPayloadBytes {
			return ActionResult{}, &OversizeRecordError{Size: len(payload), Limit: MaxRecordPayloadBytes}
		}
	}
	if r.beforeCommit != nil {
		if err := r.beforeCommit(record); err != nil {
			return ActionResult{}, err
		}
	}
	version := state.commit(req, at)
	return ActionResult{IncidentID: state.id, ActionID: req.ActionID, Version: version}, nil
}

// checkAction enforces optimistic versioning, the open/resolved state
// machine, and the link rules. It runs with the registry lock held.
func (r *Registry) checkAction(state *incidentState, req Request) error {
	if req.ExpectedVersion != state.version {
		return conflictErr(state.version,
			"expected version %d but incident %q is at version %d",
			req.ExpectedVersion, state.id, state.version)
	}
	switch req.Type {
	case ActionNote:
		if state.status != StatusOpen {
			return conflictErr(state.version, "cannot add a note to a %s incident", state.status)
		}
	case ActionLinkEvent:
		if err := r.linkConflictError(state, req); err != nil {
			return err
		}
	case ActionResolve:
		if state.status != StatusOpen {
			return conflictErr(state.version, "incident %q is already resolved", state.id)
		}
	case ActionReopen:
		if state.status != StatusResolved {
			return conflictErr(state.version, "incident %q is not resolved", state.id)
		}
	case ActionAddParticipant, ActionRemoveParticipant, ActionAssignOwner:
		if err := personnelConflictError(state, req); err != nil {
			return err
		}
	}
	return nil
}

// linkViolation identifies one broken link_event eligibility rule without
// binding it to an error vocabulary: the live request path turns it into a
// 404/409 in its own wording, while startup replay turns it into a log
// inconsistency. Keeping the judgment independent of the wording means both
// entry paths enforce the very same business constraints.
type linkViolation int

const (
	linkOK linkViolation = iota
	// linkClosed: a link was attempted while the incident was not open.
	linkClosed
	// linkMissing: the event id does not resolve in the event log (or there
	// is no event lookup at all).
	linkMissing
	// linkWrongService: the event belongs to a different service.
	linkWrongService
	// linkAlreadyLinked: the incident already links this event.
	linkAlreadyLinked
)

// checkLinkEligibility evaluates the link rules shared by the live request
// path and startup replay: an event may be linked only while the incident is
// open, only when it exists and belongs to the incident's service, and only
// once per incident. It needs the registry solely to resolve events; every
// other input comes from the in-progress state and the normalized request.
// The returned service is the resolved event's service when the event was
// found, so the callers never have to look it up a second time.
func (r *Registry) checkLinkEligibility(state *incidentState, req Request) (linkViolation, string) {
	if state.status != StatusOpen {
		return linkClosed, ""
	}
	if r.events == nil {
		return linkMissing, ""
	}
	service, found := r.events.LookupEvent(req.Content)
	if !found {
		return linkMissing, ""
	}
	if service != state.service {
		return linkWrongService, service
	}
	if _, linked := state.linkSet[req.Content]; linked {
		return linkAlreadyLinked, service
	}
	return linkOK, service
}

// linkConflictError renders a shared link violation in the live request
// path's vocabulary: a missing event is a 404, every other violation is a
// 409-style conflict carrying the current version.
func (r *Registry) linkConflictError(state *incidentState, req Request) error {
	switch violation, service := r.checkLinkEligibility(state, req); violation {
	case linkOK:
		return nil
	case linkClosed:
		return conflictErr(state.version, "cannot link an event to a %s incident", state.status)
	case linkMissing:
		return notFoundErr("event %q not found", req.Content)
	case linkWrongService:
		return conflictErr(state.version,
			"event %q belongs to service %q, not %q", req.Content, service, state.service)
	case linkAlreadyLinked:
		return conflictErr(state.version,
			"event %q is already linked to incident %q", req.Content, state.id)
	}
	return nil
}

// personnelViolation identifies one broken personnel rule without binding
// it to an error vocabulary: the live request path turns it into a conflict
// carrying the current version, while startup replay turns it into a log
// inconsistency. Keeping the judgment independent of the wording means both
// entry paths enforce the very same business constraints.
type personnelViolation int

const (
	personnelOK personnelViolation = iota
	// personnelClosed: a personnel action was attempted while not open.
	personnelClosed
	// personnelAlreadyMember: the add target already participates.
	personnelAlreadyMember
	// personnelNotMember: the remove target does not participate.
	personnelNotMember
	// personnelRemoveOwner: the remove target is the current owner.
	personnelRemoveOwner
	// personnelOwnerNonMember: the handover target does not participate.
	personnelOwnerNonMember
	// personnelOwnerUnchanged: the handover target is already the owner.
	personnelOwnerUnchanged
)

// checkPersonnelAction evaluates the personnel rules shared by the live
// request path and startup replay: personnel actions are accepted only
// while the incident is open; an added person must not already participate;
// a removed person must participate without being the current owner; an
// owner handover must target a different, already participating person.
func checkPersonnelAction(state *incidentState, req Request) personnelViolation {
	if state.status != StatusOpen {
		return personnelClosed
	}
	switch req.Type {
	case ActionAddParticipant:
		if _, member := state.participantSet[req.Content]; member {
			return personnelAlreadyMember
		}
	case ActionRemoveParticipant:
		if _, member := state.participantSet[req.Content]; !member {
			return personnelNotMember
		}
		if req.Content == state.owner {
			return personnelRemoveOwner
		}
	case ActionAssignOwner:
		if _, member := state.participantSet[req.Content]; !member {
			return personnelOwnerNonMember
		}
		if req.Content == state.owner {
			return personnelOwnerUnchanged
		}
	}
	return personnelOK
}

// personnelConflictError renders a shared personnel violation in the live
// request path's vocabulary: a 409-style conflict carrying the current
// version.
func personnelConflictError(state *incidentState, req Request) error {
	switch checkPersonnelAction(state, req) {
	case personnelOK:
		return nil
	case personnelClosed:
		return conflictErr(state.version, "cannot %s on a %s incident", participantRuleName(req.Type), state.status)
	case personnelAlreadyMember:
		return conflictErr(state.version, "%q is already a participant of incident %q", req.Content, state.id)
	case personnelNotMember:
		return conflictErr(state.version, "%q is not a participant of incident %q", req.Content, state.id)
	case personnelRemoveOwner:
		return conflictErr(state.version, "cannot remove the current owner %q from incident %q", req.Content, state.id)
	case personnelOwnerNonMember:
		return conflictErr(state.version, "cannot hand incident %q to non-participant %q", state.id, req.Content)
	case personnelOwnerUnchanged:
		return conflictErr(state.version, "%q is already the owner of incident %q", req.Content, state.id)
	}
	return nil
}

// participantRuleName renders the personnel action names in error messages.
func participantRuleName(action string) string {
	switch action {
	case ActionAddParticipant:
		return "add a participant"
	case ActionRemoveParticipant:
		return "remove a participant"
	case ActionAssignOwner:
		return "hand over ownership"
	default:
		return action
	}
}

// Get returns a point-in-time copy of one incident. The copy's people,
// status, links, version, and history all belong to the same committed
// version.
func (r *Registry) Get(id string) (Incident, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.incidents[id]
	if !ok {
		return Incident{}, notFoundErr("incident %q not found", id)
	}
	return state.snapshot(), nil
}

func newIncidentState(creation Creation, at time.Time) *incidentState {
	return &incidentState{
		id:             creation.ID,
		title:          creation.Title,
		service:        creation.Service,
		status:         StatusOpen,
		version:        1,
		participants:   []string{creation.Operator},
		participantSet: map[string]struct{}{creation.Operator: {}},
		owner:          creation.Operator,
		links:          []string{},
		linkSet:        make(map[string]struct{}),
		actions:        make(map[string]committedAction),
		history:        []Entry{{Operator: creation.Operator, Action: ActionCreate, Content: creation.Title, Version: 1, At: at}},
		creation:       creation,
	}
}

// commit advances the state by one already-validated action and returns the
// new version.
func (s *incidentState) commit(req Request, at time.Time) int {
	switch req.Type {
	case ActionLinkEvent:
		s.linkSet[req.Content] = struct{}{}
		s.links = append(s.links, req.Content)
	case ActionResolve:
		s.status = StatusResolved
	case ActionReopen:
		s.status = StatusOpen
	case ActionAddParticipant:
		s.participantSet[req.Content] = struct{}{}
		s.participants = append(s.participants, req.Content)
	case ActionRemoveParticipant:
		delete(s.participantSet, req.Content)
		s.participants = removeString(s.participants, req.Content)
	case ActionAssignOwner:
		s.owner = req.Content
	}
	s.version++
	s.history = append(s.history, Entry{
		ActionID: req.ActionID,
		Operator: req.Operator,
		Action:   req.Type,
		Content:  req.Content,
		Version:  s.version,
		At:       at,
	})
	s.actions[req.ActionID] = committedAction{request: req, resultVersion: s.version}
	return s.version
}

// removeString drops the first occurrence of value, preserving order.
func removeString(values []string, value string) []string {
	for i, v := range values {
		if v == value {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func (s *incidentState) snapshot() Incident {
	links := slices.Clone(s.links)
	history := make([]Entry, len(s.history))
	copy(history, s.history)
	participants := append([]string{}, s.participants...)
	slices.Sort(participants)
	return Incident{
		ID:           s.id,
		Title:        s.title,
		Service:      s.service,
		Status:       s.status,
		Version:      s.version,
		Participants: participants,
		Owner:        s.owner,
		Links:        links,
		History:      history,
	}
}
