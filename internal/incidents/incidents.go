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
	if r.beforeCommit != nil {
		if err := r.beforeCommit(NewActionRecord(req, at)); err != nil {
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
		if state.status != StatusOpen {
			return conflictErr(state.version, "cannot link an event to a %s incident", state.status)
		}
		if r.events == nil {
			return notFoundErr("event %q not found", req.Content)
		}
		service, found := r.events.LookupEvent(req.Content)
		if !found {
			return notFoundErr("event %q not found", req.Content)
		}
		if service != state.service {
			return conflictErr(state.version,
				"event %q belongs to service %q, not %q", req.Content, service, state.service)
		}
		if _, linked := state.linkSet[req.Content]; linked {
			return conflictErr(state.version,
				"event %q is already linked to incident %q", req.Content, state.id)
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
		// The personnel constraints are owned by checkPersonnelAction so the
		// live path and startup replay enforce exactly the same rules.
		if perr := checkPersonnelAction(state, req); perr != nil {
			return conflictErr(state.version, "%s", perr.LiveError())
		}
	}
	return nil
}

// personnelRule identifies one violated personnel constraint.
type personnelRule int

const (
	// Personnel actions are only accepted while the incident is open.
	personnelRuleNotOpen personnelRule = iota
	// add_participant: the target is already a participant.
	personnelRuleAlreadyMember
	// remove_participant: the target is not a participant.
	personnelRuleNotMember
	// remove_participant: the target is the current owner.
	personnelRuleRemoveOwner
	// assign_owner: the target is not a participant.
	personnelRuleAssignOutsider
	// assign_owner: the target is already the owner.
	personnelRuleAssignCurrentOwner
)

// personnelError describes one violated personnel rule with enough context to
// render it either as a live 409 conflict (LiveError) or as corruption found
// while replaying the durable log (LogError). Keeping both wordings next to
// the single rule list is what lets online submission and startup replay
// share one constraint definition.
type personnelError struct {
	rule       personnelRule
	action     string
	actionID   string
	target     string
	incidentID string
	status     string
}

func (e *personnelError) Error() string { return e.LiveError() }

// LiveError renders the violation as returned for an online request.
func (e *personnelError) LiveError() string {
	switch e.rule {
	case personnelRuleNotOpen:
		return fmt.Sprintf("cannot %s on a %s incident", personnelActionPhrase(e.action), e.status)
	case personnelRuleAlreadyMember:
		return fmt.Sprintf("%q is already a participant of incident %q", e.target, e.incidentID)
	case personnelRuleNotMember:
		return fmt.Sprintf("%q is not a participant of incident %q", e.target, e.incidentID)
	case personnelRuleRemoveOwner:
		return fmt.Sprintf("cannot remove the current owner %q from incident %q", e.target, e.incidentID)
	case personnelRuleAssignOutsider:
		return fmt.Sprintf("cannot hand incident %q to non-participant %q", e.incidentID, e.target)
	case personnelRuleAssignCurrentOwner:
		return fmt.Sprintf("%q is already the owner of incident %q", e.target, e.incidentID)
	default:
		return e.action
	}
}

// LogError renders the same violation as an inconsistency in the durable log.
func (e *personnelError) LogError() string {
	switch e.rule {
	case personnelRuleNotOpen:
		return fmt.Sprintf("%s action %q on %s incident in log", e.action, e.actionID, e.status)
	case personnelRuleAlreadyMember:
		return fmt.Sprintf("participant %q added twice in incident %q log", e.target, e.incidentID)
	case personnelRuleNotMember:
		return fmt.Sprintf("participant %q removed without joining in incident %q log", e.target, e.incidentID)
	case personnelRuleRemoveOwner:
		return fmt.Sprintf("current owner %q removed in incident %q log", e.target, e.incidentID)
	case personnelRuleAssignOutsider:
		return fmt.Sprintf("incident %q handed to non-participant %q in log", e.incidentID, e.target)
	case personnelRuleAssignCurrentOwner:
		return fmt.Sprintf("owner %q handed over to itself in incident %q log", e.target, e.incidentID)
	default:
		return e.action
	}
}

// checkPersonnelAction enforces the single set of business rules shared by
// online submission and startup replay: personnel actions only apply while
// the incident is open; an added person must not already participate; a
// removed person must participate and must not be the current owner; an owner
// handover must target a participant other than the current owner. Names are
// case-sensitive. It returns nil when the action satisfies every rule.
func checkPersonnelAction(state *incidentState, req Request) *personnelError {
	fail := func(rule personnelRule) *personnelError {
		return &personnelError{
			rule:       rule,
			action:     req.Type,
			actionID:   req.ActionID,
			target:     req.Content,
			incidentID: state.id,
			status:     state.status,
		}
	}
	if state.status != StatusOpen {
		return fail(personnelRuleNotOpen)
	}
	_, member := state.participantSet[req.Content]
	switch req.Type {
	case ActionAddParticipant:
		if member {
			return fail(personnelRuleAlreadyMember)
		}
	case ActionRemoveParticipant:
		if !member {
			return fail(personnelRuleNotMember)
		}
		if req.Content == state.owner {
			return fail(personnelRuleRemoveOwner)
		}
	case ActionAssignOwner:
		if !member {
			return fail(personnelRuleAssignOutsider)
		}
		if req.Content == state.owner {
			return fail(personnelRuleAssignCurrentOwner)
		}
	}
	return nil
}

// personnelActionPhrase renders the personnel action names in status messages.
func personnelActionPhrase(action string) string {
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
