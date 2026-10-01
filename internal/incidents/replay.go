package incidents

// Load applies one durable record recovered during startup. It rebuilds the
// exact committed state, including the server timestamps the first run
// assigned, so history order and times are preserved across restarts. Every
// record is assumed to have passed DecodeRecord's validation; a record that
// contradicts the already-loaded state (duplicate creation, unknown
// incident, action id reuse, non-sequential version) is an error that should
// fail startup rather than mutate data.
func (r *Registry) Load(record Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	at := record.At.UTC()
	switch record.Kind {
	case RecordCreation:
		c := *record.Creation
		if _, exists := r.incidents[c.ID]; exists {
			return validationErr("duplicate creation for incident %q in log", c.ID)
		}
		r.incidents[c.ID] = newIncidentState(c, at)
		return nil

	case RecordAction:
		req := record.Action.Request
		state, ok := r.incidents[req.IncidentID]
		if !ok {
			return validationErr("action for unknown incident %q in log", req.IncidentID)
		}
		if _, dup := state.actions[req.ActionID]; dup {
			return validationErr("duplicate action %q in incident %q log", req.ActionID, req.IncidentID)
		}
		// The durable request's expected_version is the version in force
		// just before the action; the resulting version must follow in
		// lockstep. Checking it catches any reordered or missing frames.
		if req.ExpectedVersion != state.version {
			return validationErr("action %q expected version %d but log was at %d",
				req.ActionID, req.ExpectedVersion, state.version)
		}
		if err := r.checkLoadedAction(state, req); err != nil {
			return err
		}
		state.commit(req, at)
		return nil

	default:
		return validationErr("unknown record kind %q in log", record.Kind)
	}
}

// checkLoadedAction mirrors the live state-machine and link checks. Events
// are never removed, so an event linked at action time must still resolve and
// still match the incident's service; a failure means the log is internally
// inconsistent and must abort startup rather than mutate data.
func (r *Registry) checkLoadedAction(state *incidentState, req Request) error {
	switch req.Type {
	case ActionNote:
		if state.status != StatusOpen {
			return validationErr("note action %q on %s incident in log", req.ActionID, state.status)
		}
	case ActionLinkEvent:
		if state.status != StatusOpen {
			return validationErr("link action %q on %s incident in log", req.ActionID, state.status)
		}
		if _, linked := state.linkSet[req.Content]; linked {
			return validationErr("event %q linked twice in incident %q log", req.Content, state.id)
		}
		if r.events == nil {
			return validationErr("event %q missing while replaying incident log", req.Content)
		}
		service, found := r.events.LookupEvent(req.Content)
		if !found {
			return validationErr("linked event %q no longer exists in the event log", req.Content)
		}
		if service != state.service {
			return validationErr("linked event %q service %q does not match incident %q service %q",
				req.Content, service, state.id, state.service)
		}
	case ActionResolve:
		if state.status != StatusOpen {
			return validationErr("resolve action %q on non-open incident in log", req.ActionID)
		}
	case ActionReopen:
		if state.status != StatusResolved {
			return validationErr("reopen action %q on non-resolved incident in log", req.ActionID)
		}
	default:
		return validationErr("unknown action %q in log", req.Type)
	}
	return nil
}
