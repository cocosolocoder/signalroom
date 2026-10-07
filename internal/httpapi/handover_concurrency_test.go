package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// controllableIncidentStorage is the incident counterpart of fakeStorage:
// besides counting records and poisoning on failure, armStall holds one
// AppendRecord open (while the registry lock is held and before the in-memory
// state advances) until the test releases it. The stall deterministically
// opens the "save in progress" window in which detail reads must not observe
// a half-applied handover.
type controllableIncidentStorage struct {
	mu       sync.Mutex
	records  []incidents.Record
	failNext bool
	poisoned bool

	stall        chan struct{}
	stallEntered chan struct{}
}

func (s *controllableIncidentStorage) AppendRecord(record incidents.Record) error {
	s.mu.Lock()
	stall := s.stall
	entered := s.stallEntered
	s.stall = nil
	s.stallEntered = nil
	s.mu.Unlock()
	if stall != nil {
		// The registry lock is held across this call, so signalling here
		// means the handover has passed validation but not committed.
		close(entered)
		<-stall
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext {
		s.poisoned = true
		s.failNext = false
		return errBoom
	}
	s.records = append(s.records, record)
	return nil
}

func (s *controllableIncidentStorage) Poisoned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.poisoned
}

// armStall makes the next AppendRecord block until the returned release
// function is called. entered closes once that AppendRecord is waiting.
func (s *controllableIncidentStorage) armStall() (release func(), entered chan struct{}) {
	ch := make(chan struct{})
	entered = make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	s.mu.Lock()
	s.stall = ch
	s.stallEntered = entered
	s.mu.Unlock()
	return release, entered
}

func (s *controllableIncidentStorage) snapshotRecords() []incidents.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]incidents.Record, len(s.records))
	copy(out, s.records)
	return out
}

// countAssignments returns how many durable assign_owner records the given
// incident currently has.
func (s *controllableIncidentStorage) countAssignments(incidentID string) int {
	n := 0
	for _, rec := range s.snapshotRecords() {
		if rec.Kind == incidents.RecordAction && rec.Action != nil &&
			rec.Action.Request.IncidentID == incidentID &&
			rec.Action.Request.Type == incidents.ActionAssignOwner {
			n++
		}
	}
	return n
}

func newControlledIncidentServer(t *testing.T) (*httptest.Server, *controllableIncidentStorage, *incidents.Registry) {
	t.Helper()
	tl := events.NewTimeline()
	eventStore := &fakeStorage{cursorKey: []byte("test-cursor-key-0123456789ab")}
	incStore := &controllableIncidentStorage{}
	reg := incidents.NewRegistry(tl, incStore.AppendRecord)
	server := httptest.NewServer(NewHandler(tl, eventStore, eventStore, WithIncidents(reg, incStore)))
	t.Cleanup(server.Close)
	return server, incStore, reg
}

// httpResult is one decoded JSON response (status -1 marks a transport error).
type httpResult struct {
	status int
	body   map[string]any
}

// doControlledRequest performs one request without stopping the test on a
// transport error, so it is safe to call from goroutines.
func doControlledRequest(client *http.Client, base, method, path, body string) httpResult {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		return httpResult{status: -1}
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return httpResult{status: -1}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpResult{status: resp.StatusCode}
	}
	out := httpResult{status: resp.StatusCode}
	if len(bytes.TrimSpace(raw)) > 0 {
		_ = json.Unmarshal(raw, &out.body)
	}
	return out
}

// checkSnapshotConsistency pins the regression contract: one successful
// detail response must be a single committed state, never a mixture. It
// derives the expected owner from the history chain, so the check catches:
//   - an owner change with version/history still at the pre-handover state;
//   - the same handover appended to history more than once (or missing);
//   - a detail version that differs from the tail history entry's version.
//
// Every history entry must line up 1:1 with a version in order and carry a
// unique action_id, and the owner must equal the target of the last
// assign_owner entry (or the creation operator when none exists).
func checkSnapshotConsistency(t *testing.T, body map[string]any, creationOperator string) {
	t.Helper()
	version := number(body["version"])
	history, ok := body["history"].([]any)
	if !ok {
		t.Fatalf("detail response has no history array: %v", body)
	}
	if len(history) != version {
		t.Fatalf("torn snapshot: detail version %d but history has %d entries; "+
			"owner, version and history must belong to one committed state", version, len(history))
	}

	wantOwner := creationOperator
	seenActions := make(map[string]struct{})
	for i, raw := range history {
		entry, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("history[%d] is not an object: %v", i, raw)
		}
		if got := number(entry["version"]); got != i+1 {
			t.Fatalf("history[%d] carries version %d, out of sequence with detail version %d",
				i, got, version)
		}
		if id, _ := entry["action_id"].(string); id != "" {
			if _, dup := seenActions[id]; dup {
				t.Fatalf("handover/action %q was appended to history more than once", id)
			}
			seenActions[id] = struct{}{}
		}
		if entry["action"] == "assign_owner" {
			target, _ := entry["content"].(string)
			if target == "" {
				t.Fatalf("history[%d] assign_owner entry missing its target: %v", i, entry)
			}
			wantOwner = target
		}
	}

	if owner, _ := body["owner"].(string); owner != wantOwner {
		t.Fatalf("torn snapshot: owner %q does not match the committed handover chain %q "+
			"(detail version %d, %d history entries)", owner, wantOwner, version, len(history))
	}
	last, _ := history[len(history)-1].(map[string]any)
	if got := number(last["version"]); got != version {
		t.Fatalf("detail version %d disagrees with the tail history entry version %d", version, got)
	}
}

// checkParticipantsAndStatus verifies the handover never changes membership
// (the previous owner stays a participant) and cannot move incident status.
func checkParticipantsAndStatus(t *testing.T, body map[string]any, want []string) {
	t.Helper()
	if body["status"] != "open" {
		t.Fatalf("handover must not change incident status: %v", body["status"])
	}
	got, ok := body["participants"].([]any)
	if !ok || len(got) != len(want) {
		t.Fatalf("participants %v, want %v", body["participants"], want)
	}
	for i, name := range want {
		if got[i] != name {
			t.Fatalf("participants %v, want %v (handover must not remove the previous owner)",
				body["participants"], want)
		}
	}
}

func checkTailEntry(t *testing.T, body map[string]any, actionID, operator, action, content string, version int) {
	t.Helper()
	history, _ := body["history"].([]any)
	last, _ := history[len(history)-1].(map[string]any)
	if last["action_id"] != actionID || last["operator"] != operator ||
		last["action"] != action || last["content"] != content ||
		number(last["version"]) != version {
		t.Fatalf("tail history entry %v, want action_id=%q operator=%q action=%q content=%q version=%d",
			last, actionID, operator, action, content, version)
	}
	if last["at"] == nil || last["at"] == "" {
		t.Fatalf("tail history entry missing saved timestamp: %v", last)
	}
}

// TestHTTPOwnerHandoverReadOverlapAtomic covers the regression where a
// handover and a detail read overlap: while the handover save is unfinished,
// no successful detail response may show bob already owning the incident, nor
// any mixture (new owner with old version/history, duplicated history, or a
// tail history version that disagrees with the detail version). Reads may
// wait for the handover to finish. Once the handover is confirmed, every read
// sees exactly one new version and one new history entry describing that
// request; membership, status, and earlier history stay untouched.
func TestHTTPOwnerHandoverReadOverlapAtomic(t *testing.T) {
	server, store, _ := newControlledIncidentServer(t)
	client := &http.Client{Timeout: 10 * time.Second}
	base := server.URL

	// Still-open incident: alice owns, bob participates, at version 2.
	createIncident(t, server)
	if status, add := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); status != http.StatusOK || number(add["version"]) != 2 {
		t.Fatalf("add bob %d %v", status, add)
	}
	status, before := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if status != http.StatusOK || before["owner"] != "alice" || number(before["version"]) != 2 {
		t.Fatalf("baseline detail: %d %v", status, before)
	}
	beforeHistory, _ := before["history"].([]any)
	if len(beforeHistory) != 2 {
		t.Fatalf("baseline history: %v", before["history"])
	}

	const handover = `{"action_id":"h1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`

	// Hold the durable write open: validation is done, the registry lock is
	// held, but owner/version/history have not advanced. The release always
	// runs before teardown, so a failed assertion below cannot strand the
	// blocked handler.
	release, entered := store.armStall()
	defer release()
	postOutcome := make(chan httpResult, 1)
	go func() {
		postOutcome <- doControlledRequest(client, base, http.MethodPost,
			"/incidents/INC-1/actions", handover)
	}()
	<-entered

	const readers = 12
	results := make(chan httpResult, readers)
	for i := 0; i < readers; i++ {
		go func() {
			results <- doControlledRequest(client, base, http.MethodGet, "/incidents/INC-1", "")
		}()
	}

	// Anything that answers before the save is released must be the complete
	// pre-handover state; the post state must not leak early.
	var early []httpResult
	earlyDeadline := time.After(50 * time.Millisecond)
drainEarly:
	for {
		select {
		case res := <-results:
			early = append(early, res)
		case <-earlyDeadline:
			break drainEarly
		}
	}

	// Finish the save; the handover request confirms once.
	release()
	var saved httpResult
	select {
	case saved = <-postOutcome:
	case <-time.After(5 * time.Second):
		t.Fatal("handover request never returned after the save completed")
	}

	// From here on every assertion happens with the save finished; deferred
	// release is already a no-op.
	if saved.status != http.StatusOK || number(saved.body["version"]) != 3 {
		t.Fatalf("handover confirmation %d %v", saved.status, saved.body)
	}

	for _, res := range early {
		if res.status != http.StatusOK {
			t.Fatalf("overlapping read must succeed (it may wait), got %d", res.status)
		}
		checkSnapshotConsistency(t, res.body, "alice")
		checkParticipantsAndStatus(t, res.body, []string{"alice", "bob"})
		if v := number(res.body["version"]); v != 2 {
			t.Fatalf("detail read before handover confirmation must not show the post-handover version, got %d", v)
		}
		if res.body["owner"] != "alice" {
			t.Fatalf("bob must not become visible before the handover is saved: %v", res.body["owner"])
		}
	}

	// Every still-outstanding read now returns; each body must be one whole
	// committed state at version 2 or 3, never a mixture.
	var late []httpResult
	lateDeadline := time.After(5 * time.Second)
	for len(early)+len(late) < readers {
		select {
		case res := <-results:
			late = append(late, res)
		case <-lateDeadline:
			t.Fatalf("only %d of %d overlapping reads returned", len(early)+len(late), readers)
		}
	}
	for _, res := range late {
		if res.status != http.StatusOK {
			t.Fatalf("overlapping read after save: status %d", res.status)
		}
		checkSnapshotConsistency(t, res.body, "alice")
		checkParticipantsAndStatus(t, res.body, []string{"alice", "bob"})
		switch v := number(res.body["version"]); v {
		case 2, 3:
		default:
			t.Fatalf("detail read during overlap must be a pre- or post-handover snapshot, got version %d", v)
		}
	}

	// A read launched only after the handover is confirmed must see the
	// complete post-handover state.
	gotStatus, after := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if gotStatus != http.StatusOK {
		t.Fatalf("detail after confirmation: %d", gotStatus)
	}
	if after["owner"] != "bob" || after["status"] != "open" || number(after["version"]) != 3 {
		t.Fatalf("post-handover head: %v", after)
	}
	checkParticipantsAndStatus(t, after, []string{"alice", "bob"})

	history, _ := after["history"].([]any)
	if len(history) != 3 {
		t.Fatalf("exactly one version/history entry may be added by the handover, got %d entries", len(history))
	}
	// The earlier history keeps the same content as before.
	for i := 0; i < len(beforeHistory); i++ {
		want := beforeHistory[i].(map[string]any)
		got := history[i].(map[string]any)
		if got["action"] != want["action"] || got["operator"] != want["operator"] ||
			got["content"] != want["content"] || number(got["version"]) != number(want["version"]) {
			t.Fatalf("handover rewrote earlier history[%d]: %v vs %v", i, got, want)
		}
	}
	// The single new tail entry corresponds to the confirmed request in
	// target name, operator, action id, and version.
	checkTailEntry(t, after, "h1", "alice", "assign_owner", "bob", 3)

	// Exactly one durable handover record was written.
	if n := store.countAssignments("INC-1"); n != 1 {
		t.Fatalf("handover must persist exactly once, got %d assignment records", n)
	}
}

// TestHTTPOwnerHandoverStaysConsistentAcrossRepeatedOverlaps drives several
// alternating handovers (alice->bob->alice ...), each with detail reads
// overlapping its save window. Every returned snapshot must pass the
// single-committed-state consistency check, and the final detail must line
// up 1:1 with the six confirmed handovers.
func TestHTTPOwnerHandoverStaysConsistentAcrossRepeatedOverlaps(t *testing.T) {
	server, store, _ := newControlledIncidentServer(t)
	client := &http.Client{Timeout: 10 * time.Second}
	base := server.URL

	createIncident(t, server)
	if status, _ := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); status != http.StatusOK {
		t.Fatalf("add bob status %d", status)
	}

	rounds := []struct {
		actionID string
		operator string
		target   string
		expected int
		next     int
	}{
		{"h1", "alice", "bob", 2, 3},
		{"h2", "bob", "alice", 3, 4},
		{"h3", "alice", "bob", 4, 5},
		{"h4", "bob", "alice", 5, 6},
		{"h5", "alice", "bob", 6, 7},
		{"h6", "bob", "alice", 7, 8},
	}
	const readersPerRound = 8

	for _, r := range rounds {
		body := `{"action_id":"` + r.actionID + `","operator":"` + r.operator +
			`","expected_version":` + strconv.Itoa(r.expected) +
			`,"action":"assign_owner","participant":"` + r.target + `"}`

		// One closure per round scopes the deferred release so a failure in
		// this round unblocks the stalled handler before moving on.
		func() {
			release, entered := store.armStall()
			defer release()
			postOutcome := make(chan httpResult, 1)
			go func() {
				postOutcome <- doControlledRequest(client, base, http.MethodPost,
					"/incidents/INC-1/actions", body)
			}()
			<-entered

			results := make(chan httpResult, readersPerRound)
			for i := 0; i < readersPerRound; i++ {
				go func() {
					results <- doControlledRequest(client, base, http.MethodGet, "/incidents/INC-1", "")
				}()
			}

			var early []httpResult
			earlyDeadline := time.After(20 * time.Millisecond)
		drainEarly:
			for {
				select {
				case res := <-results:
					early = append(early, res)
				case <-earlyDeadline:
					break drainEarly
				}
			}

			release()
			var saved httpResult
			select {
			case saved = <-postOutcome:
			case <-time.After(5 * time.Second):
				t.Fatalf("round %s handover never returned", r.actionID)
			}
			if saved.status != http.StatusOK || number(saved.body["version"]) != r.next {
				t.Fatalf("round %s confirmation %d %v", r.actionID, saved.status, saved.body)
			}

			var late []httpResult
			lateDeadline := time.After(5 * time.Second)
			for len(early)+len(late) < readersPerRound {
				select {
				case res := <-results:
					late = append(late, res)
				case <-lateDeadline:
					t.Fatalf("round %s: only %d of %d overlapping reads returned",
						r.actionID, len(early)+len(late), readersPerRound)
				}
			}

			// All checks happen after the save finished, with release already
			// a no-op; failing here still unblocks the handler via defer.
			for _, res := range early {
				if res.status != http.StatusOK {
					t.Fatalf("round %s early read status %d", r.actionID, res.status)
				}
				checkSnapshotConsistency(t, res.body, "alice")
				// Before the save completes the reader must still see the
				// previous owner — the operator of this round's handover.
				if number(res.body["version"]) != r.expected || res.body["owner"] != r.operator {
					t.Fatalf("round %s: early read leaked an advanced state: %v", r.actionID, res.body)
				}
			}
			for _, res := range late {
				if res.status != http.StatusOK {
					t.Fatalf("round %s late read status %d", r.actionID, res.status)
				}
				checkSnapshotConsistency(t, res.body, "alice")
				checkParticipantsAndStatus(t, res.body, []string{"alice", "bob"})
				switch v := number(res.body["version"]); v {
				case r.expected, r.next:
				default:
					t.Fatalf("round %s read at unexpected version %d", r.actionID, v)
				}
			}
		}()

		// A read strictly after this confirmation sees the new owner/version.
		gotStatus, got := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
		if gotStatus != http.StatusOK || got["owner"] != r.target || number(got["version"]) != r.next {
			t.Fatalf("round %s post-confirm detail: %d %v", r.actionID, gotStatus, got)
		}
	}

	gotStatus, final := doJSON(t, server, http.MethodGet, "/incidents/INC-1", "")
	if gotStatus != http.StatusOK || number(final["version"]) != 8 || final["owner"] != "alice" {
		t.Fatalf("final head: %d %v", gotStatus, final)
	}
	checkParticipantsAndStatus(t, final, []string{"alice", "bob"})
	history, _ := final["history"].([]any)
	if len(history) != 8 {
		t.Fatalf("six handovers must add six history entries, got %d", len(history))
	}
	for i, r := range rounds {
		entry, _ := history[i+2].(map[string]any)
		if entry["action_id"] != r.actionID || entry["operator"] != r.operator ||
			entry["action"] != "assign_owner" || entry["content"] != r.target ||
			number(entry["version"]) != r.next {
			t.Fatalf("history[%d] %v does not match the confirmed handover %+v", i+2, entry, r)
		}
	}
	if n := store.countAssignments("INC-1"); n != 6 {
		t.Fatalf("six successful handovers, %d durable assignment records", n)
	}
}

// TestHTTPConcurrentHandoverSameVersionOneWins: two distinct actions handed
// in at the same current version, targeting two different existing
// participants, must not both commit. Exactly one returns 200; the other gets
// 409 carrying the then-current version. The final owner and the sole new
// history entry come from the winner, and only one durable record is added.
func TestHTTPConcurrentHandoverSameVersionOneWins(t *testing.T) {
	const rounds = 10
	for round := 0; round < rounds; round++ {
		t.Run("", func(t *testing.T) {
			server, store, _ := newControlledIncidentServer(t)
			client := &http.Client{Timeout: 10 * time.Second}
			id := "INC-C" + strconv.Itoa(round)

			if status, created := doJSON(t, server, http.MethodPost, "/incidents",
				`{"id":"`+id+`","title":"Outage","service":"gateway","operator":"alice"}`); status != http.StatusOK || number(created["version"]) != 1 {
				t.Fatalf("create %d %v", status, created)
			}
			for _, add := range []struct {
				id     string
				name   string
				expect int
				next   int
			}{
				{"pb", "bob", 1, 2},
				{"pc", "carol", 2, 3},
			} {
				status, res := postAction(t, server, id,
					`{"action_id":"`+add.id+`","operator":"alice","expected_version":`+strconv.Itoa(add.expect)+
						`,"action":"add_participant","participant":"`+add.name+`"}`)
				if status != http.StatusOK || number(res["version"]) != add.next {
					t.Fatalf("add %s %d %v", add.name, status, res)
				}
			}

			toBob := `{"action_id":"hb","operator":"alice","expected_version":3,"action":"assign_owner","participant":"bob"}`
			toCarol := `{"action_id":"hc","operator":"alice","expected_version":3,"action":"assign_owner","participant":"carol"}`

			start := make(chan struct{})
			outcomes := make(chan httpResult, 2)
			go func() {
				<-start
				outcomes <- doControlledRequest(client, server.URL, http.MethodPost,
					"/incidents/"+id+"/actions", toBob)
			}()
			go func() {
				<-start
				outcomes <- doControlledRequest(client, server.URL, http.MethodPost,
					"/incidents/"+id+"/actions", toCarol)
			}()
			close(start)

			first := <-outcomes
			second := <-outcomes

			var winner, loser httpResult
			switch {
			case first.status == http.StatusOK && second.status == http.StatusConflict:
				winner, loser = first, second
			case second.status == http.StatusOK && first.status == http.StatusConflict:
				winner, loser = second, first
			default:
				t.Fatalf("same-version handovers: want one 200 and one 409, got %d and %d (%v / %v)",
					first.status, second.status, first.body, second.body)
			}
			if number(winner.body["version"]) != 4 {
				t.Fatalf("winner must reach version 4: %v", winner.body)
			}
			if loser.body["error"] == "" || number(loser.body["current_version"]) != 4 {
				t.Fatalf("loser must be 409 naming current_version 4: %v", loser.body)
			}

			// The final state belongs wholly to the winning action.
			winnerTarget := map[string]string{"hb": "bob", "hc": "carol"}
			winnerID, _ := winner.body["action_id"].(string)
			target := winnerTarget[winnerID]
			loserID := "hc"
			if winnerID == "hc" {
				loserID = "hb"
			}

			status, got := doJSON(t, server, http.MethodGet, "/incidents/"+id, "")
			if status != http.StatusOK {
				t.Fatalf("final detail %d", status)
			}
			checkSnapshotConsistency(t, got, "alice")
			if got["owner"] != target || number(got["version"]) != 4 || got["status"] != "open" {
				t.Fatalf("final head must match the winner: %v", got)
			}
			checkParticipantsAndStatus(t, got, []string{"alice", "bob", "carol"})
			hist, _ := got["history"].([]any)
			if len(hist) != 4 {
				t.Fatalf("exactly one handover entry, got %d", len(hist))
			}
			last, _ := hist[3].(map[string]any)
			if last["action"] != "assign_owner" || last["action_id"] != winnerID ||
				last["content"] != target || last["operator"] != "alice" ||
				number(last["version"]) != 4 {
				t.Fatalf("sole new history entry must be the winner's: %v", last)
			}
			for _, entry := range hist {
				e := entry.(map[string]any)
				if e["action_id"] == loserID {
					t.Fatalf("the losing handover %s must not appear in history", loserID)
				}
			}
			if n := store.countAssignments(id); n != 1 {
				t.Fatalf("exactly one handover may persist, got %d", n)
			}
		})
	}
}

// TestHTTPFailedHandoverSaveIs503AndDoesNotAdvance: an otherwise legal
// handover whose durable save genuinely fails must answer 503 and leave
// owner, version, and history exactly where they were. Later incident
// requests keep failing under the existing poisoning rule, and resubmitting
// cannot promote the failed handover into a success.
func TestHTTPFailedHandoverSaveIs503AndDoesNotAdvance(t *testing.T) {
	server, store, reg := newControlledIncidentServer(t)

	createIncident(t, server)
	if status, add := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`); status != http.StatusOK || number(add["version"]) != 2 {
		t.Fatalf("add bob %d %v", status, add)
	}

	// The next save is a real failure; the handover itself is legal in every
	// respect (open, bob participates, current version).
	store.mu.Lock()
	store.failNext = true
	store.mu.Unlock()
	status, failed := postAction(t, server, "INC-1",
		`{"action_id":"h1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`)
	if status != http.StatusServiceUnavailable || failed["error"] == "" {
		t.Fatalf("failed handover save must be 503 with an error, got %d %v", status, failed)
	}

	// The failed request committed nothing: owner, version, history unchanged.
	state, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Owner != "alice" || state.Version != 2 || len(state.History) != 2 {
		t.Fatalf("failed handover advanced the state: owner=%q version=%d history=%d",
			state.Owner, state.Version, len(state.History))
	}
	last := state.History[len(state.History)-1]
	if last.Action != incidents.ActionAddParticipant || last.ActionID != "p1" || last.Version != 2 {
		t.Fatalf("tail history must still be the pre-handover entry: %+v", last)
	}
	if n := store.countAssignments("INC-1"); n != 0 {
		t.Fatalf("the failed handover must not be durable, found %d assignment records", n)
	}

	// Existing rule: after an incident storage failure, incident requests
	// stay 503 in this process — reads, retries, other actions, creates.
	for _, try := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"detail read", http.MethodGet, "/incidents/INC-1", ""},
		{"retry same handover", http.MethodPost, "/incidents/INC-1/actions",
			`{"action_id":"h1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`},
		{"different handover", http.MethodPost, "/incidents/INC-1/actions",
			`{"action_id":"h2","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`},
		{"new incident", http.MethodPost, "/incidents",
			`{"id":"INC-2","title":"t","service":"gateway","operator":"alice"}`},
	} {
		res := doControlledRequest(&http.Client{Timeout: 5 * time.Second}, server.URL, try.method, try.path, try.body)
		if res.status != http.StatusServiceUnavailable {
			t.Fatalf("%s after save failure: want 503, got %d", try.name, res.status)
		}
	}

	// Resubmitting cannot make the failed handover count: state and durable
	// log remain exactly as they were before the attempt.
	state, err = reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Owner != "alice" || state.Version != 2 || len(state.History) != 2 {
		t.Fatalf("retries must not promote the failed handover: owner=%q version=%d history=%d",
			state.Owner, state.Version, len(state.History))
	}
	if n := store.countAssignments("INC-1"); n != 0 {
		t.Fatalf("retries after failure must not persist a handover, found %d records", n)
	}
}
