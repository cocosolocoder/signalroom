package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/incidents"
)

// gatedIncidentStorage is an incident store that can hold one AppendRecord
// call mid-commit, so a test can read the incident while a handover's durable
// save is still in progress.
type gatedIncidentStorage struct {
	mu      sync.Mutex
	records []incidents.Record

	// stall, when non-nil, blocks one AppendRecord (with the registry lock
	// held) until the channel is closed; stallEntered is closed once that
	// AppendRecord reaches the wait. armStall sets both up.
	stall        chan struct{}
	stallEntered chan struct{}
}

// armStall makes the next AppendRecord block until the returned release
// channel is closed. The returned entered channel closes once that
// AppendRecord is waiting, so the test knows the handover is held mid commit.
func (g *gatedIncidentStorage) armStall() (release, entered chan struct{}) {
	release = make(chan struct{})
	entered = make(chan struct{})
	g.mu.Lock()
	g.stall = release
	g.stallEntered = entered
	g.mu.Unlock()
	return release, entered
}

func (g *gatedIncidentStorage) AppendRecord(record incidents.Record) error {
	g.mu.Lock()
	stall := g.stall
	entered := g.stallEntered
	g.stall = nil
	g.stallEntered = nil
	g.mu.Unlock()
	if stall != nil {
		// Only the first AppendRecord blocks: the stalled action still holds
		// the registry lock, so no other AppendRecord can reach here concurrently.
		close(entered)
		<-stall
	}
	g.mu.Lock()
	g.records = append(g.records, record)
	g.mu.Unlock()
	return nil
}

func (g *gatedIncidentStorage) Poisoned() bool { return false }

func newGatedIncidentServer(t *testing.T) (*httptest.Server, *gatedIncidentStorage) {
	t.Helper()
	tl := events.NewTimeline()
	eventStore := &fakeStorage{cursorKey: []byte("test-cursor-key-0123456789ab")}
	incStore := &gatedIncidentStorage{}
	reg := incidents.NewRegistry(tl, incStore.AppendRecord)
	server := httptest.NewServer(NewHandler(tl, eventStore, eventStore, WithIncidents(reg, incStore)))
	t.Cleanup(server.Close)
	return server, incStore
}

// handoverDetail is the decoded GET /incidents/{id} response, restricted to
// the fields the handover regression checks.
type handoverDetail struct {
	Status       string               `json:"status"`
	Version      int                  `json:"version"`
	Owner        string               `json:"owner"`
	Participants []string             `json:"participants"`
	History      []handoverHistoryEnt `json:"history"`
}

type handoverHistoryEnt struct {
	ActionID string `json:"action_id"`
	Operator string `json:"operator"`
	Action   string `json:"action"`
	Content  string `json:"content"`
	Version  int    `json:"version"`
}

type detailResponse struct {
	status int
	detail handoverDetail
	err    error
}

// getDetailAsync issues GET /incidents/{id} in the background so the test can
// overlap it with an in-flight handover.
func getDetailAsync(server *httptest.Server, id string) <-chan detailResponse {
	ch := make(chan detailResponse, 1)
	go func() {
		resp, err := http.Get(server.URL + "/incidents/" + id)
		if err != nil {
			ch <- detailResponse{err: err}
			return
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			ch <- detailResponse{err: err}
			return
		}
		var d handoverDetail
		if err := json.Unmarshal(raw, &d); err != nil {
			ch <- detailResponse{err: fmt.Errorf("decode %q: %w", raw, err)}
			return
		}
		ch <- detailResponse{status: resp.StatusCode, detail: d}
	}()
	return ch
}

func getDetail(t *testing.T, server *httptest.Server, id string) handoverDetail {
	t.Helper()
	r := <-getDetailAsync(server, id)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.status != http.StatusOK {
		t.Fatalf("get /incidents/%s status %d", id, r.status)
	}
	return r.detail
}

type actionHTTPResponse struct {
	status int
	body   map[string]any
	err    error
}

// postActionAsync issues one action request in the background, waiting on
// start first when it is non-nil. Transport and decode problems come back on
// the channel so no test helper fails from a goroutine.
func postActionAsync(start <-chan struct{}, server *httptest.Server, id, body string) <-chan actionHTTPResponse {
	ch := make(chan actionHTTPResponse, 1)
	go func() {
		if start != nil {
			<-start
		}
		req, err := http.NewRequest(http.MethodPost, server.URL+"/incidents/"+id+"/actions",
			strings.NewReader(body))
		if err != nil {
			ch <- actionHTTPResponse{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			ch <- actionHTTPResponse{err: err}
			return
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			ch <- actionHTTPResponse{err: err}
			return
		}
		out := map[string]any{}
		if err := json.Unmarshal(raw, &out); err != nil {
			ch <- actionHTTPResponse{err: fmt.Errorf("decode %q: %w", raw, err)}
			return
		}
		ch <- actionHTTPResponse{status: resp.StatusCode, body: out}
	}()
	return ch
}

// assertCoherentSnapshot verifies that one returned detail is a single
// committed state: exactly one history entry per version, versions advancing
// one at a time without repeats, the tail entry carrying the head version,
// and the owner drawn from the membership. This is what rejects a detail
// whose owner moved while its version or history stayed behind, a duplicated
// history entry, or a head version that disagrees with the history tail.
func assertCoherentSnapshot(t *testing.T, d handoverDetail) {
	t.Helper()
	if len(d.History) != d.Version {
		t.Fatalf("version %d with %d history entries: detail mixes committed states: %+v",
			d.Version, len(d.History), d)
	}
	for i, entry := range d.History {
		if entry.Version != i+1 {
			t.Fatalf("history[%d] carries version %d: entries must advance one version "+
				"at a time with no repeats: %+v", i, entry.Version, d)
		}
	}
	if !slices.Contains(d.Participants, d.Owner) {
		t.Fatalf("owner %q is not among participants %v", d.Owner, d.Participants)
	}
}

// TestHTTPHandoverReadDuringCommitSeesCoherentSnapshot pins the read side of
// an owner handover: while the handover's durable save is still in progress,
// GET /incidents/{id} must return either the complete pre-handover state or
// the complete post-handover state — never bob as owner with the pre-handover
// version or history, never an extra history entry. Once the handover is
// confirmed, a later read must see the post-handover state: owner bob,
// version up by exactly one, and a single new history tail whose target,
// operator, action id, and version match the successful request. Membership,
// status, and the earlier history stay as they were.
func TestHTTPHandoverReadDuringCommitSeesCoherentSnapshot(t *testing.T) {
	server, incStore := newGatedIncidentServer(t)
	createIncident(t, server)
	status, body := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`)
	if status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("add participant %d %v", status, body)
	}

	preHistory := []handoverHistoryEnt{
		{ActionID: "", Operator: "alice", Action: "create", Content: "Outage", Version: 1},
		{ActionID: "p1", Operator: "alice", Action: "add_participant", Content: "bob", Version: 2},
	}
	wantPre := handoverDetail{
		Status: "open", Version: 2, Owner: "alice",
		Participants: []string{"alice", "bob"},
		History:      preHistory,
	}
	wantPost := handoverDetail{
		Status: "open", Version: 3, Owner: "bob",
		Participants: []string{"alice", "bob"},
		History: append(slices.Clone(preHistory), handoverHistoryEnt{
			ActionID: "h1", Operator: "alice", Action: "assign_owner", Content: "bob", Version: 3,
		}),
	}

	release, entered := incStore.armStall()
	handoverCh := postActionAsync(nil, server, "INC-1",
		`{"action_id":"h1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`)

	// The handover's durable save is now in progress; the commit has not
	// completed. A read issued now may wait for the save, but whatever it
	// returns must be one whole committed state.
	<-entered
	readCh := getDetailAsync(server, "INC-1")
	time.Sleep(50 * time.Millisecond)
	close(release)

	hr := <-handoverCh
	if hr.err != nil {
		t.Fatal(hr.err)
	}
	if hr.status != http.StatusOK || number(hr.body["version"]) != 3 {
		t.Fatalf("handover %d %v", hr.status, hr.body)
	}

	rd := <-readCh
	if rd.err != nil {
		t.Fatal(rd.err)
	}
	if rd.status != http.StatusOK {
		t.Fatalf("overlapping read status %d", rd.status)
	}
	assertCoherentSnapshot(t, rd.detail)
	if !reflect.DeepEqual(rd.detail, wantPre) && !reflect.DeepEqual(rd.detail, wantPost) {
		t.Fatalf("overlapping read must be the complete pre- or post-handover state, got %+v", rd.detail)
	}

	// A read issued after the confirmed success must see the post-handover
	// state: bob owns it, the version moved by exactly one, and the history
	// gained exactly the one handover entry. Alice stays a participant.
	after := getDetail(t, server, "INC-1")
	assertCoherentSnapshot(t, after)
	if !reflect.DeepEqual(after, wantPost) {
		t.Fatalf("post-success read %+v want %+v", after, wantPost)
	}
}

// TestHTTPConcurrentHandoversSameVersionSingleWinner pins the conflict rule:
// two different handovers submitted against the same current version, naming
// two different existing participants, cannot both commit. Exactly one
// succeeds; the other gets 409 carrying the current version. The final owner
// and the single new history entry come from the winning action alone.
func TestHTTPConcurrentHandoversSameVersionSingleWinner(t *testing.T) {
	server, store, _, _ := newIncidentServer(t)
	createIncident(t, server)
	for i, p := range []struct{ actionID, name string }{{"p1", "bob"}, {"p2", "carol"}} {
		status, body := postAction(t, server, "INC-1", fmt.Sprintf(
			`{"action_id":%q,"operator":"alice","expected_version":%d,"action":"add_participant","participant":%q}`,
			p.actionID, i+1, p.name))
		if status != http.StatusOK {
			t.Fatalf("add %s: %d %v", p.name, status, body)
		}
	}

	// Both handovers observe version 3 and name a different existing
	// participant; the operators differ so the winner's history entry is
	// attributable to exactly one request.
	targets := map[string]struct{ participant, operator string }{
		"h-bob":   {"bob", "alice"},
		"h-carol": {"carol", "carol"},
	}
	start := make(chan struct{})
	bobCh := postActionAsync(start, server, "INC-1",
		`{"action_id":"h-bob","operator":"alice","expected_version":3,"action":"assign_owner","participant":"bob"}`)
	carolCh := postActionAsync(start, server, "INC-1",
		`{"action_id":"h-carol","operator":"carol","expected_version":3,"action":"assign_owner","participant":"carol"}`)
	close(start)

	results := map[string]actionHTTPResponse{}
	for actionID, ch := range map[string]<-chan actionHTTPResponse{"h-bob": bobCh, "h-carol": carolCh} {
		r := <-ch
		if r.err != nil {
			t.Fatalf("%s: %v", actionID, r.err)
		}
		results[actionID] = r
	}

	var winner, loser string
	for actionID, r := range results {
		switch r.status {
		case http.StatusOK:
			if winner != "" {
				t.Fatalf("both handovers succeeded: %v", results)
			}
			winner = actionID
		case http.StatusConflict:
			loser = actionID
		default:
			t.Fatalf("%s: unexpected status %d %v", actionID, r.status, r.body)
		}
	}
	if winner == "" || loser == "" {
		t.Fatalf("want exactly one 200 and one 409, got %v", results)
	}
	if got := results[winner].body; number(got["version"]) != 4 || got["action_id"] != winner {
		t.Fatalf("winning handover body %v", got)
	}
	if got := results[loser].body; number(got["current_version"]) != 4 {
		t.Fatalf("losing handover must report the current version 4: %v", got)
	}

	// The losing request must not have persisted anything: create, two
	// additions, and the one winning handover.
	if len(store.records) != 4 {
		t.Fatalf("durable records %d, want 4", len(store.records))
	}

	detail := getDetail(t, server, "INC-1")
	assertCoherentSnapshot(t, detail)
	target := targets[winner]
	if detail.Owner != target.participant {
		t.Fatalf("owner %q, want the winning target %q", detail.Owner, target.participant)
	}
	if detail.Version != 4 || detail.Status != "open" {
		t.Fatalf("head %+v", detail)
	}
	if !reflect.DeepEqual(detail.Participants, []string{"alice", "bob", "carol"}) {
		t.Fatalf("participants %+v", detail.Participants)
	}
	wantHistory := []handoverHistoryEnt{
		{ActionID: "", Operator: "alice", Action: "create", Content: "Outage", Version: 1},
		{ActionID: "p1", Operator: "alice", Action: "add_participant", Content: "bob", Version: 2},
		{ActionID: "p2", Operator: "alice", Action: "add_participant", Content: "carol", Version: 3},
		{ActionID: winner, Operator: target.operator, Action: "assign_owner", Content: target.participant, Version: 4},
	}
	if !reflect.DeepEqual(detail.History, wantHistory) {
		t.Fatalf("history %+v want %+v (the loser must leave no entry)", detail.History, wantHistory)
	}
}

// TestHTTPHandoverStorageFailureLeavesStateUntouched pins the failure side of
// a handover: when the durable save of an otherwise legal handover genuinely
// fails, the request is a 503 and the incident does not move — owner,
// version, and history all stay at the pre-handover state, and the failed
// record is not persisted. Every later incident request keeps returning 503,
// so the failed handover cannot be turned into a success by resubmitting it.
func TestHTTPHandoverStorageFailureLeavesStateUntouched(t *testing.T) {
	server, store, _, reg := newIncidentServer(t)
	createIncident(t, server)
	status, body := postAction(t, server, "INC-1",
		`{"action_id":"p1","operator":"alice","expected_version":1,"action":"add_participant","participant":"bob"}`)
	if status != http.StatusOK || number(body["version"]) != 2 {
		t.Fatalf("add participant %d %v", status, body)
	}

	handover := `{"action_id":"h1","operator":"alice","expected_version":2,"action":"assign_owner","participant":"bob"}`
	store.failNext = true
	status, body = postAction(t, server, "INC-1", handover)
	if status != http.StatusServiceUnavailable || body["error"] == "" {
		t.Fatalf("failed handover save %d %v", status, body)
	}

	// The failed save must not have advanced anything: still alice at
	// version 2 with the two original history entries, and no record stored.
	inc, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if inc.Owner != "alice" || inc.Version != 2 || inc.Status != incidents.StatusOpen {
		t.Fatalf("incident moved despite the failed save: %+v", inc)
	}
	if len(inc.History) != 2 {
		t.Fatalf("history advanced despite the failed save: %+v", inc.History)
	}
	for _, entry := range inc.History {
		if entry.ActionID == "h1" {
			t.Fatalf("failed handover must not appear in history: %+v", inc.History)
		}
	}
	if !reflect.DeepEqual(inc.Participants, []string{"alice", "bob"}) {
		t.Fatalf("participants %+v", inc.Participants)
	}
	if len(store.records) != 2 {
		t.Fatalf("failed handover must not persist a record: %d records", len(store.records))
	}

	// Incident requests stay 503: reads, and any resubmission of the failed
	// handover — it must not come back as a success or a replay.
	if status, _ := doJSON(t, server, http.MethodGet, "/incidents/INC-1", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("get after failure: want 503 got %d", status)
	}
	if status, _ := postAction(t, server, "INC-1", handover); status != http.StatusServiceUnavailable {
		t.Fatalf("resubmitted handover: want 503 got %d", status)
	}
	again, err := reg.Get("INC-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Owner != "alice" || again.Version != 2 || len(again.History) != 2 {
		t.Fatalf("resubmission must not commit the failed handover: %+v", again)
	}
}
