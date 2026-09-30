package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

func newTestServer(t *testing.T) (*httptest.Server, *events.PersistentStore) {
	t.Helper()
	dir := t.TempDir()
	store, err := events.OpenStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := httptest.NewServer(NewServer(store).Handler())
	t.Cleanup(func() {
		srv.Close()
		store.Close()
	})
	return srv, store
}

func postEvents(t *testing.T, srv *httptest.Server, body string) (int, map[string]interface{}) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/events", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, result
}

func getEvents(t *testing.T, srv *httptest.Server, query string) (int, []map[string]interface{}) {
	t.Helper()
	resp, err := http.Get(srv.URL + "/events?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, result
}

func eventJSON(id, service, severity, message, at string) string {
	return `{"id":` + jsonStr(id) + `,"service":` + jsonStr(service) + `,"severity":` + jsonStr(severity) + `,"message":` + jsonStr(message) + `,"at":` + jsonStr(at) + `}`
}

func jsonStr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestPostEventsHappy(t *testing.T) {
	srv, _ := newTestServer(t)
	body := `{"events":[` +
		eventJSON("evt-1", "gateway", "critical", "error rate spike", "2026-10-01T10:00:00.123456789Z") + "," +
		eventJSON("evt-2", "checkout", "warning", "latency elevated", "2026-10-01T10:01:00Z") +
		`]}`
	status, result := postEvents(t, srv, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d, want 200: %v", status, result)
	}
	if result["created"] != float64(2) || result["replayed"] != float64(0) {
		t.Fatalf("got created=%v replayed=%v, want 2/0", result["created"], result["replayed"])
	}

	status, events := getEvents(t, srv, "")
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0]["id"] != "evt-1" || events[1]["id"] != "evt-2" {
		t.Fatalf("wrong order: %v", events)
	}
	if events[0]["at"] != "2026-10-01T10:00:00.123456789Z" {
		t.Fatalf("at not RFC3339Nano: %v", events[0]["at"])
	}
}

func TestPostEventsReplayAndConflict(t *testing.T) {
	srv, _ := newTestServer(t)
	at := "2026-10-01T10:00:00Z"
	body := `{"events":[` + eventJSON("evt-1", "gateway", "critical", "spike", at) + `]}`

	status, result := postEvents(t, srv, body)
	if status != http.StatusOK || result["created"] != float64(1) {
		t.Fatalf("first post: status=%d result=%v", status, result)
	}

	// Identical replay is idempotent.
	status, result = postEvents(t, srv, body)
	if status != http.StatusOK || result["created"] != float64(0) || result["replayed"] != float64(1) {
		t.Fatalf("replay: status=%d result=%v", status, result)
	}

	// Same id, different content conflicts.
	conflict := `{"events":[` + eventJSON("evt-1", "gateway", "critical", "different", at) + `]}`
	status, result = postEvents(t, srv, conflict)
	if status != http.StatusConflict {
		t.Fatalf("conflict status=%d, want 409: %v", status, result)
	}
	if _, ok := result["error"].(string); !ok || result["error"] == "" {
		t.Fatalf("conflict response missing error: %v", result)
	}

	// Equivalent RFC3339 offsets compare as the same instant.
	equiv := `{"events":[` + eventJSON("evt-1", "gateway", "critical", "spike", "2026-10-01T18:00:00+08:00") + `]}`
	status, result = postEvents(t, srv, equiv)
	if status != http.StatusOK || result["replayed"] != float64(1) {
		t.Fatalf("equivalent offset: status=%d result=%v", status, result)
	}
}

func TestPostEventsValidation(t *testing.T) {
	srv, _ := newTestServer(t)
	at := "2026-10-01T10:00:00Z"

	for _, tc := range []struct {
		name string
		body string
	}{
		{"invalid json", `{not json}`},
		{"unknown top field", `{"events":[],"extra":1}`},
		{"unknown event field", `{"events":[{"id":"e","service":"s","severity":"i","message":"m","at":` + jsonStr(at) + `,"extra":1}]}`},
		{"empty array", `{"events":[]}`},
		{"missing events", `{"data":[]}`},
		{"events null", `{"events":null}`},
		{"missing id", `{"events":[{"service":"s","severity":"i","message":"m","at":` + jsonStr(at) + `}]}`},
		{"empty service", `{"events":[{"id":"e","service":"  ","severity":"i","message":"m","at":` + jsonStr(at) + `}]}`},
		{"missing message", `{"events":[{"id":"e","service":"s","severity":"i","at":` + jsonStr(at) + `}]}`},
		{"missing at", `{"events":[{"id":"e","service":"s","severity":"i","message":"m"}]}`},
		{"bad at", `{"events":[{"id":"e","service":"s","severity":"i","message":"m","at":"not-a-time"}]}`},
		{"trailing data", `{"events":[]}{}`},
		{"batch dup ids", `{"events":[` +
			eventJSON("evt-1", "s", "i", "m", at) + "," +
			eventJSON(" evt-1", "s", "i", "m", at) + `]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, result := postEvents(t, srv, tc.body)
			if status != http.StatusBadRequest && status != http.StatusConflict {
				t.Fatalf("status=%d, want 400 or 409: %v", status, result)
			}
			if msg, ok := result["error"].(string); !ok || msg == "" {
				t.Fatalf("response missing error: %v", result)
			}
		})
	}

	// Nothing was written.
	_, events := getEvents(t, srv, "")
	if len(events) != 0 {
		t.Fatalf("validation failures wrote data: %v", events)
	}
}

func TestGetEventsFiltersAndOrder(t *testing.T) {
	srv, _ := newTestServer(t)
	base := "2026-10-01T10:00:00Z"
	postEvents(t, srv, `{"events":[`+
		eventJSON("evt-2", "checkout", "warning", "b", "2026-10-01T10:02:00Z")+","+
		eventJSON("evt-1", "gateway", "critical", "a", base)+","+
		eventJSON("evt-3", "gateway", "info", "c", "2026-10-01T10:01:00Z")+
		`]}`)

	for _, tc := range []struct {
		name  string
		query string
		want  []string
	}{
		{"all", "", []string{"evt-1", "evt-3", "evt-2"}},
		{"service", "service=gateway", []string{"evt-1", "evt-3"}},
		{"severity", "severity=critical", []string{"evt-1"}},
		{"since inclusive", "since=2026-10-01T10:01:00Z", []string{"evt-3", "evt-2"}},
		{"until inclusive", "until=2026-10-01T10:01:00Z", []string{"evt-1", "evt-3"}},
		{"range both", "since=2026-10-01T10:01:00Z&until=2026-10-01T10:01:00Z", []string{"evt-3"}},
		{"empty range", "since=2026-10-02T00:00:00Z", []string{}},
		{"severity case", "severity=CRITICAL", []string{"evt-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, events := getEvents(t, srv, tc.query)
			if status != http.StatusOK {
				t.Fatalf("status=%d", status)
			}
			got := make([]string, len(events))
			for i, ev := range events {
				got[i] = ev["id"].(string)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestGetEventsBadTimeRange(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"bad since", "since=not-a-time"},
		{"bad until", "until=not-a-time"},
		{"inverted", "since=2026-10-02T00:00:00Z&until=2026-10-01T00:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/events?" + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400", resp.StatusCode)
			}
			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				t.Fatal(err)
			}
			if msg, ok := result["error"].(string); !ok || msg == "" {
				t.Fatalf("missing error: %v", result)
			}
		})
	}
}

func TestConcurrentPostEvents(t *testing.T) {
	srv, _ := newTestServer(t)
	at := "2026-10-01T10:00:00Z"
	body := `{"events":[` + eventJSON("evt-1", "gateway", "critical", "spike", at) + `]}`

	// Identical concurrent submissions: all succeed, only one event stored.
	var wg sync.WaitGroup
	statuses := make([]int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i], _ = postEvents(t, srv, body)
		}(i)
	}
	wg.Wait()
	for i, s := range statuses {
		if s != http.StatusOK {
			t.Fatalf("request %d status=%d, want 200", i, s)
		}
	}
	_, events := getEvents(t, srv, "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}

	// Conflicting concurrent submissions: exactly one version wins. Identical
	// submissions of the winning version still succeed as replays.
	conflictA := `{"events":[` + eventJSON("evt-2", "s", "i", "version-a", at) + `]}`
	conflictB := `{"events":[` + eventJSON("evt-2", "s", "i", "version-b", at) + `]}`
	statuses = make([]int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				statuses[i], _ = postEvents(t, srv, conflictA)
			} else {
				statuses[i], _ = postEvents(t, srv, conflictB)
			}
		}(i)
	}
	wg.Wait()
	for _, s := range statuses {
		if s != http.StatusOK && s != http.StatusConflict {
			t.Fatalf("unexpected status %d", s)
		}
	}
	_, events = getEvents(t, srv, "")
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	stored := events[1]["message"].(string)
	if stored != "version-a" && stored != "version-b" {
		t.Fatalf("unexpected stored message %q", stored)
	}
	// The losing version's requests must all have conflicted.
	for i, s := range statuses {
		if (i%2 == 0 && stored == "version-b") || (i%2 == 1 && stored == "version-a") {
			if s != http.StatusConflict {
				t.Fatalf("losing request %d status=%d, want 409", i, s)
			}
		}
	}
}

func TestErrorResponsesAreJSON(t *testing.T) {
	srv, _ := newTestServer(t)

	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/nope", http.StatusNotFound},
		{http.MethodPost, "/nope", http.StatusNotFound},
		{http.MethodPut, "/events", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/events", http.StatusMethodNotAllowed},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status=%d, want %d", resp.StatusCode, tc.status)
			}
			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				t.Fatalf("response not JSON: %v", err)
			}
			if msg, ok := result["error"].(string); !ok || msg == "" {
				t.Fatalf("missing error: %v", result)
			}
		})
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	at := "2026-10-01T10:00:00Z"

	store1, err := events.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv1 := httptest.NewServer(NewServer(store1).Handler())
	body := `{"events":[` + eventJSON("evt-1", "gateway", "critical", "spike", at) + `]}`
	status, _ := postEvents(t, srv1, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	srv1.Close()
	store1.Close()

	store2, err := events.OpenStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	srv2 := httptest.NewServer(NewServer(store2).Handler())
	defer srv2.Close()

	_, evs := getEvents(t, srv2, "")
	if len(evs) != 1 || evs[0]["id"] != "evt-1" {
		t.Fatalf("did not recover events: %v", evs)
	}

	// Replay after restart is idempotent.
	status, result := postEvents(t, srv2, body)
	if status != http.StatusOK || result["replayed"] != float64(1) {
		t.Fatalf("replay after restart: status=%d result=%v", status, result)
	}
}

// TestServer503OnStoreFailure runs in a subprocess with RLIMIT_FSIZE set to
// 0. The first write fails, the store breaks, and every subsequent request
// returns 503 until restart.
func TestServer503OnStoreFailure(t *testing.T) {
	if os.Getenv("SIGNALROOM_TEST_RLIMIT") == "1" {
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 0, Max: 0}); err != nil {
			t.Fatalf("setrlimit: %v", err)
		}
		signal.Ignore(syscall.SIGXFSZ)

		dir := t.TempDir()
		store, err := events.OpenStore(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		srv := httptest.NewServer(NewServer(store).Handler())
		defer srv.Close()

		at := "2026-10-01T10:00:00Z"
		body := `{"events":[` + eventJSON("evt-1", "gateway", "critical", "spike", at) + `]}`
		status, result := postEvents(t, srv, body)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("first post status=%d, want 503: %v", status, result)
		}
		if msg, ok := result["error"].(string); !ok || msg == "" {
			t.Fatalf("missing error: %v", result)
		}

		// Subsequent requests also return 503.
		status, _ = postEvents(t, srv, body)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("second post status=%d, want 503", status)
		}
		resp, err := http.Get(srv.URL + "/events")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("get status=%d, want 503", resp.StatusCode)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestServer503OnStoreFailure")
	cmd.Env = append(os.Environ(), "SIGNALROOM_TEST_RLIMIT=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, out)
	}
}

// Ensure the time package is used (RFC3339Nano round-trip).
var _ = time.RFC3339Nano
var _ = bytes.NewReader
