package httpapi

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

// The scenario shared by the batch-atomicity tests: one-minute windows over
// a three-minute range, threshold two, two consecutive windows at threshold
// to trigger, and one window below threshold to recover.
const atomicPreviewBody = `{
	"since":"2026-10-01T10:00:00Z",
	"until":"2026-10-01T10:03:00Z",
	"window_seconds":60,
	"threshold":2,
	"trigger_windows":2,
	"recover_windows":1,
	"service":"gateway",
	"group_labels":["env"]
}`

// atomicSeed leaves the prod group one event short of a trigger run: the
// first window reaches the threshold, the second does not, and the third
// window stays empty. No alert can fire from this state.
const atomicSeed = `{"events":[
	{"id":"p1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:10Z","labels":{"env":"prod"}},
	{"id":"p2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:50Z","labels":{"env":"prod"}},
	{"id":"p3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:10Z","labels":{"env":"prod"}}
]}`

// atomicBatch completes the prod trigger run with one more event in the
// second window and creates a second label group whose first two windows
// both reach the threshold, all as one commit.
const atomicBatch = `{"events":[
	{"id":"p4","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:40Z","labels":{"env":"prod"}},
	{"id":"s1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"staging"}},
	{"id":"s2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:45Z","labels":{"env":"staging"}},
	{"id":"s3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:05Z","labels":{"env":"staging"}},
	{"id":"s4","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:55Z","labels":{"env":"staging"}}
]}`

// alertCycle is one triggered/recovered pair; RecoveredAt is empty when the
// alert is still open at the range end.
type alertCycle struct {
	TriggeredAt string
	RecoveredAt string
}

// groupState is the observable state of one preview group: its label value,
// per-window counts and end-of-window statuses, and its alert cycles.
type groupState struct {
	Env      string
	Counts   []int
	Statuses []string
	Alerts   []alertCycle
}

// summarizePreview reduces a decoded preview response to the per-group state
// the atomicity tests compare, so a mixed observation (one group pre-commit,
// another post-commit, or counts disagreeing with alerts) cannot match either
// expected state.
func summarizePreview(t *testing.T, out map[string]any) []groupState {
	t.Helper()
	groups, ok := out["groups"].([]any)
	if !ok {
		t.Fatalf("groups missing or wrong type: %v", out)
	}
	states := make([]groupState, 0, len(groups))
	for _, g := range groups {
		group := g.(map[string]any)
		values := group["label_values"].([]any)
		if len(values) != 1 {
			t.Fatalf("one group label -> one label value, got %v", values)
		}
		state := groupState{Env: values[0].(string)}
		for _, w := range group["windows"].([]any) {
			window := w.(map[string]any)
			state.Counts = append(state.Counts, int(window["count"].(float64)))
			state.Statuses = append(state.Statuses, window["status"].(string))
		}
		for _, a := range group["alerts"].([]any) {
			alert := a.(map[string]any)
			cycle := alertCycle{TriggeredAt: alert["triggered_at"].(string)}
			if recovered, ok := alert["recovered_at"].(string); ok {
				cycle.RecoveredAt = recovered
			}
			state.Alerts = append(state.Alerts, cycle)
		}
		states = append(states, state)
	}
	return states
}

// wantPreCommit is the complete state before atomicBatch lands: only the
// prod group exists, its second window stays below the threshold, and no
// alert ever fires.
var wantPreCommit = []groupState{
	{
		Env:      "prod",
		Counts:   []int{2, 1, 0},
		Statuses: []string{"normal", "normal", "normal"},
	},
}

// wantPostCommit is the complete state after atomicBatch lands: both groups
// trigger at the end of the second window and recover at the end of the
// third, and the empty third window is still reported with a zero count.
var wantPostCommit = []groupState{
	{
		Env:      "prod",
		Counts:   []int{2, 2, 0},
		Statuses: []string{"normal", "alerting", "normal"},
		Alerts:   []alertCycle{{TriggeredAt: "2026-10-01T10:02:00Z", RecoveredAt: "2026-10-01T10:03:00Z"}},
	},
	{
		Env:      "staging",
		Counts:   []int{2, 2, 0},
		Statuses: []string{"normal", "alerting", "normal"},
		Alerts:   []alertCycle{{TriggeredAt: "2026-10-01T10:02:00Z", RecoveredAt: "2026-10-01T10:03:00Z"}},
	},
}

func TestPreviewReflectsWholeCommittedBatch(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, atomicSeed); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, out := postPreview(t, server, atomicPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("pre-commit preview: %d %v", status, out)
	}
	if names := out["group_labels"].([]any); len(names) != 1 || names[0] != "env" {
		t.Fatalf("group labels: %v", names)
	}
	if got := summarizePreview(t, out); !reflect.DeepEqual(got, wantPreCommit) {
		t.Fatalf("pre-commit state:\ngot  %+v\nwant %+v", got, wantPreCommit)
	}

	status, out = post(t, server, atomicBatch)
	if status != http.StatusOK || out["created"].(float64) != 5 {
		t.Fatalf("commit batch: %d %v", status, out)
	}

	// Once the commit has returned, every identical preview must show the
	// complete post-commit state, including the zero-count third window.
	for attempt := 0; attempt < 3; attempt++ {
		status, out = postPreview(t, server, atomicPreviewBody)
		if status != http.StatusOK {
			t.Fatalf("post-commit preview %d: %d %v", attempt, status, out)
		}
		if got := summarizePreview(t, out); !reflect.DeepEqual(got, wantPostCommit) {
			t.Fatalf("post-commit preview %d:\ngot  %+v\nwant %+v", attempt, got, wantPostCommit)
		}
	}
}

// fetchPreview posts one preview without failing the test from a goroutine
// and returns the raw response bytes; the server's JSON encoding is
// deterministic, so equal states produce byte-identical bodies.
func fetchPreview(server *httptest.Server, body string) (int, []byte, error) {
	resp, err := http.Post(server.URL+"/alerts/preview", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

func TestPreviewOverlappingCommitSeesWholeBatch(t *testing.T) {
	// Each round races one batch commit against a pool of concurrent
	// previews; every observed response must be byte-identical to either the
	// complete pre-commit or the complete post-commit state, never a mix.
	for round := 0; round < 15; round++ {
		server, _, _ := newTestServer(t)
		if status, _ := post(t, server, atomicSeed); status != http.StatusOK {
			t.Fatal("seed")
		}

		status, before, err := fetchPreview(server, atomicPreviewBody)
		if err != nil || status != http.StatusOK {
			t.Fatalf("round %d pre-commit preview: %d %v", round, status, err)
		}

		stop := make(chan struct{})
		observed := make(chan []byte, 256)
		var wg sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					status, raw, err := fetchPreview(server, atomicPreviewBody)
					if err != nil || status != http.StatusOK {
						observed <- nil
						continue
					}
					observed <- raw
				}
			}()
		}

		if status, out := post(t, server, atomicBatch); status != http.StatusOK {
			t.Fatalf("round %d commit: %d %v", round, status, out)
		}
		close(stop)
		wg.Wait()
		close(observed)

		status, after, err := fetchPreview(server, atomicPreviewBody)
		if err != nil || status != http.StatusOK {
			t.Fatalf("round %d post-commit preview: %d %v", round, status, err)
		}
		if bytes.Equal(before, after) {
			t.Fatalf("round %d: committing the batch must change the preview", round)
		}

		count := 0
		for raw := range observed {
			count++
			if raw == nil {
				t.Fatalf("round %d: preview failed during the commit", round)
			}
			if !bytes.Equal(raw, before) && !bytes.Equal(raw, after) {
				t.Fatalf("round %d: preview is neither the complete pre-commit nor the complete post-commit state:\n%s", round, raw)
			}
		}
		if count == 0 {
			t.Fatalf("round %d: no overlapping previews observed", round)
		}
	}
}

func TestPreviewUnchangedWhenBatchConflicts(t *testing.T) {
	server, store, _ := newTestServer(t)
	if status, _ := post(t, server, atomicSeed); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, out := postPreview(t, server, atomicPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("pre-commit preview: %d %v", status, out)
	}
	before := mustJSON(out)

	// One event clashes with an already-stored id under different content,
	// so the whole batch — including events that would create the staging
	// group and complete the prod trigger run — must be rejected.
	conflicting := `{"events":[
		{"id":"p1","service":"gateway","severity":"critical","message":"changed","at":"2026-10-01T10:00:10Z","labels":{"env":"prod"}},
		{"id":"p4","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:40Z","labels":{"env":"prod"}},
		{"id":"s1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"staging"}},
		{"id":"s2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:45Z","labels":{"env":"staging"}},
		{"id":"s3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:05Z","labels":{"env":"staging"}},
		{"id":"s4","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:55Z","labels":{"env":"staging"}}
	]}`
	if status, out := post(t, server, conflicting); status != http.StatusConflict {
		t.Fatalf("want 409, got %d %v", status, out)
	}

	status, out = postPreview(t, server, atomicPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("post-conflict preview: %d %v", status, out)
	}
	if !bytes.Equal(mustJSON(out), before) {
		t.Fatalf("a rejected batch must not change the preview:\nbefore %s\nafter  %s", before, mustJSON(out))
	}
	if got := summarizePreview(t, out); !reflect.DeepEqual(got, wantPreCommit) {
		t.Fatalf("post-conflict state:\ngot  %+v\nwant %+v", got, wantPreCommit)
	}

	// Nothing was persisted beyond the seed, and the timeline still holds
	// exactly the three original events.
	if len(store.appended) != 1 || store.appended[0] != 3 {
		t.Fatalf("rejected batch must not persist: %v", store.appended)
	}
	status, rows := get(t, server, "")
	if status != http.StatusOK || len(rows.([]any)) != 3 {
		t.Fatalf("timeline must keep only the seed events: %d %v", status, rows)
	}
}
