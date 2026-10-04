package httpapi

import (
	"fmt"
	"net/http"
	"testing"
)

// TestPostRejectsDuplicateJSONFields pins the rule that no object in a
// POST /events body may name the same field twice: not the outer envelope,
// not an event object, and not a labels object. The earlier behavior let a
// second, legal value overwrite an earlier illegal one.
func TestPostRejectsDuplicateJSONFields(t *testing.T) {
	server, store, _ := newTestServer(t)
	good := `{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}`
	cases := map[string]string{
		"duplicate top field":        `{"events":[` + good + `],"events":[` + good + `]}`,
		"duplicate event id":         `{"events":[{"id":"e1","id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`,
		"duplicate event same value": `{"events":[{"id":"e1","id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`,
		"duplicate event field mid": `{"events":[
			{"id":"ok","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"},
			{"id":"bad","service":"s","service":"s2","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}
		]}`,
		"illegal labels then legal labels": `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":1},"labels":{"env":"prod"}}]}`,
		"null labels then legal labels":    `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":null,"labels":{"env":"prod"}}]}`,
		"duplicate labels member same":     `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod"},"labels":{"env":"prod"}}]}`,
		"duplicate label name":             `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"a","env":"b"}}]}`,
	}
	// A literal key and its \uXXXX spelling name the same member once JSON
	// is decoded; build the escape without a literal backslash in source.
	esc := func(hex string) string { return string(rune(92)) + "u" + hex }
	cases["duplicate top field unicode"] = `{"events":[` + good + `],"` + esc("0065vents") + `":1}`
	cases["duplicate event id unicode"] = `{"events":[{"id":"e1","` + esc("0069d") + `":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`
	cases["duplicate label name unicode"] = `{"events":[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"a","` + esc("0065nv") + `":"b"}}]}`

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			status, out := post(t, server, body)
			if status != http.StatusBadRequest {
				t.Fatalf("want 400, got %d (%v)", status, out)
			}
			if msg, _ := out["error"].(string); msg == "" {
				t.Fatal("error response needs a non-empty error string")
			}
		})
	}
	if len(store.appended) != 0 {
		t.Fatalf("rejected requests must not persist anything: %v", store.appended)
	}
}

// TestPostDuplicateFieldRejectsWholeBatch verifies atomicity around an
// ambiguous event: neither the valid event before it nor the valid event
// after it becomes visible, and an already-stored event is left untouched.
func TestPostDuplicateFieldRejectsWholeBatch(t *testing.T) {
	server, store, _ := newTestServer(t)
	seed := `{"events":[{"id":"keep","service":"s","severity":"info","message":"v1","at":"2026-10-01T09:00:00Z"}]}`
	if status, out := post(t, server, seed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}

	event := func(id, msg string) string {
		return fmt.Sprintf(`{"id":%q,"service":"s","severity":"info","message":%q,"at":"2026-10-01T09:01:00Z"}`, id, msg)
	}
	// Middle event repeats its own id field; both neighbors are otherwise new
	// and legal and must not be written.
	bad := `{"events":[` +
		event("before", "ok") + `,` +
		`{"id":"mid","id":"mid2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:02:00Z"},` +
		event("after", "ok") + `]}`
	if status, out := post(t, server, bad); status != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %v", status, out)
	}

	status, rows := get(t, server, "")
	if status != http.StatusOK {
		t.Fatalf("get: %d", status)
	}
	arr := rows.([]any)
	if len(arr) != 1 || arr[0].(map[string]any)["id"] != "keep" {
		t.Fatalf("only the pre-existing event must remain, got %v", arr)
	}
	if got := arr[0].(map[string]any)["message"]; got != "v1" {
		t.Fatalf("existing event must be unchanged, got %v", got)
	}
	if len(store.appended) != 1 {
		t.Fatalf("rejected batch must not persist: %v", store.appended)
	}
}

// TestPostDuplicateFieldPrecedesConflict checks that input invalidity wins
// over an id clash that would otherwise be a 409, and that the rejected
// request leaves the service healthy for a following legal batch.
func TestPostDuplicateFieldPrecedesConflict(t *testing.T) {
	server, store, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"dup","service":"s","severity":"info","message":"v1","at":"2026-10-01T09:00:00Z"}]}`)

	// First event clashes with the stored id (would be 409); a later event
	// is syntactically ambiguous (duplicate field). The whole batch is 400.
	mixed := `{"events":[
		{"id":"dup","service":"s","severity":"info","message":"v2","at":"2026-10-01T09:00:00Z"},
		{"id":"new","id":"new2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z"}
	]}`
	if status, out := post(t, server, mixed); status != http.StatusBadRequest {
		t.Fatalf("invalid input must take precedence over conflict, got %d %v", status, out)
	}
	if len(store.appended) != 1 {
		t.Fatalf("rejected batch must not persist: %v", store.appended)
	}

	// A malformed request is an input error, not a storage failure: a legal
	// batch immediately afterwards must succeed.
	good := `{"events":[{"id":"later","service":"s","severity":" CRITICAL ","message":" ok ","at":"2026-10-01T09:02:00Z"}]}`
	status, out := post(t, server, good)
	if status != http.StatusOK || out["created"].(float64) != 1 {
		t.Fatalf("legal batch after rejection must succeed, got %d %v", status, out)
	}
	status, rows := get(t, server, "")
	if status != http.StatusOK {
		t.Fatalf("get: %d", status)
	}
	var found map[string]any
	for _, row := range rows.([]any) {
		r := row.(map[string]any)
		if r["id"] == "later" {
			found = r
		}
	}
	if found == nil {
		t.Fatalf("later event must be visible after recovery: %v", rows)
	}
	if found["severity"] != "critical" || found["message"] != "ok" {
		t.Fatalf("normalization must still apply: %v", found)
	}
}
