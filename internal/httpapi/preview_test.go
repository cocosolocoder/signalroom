package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func postPreview(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/alerts/preview", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post preview: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, out
}

const goodPreviewBody = `{
	"since":"2026-10-01T10:00:00Z",
	"until":"2026-10-01T10:05:00Z",
	"window_seconds":60,
	"threshold":1,
	"trigger_windows":1,
	"recover_windows":1
}`

func TestPreviewShapeTriggerRecoverUnresolved(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"w0","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:10Z"},
		{"id":"w1a","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:10Z"},
		{"id":"w1b","service":"gateway","severity":"info","message":"m","at":"2026-10-01T10:01:50Z"},
		{"id":"w3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:03:10Z"},
		{"id":"w4a","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:04:10Z"},
		{"id":"w4b","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:04:59.999999999Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, out := postPreview(t, server, goodPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	gl, ok := out["group_labels"].([]any)
	if !ok || len(gl) != 0 {
		t.Fatalf("group_labels must be an empty array, got %v", out["group_labels"])
	}
	groups := out["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("one service -> one group, got %d", len(groups))
	}
	group := groups[0].(map[string]any)
	if group["service"] != "gateway" {
		t.Fatalf("service: %v", group["service"])
	}
	if lvs := group["label_values"].([]any); len(lvs) != 0 {
		t.Fatalf("ungrouped label_values must be [], got %v", lvs)
	}

	windows := group["windows"].([]any)
	if len(windows) != 5 {
		t.Fatalf("five windows, got %d", len(windows))
	}
	first := windows[0].(map[string]any)
	if first["since"] != "2026-10-01T10:00:00Z" || first["until"] != "2026-10-01T10:01:00Z" {
		t.Fatalf("first boundaries: %v", first)
	}
	if first["count"].(float64) != 1 || first["status"] != "alerting" {
		t.Fatalf("threshold 1/trigger 1 opens at end of w0: %v", first)
	}
	quiet := windows[2].(map[string]any)
	if quiet["count"].(float64) != 0 || quiet["status"] != "normal" {
		t.Fatalf("w2 quiet recovers: %v", quiet)
	}
	// w1 carries both severities (the severity filter is absent).
	if windows[1].(map[string]any)["count"].(float64) != 2 {
		t.Fatalf("w1 count = %v", windows[1])
	}
	// The nanosecond-before-end event stays inside the final window.
	if windows[4].(map[string]any)["count"].(float64) != 2 {
		t.Fatalf("w4 count = %v", windows[4])
	}

	alerts := group["alerts"].([]any)
	if len(alerts) != 2 {
		t.Fatalf("open w0, recover w2, reopen w3 unresolved: got %v", alerts)
	}
	a0 := alerts[0].(map[string]any)
	if a0["triggered_at"] != "2026-10-01T10:01:00Z" || a0["recovered_at"] != "2026-10-01T10:03:00Z" {
		t.Fatalf("first cycle boundaries: %v", a0)
	}
	a1 := alerts[1].(map[string]any)
	if a1["triggered_at"] != "2026-10-01T10:04:00Z" || a1["recovered_at"] != nil {
		t.Fatalf("second cycle must be unresolved at range end: %v", a1)
	}
}

func TestPreviewNanosecondsAndUTC(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, `{"events":[
		{"id":"x","service":"s","severity":"info","message":"m","at":"2026-10-01T18:00:00.123456789+08:00"}
	]}`); status != http.StatusOK {
		t.Fatal("seed")
	}
	body := `{
		"since":"2026-10-01T18:00:00.123456789+08:00",
		"until":"2026-10-01T18:00:02.123456789+08:00",
		"window_seconds":1,"threshold":1,"trigger_windows":1,"recover_windows":1
	}`
	status, out := postPreview(t, server, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d %v", status, out)
	}
	window := out["groups"].([]any)[0].(map[string]any)["windows"].([]any)[0].(map[string]any)
	if window["since"] != "2026-10-01T10:00:00.123456789Z" {
		t.Fatalf("UTC nanosecond boundary: %v", window["since"])
	}
	if window["count"].(float64) != 1 {
		t.Fatalf("the +08:00 instant is 10:00 UTC and must count: %v", window)
	}
}

func TestPreviewGroupsByLabelsWithNullAndOrdering(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, `{"events":[
		{"id":"1","service":"zeta","severity":"info","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"prod"}},
		{"id":"2","service":"zeta","severity":"info","message":"m","at":"2026-10-01T10:00:05Z"},
		{"id":"3","service":"alpha","severity":"info","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"zzz"}},
		{"id":"4","service":"alpha","severity":"info","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"aaa"}},
		{"id":"5","service":"alpha","severity":"info","message":"m","at":"2026-10-01T10:00:05Z"}
	]}`); status != http.StatusOK {
		t.Fatal("seed")
	}
	body := `{
		"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:01:00Z",
		"window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,
		"group_labels":[" env "]
	}`
	status, out := postPreview(t, server, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d %v", status, out)
	}
	if names := out["group_labels"].([]any); len(names) != 1 || names[0] != "env" {
		t.Fatalf("trimmed, sorted names: %v", names)
	}
	type row struct {
		service string
		value   any
	}
	groups := out["groups"].([]any)
	got := make([]row, 0, len(groups))
	for _, g := range groups {
		gm := g.(map[string]any)
		lvs := gm["label_values"].([]any)
		got = append(got, row{gm["service"].(string), lvs[0]})
	}
	want := []row{
		{"alpha", nil},
		{"alpha", "aaa"},
		{"alpha", "zzz"},
		{"zeta", nil},
		{"zeta", "prod"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("group %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestPreviewEmptyData(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, out := postPreview(t, server, goodPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	gs := out["groups"].([]any)
	if len(gs) != 0 {
		t.Fatalf("no events -> empty groups, got %v", gs)
	}
}

func TestPreviewReplayDoesNotChangeResult(t *testing.T) {
	server, _, _ := newTestServer(t)
	event := `{"events":[{"id":"1","service":"s","severity":"info","message":"m","at":"2026-10-01T10:00:05Z"}]}`
	if status, _ := post(t, server, event); status != http.StatusOK {
		t.Fatal("seed")
	}
	_, first := postPreview(t, server, goodPreviewBody)
	if status, _ := post(t, server, event); status != http.StatusOK {
		t.Fatal("replay")
	}
	_, second := postPreview(t, server, goodPreviewBody)
	if !bytes.Equal(mustJSON(first), mustJSON(second)) {
		t.Fatalf("replaying the same event changed the preview:\n%v\n%v", first, second)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestPreviewFiltersMatchExistingRules(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, `{"events":[
		{"id":"1","service":"gateway","severity":"CRITICAL","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"prod"}},
		{"id":"2","service":"api","severity":"info","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"prod"}},
		{"id":"3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:05Z","labels":{"env":"dev"}}
	]}`); status != http.StatusOK {
		t.Fatal("seed")
	}
	body := `{
		"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:01:00Z",
		"window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,
		"service":" gateway ","severity":" critical ","labels":{" env ":" prod "}
	}`
	status, out := postPreview(t, server, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d %v", status, out)
	}
	groups := out["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("only trimmed gateway + case-insensitive critical + label match must remain: %v", groups)
	}
	if groups[0].(map[string]any)["windows"].([]any)[0].(map[string]any)["count"].(float64) != 1 {
		t.Fatalf("count: %v", groups[0])
	}
}

func TestPreviewBadRequests(t *testing.T) {
	server, _, _ := newTestServer(t)
	cases := map[string]string{
		"not json":                  `{nope`,
		"not object":                `[1,2,3]`,
		"null body":                 `null`,
		"empty object":              `{}`,
		"unknown field":             `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"extra":1}`,
		"duplicate field":           `{"since":"2026-10-01T10:00:00Z","since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"trailing tokens":           goodPreviewBody + ` junk`,
		"missing since":             `{"until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"null since":                `{"since":null,"until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"bad since":                 `{"since":"soon","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"numeric since":             `{"since":1,"until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"inverted range":            `{"since":"2026-10-01T10:05:00Z","until":"2026-10-01T10:00:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"equal range":               `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:00:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window zero":               `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":0,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window fractional":         `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":2.5,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window float-looking":      `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60.0,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window exponent":           `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":6e1,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window string":             `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":"60","threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window negative":           `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":-1,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"window too big":            `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":86401,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
		"threshold zero":            `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":0,"trigger_windows":1,"recover_windows":1}`,
		"threshold too big":         `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1000001,"trigger_windows":1,"recover_windows":1}`,
		"trigger zero":              `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":0,"recover_windows":1}`,
		"recover too big":           `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":101}`,
		"service wrong type":        `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"service":5}`,
		"severity wrong type":       `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"severity":true}`,
		"labels not object":         `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"labels":[1]}`,
		"labels non-string value":   `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"labels":{"env":5}}`,
		"labels duplicate keys":     `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"labels":{"env":"a","env":"b"}}`,
		"labels collide after trim": `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"labels":{"env":"a"," env ":"b"}}`,
		"labels empty value":        `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"labels":{"env":" "}}`,
		"group labels wrong type":   `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"group_labels":"env"}`,
		"group labels non-string":   `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"group_labels":[5]}`,
		"too many group labels":     `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"group_labels":["a","b","c","d","e"]}`,
		"group label duplicates":    `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"group_labels":["env"," env "]}`,
		"group label empty":         `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:05:00Z","window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1,"group_labels":["  "]}`,
		"over 10000 windows":        `{"since":"2026-10-01T00:00:00Z","until":"2026-10-01T02:46:41Z","window_seconds":1,"threshold":1,"trigger_windows":1,"recover_windows":1}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			status, out := postPreview(t, server, body)
			if status != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %v", status, out)
			}
			if msg, _ := out["error"].(string); msg == "" {
				t.Fatal("400 needs a non-empty error")
			}
		})
	}
}

func TestPreviewOverGroupWindowCap(t *testing.T) {
	server, _, _ := newTestServer(t)
	var sb strings.Builder
	sb.WriteString(`{"events":[`)
	for i := 0; i < 11; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"id":"e` + string(rune('a'+i)) + `","service":"s` + string(rune('a'+i)) +
			`","severity":"info","message":"m","at":"2026-10-01T00:00:05Z"}`)
	}
	sb.WriteString(`]}`)
	if status, _ := post(t, server, sb.String()); status != http.StatusOK {
		t.Fatal("seed 11 services")
	}
	body := `{"since":"2026-10-01T00:00:00Z","until":"2026-10-01T02:46:40Z","window_seconds":1,"threshold":1,"trigger_windows":1,"recover_windows":1}`
	status, out := postPreview(t, server, body)
	if status != http.StatusBadRequest {
		t.Fatalf("11 groups x 10000 windows = 110000 must be 400, got %d %v", status, out)
	}
}

func TestPreviewMethodNotAllowed(t *testing.T) {
	server, _, _ := newTestServer(t)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req, _ := http.NewRequest(method, server.URL+"/alerts/preview", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s: want 405, got %d %s", method, resp.StatusCode, body)
		}
		if resp.Header.Get("Allow") != "POST" {
			t.Fatalf("%s Allow = %q", method, resp.Header.Get("Allow"))
		}
	}
}

func TestPreview503AfterStorageFailure(t *testing.T) {
	server, store, _ := newTestServer(t)
	store.failNext = true
	if status, _ := post(t, server, `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`); status != http.StatusServiceUnavailable {
		t.Fatal("seed the poisoned state")
	}
	status, out := postPreview(t, server, goodPreviewBody)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("preview must be 503 once storage is poisoned, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatal("503 needs a non-empty error")
	}
}

func TestPreviewDoesNotExistForAlertsRoot(t *testing.T) {
	server, _, _ := newTestServer(t)
	resp, err := http.Post(server.URL+"/alerts", "application/json", strings.NewReader(goodPreviewBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/alerts must not be a route, got %d", resp.StatusCode)
	}
}
