package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

const goodPreviewBody = `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:06:00Z",` +
	`"window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`

func postPreview(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(server.URL+"/alerts/preview", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post preview: %v", err)
	}
	defer resp.Body.Close()
	return decode(t, resp)
}

func TestPreviewResponseShape(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a1","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:10Z"},
		{"id":"a2","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:20Z"},
		{"id":"a3","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:30Z"},
		{"id":"b1","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:01:10Z"},
		{"id":"b2","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:01:20Z"},
		{"id":"b3","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:01:30Z"},
		{"id":"c1","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:04:10Z"},
		{"id":"c2","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:04:20Z"},
		{"id":"c3","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:04:30Z"},
		{"id":"d1","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:05:10Z"},
		{"id":"d2","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:05:20Z"},
		{"id":"d3","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:05:30Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, out := postPreview(t, server, goodPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	groups := out["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("groups: %v", out)
	}
	group := groups[0].(map[string]any)
	if group["service"] != "svc" {
		t.Errorf("service: %v", group["service"])
	}
	// No group_by means the labels field is omitted.
	if _, ok := group["labels"]; ok {
		t.Errorf("labels should be omitted without group_by: %v", group)
	}
	windows := group["windows"].([]any)
	if len(windows) != 6 {
		t.Fatalf("windows: %v", windows)
	}
	wantCounts := []float64{3, 3, 0, 0, 3, 3}
	wantStates := []string{"normal", "alerting", "alerting", "normal", "normal", "alerting"}
	for i := range wantCounts {
		w := windows[i].(map[string]any)
		if w["count"].(float64) != wantCounts[i] {
			t.Errorf("window %d count: %v", i, w["count"])
		}
		if w["state"] != wantStates[i] {
			t.Errorf("window %d state: %v", i, w["state"])
		}
		wantSince := fmt.Sprintf("2026-10-01T10:%02d:00Z", i)
		wantUntil := fmt.Sprintf("2026-10-01T10:%02d:00Z", i+1)
		if w["since"] != wantSince || w["until"] != wantUntil {
			t.Errorf("window %d boundaries: got %v..%v, want %s..%s", i, w["since"], w["until"], wantSince, wantUntil)
		}
	}
	alerts := out["alerts"].([]any)
	if len(alerts) != 2 {
		t.Fatalf("alerts: %v", alerts)
	}
	first := alerts[0].(map[string]any)
	if first["triggered_at"] != "2026-10-01T10:02:00Z" || first["recovered_at"] != "2026-10-01T10:04:00Z" {
		t.Errorf("first alert: %v", first)
	}
	second := alerts[1].(map[string]any)
	if second["triggered_at"] != "2026-10-01T10:06:00Z" || second["recovered_at"] != nil {
		t.Errorf("second alert: %v", second)
	}
}

func TestPreviewGroupByResponse(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a1","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:01:10Z","labels":{"env":"prod","region":"us"}},
		{"id":"a2","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:01:20Z","labels":{"env":"prod","region":"us"}},
		{"id":"a3","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:01:30Z","labels":{"env":"prod","region":"us"}},
		{"id":"b1","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:02:10Z","labels":{"env":"staging"}},
		{"id":"b2","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:02:20Z","labels":{"env":"staging"}},
		{"id":"b3","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:02:30Z","labels":{"env":"staging"}}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}
	previewBody := `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:03:00Z",` +
		`"window_seconds":60,"threshold":3,"trigger_windows":1,"recover_windows":1,` +
		`"group_by":["env","region"]}`
	status, out := postPreview(t, server, previewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	groups := out["groups"].([]any)
	if len(groups) != 2 {
		t.Fatalf("groups: %v", out)
	}
	first := groups[0].(map[string]any)
	labels := first["labels"].(map[string]any)
	if labels["env"] != "prod" || labels["region"] != "us" {
		t.Errorf("first group labels: %v", labels)
	}
	second := groups[1].(map[string]any)
	labels2 := second["labels"].(map[string]any)
	if labels2["env"] != "staging" || labels2["region"] != nil {
		t.Errorf("second group labels (missing region must be null): %v", labels2)
	}
	alerts := out["alerts"].([]any)
	if len(alerts) != 2 {
		t.Fatalf("alerts: %v", alerts)
	}
	for _, raw := range alerts {
		alert := raw.(map[string]any)
		if _, ok := alert["service"]; !ok {
			t.Errorf("alert missing service: %v", alert)
		}
		if _, ok := alert["labels"]; !ok {
			t.Errorf("alert missing labels: %v", alert)
		}
	}
}

func TestPreviewEmptyResult(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, out := postPreview(t, server, goodPreviewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	groups := out["groups"].([]any)
	if len(groups) != 0 {
		t.Errorf("groups: %v", groups)
	}
	alerts := out["alerts"].([]any)
	if len(alerts) != 0 {
		t.Errorf("alerts: %v", alerts)
	}
}

func TestPreviewValidationErrors(t *testing.T) {
	server, _, _ := newTestServer(t)
	cases := []struct {
		name string
		body string
	}{
		{"missing since", `{"until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"missing until", `{"since":"2026-10-01T10:00:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"inverted range", `{"since":"2026-10-01T10:10:00Z","until":"2026-10-01T10:00:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"window zero", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":0,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"window too large", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":86401,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"window float", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60.5,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"window string", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":"60","threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"threshold zero", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":0,"trigger_windows":2,"recover_windows":2}`},
		{"threshold too large", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":1000001,"trigger_windows":2,"recover_windows":2}`},
		{"trigger zero", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":0,"recover_windows":2}`},
		{"trigger too large", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":101,"recover_windows":2}`},
		{"recover zero", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":0}`},
		{"recover too large", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":101}`},
		{"bad timestamp", `{"since":"not-a-time","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"unknown field", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"bogus":1}`},
		{"duplicate field", `{"since":"2026-10-01T10:00:00Z","since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"duplicate label field", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"labels":{"env":"a","env":"b"}}`},
		{"label non-string value", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"labels":{"env":1}}`},
		{"empty label name", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"labels":{"":"x"}}`},
		{"empty label value", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"labels":{"env":""}}`},
		{"too many group by", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"group_by":["a","b","c","d","e"]}`},
		{"group by dup after trim", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"group_by":[" env ","env"]}`},
		{"group by empty name", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"group_by":[""]}`},
		{"group by non-string", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2,"group_by":[1]}`},
		{"range too many windows", `{"since":"2026-10-01T00:00:00Z","until":"2026-10-08T00:00:01Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2}`},
		{"not an object", `[1,2,3]`},
		{"trailing data", `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:10:00Z","window_seconds":60,"threshold":3,"trigger_windows":2,"recover_windows":2} {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, out := postPreview(t, server, tc.body)
			if status != http.StatusBadRequest {
				t.Errorf("status: got %d, want 400 (body=%v)", status, out)
			}
			if msg, _ := out["error"].(string); msg == "" {
				t.Errorf("empty or missing error: %v", out)
			}
		})
	}
}

func TestPreviewMethodNotAllowed(t *testing.T) {
	server, _, _ := newTestServer(t)
	resp, err := http.Get(server.URL + "/alerts/preview")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET must be 405, got %d", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); allow != "POST" {
		t.Errorf("Allow header: %q", allow)
	}
}

func TestPreviewPoisonedStore(t *testing.T) {
	server, store, _ := newTestServer(t)
	store.failNext = true
	if status, _ := post(t, server, `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("setup: %d", status)
	}
	status, out := postPreview(t, server, goodPreviewBody)
	if status != http.StatusServiceUnavailable {
		t.Errorf("preview after poison must be 503, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Errorf("empty error: %v", out)
	}
}

func TestPreviewFilters(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"p1","service":"payments","severity":"critical","message":"m","at":"2026-10-01T10:00:10Z","labels":{"env":"prod"}},
		{"id":"p2","service":"payments","severity":"critical","message":"m","at":"2026-10-01T10:00:20Z","labels":{"env":"prod"}},
		{"id":"p3","service":"payments","severity":"critical","message":"m","at":"2026-10-01T10:00:30Z","labels":{"env":"prod"}},
		{"id":"s1","service":"payments","severity":"info","message":"m","at":"2026-10-01T10:00:10Z","labels":{"env":"staging"}},
		{"id":"o1","service":"orders","severity":"critical","message":"m","at":"2026-10-01T10:00:10Z","labels":{"env":"prod"}}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}
	previewBody := `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:01:00Z",` +
		`"window_seconds":60,"threshold":3,"trigger_windows":1,"recover_windows":1,` +
		`"service":"payments","severity":"CRITICAL","labels":{"env":"prod"}}`
	status, out := postPreview(t, server, previewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	groups := out["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("groups: %v", groups)
	}
	group := groups[0].(map[string]any)
	if group["service"] != "payments" {
		t.Errorf("service: %v", group["service"])
	}
	windows := group["windows"].([]any)
	if windows[0].(map[string]any)["count"].(float64) != 3 {
		t.Errorf("count: %v", windows)
	}
}

func TestPreviewTimezoneNormalization(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a1","service":"svc","severity":"critical","message":"m","at":"2026-10-01T18:00:10+08:00"},
		{"id":"a2","service":"svc","severity":"critical","message":"m","at":"2026-10-01T18:00:20+08:00"},
		{"id":"a3","service":"svc","severity":"critical","message":"m","at":"2026-10-01T18:00:30+08:00"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}
	// 18:00 +08:00 is 10:00 UTC; query with the offset representation.
	previewBody := `{"since":"2026-10-01T18:00:00+08:00","until":"2026-10-01T18:01:00+08:00",` +
		`"window_seconds":60,"threshold":3,"trigger_windows":1,"recover_windows":1}`
	status, out := postPreview(t, server, previewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	groups := out["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("groups: %v", groups)
	}
	windows := groups[0].(map[string]any)["windows"].([]any)
	first := windows[0].(map[string]any)
	if first["since"] != "2026-10-01T10:00:00Z" || first["until"] != "2026-10-01T10:01:00Z" {
		t.Errorf("boundaries must be UTC: %v", first)
	}
}

func TestPreviewPreservesNanoseconds(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"a1","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:00.123456789Z"},
		{"id":"a2","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:00.223456789Z"},
		{"id":"a3","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:00.323456789Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}
	previewBody := `{"since":"2026-10-01T10:00:00.000000000Z","until":"2026-10-01T10:00:01.000000000Z",` +
		`"window_seconds":1,"threshold":3,"trigger_windows":1,"recover_windows":1}`
	status, out := postPreview(t, server, previewBody)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	windows := out["groups"].([]any)[0].(map[string]any)["windows"].([]any)
	first := windows[0].(map[string]any)
	if first["since"] != "2026-10-01T10:00:00Z" || first["until"] != "2026-10-01T10:00:01Z" {
		t.Errorf("boundaries: %v", first)
	}
	alerts := out["alerts"].([]any)
	if len(alerts) != 1 {
		t.Fatalf("alerts: %v", alerts)
	}
	if alerts[0].(map[string]any)["triggered_at"] != "2026-10-01T10:00:01Z" {
		t.Errorf("trigger must be at window end with nanoseconds: %v", alerts[0])
	}
}

// TestPreviewConcurrentBatchAtomicity hammers ingestion and preview together.
// Every ingested batch holds exactly five events at one instant, so every
// preview window count must be a multiple of five: a torn batch would show a
// remainder.
func TestPreviewConcurrentBatchAtomicity(t *testing.T) {
	server, _, tl := newTestServer(t)
	const batchSize = 5
	const batches = 40

	var wg sync.WaitGroup
	for b := 0; b < batches; b++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			events := make([]string, batchSize)
			for i := range events {
				events[i] = fmt.Sprintf(`{"id":"b%d-%d","service":"svc","severity":"critical","message":"m","at":"2026-10-01T10:00:30Z"}`, b, i)
			}
			body := `{"events":[` + strings.Join(events, ",") + `]}`
			if status, _ := post(t, server, body); status != http.StatusOK {
				t.Errorf("batch %d: status %d", b, status)
			}
		}(b)
	}

	previewBody := `{"since":"2026-10-01T10:00:00Z","until":"2026-10-01T10:01:00Z",` +
		`"window_seconds":60,"threshold":1,"trigger_windows":1,"recover_windows":1}`
	for p := 0; p < 20; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, out := postPreview(t, server, previewBody)
			if status != http.StatusOK {
				t.Errorf("preview status %d: %v", status, out)
				return
			}
			groups := out["groups"].([]any)
			if len(groups) > 1 {
				t.Errorf("expected at most one group, got %d", len(groups))
				return
			}
			if len(groups) == 1 {
				windows := groups[0].(map[string]any)["windows"].([]any)
				count := int(windows[0].(map[string]any)["count"].(float64))
				if count%batchSize != 0 {
					t.Errorf("count %d is not a multiple of %d: torn batch", count, batchSize)
				}
			}
		}()
	}
	wg.Wait()

	// Every batch is fully present in the final timeline.
	preview, err := tl.PreviewAlerts(events.AlertPreviewQuery{
		Since:  time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC),
		Until:  time.Date(2026, 10, 1, 10, 1, 0, 0, time.UTC),
		Window: time.Minute, Threshold: 1, TriggerWindows: 1, RecoverWindows: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, group := range preview.Groups {
		for _, w := range group.Windows {
			total += w.Count
		}
	}
	if total != batches*batchSize {
		t.Errorf("final total: got %d, want %d", total, batches*batchSize)
	}
}

func TestPreviewContentType(t *testing.T) {
	server, _, _ := newTestServer(t)
	resp, err := http.Post(server.URL+"/alerts/preview", "application/json", strings.NewReader(goodPreviewBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: %q", ct)
	}
}
