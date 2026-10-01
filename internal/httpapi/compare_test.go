package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const goodCompareQuery = "?baseline_since=2026-10-01T09:00:00Z&baseline_until=2026-10-01T09:10:00Z" +
	"&since=2026-10-01T10:00:00Z&until=2026-10-01T10:10:00Z&step=240"

func getCompare(t *testing.T, server *httptest.Server, rawQuery string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(server.URL + "/events/compare" + rawQuery)
	if err != nil {
		t.Fatalf("get compare: %v", err)
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

// withParam replaces the first occurrence of key=oldValue in the query.
func withParam(query, key, oldValue, newValue string) string {
	return strings.Replace(query, key+"="+oldValue, key+"="+newValue, 1)
}

func TestCompareResponseShapeAndCounts(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"b1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:00:00.5Z"},
		{"id":"b2","service":"gateway","severity":"info","message":"m","at":"2026-10-01T09:05:00Z"},
		{"id":"o1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:00.25Z"},
		{"id":"o2","service":"api","severity":"info","message":"m","at":"2026-10-01T10:02:00Z"},
		{"id":"o3","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:09:59.999999999Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, out := getCompare(t, server, goodCompareQuery)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	if out["baseline_total"].(float64) != 2 || out["observation_total"].(float64) != 3 {
		t.Fatalf("totals wrong: %v", out)
	}
	if out["difference"].(float64) != 1 {
		t.Fatalf("difference: %v", out["difference"])
	}
	ratio, ok := out["ratio"].(float64)
	if !ok || ratio != 0.5 {
		t.Fatalf("ratio must be numeric 0.5, got %v", out["ratio"])
	}

	segments := out["segments"].([]any)
	if len(segments) != 3 {
		t.Fatalf("ten minutes / 240s must give three segments, got %d", len(segments))
	}
	first := segments[0].(map[string]any)
	if first["baseline_since"] != "2026-10-01T09:00:00Z" ||
		first["baseline_until"] != "2026-10-01T09:04:00Z" ||
		first["since"] != "2026-10-01T10:00:00Z" ||
		first["until"] != "2026-10-01T10:04:00Z" {
		t.Fatalf("first segment boundaries wrong: %v", first)
	}
	if first["baseline_count"].(float64) != 1 || first["observation_count"].(float64) != 2 ||
		first["difference"].(float64) != 1 {
		t.Fatalf("first segment counts wrong: %v", first)
	}
	middle := segments[1].(map[string]any)
	if middle["baseline_since"] != "2026-10-01T09:04:00Z" ||
		middle["baseline_until"] != "2026-10-01T09:08:00Z" {
		t.Fatalf("middle segment boundaries wrong: %v", middle)
	}
	if middle["baseline_count"].(float64) != 1 || middle["observation_count"].(float64) != 0 {
		t.Fatalf("middle segment counts wrong (b2 at 09:05): %v", middle)
	}
	last := segments[2].(map[string]any)
	if last["baseline_since"] != "2026-10-01T09:08:00Z" ||
		last["baseline_until"] != "2026-10-01T09:10:00Z" ||
		last["since"] != "2026-10-01T10:08:00Z" ||
		last["until"] != "2026-10-01T10:10:00Z" {
		t.Fatalf("last (short) segment boundaries wrong: %v", last)
	}
	// The 09:59:59.999999999 nanoseconds-before-end event stays inside; the
	// baseline side of the final short segment has no events.
	if last["baseline_count"].(float64) != 0 || last["observation_count"].(float64) != 1 {
		t.Fatalf("last segment counts wrong (o3 with nanos): %v", last)
	}
}

func TestCompareFilteringUsesSameRulesAsEventsQuery(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"b1","service":"gateway","severity":"CRITICAL","message":"m","at":"2026-10-01T09:01:00Z"},
		{"id":"b2","service":"api","severity":"info","message":"m","at":"2026-10-01T09:02:00Z"},
		{"id":"o1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:00Z"},
		{"id":"o2","service":"api","severity":"critical","message":"m","at":"2026-10-01T10:02:00Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}
	q := "?baseline_since=2026-10-01T09:00:00Z&baseline_until=2026-10-01T09:10:00Z" +
		"&since=2026-10-01T10:00:00Z&until=2026-10-01T10:10:00Z&step=600" +
		"&service=%20gateway%20&severity=critical"
	status, out := getCompare(t, server, q)
	if status != http.StatusOK {
		t.Fatalf("status=%d %v", status, out)
	}
	if out["baseline_total"].(float64) != 1 || out["observation_total"].(float64) != 1 {
		t.Fatalf("filters must leave one event per window: %v", out)
	}
}

func TestCompareNullRatioWhenBaselineZero(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, `{"events":[
		{"id":"o1","service":"s","severity":"info","message":"m","at":"2026-10-01T10:01:00Z"}
	]}`); status != http.StatusOK {
		t.Fatal("seed")
	}
	status, out := getCompare(t, server, withParam(goodCompareQuery, "step", "240", "300"))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if out["ratio"] != nil {
		t.Fatalf("ratio must be null when baseline is zero, got %v", out["ratio"])
	}
	if out["difference"].(float64) != 1 {
		t.Fatalf("difference: %v", out["difference"])
	}
	if segments := out["segments"].([]any); len(segments) != 2 {
		t.Fatalf("two segments, got %v", segments)
	}
}

func TestCompareEmptyDataReturnsFullSegments(t *testing.T) {
	server, _, _ := newTestServer(t)
	status, out := getCompare(t, server, goodCompareQuery)
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	segments := out["segments"].([]any)
	if len(segments) != 3 {
		t.Fatalf("empty data still needs the complete segment set, got %d", len(segments))
	}
	for i, raw := range segments {
		segment := raw.(map[string]any)
		if segment["baseline_count"].(float64) != 0 || segment["observation_count"].(float64) != 0 {
			t.Fatalf("segment %d not zeroed: %v", i, segment)
		}
	}
}

func TestCompareOutputIsUTCAndTimezoneInvariant(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, `{"events":[
		{"id":"b1","service":"s","severity":"info","message":"m","at":"2026-10-01T17:00:00.123456789+08:00"}
	]}`); status != http.StatusOK {
		t.Fatal("seed")
	}
	// Same instants as goodCompareQuery, expressed with a +08:00 offset.
	q := "?baseline_since=2026-10-01T17:00:00%2B08:00&baseline_until=2026-10-01T17:10:00%2B08:00" +
		"&since=2026-10-01T18:00:00%2B08:00&until=2026-10-01T18:10:00%2B08:00&step=240"
	status, out := getCompare(t, server, q)
	if status != http.StatusOK {
		t.Fatalf("status=%d %v", status, out)
	}
	segments := out["segments"].([]any)
	first := segments[0].(map[string]any)
	if first["baseline_since"] != "2026-10-01T09:00:00Z" {
		t.Fatalf("output times must be UTC RFC3339Nano, got %v", first["baseline_since"])
	}
	if first["baseline_count"].(float64) != 1 {
		t.Fatalf("the +08:00 instant is 09:00 UTC and must be counted: %v", first)
	}

	// Counts must match the all-UTC request exactly.
	_, utcOut := getCompare(t, server, goodCompareQuery)
	utcSegments := utcOut["segments"].([]any)
	for i := range utcSegments {
		a := segments[i].(map[string]any)
		b := utcSegments[i].(map[string]any)
		if a["baseline_count"] != b["baseline_count"] || a["observation_count"] != b["observation_count"] {
			t.Fatalf("segment %d counts differ by timezone: %v vs %v", i, a, b)
		}
	}
}

func TestCompareValidationFailures(t *testing.T) {
	server, _, _ := newTestServer(t)
	cases := map[string]struct {
		query string
		want  string
	}{
		"missing baseline_since": {
			"?baseline_until=2026-10-01T09:10:00Z&since=2026-10-01T10:00:00Z&until=2026-10-01T10:10:00Z&step=240",
			"baseline_since is required and must be a single RFC3339Nano timestamp",
		},
		"missing step": {
			"?baseline_since=2026-10-01T09:00:00Z&baseline_until=2026-10-01T09:10:00Z&since=2026-10-01T10:00:00Z&until=2026-10-01T10:10:00Z",
			"step is required and must be a single integer number of seconds",
		},
		"unparsable timestamp":    {withParam(goodCompareQuery, "baseline_since", "2026-10-01T09:00:00Z", "banana"), "baseline_since must be an RFC3339Nano timestamp"},
		"step zero":               {withParam(goodCompareQuery, "step", "240", "0"), "step must be an integer from 1 to 86400 seconds"},
		"step negative":           {withParam(goodCompareQuery, "step", "240", "-5"), "step must be an integer from 1 to 86400 seconds"},
		"step fractional":         {withParam(goodCompareQuery, "step", "240", "2.5"), "step must be an integer from 1 to 86400 seconds"},
		"step non-numeric":        {withParam(goodCompareQuery, "step", "240", "soon"), "step must be an integer from 1 to 86400 seconds"},
		"step with sign":          {withParam(goodCompareQuery, "step", "240", "+240"), "step must be an integer from 1 to 86400 seconds"},
		"step above max":          {withParam(goodCompareQuery, "step", "240", "86401"), "step must be an integer from 1 to 86400 seconds"},
		"step way too large":      {withParam(goodCompareQuery, "step", "240", "99999999999999999999"), "step must be an integer from 1 to 86400 seconds"},
		"window inverted":         {withParam(goodCompareQuery, "baseline_until", "2026-10-01T09:10:00Z", "2026-10-01T08:50:00Z"), "baseline_since must be before baseline_until"},
		"zero length window":      {withParam(goodCompareQuery, "baseline_until", "2026-10-01T09:10:00Z", "2026-10-01T09:00:00Z"), "baseline_since must be before baseline_until"},
		"observation inverted":    {withParam(goodCompareQuery, "until", "2026-10-01T10:10:00Z", "2026-10-01T09:50:00Z"), "since must be before until"},
		"unequal lengths":         {withParam(goodCompareQuery, "until", "2026-10-01T10:10:00Z", "2026-10-01T10:11:00Z"), "baseline and observation windows must have equal length"},
		"unequal by nanosecond":   {withParam(goodCompareQuery, "until", "2026-10-01T10:10:00Z", "2026-10-01T10:10:00.000000001Z"), "baseline and observation windows must have equal length"},
		"repeated step":           {goodCompareQuery + "&step=60", "step is required and must be a single integer number of seconds"},
		"repeated baseline_since": {goodCompareQuery + "&baseline_since=2026-10-01T08:00:00Z", "baseline_since is required and must be a single RFC3339Nano timestamp"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			status, out := getCompare(t, server, tc.query)
			if status != http.StatusBadRequest {
				t.Fatalf("%s: want 400, got %d %v", name, status, out)
			}
			if got := out["error"]; got != tc.want {
				t.Fatalf("%s: error = %q, want %q", name, got, tc.want)
			}
		})
	}

	// Precedence checks matching the documented evaluation order.
	precedence := map[string]string{
		// Timestamps (presence then parse) beat step problems.
		"timestamps before step": withParam(withParam(goodCompareQuery, "baseline_since", "2026-10-01T09:00:00Z", "banana"), "step", "240", "abc"),
		// Baseline order beats observation order and equal-length.
		"baseline before observation": "?baseline_since=2026-10-01T09:10:00Z&baseline_until=2026-10-01T09:00:00Z&since=2026-10-01T10:11:00Z&until=2026-10-01T10:10:00Z&step=240",
		// Observation order beats equal-length.
		"observation before length": "?baseline_since=2026-10-01T09:00:00Z&baseline_until=2026-10-01T09:10:00Z&since=2026-10-01T10:11:00Z&until=2026-10-01T10:10:00Z&step=240",
	}
	wantByCase := map[string]string{
		"timestamps before step":      "baseline_since must be an RFC3339Nano timestamp",
		"baseline before observation": "baseline_since must be before baseline_until",
		"observation before length":   "since must be before until",
	}
	for name, query := range precedence {
		status, out := getCompare(t, server, query)
		if status != http.StatusBadRequest || out["error"] != wantByCase[name] {
			t.Fatalf("%s: got %d %v, want 400 %q", name, status, out["error"], wantByCase[name])
		}
	}

	// Two equal-length 10001-second windows with a one-second step exceed the
	// 10000-segment cap and must be rejected rather than truncated.
	tooMany := "?baseline_since=2026-10-01T09:00:00Z&baseline_until=2026-10-01T11:46:41Z" +
		"&since=2026-10-01T12:00:00Z&until=2026-10-01T14:46:41Z&step=1"
	status, out := getCompare(t, server, tooMany)
	if status != http.StatusBadRequest || out["error"] != "windows must not require more than 10000 segments" {
		t.Fatalf("over 10000 segments: got %d %v", status, out)
	}

	// Boundaries of the accepted step range succeed; a leading zero is fine.
	for _, step := range []string{"1", "01", "86400"} {
		if status, out := getCompare(t, server, withParam(goodCompareQuery, "step", "240", step)); status != http.StatusOK {
			t.Fatalf("step %s must be accepted: %d %v", step, status, out)
		}
	}
}

func TestCompareMethodNotAllowed(t *testing.T) {
	server, _, _ := newTestServer(t)
	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		req, _ := http.NewRequest(method, server.URL+"/events/compare"+goodCompareQuery, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s: want 405, got %d %s", method, resp.StatusCode, body)
		}
		if allow := resp.Header.Get("Allow"); allow != "GET" {
			t.Fatalf("%s: Allow header = %q, want GET", method, allow)
		}
	}
}

func TestCompare503AfterStorageFailure(t *testing.T) {
	server, store, _ := newTestServer(t)
	store.failNext = true
	if status, _ := post(t, server, `{"events":[{"id":"a","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`); status != http.StatusServiceUnavailable {
		t.Fatal("seed the poisoned state")
	}
	status, out := getCompare(t, server, goodCompareQuery)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("compare must be 503 once storage is poisoned, got %d %v", status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatal("503 needs a non-empty error")
	}
}

func TestCompareRouteDoesNotChangeEventsCollection(t *testing.T) {
	server, _, _ := newTestServer(t)
	// A bare GET /events still lists events and does not collide with the
	// more specific /events/compare pattern.
	if status, rows := get(t, server, ""); status != http.StatusOK || len(rows.([]any)) != 0 {
		t.Fatalf("GET /events changed: %d %v", status, rows)
	}
}
