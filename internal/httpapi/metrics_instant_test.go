package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// metricSingleSegment queries a window expected to match exactly one series
// with exactly one segment and returns that segment.
func metricSingleSegment(t *testing.T, server *httptest.Server, query string) map[string]any {
	t.Helper()
	status, out := aggregateGet(t, server, query)
	if status != http.StatusOK || len(out) != 1 {
		t.Fatalf("aggregate %s: %d %v", query, status, out)
	}
	segments, ok := out[0]["segments"].([]any)
	if !ok || len(segments) != 1 {
		t.Fatalf("aggregate %s: want one segment, got %v", query, out[0]["segments"])
	}
	return segments[0].(map[string]any)
}

// 1600-01-01 predates the UnixNano range (1678–2262) and 2184-07-20 postdates
// it, yet both are legal RFC3339Nano instants: samples at the two must be
// accepted as two distinct points of one series, in either submission order.
func TestPostMetricsExtremeDatesDistinctPoints(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"old","service":"gw","name":"requests","at":"1600-01-01T00:00:00Z","value":10},
		{"id":"far","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551616Z","value":25}
	]}`)
	if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
		t.Fatalf("extreme batch: %v", out)
	}

	oldWindow := "?name=requests&service=gw&since=1600-01-01T00:00:00Z&until=1600-01-01T00:01:00Z&step=60"
	farWindow := "?name=requests&service=gw&since=2184-07-20T23:34:00Z&until=2184-07-20T23:35:00Z&step=60"

	oldSeg := metricSingleSegment(t, server, oldWindow)
	if int(number(oldSeg["count"])) != 1 || oldSeg["delta"].(float64) != 0 {
		t.Fatalf("1600 window: %v", oldSeg)
	}
	if oldSeg["since"] != "1600-01-01T00:00:00Z" || oldSeg["until"] != "1600-01-01T00:01:00Z" {
		t.Fatalf("1600 window boundaries: %v", oldSeg)
	}

	// The 1600 sample is the 2184 sample's predecessor, so the far window
	// carries the +15 delta: the two instants neither alias nor swap order.
	farSeg := metricSingleSegment(t, server, farWindow)
	if int(number(farSeg["count"])) != 1 || farSeg["delta"].(float64) != 15 {
		t.Fatalf("2184 window: %v", farSeg)
	}
	if farSeg["since"] != "2184-07-20T23:34:00Z" || farSeg["until"] != "2184-07-20T23:35:00Z" {
		t.Fatalf("2184 window boundaries: %v", farSeg)
	}

	// Reversed submission order ingests and aggregates identically.
	reversed := newMetricsServer(t, nil, nil)
	out = postSamples(t, reversed, `{"samples":[
		{"id":"far","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551616Z","value":25},
		{"id":"old","service":"gw","name":"requests","at":"1600-01-01T00:00:00Z","value":10}
	]}`)
	if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
		t.Fatalf("reversed extreme batch: %v", out)
	}
	revOld := metricSingleSegment(t, reversed, oldWindow)
	revFar := metricSingleSegment(t, reversed, farWindow)
	if int(number(revOld["count"])) != 1 || revOld["delta"].(float64) != 0 {
		t.Fatalf("reversed 1600 window: %v", revOld)
	}
	if int(number(revFar["count"])) != 1 || revFar["delta"].(float64) != 15 {
		t.Fatalf("reversed 2184 window: %v", revFar)
	}
}

// Two samples one nanosecond apart inside the same second are distinct
// points and coexist, also beyond the UnixNano range.
func TestPostMetricsNanosecondApartSamplesCoexist(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"n1","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551616Z","value":100},
		{"id":"n2","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551617Z","value":150}
	]}`)
	if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
		t.Fatalf("nanosecond batch: %v", out)
	}

	seg := metricSingleSegment(t, server,
		"?name=requests&service=gw&since=2184-07-20T23:34:33Z&until=2184-07-20T23:34:34Z&step=1")
	if int(number(seg["count"])) != 2 || seg["delta"].(float64) != 50 {
		t.Fatalf("nanosecond window: %v", seg)
	}
}

// The same absolute instant written with another timezone offset is the same
// sampling point: a different id may not claim it, whether the alias arrives
// against already-stored data or inside a single batch.
func TestPostMetricsExtremeInstantTimezoneConflict(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"1600-01-01T00:00:00Z","value":10}
	]}`)

	// 1599-12-31T19:00:00-05:00 is the stored 1600-01-01T00:00:00Z.
	status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s2","service":"gw","name":"requests","at":"1599-12-31T19:00:00-05:00","value":11}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("stored-instant timezone alias must be 409, got %d %v", status, out)
	}

	// 2184-07-21T07:34:33.709551616+08:00 is 2184-07-20T23:34:33.709551616Z.
	status, out = metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s3","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551616Z","value":1},
		{"id":"s4","service":"gw","name":"requests","at":"2184-07-21T07:34:33.709551616+08:00","value":2}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("in-batch timezone alias must be 409, got %d %v", status, out)
	}
}

// A batch mixing a timezone-aliased conflict with a legal new sample is
// rejected as a whole: the stored data is untouched and the legal sample
// still ingests as new when submitted on its own.
func TestPostMetricsExtremeConflictRejectsWholeBatch(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"1600-01-01T00:00:00Z","value":10}
	]}`)
	window := "?name=requests&service=gw&since=1600-01-01T00:00:00Z&until=1600-01-01T00:01:00Z&step=60"

	before := metricSingleSegment(t, server, window)
	if int(number(before["count"])) != 1 || before["delta"].(float64) != 0 {
		t.Fatalf("baseline: %v", before)
	}

	status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s2","service":"gw","name":"requests","at":"1599-12-31T19:00:00-05:00","value":99},
		{"id":"s3","service":"gw","name":"requests","at":"1600-01-01T00:00:30Z","value":12}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("mixed batch must be 409, got %d %v", status, out)
	}

	after := metricSingleSegment(t, server, window)
	if after["count"] != before["count"] || after["delta"] != before["delta"] {
		t.Fatalf("stored series changed after rejection: before %v, after %v", before, after)
	}

	// No partial acceptance: the legal sample from the rejected batch is new.
	out = postSamples(t, server, `{"samples":[
		{"id":"s3","service":"gw","name":"requests","at":"1600-01-01T00:00:30Z","value":12}
	]}`)
	if int(number(out["created"])) != 1 || int(number(out["replayed"])) != 0 {
		t.Fatalf("legal sample must ingest as new: %v", out)
	}
	seg := metricSingleSegment(t, server, window)
	if int(number(seg["count"])) != 2 || seg["delta"].(float64) != 2 {
		t.Fatalf("window after recovery: %v", seg)
	}
}

// Re-sending one id with only the timezone representation changed replays;
// the same id at an instant truly moved by one nanosecond conflicts.
func TestPostMetricsExtremeReplayAndNanosecondChange(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551616Z","value":42}
	]}`)

	out := postSamples(t, server, `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2184-07-21T07:34:33.709551616+08:00","value":42}
	]}`)
	if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 1 {
		t.Fatalf("timezone-only retry must replay: %v", out)
	}

	window := "?name=requests&service=gw&since=2184-07-20T23:34:00Z&until=2184-07-20T23:35:00Z&step=60"
	if seg := metricSingleSegment(t, server, window); int(number(seg["count"])) != 1 {
		t.Fatalf("replay must not add a sample: %v", seg)
	}

	status, out := metricsDo(t, server, http.MethodPost, "/metrics", `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2184-07-20T23:34:33.709551617Z","value":42}
	]}`)
	if status != http.StatusConflict || out["error"] == "" {
		t.Fatalf("one-nanosecond move must be 409, got %d %v", status, out)
	}
}

// The instant rule applies within one series only: two series with different
// label sets may each hold a sample at the same extreme absolute instant.
func TestPostMetricsExtremeSameInstantDifferentSeries(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"p1","service":"gw","name":"requests","at":"1600-01-01T00:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"p2","service":"gw","name":"requests","at":"1599-12-31T19:00:00-05:00","value":2,"labels":{"env":"staging"}}
	]}`)
	if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
		t.Fatalf("distinct series at one instant must ingest: %v", out)
	}

	status, agg := aggregateGet(t, server,
		"?name=requests&service=gw&since=1600-01-01T00:00:00Z&until=1600-01-01T00:01:00Z&step=60")
	if status != http.StatusOK || len(agg) != 2 {
		t.Fatalf("want two series, got %d %v", status, agg)
	}
	for _, series := range agg {
		segments := series["segments"].([]any)
		if len(segments) != 1 || int(number(segments[0].(map[string]any)["count"])) != 1 {
			t.Fatalf("each series holds its own sample: %v", series)
		}
	}
}
