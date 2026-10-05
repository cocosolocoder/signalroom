package httpapi

import (
	"net/http"
	"reflect"
	"testing"
)

// Regression coverage for rejecting duplicate JSON member names on
// POST /metrics. A conventional decode into a struct or map keeps the last
// value and hides the ambiguity; the strict decoders must instead reject the
// whole request with 400. These tests pin that behavior at every object
// level (outer body, each sample, each labels object), the decoded-text key
// comparison (\uXXXX escapes), the whole-batch rejection semantics, and the
// distinction from a repeated sample id across distinct samples, which stays
// a 409 content conflict.

const dupWindowQuery = "?name=requests&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60"

// assertBadRequest pins the error contract: status 400, a non-empty error
// string, and no successful-ingestion counters leaking into the response.
func assertBadRequest(t *testing.T, name string, status int, out map[string]any) {
	t.Helper()
	if status != http.StatusBadRequest {
		t.Fatalf("%s: want 400, got %d (%v)", name, status, out)
	}
	if msg, _ := out["error"].(string); msg == "" {
		t.Fatalf("%s: response needs a non-empty error, got %v", name, out)
	}
	if _, present := out["created"]; present {
		t.Fatalf("%s: rejected batch must not report created: %v", name, out)
	}
	if _, present := out["replayed"]; present {
		t.Fatalf("%s: rejected batch must not report replayed: %v", name, out)
	}
}

func TestPostMetricsRejectsDuplicateJSONFields(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, nil, mstore)
	const one = `{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}`

	cases := map[string]string{
		// Outer request object: a member name may appear once even when the
		// two values agree or the first one is null.
		"duplicate outer field":        `{"samples":[` + one + `],"samples":[]}`,
		"null then valid outer field":  `{"samples":null,"samples":[` + one + `]}`,
		"unicode outer key duplicates": `{"samples":[],"\u0073amples":[` + one + `]}`,
		// Sample object: identical values still make the input ambiguous.
		"duplicate sample id same value":  `{"samples":[{"id":"a","id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`,
		"duplicate sample value":          `{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"value":1}]}`,
		"duplicate sample service":        `{"samples":[{"id":"a","service":"gw","service":"api","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`,
		"null then valid sample id":       `{"samples":[{"id":null,"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`,
		"null then valid sample value":    `{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":null,"value":1}]}`,
		"unicode sample field duplicates": `{"samples":[{"id":"a","\u0069d":"b","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1}]}`,
		"dup field appears in later sample": `{"samples":[
			{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1},
			{"id":"b","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":2,"value":2}
		]}`,
		// The labels member itself repeated on one sample.
		"sample labels member repeats": `{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod"},"labels":{"env":"prod"}}]}`,
		// Duplicate names inside one labels object.
		"duplicate label name same value": `{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod","env":"prod"}}]}`,
		"valid then null label value":     `{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod","env":null}}]}`,
		"unicode label name duplicates":   `{"samples":[{"id":"a","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"\u0065nv":"prod","env":"dev"}}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			status, out := postSamplesRaw(t, server, body)
			assertBadRequest(t, name, status, out)
		})
	}

	// Syntax rejection happens before anything can be persisted, and it is
	// not a storage failure: later valid batches still ingest.
	if len(mstore.batches) != 0 {
		t.Fatalf("malformed batches must reach durable storage zero times, got %v", mstore.batches)
	}
	if status, out := postSamplesRaw(t, server, `{"samples":[`+one+`]}`); status != http.StatusOK {
		t.Fatalf("valid batch after rejected ones must succeed, got %d %v", status, out)
	} else if int(number(out["created"])) != 1 || int(number(out["replayed"])) != 0 {
		t.Fatalf("created/replayed after recovery: %v", out)
	}
}

// TestPostMetricsDuplicateAfterValidSampleRejectsBatch pins the atomicity
// rule: a duplicate field on a sample later in a batch rejects the whole
// request, including a perfectly valid new sample earlier in it. Nothing is
// persisted, the stored aggregate is unchanged in the window, and the failed
// attempt reserves neither the earlier sample's id nor its timestamp.
func TestPostMetricsDuplicateAfterValidSampleRejectsBatch(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, nil, mstore)
	seed := `{"samples":[
		{"id":"base","service":"gw","name":"requests","at":"2026-10-01T09:59:00Z","value":90},
		{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":100}
	]}`
	if status, out := postSamplesRaw(t, server, seed); status != http.StatusOK || int(number(out["created"])) != 2 {
		t.Fatalf("seed: %d %v", status, out)
	}

	status, before := aggregateGet(t, server, dupWindowQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate before: %d", status)
	}
	assertOneWindowSeries(t, before, []int{1, 0}, []float64{10, 0})

	// s1 is a brand-new sample that would commit on its own; s2 follows it
	// with a duplicated value member. The whole request must be refused.
	bad := `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":110},
		{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T10:01:30Z","value":1,"value":2}
	]}`
	if status, out := postSamplesRaw(t, server, bad); status != http.StatusBadRequest {
		t.Fatalf("later duplicate field must reject whole batch with 400, got %d %v", status, out)
	} else if msg, _ := out["error"].(string); msg == "" {
		t.Fatalf("need non-empty error, got %v", out)
	}

	// Nothing reached durable storage beyond the seed...
	if len(mstore.batches) != 1 || len(mstore.batches[0]) != 2 {
		t.Fatalf("rejected batch must persist nothing, stored batches=%v", mstore.batches)
	}
	// ...and the existing series' in-window counts and deltas are untouched.
	status, after := aggregateGet(t, server, dupWindowQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after rejection: %d", status)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected batch must leave the aggregate untouched:\n%+v\n%+v", after, before)
	}

	// Removing the duplicate and resubmitting the same identities (s1's id
	// and instant, s2's instant) must ingest normally: the failed parse
	// reserved nothing. s3 lands exactly at the window end to pin the
	// half-open [since, until) boundary.
	clean := `{"samples":[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":110},
		{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T10:01:30Z","value":120},
		{"id":"s3","service":"gw","name":"requests","at":"2026-10-01T10:02:00Z","value":999}
	]}`
	if status, out := postSamplesRaw(t, server, clean); status != http.StatusOK {
		t.Fatalf("clean retry must succeed, got %d %v", status, out)
	} else if int(number(out["created"])) != 3 || int(number(out["replayed"])) != 0 {
		t.Fatalf("clean retry counts: %v", out)
	}
	status, final := aggregateGet(t, server, dupWindowQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate after clean retry: %d", status)
	}
	// s1: +10 in segment 1; s2: +10 in segment 1. s3 at the exact end is
	// excluded, so segment 0 is unchanged and no sample appears for s3.
	assertOneWindowSeries(t, final, []int{1, 2}, []float64{10, 20})
}

// TestPostMetricsDuplicateFieldPrecedesContentConflict guarantees malformed
// JSON is a 400 input error even when the same batch also contains content
// that would otherwise be a 409 conflict: duplicate field detection happens
// before the store ever sees the batch.
func TestPostMetricsDuplicateFieldPrecedesContentConflict(t *testing.T) {
	mstore := &fakeMetricStorage{}
	server := newMetricsServer(t, nil, mstore)
	seed := `{"samples":[
		{"id":"base","service":"gw","name":"requests","at":"2026-10-01T09:59:00Z","value":90},
		{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":100}
	]}`
	postSamples(t, server, seed)
	status, before := aggregateGet(t, server, dupWindowQuery)
	if status != http.StatusOK {
		t.Fatalf("aggregate before: %d", status)
	}

	// Each malformed body carries a genuine conflict plus a duplicate field.
	malformed := map[string]string{
		"conflicting sample duplicates its own value": `{"samples":[
			{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":555,"value":555}
		]}`,
		"stored id clash followed by duplicate field": `{"samples":[
			{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":555},
			{"id":"x1","id":"x2","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":1}
		]}`,
		"repeated id and repeated field in one batch": `{"samples":[
			{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":1},
			{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:01:30Z","value":2,"name":"requests"}
		]}`,
	}
	for name, body := range malformed {
		t.Run(name, func(t *testing.T) {
			status, out := postSamplesRaw(t, server, body)
			assertBadRequest(t, name, status, out)
		})
	}
	if len(mstore.batches) != 1 {
		t.Fatalf("malformed batches must persist nothing, got %d batches", len(mstore.batches))
	}
	status, after := aggregateGet(t, server, dupWindowQuery)
	if status != http.StatusOK || !reflect.DeepEqual(after, before) {
		t.Fatalf("400 batches must leave the aggregate untouched: %d\n%+v\n%+v", status, after, before)
	}

	// Contrast: the same conflicts without the duplicate fields are 409s,
	// proving the 400 above was not vacuous.
	conflicts := map[string]string{
		"stored id with changed content": `{"samples":[
			{"id":"s0","service":"gw","name":"requests","at":"2026-10-01T10:00:30Z","value":555}
		]}`,
		"same id repeated across samples": `{"samples":[
			{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:01:00Z","value":1},
			{"id":"d","service":"gw","name":"requests","at":"2026-10-01T10:01:30Z","value":2}
		]}`,
	}
	for name, body := range conflicts {
		t.Run(name, func(t *testing.T) {
			status, out := postSamplesRaw(t, server, body)
			if status != http.StatusConflict {
				t.Fatalf("%s: want 409 without the duplicate field, got %d (%v)", name, status, out)
			}
			if msg, _ := out["error"].(string); msg == "" {
				t.Fatalf("%s: 409 needs a non-empty error", name)
			}
		})
	}
}

// TestPostMetricsNoLabelsSpellingsAndHalfOpenWindow preserves the legitimate
// shapes alongside the duplicate rule: absent labels, null, and {} all mean
// the same label-less series, and the aggregate window stays half-open.
func TestPostMetricsNoLabelsSpellingsAndHalfOpenWindow(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	body := `{"samples":[
		{"id":"n1","service":"gw","name":"hits","at":"2026-10-01T10:00:00Z","value":10},
		{"id":"n2","service":"gw","name":"hits","at":"2026-10-01T10:00:30Z","value":12,"labels":null},
		{"id":"n3","service":"gw","name":"hits","at":"2026-10-01T10:01:00Z","value":18,"labels":{}}
	]}`
	if status, out := postSamplesRaw(t, server, body); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	} else if int(number(out["created"])) != 3 || int(number(out["replayed"])) != 0 {
		t.Fatalf("no-label spellings counts: %v", out)
	}

	query := "?name=hits&since=2026-10-01T10:00:00Z&until=2026-10-01T10:02:00Z&step=60"
	status, out := aggregateGet(t, server, query)
	if status != http.StatusOK {
		t.Fatalf("aggregate: %d", status)
	}
	if len(out) != 1 {
		t.Fatalf("all three spellings must merge into one label-less series, got %d: %+v", len(out), out)
	}
	if _, hasLabels := out[0]["labels"]; hasLabels {
		t.Fatalf("label-less series must omit labels: %+v", out[0])
	}
	// First reading has no predecessor, so it adds count but no delta; the
	// next two contribute +2 and +6 in their respective segments.
	assertSegments(t, out[0], []int{2, 1}, []float64{2, 6})

	// A sample exactly at Until ingests but stays outside the half-open
	// window: counts and deltas above must not move.
	if status, out := postSamplesRaw(t, server, `{"samples":[
		{"id":"n4","service":"gw","name":"hits","at":"2026-10-01T10:02:00Z","value":50}
	]}`); status != http.StatusOK || int(number(out["created"])) != 1 {
		t.Fatalf("end-boundary sample: %d %v", status, out)
	}
	status, bounded := aggregateGet(t, server, query)
	if status != http.StatusOK || len(bounded) != 1 {
		t.Fatalf("aggregate after end sample: %d %+v", status, bounded)
	}
	assertSegments(t, bounded[0], []int{2, 1}, []float64{2, 6})
}

// TestPostMetricsSameNameAcrossObjectsStaysValid pins the scope of duplicate
// detection: only repeats inside one object count. Field names shared by
// distinct samples and label names shared by distinct labels objects are
// ordinary input and form distinct series.
func TestPostMetricsSameNameAcrossObjectsStaysValid(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	body := `{"samples":[
		{"id":"l1","service":"gw","name":"hits","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"l2","service":"gw","name":"hits","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"canary"}}
	]}`
	if status, out := postSamplesRaw(t, server, body); status != http.StatusOK {
		t.Fatalf("cross-object names must be valid, got %d %v", status, out)
	} else if int(number(out["created"])) != 2 {
		t.Fatalf("created: %v", out)
	}
	query := "?name=hits&since=2026-10-01T10:00:00Z&until=2026-10-01T10:01:00Z&step=60"
	status, out := aggregateGet(t, server, query)
	if status != http.StatusOK {
		t.Fatalf("aggregate: %d", status)
	}
	if len(out) != 2 {
		t.Fatalf("separate label objects must form two series, got %d: %+v", len(out), out)
	}
	envs := map[string]bool{}
	for _, series := range out {
		labels, ok := series["labels"].(map[string]any)
		if !ok {
			t.Fatalf("series missing labels: %+v", series)
		}
		env, _ := labels["env"].(string)
		envs[env] = true
	}
	if !envs["prod"] || !envs["canary"] {
		t.Fatalf("both independent label sets must survive, got %v", envs)
	}
}

// assertOneWindowSeries checks the dupWindowQuery response holds exactly one
// label-less gw series with the expected per-segment vectors.
func assertOneWindowSeries(t *testing.T, out []map[string]any, wantCounts []int, wantDeltas []float64) {
	t.Helper()
	if len(out) != 1 {
		t.Fatalf("want exactly one series, got %d: %+v", len(out), out)
	}
	if out[0]["service"] != "gw" {
		t.Fatalf("service: %v", out[0]["service"])
	}
	if _, hasLabels := out[0]["labels"]; hasLabels {
		t.Fatalf("series must be label-less: %+v", out[0])
	}
	assertSegments(t, out[0], wantCounts, wantDeltas)
}

// assertSegments compares one series' segment count/delta vectors.
func assertSegments(t *testing.T, series map[string]any, wantCounts []int, wantDeltas []float64) {
	t.Helper()
	rawSegs, ok := series["segments"].([]any)
	if !ok || len(rawSegs) != len(wantCounts) {
		t.Fatalf("segments shape: %v", series["segments"])
	}
	for i, rawSeg := range rawSegs {
		seg := rawSeg.(map[string]any)
		if int(seg["count"].(float64)) != wantCounts[i] {
			t.Fatalf("segment %d count = %v, want %d (full: %+v)", i, seg["count"], wantCounts[i], series)
		}
		if seg["delta"].(float64) != wantDeltas[i] {
			t.Fatalf("segment %d delta = %v, want %v (full: %+v)", i, seg["delta"], wantDeltas[i], series)
		}
	}
}
