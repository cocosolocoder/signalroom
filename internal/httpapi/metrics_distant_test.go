package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The two instants below are centuries apart but time.UnixNano reduces both
// to the same int64 through overflow, which previously made the batch
// conflict check treat them as one sampling point.
const (
	distantEarlyRFC = "2000-01-01T00:00:00Z"
	distantLateRFC  = "2584-07-20T23:34:33.709551616Z"
	distantLateOff  = "2584-07-21T07:34:33.709551616+08:00" // same instant as late, different offset
	distantLatePrev = "2584-07-20T23:34:33.709551615Z"      // one nanosecond earlier
)

func distantSamplesBody(earlyID, lateID string) string {
	return `{"samples":[
		{"id":"` + earlyID + `","service":"gateway","name":"requests","at":"` + distantEarlyRFC + `","value":100,"labels":{"env":"prod"}},
		{"id":"` + lateID + `","service":"gateway","name":"requests","at":"` + distantLateRFC + `","value":130,"labels":{"env":"prod"}}
	]}`
}

func TestPostMetricsDistantInstantsAccepted(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"chronological", distantSamplesBody("a", "b")},
		{"reversed", `{"samples":[
			{"id":"b","service":"gateway","name":"requests","at":"` + distantLateRFC + `","value":130,"labels":{"env":"prod"}},
			{"id":"a","service":"gateway","name":"requests","at":"` + distantEarlyRFC + `","value":100,"labels":{"env":"prod"}}
		]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newMetricsServer(t, nil, nil)
			out := postSamples(t, server, tc.body)
			if int(number(out["created"])) != 2 || int(number(out["replayed"])) != 0 {
				t.Fatalf("response: %v", out)
			}
			assertDistantWindowsHTTP(t, server)
		})
	}
}

func TestPostMetricsDistantInstantsSeparateThenReplay(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"a","service":"gateway","name":"requests","at":"`+distantEarlyRFC+`","value":100,"labels":{"env":"prod"}}
	]}`)
	if int(number(out["created"])) != 1 {
		t.Fatalf("first post: %v", out)
	}
	out = postSamples(t, server, `{"samples":[
		{"id":"b","service":"gateway","name":"requests","at":"`+distantLateRFC+`","value":130,"labels":{"env":"prod"}}
	]}`)
	if int(number(out["created"])) != 1 {
		t.Fatalf("second post: %v", out)
	}
	assertDistantWindowsHTTP(t, server)

	// The combined batch now replays, matching stored samples on their
	// absolute instants even though they were committed separately.
	out = postSamples(t, server, distantSamplesBody("a", "b"))
	if int(number(out["created"])) != 0 || int(number(out["replayed"])) != 2 {
		t.Fatalf("combined replay: %v", out)
	}
}

// assertDistantWindowsHTTP queries a one-minute window starting at each
// sample: each holds one sample; the early window has no predecessor (delta
// 0) and the late window rises by 30 over the nearest earlier sample.
func assertDistantWindowsHTTP(t *testing.T, server *httptest.Server) {
	t.Helper()
	cases := []struct {
		name  string
		query string
		delta float64
	}{
		{"early", "?name=requests&service=gateway&label=env=prod&since=" + distantEarlyRFC +
			"&until=2000-01-01T00:01:00Z&step=60", 0},
		{"late", "?name=requests&service=gateway&label=env=prod&since=" + distantLateRFC +
			"&until=2584-07-20T23:35:33.709551616Z&step=60", 30},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, got := aggregateGet(t, server, tc.query)
			if status != http.StatusOK || len(got) != 1 {
				t.Fatalf("status=%d series=%v", status, got)
			}
			segments := got[0]["segments"].([]any)
			if len(segments) != 1 {
				t.Fatalf("want one segment, got %v", segments)
			}
			seg := segments[0].(map[string]any)
			if seg["count"].(float64) != 1 || seg["delta"].(float64) != tc.delta {
				t.Fatalf("%s window count=%v delta=%v, want 1/%v", tc.name, seg["count"], seg["delta"], tc.delta)
			}
		})
	}
}

func TestPostMetricsDistantRealConflictsStillRejected(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	postSamples(t, server, `{"samples":[
		{"id":"a","service":"gateway","name":"requests","at":"`+distantLateRFC+`","value":1,"labels":{"env":"prod"}}
	]}`)

	cases := []struct {
		name string
		body string
	}{
		{
			"in-batch same instant different offsets",
			`{"samples":[
				{"id":"x","service":"gateway","name":"requests","at":"` + distantLateRFC + `","value":1,"labels":{"env":"prod"}},
				{"id":"y","service":"gateway","name":"requests","at":"` + distantLateOff + `","value":2,"labels":{"env":"prod"}}
			]}`,
		},
		{
			"new sample vs stored same instant different offsets",
			`{"samples":[
				{"id":"b","service":"gateway","name":"requests","at":"` + distantLateOff + `","value":2,"labels":{"env":"prod"}}
			]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, out := metricsDo(t, server, http.MethodPost, "/metrics", tc.body)
			if status != http.StatusConflict {
				t.Fatalf("want 409, got %d (%v)", status, out)
			}
			if errMsg, _ := out["error"].(string); errMsg == "" {
				t.Fatalf("non-empty error required, got %v", out)
			}
		})
	}

	// The rejected two-new-id batch left nothing: aggregate around the
	// instant still sees only the originally stored sample.
	status, got := aggregateGet(t, server,
		"?name=requests&label=env=prod&since="+distantLateRFC+"&until=2584-07-20T23:35:33.709551616Z&step=60")
	if status != http.StatusOK || len(got) != 1 {
		t.Fatalf("status=%d series=%v", status, got)
	}
	segments := got[0]["segments"].([]any)
	if segments[0].(map[string]any)["count"].(float64) != 1 {
		t.Fatalf("rejected batch must not persist samples: %v", segments)
	}
}

func TestPostMetricsDistantOneNanosecondApart(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"a","service":"gateway","name":"requests","at":"`+distantLatePrev+`","value":1,"labels":{"env":"prod"}},
		{"id":"b","service":"gateway","name":"requests","at":"`+distantLateRFC+`","value":2,"labels":{"env":"prod"}}
	]}`)
	if int(number(out["created"])) != 2 {
		t.Fatalf("one-nanosecond-apart points are distinct: %v", out)
	}
}

func TestPostMetricsDistantSameInstantDifferentLabelsAccepted(t *testing.T) {
	server := newMetricsServer(t, nil, nil)
	out := postSamples(t, server, `{"samples":[
		{"id":"a","service":"gateway","name":"requests","at":"`+distantLateRFC+`","value":1,"labels":{"env":"prod"}},
		{"id":"b","service":"gateway","name":"requests","at":"`+distantLateRFC+`","value":1,"labels":{"env":"staging"}}
	]}`)
	if int(number(out["created"])) != 2 {
		t.Fatalf("different label sets are different series: %v", out)
	}
}
