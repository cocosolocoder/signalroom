package httpapi

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

const labeledSeed = `{"events":[
	{"id":"l1","service":"gateway","severity":"critical","message":"spike","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}},
	{"id":"l2","service":"gateway","severity":"info","message":"ok","at":"2026-10-01T09:01:00Z","labels":{"env":"stage"}},
	{"id":"l3","service":"api","severity":"info","message":"ok","at":"2026-10-01T09:02:00Z"}
]}`

func TestLabelsRoundTripAndOmission(t *testing.T) {
	server, store, _ := newTestServer(t)
	if status, out := post(t, server, labeledSeed); status != http.StatusOK {
		t.Fatalf("seed: %d %v", status, out)
	}
	if len(store.appended) != 1 {
		t.Fatalf("one durable batch expected: %v", store.appended)
	}

	status, rows := get(t, server, "")
	if status != http.StatusOK {
		t.Fatalf("get: %d", status)
	}
	arr := rows.([]any)
	first := arr[0].(map[string]any)
	labels, ok := first["labels"].(map[string]any)
	if !ok || labels["env"] != "prod" || labels["version"] != "v2" {
		t.Fatalf("labels must be returned in full: %v", first)
	}
	if _, present := arr[2].(map[string]any)["labels"]; present {
		t.Fatalf("label-less event must omit labels: %v", arr[2])
	}
}

func TestLabelsReplayEquivalenceAndConflict(t *testing.T) {
	server, store, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"r1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod","version":"v2"}}]}`)

	// Reordered keys and padded whitespace are the same event.
	retry := `{"events":[{"id":"r1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{" version ":" v2 ","env":"prod"}}]}`
	status, out := post(t, server, retry)
	if status != http.StatusOK || out["replayed"].(float64) != 1 || out["created"].(float64) != 0 {
		t.Fatalf("retry must replay: %d %v", status, out)
	}
	if len(store.appended) != 1 {
		t.Fatal("label-equivalent retry must not persist a new batch")
	}

	// The three no-label spellings are interchangeable.
	post(t, server, `{"events":[{"id":"r2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]}`)
	for _, spelling := range []string{`"labels":null`, `"labels":{}`, ``} {
		body := `{"events":[{"id":"r2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"`
		if spelling != "" {
			body += "," + spelling
		}
		body += `}]}`
		status, out = post(t, server, body)
		if status != http.StatusOK || out["replayed"].(float64) != 1 {
			t.Fatalf("%s must replay: %d %v", spelling, status, out)
		}
	}
	if len(store.appended) != 2 {
		t.Fatalf("no-label retries must not persist: %v", store.appended)
	}

	// Adding, removing, or changing a label conflicts.
	for name, labels := range map[string]string{
		"added":   `"labels":{"env":"prod","version":"v2","region":"cn1"}`,
		"removed": `"labels":{"env":"prod"}`,
		"changed": `"labels":{"env":"prod","version":"v3"}`,
		"cleared": `"labels":null`,
	} {
		t.Run(name, func(t *testing.T) {
			body := `{"events":[{"id":"r1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z",` + labels + `}]}`
			if status, out := post(t, server, body); status != http.StatusConflict {
				t.Fatalf("want 409, got %d %v", status, out)
			}
		})
	}
	if len(store.appended) != 2 {
		t.Fatalf("conflicts must not persist: %v", store.appended)
	}
}

func TestLabelsRejectedIngest(t *testing.T) {
	server, store, _ := newTestServer(t)
	longName := strings.Repeat("n", 65)
	longValue := strings.Repeat("v", 257)
	many := make([]string, 0, 33)
	for i := 0; i < 33; i++ {
		many = append(many, fmt.Sprintf(`"k%d":"v"`, i))
	}
	cases := map[string]string{
		"non-string value":  `"labels":{"env":1}`,
		"duplicate keys":    `"labels":{"env":"a","env":"a"}`,
		"trim collision":    `"labels":{"env":"a"," env ":"a"}`,
		"empty name":        `"labels":{"  ":"v"}`,
		"empty value":       `"labels":{"env":" "}`,
		"name too long":     `"labels":{"` + longName + `":"v"}`,
		"value too long":    `"labels":{"env":"` + longValue + `"}`,
		"too many labels":   `{` + strings.Join(many, ",") + `}`,
		"labels not object": `"labels":["env"]`,
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			field := labels
			if !strings.HasPrefix(labels, `"labels"`) {
				field = `"labels":` + labels
			}
			body := `{"events":[{"id":"x","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z",` + field + `}]}`
			status, out := post(t, server, body)
			if status != http.StatusBadRequest {
				t.Fatalf("want 400, got %d %v", status, out)
			}
			if msg, _ := out["error"].(string); msg == "" {
				t.Fatal("error response needs a non-empty error string")
			}
		})
	}
	if len(store.appended) != 0 {
		t.Fatalf("rejected batches must not persist: %v", store.appended)
	}
}

func TestLabelValidationPrecedesConflict(t *testing.T) {
	server, store, _ := newTestServer(t)
	post(t, server, `{"events":[{"id":"dup","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod"}}]}`)
	// The batch both clashes with the stored event and carries an invalid
	// label on a different event: validation must win with a 400.
	mixed := `{"events":[
		{"id":"dup","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"stage"}},
		{"id":"new","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z","labels":{"":"v"}}
	]}`
	if status, _ := post(t, server, mixed); status != http.StatusBadRequest {
		t.Fatalf("label validation must win over conflict, got %d", status)
	}
	if len(store.appended) != 1 {
		t.Fatalf("rejected batch must not persist: %v", store.appended)
	}
}

func TestGetEventsLabelFilter(t *testing.T) {
	server, _, _ := newTestServer(t)
	if status, _ := post(t, server, labeledSeed); status != http.StatusOK {
		t.Fatal("seed")
	}

	ids := func(rows any) []string {
		var out []string
		for _, row := range rows.([]any) {
			out = append(out, row.(map[string]any)["id"].(string))
		}
		return out
	}
	cases := []struct {
		query string
		want  []string
	}{
		{"?label=env=prod", []string{"l1"}},
		{"?label=env=prod&label=version=v2", []string{"l1"}},
		{"?label=env=prod&label=version=v3", nil},
		{"?label=%20env%20=%20prod%20", []string{"l1"}}, // whitespace trimmed
		{"?label=env=Prod", nil},                        // case-sensitive
		{"?label=version=v2", []string{"l1"}},
		{"?label=env=prod&service=gateway&severity=critical", []string{"l1"}},
		{"?label=env=prod&service=api", nil},
	}
	for _, tc := range cases {
		status, rows := get(t, server, tc.query)
		if status != http.StatusOK {
			t.Fatalf("%s: status %d", tc.query, status)
		}
		got := ids(rows)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: want %v, got %v", tc.query, tc.want, got)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: want %v, got %v", tc.query, tc.want, got)
			}
		}
	}
}

func TestGetEventsLabelFilterRejectsInvalid(t *testing.T) {
	server, _, _ := newTestServer(t)
	tooMany := make([]string, 0, 33)
	for i := 0; i < 33; i++ {
		tooMany = append(tooMany, fmt.Sprintf("label=k%d=v", i))
	}
	cases := []string{
		"?label=",
		"?label=env",
		"?label==prod",
		"?label=env=",
		"?label=env=prod&label=env=stage",
		"?label=env=prod&label=%20env%20=prod",
		"?label=" + strings.Repeat("n", 65) + "=v",
		"?label=env=" + strings.Repeat("v", 257),
		"?" + strings.Join(tooMany, "&"),
	}
	for _, q := range cases {
		status, out := get(t, server, q)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: want 400, got %d %v", q, status, out)
		}
		if msg, _ := out.(map[string]any)["error"].(string); msg == "" {
			t.Fatalf("%s: missing error string", q)
		}
	}
	// A value may itself contain equals signs.
	if status, _ := post(t, server, `{"events":[{"id":"eq","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"note":"a=b=c"}}]}`); status != http.StatusOK {
		t.Fatal("seed")
	}
	status, rows := get(t, server, "?label=note=a=b=c")
	if status != http.StatusOK || len(rows.([]any)) != 1 {
		t.Fatalf("equals inside value must match: %d %v", status, rows)
	}
}

func TestCompareLabelFilter(t *testing.T) {
	server, _, _ := newTestServer(t)
	body := `{"events":[
		{"id":"b1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod"}},
		{"id":"b2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T09:01:00Z","labels":{"env":"stage"}},
		{"id":"o1","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:00:00Z","labels":{"env":"prod","version":"v2"}},
		{"id":"o2","service":"gateway","severity":"critical","message":"m","at":"2026-10-01T10:01:00Z"}
	]}`
	if status, _ := post(t, server, body); status != http.StatusOK {
		t.Fatal("seed")
	}

	status, out := getCompare(t, server, goodCompareQuery+"&label=env=prod")
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	if out["baseline_total"].(float64) != 1 || out["observation_total"].(float64) != 1 {
		t.Fatalf("label filter must restrict totals: %v", out)
	}
	segment := out["segments"].([]any)[0].(map[string]any)
	if segment["baseline_count"].(float64) != 1 || segment["observation_count"].(float64) != 1 {
		t.Fatalf("segment counts: %v", segment)
	}

	// Multiple conditions AND together; unlabeled events never match.
	status, out = getCompare(t, server, goodCompareQuery+"&label=env=prod&label=version=v2")
	if status != http.StatusOK || out["baseline_total"].(float64) != 0 || out["observation_total"].(float64) != 1 {
		t.Fatalf("ANDed conditions: %d %v", status, out)
	}

	for _, q := range []string{"&label=", "&label=env", "&label=env=prod&label=env=stage"} {
		status, out = getCompare(t, server, goodCompareQuery+q)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: want 400, got %d %v", q, status, out)
		}
	}
}
