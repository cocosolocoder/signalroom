package events

import (
	"strings"
	"testing"
	"time"
)

func TestDecodeBatchLabels(t *testing.T) {
	at := "2026-10-01T09:00:00Z"
	cases := map[string]struct {
		labels string
		want   map[string]string
	}{
		"absent":     {``, nil},
		"null":       {`,"labels":null`, nil},
		"empty":      {`,"labels":{}`, nil},
		"populated":  {`,"labels":{"env":"prod","version":"v2"}`, map[string]string{"env": "prod", "version": "v2"}},
		"whitespace": {`,"labels":{" env ":" prod "}`, map[string]string{" env ": " prod "}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			body := `[{"id":"e1","service":"s","severity":"info","message":"m","at":"` + at + `"` + tc.labels + `}]`
			batch, err := DecodeBatch([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			got := batch[0].Labels
			if len(got) != len(tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("want %v, got %v", tc.want, got)
				}
			}
		})
	}
}

func TestDecodeBatchRejectsDuplicateEventFields(t *testing.T) {
	head := `[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"`
	cases := map[string]string{
		"duplicate id":            head + `,"id":"e2"}]`,
		"duplicate id same value": head + `,"id":"e1"}]`,
		"duplicate service":       head + `,"service":"s2"}]`,
		"duplicate severity":      head + `,"severity":"info"}]`,
		"duplicate message":       head + `,"message":"m2"}]`,
		"duplicate at":            head + `,"at":"2026-10-01T09:01:00Z"}]`,
		// A bad labels object hidden by a second, legal one must not survive.
		"duplicate labels bad first":  head + `,"labels":{"env":1},"labels":{"env":"prod"}}]`,
		"duplicate labels null first": head + `,"labels":null,"labels":{"env":"prod"}}]`,
		"duplicate labels identical":  head + `,"labels":{"env":"prod"},"labels":{"env":"prod"}}]`,
		// The same name written literally and as a Unicode escape collides after
		// JSON decoding: \u0069d == "id", \u0065nv == "env", \u006cabels == "labels".
		"duplicate id unicode escape":     `[{"id":"e1","\u0069d":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]`,
		"duplicate labels unicode escape": `[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"a","\u0065nv":"b"}}]`,
		"duplicate labels member unicode": head + `,"labels":{"env":"prod"},"\u006cabels":{"env":"prod"}}]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBatch([]byte(body)); err == nil {
				t.Fatal("want decode error for duplicate field")
			}
		})
	}
}

func TestDecodeBatchDuplicateFieldScopedToObject(t *testing.T) {
	// The same field name in two different event objects is normal; only a
	// repeat within one object is ambiguous.
	body := `[
		{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod"}},
		{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z","labels":{"env":"stage"}}
	]`
	batch, err := DecodeBatch([]byte(body))
	if err != nil {
		t.Fatalf("same-named fields across events must decode: %v", err)
	}
	if len(batch) != 2 || batch[0].Labels["env"] != "prod" || batch[1].Labels["env"] != "stage" {
		t.Fatalf("unexpected batch: %+v", batch)
	}
}

func TestDecodeBatchRejectsNonObjectEvent(t *testing.T) {
	for _, body := range []string{`[1]`, `["x"]`, `[null]`, `[[{}]]`} {
		if _, err := DecodeBatch([]byte(body)); err == nil {
			t.Fatalf("non-object event must fail: %s", body)
		}
	}
}

func TestDecodeBatchRejectsBadLabels(t *testing.T) {
	cases := map[string]string{
		"duplicate keys":   `"labels":{"env":"a","env":"a"}`,
		"duplicate differ": `"labels":{"env":"a","env":"b"}`,
		"number value":     `"labels":{"env":1}`,
		"bool value":       `"labels":{"env":true}`,
		"null value":       `"labels":{"env":null}`,
		"nested object":    `"labels":{"env":{"x":"y"}}`,
		"array":            `"labels":["env"]`,
		"string":           `"labels":"env"`,
		"number":           `"labels":3`,
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			body := `[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z",` + labels + `}]`
			if _, err := DecodeBatch([]byte(body)); err == nil {
				t.Fatal("want decode error")
			}
		})
	}
}

func TestMarshalBatchLabelsRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	batch := []Event{
		{ID: "e1", Service: "s", Severity: "info", Message: "m", At: at, Labels: map[string]string{"env": "prod"}},
		{ID: "e2", Service: "s", Severity: "info", Message: "m", At: at},
	}
	data, err := MarshalBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"labels":{"env":"prod"}`) {
		t.Fatalf("labels must be encoded: %s", data)
	}
	// The label-less event must not carry a labels member at all.
	if strings.Count(string(data), `"labels"`) != 1 {
		t.Fatalf("labels must be omitted when absent: %s", data)
	}
	decoded, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded[0].Labels["env"] != "prod" || decoded[1].Labels != nil {
		t.Fatalf("round trip wrong: %+v", decoded)
	}
}

func TestDecodeBatchLegacyFrameWithoutLabels(t *testing.T) {
	// Frames written before labels existed have no labels member and must
	// still decode as label-less events.
	legacy := `[{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z"}]`
	batch, err := DecodeBatch([]byte(legacy))
	if err != nil {
		t.Fatal(err)
	}
	if batch[0].Labels != nil {
		t.Fatalf("legacy event must have no labels: %+v", batch[0])
	}
}
