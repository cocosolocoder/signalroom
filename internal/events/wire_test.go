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

func TestDecodeBatchRejectsDuplicateEventFields(t *testing.T) {
	at := `"at":"2026-10-01T09:00:00Z"`
	cases := map[string]string{
		"duplicate id same value":         `[{"id":"e1","id":"e1","service":"s","severity":"info","message":"m",` + at + `}]`,
		"duplicate id differing":          `[{"id":"e1","id":"e2","service":"s","severity":"info","message":"m",` + at + `}]`,
		"duplicate at":                    `[{"id":"e1","service":"s","severity":"info","message":"m",` + at + `,` + at + `}]`,
		"bad labels then good labels":     `[{"id":"e1","service":"s","severity":"info","message":"m",` + at + `,"labels":{"env":1},"labels":{"env":"prod"}}]`,
		"null then real labels":           `[{"id":"e1","service":"s","severity":"info","message":"m",` + at + `,"labels":null,"labels":{"env":"prod"}}]`,
		"unicode escape duplicates lit":   `[{"id":"e1","service":"s","severity":"info","message":"m",` + at + `,"labels":{"\u0065nv":"a","env":"b"}}]`,
		"unicode escaped field repeats":   `[{"id":"e1","\u0069\u0064":"e2","service":"s","severity":"info","message":"m",` + at + `}]`,
		"second duplicate in later event": `[{"id":"e1","service":"s","severity":"info","message":"m",` + at + `},{"id":"e2","id":"e3","service":"s","severity":"info","message":"m",` + at + `}]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBatch([]byte(body)); err == nil {
				t.Fatal("want decode error for duplicate field")
			}
		})
	}
}

func TestDecodeBatchSameFieldInDifferentEventsIsFine(t *testing.T) {
	body := `[
		{"id":"e1","service":"s","severity":"info","message":"m","at":"2026-10-01T09:00:00Z","labels":{"env":"prod"}},
		{"id":"e2","service":"s","severity":"info","message":"m","at":"2026-10-01T09:01:00Z","labels":{"env":"canary"}}
	]`
	batch, err := DecodeBatch([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].Labels["env"] != "prod" || batch[1].Labels["env"] != "canary" {
		t.Fatalf("independent objects must not share duplicate detection: %+v", batch)
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
