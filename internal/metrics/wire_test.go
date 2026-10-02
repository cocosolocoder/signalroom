package metrics

import (
	"testing"
)

func TestDecodeBatchStrict(t *testing.T) {
	good := `[
		{"id":"a","service":"gw","name":"req","at":"2026-10-01T10:00:00Z","value":1},
		{"id":"b","service":"gw","name":"req","at":"2026-10-01T10:00:00.5+08:00","value":2.5,"labels":{"env":"prod"}}
	]`
	batch, err := DecodeBatch([]byte(good))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(batch) != 2 || batch[0].Value != 1 || batch[1].Value != 2.5 {
		t.Fatalf("batch: %+v", batch)
	}
	if batch[1].Labels["env"] != "prod" {
		t.Fatalf("labels: %+v", batch[1].Labels)
	}

	bad := []string{
		`{}`,           // not an array
		`[{"id":"a"}]`, // missing value (and more)
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":"1"}]`,                                   // value string
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":-1}]`,                                    // negative
		`[{"id":"a","service":"gw","name":"n","at":"nope","value":1}]`,                                                     // bad time
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"extra":2}]`,                           // unknown field
		`[{"id":"a","id":"b","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1}]`,                            // duplicate field
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod","env":"dev"}}]`, // dup label
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":1}}]`,                  // non-string label
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1}]garbage`,                              // trailing data
	}
	for i, in := range bad {
		if _, err := DecodeBatch([]byte(in)); err == nil {
			t.Fatalf("bad input %d must fail: %s", i, in)
		}
	}
}

func TestMarshalDecodeRoundTrip(t *testing.T) {
	original := []Sample{
		{ID: "a", Service: "gw", Name: "req", At: at(10), Value: 1.0},
		{ID: "b", Service: "gw", Name: "req", At: at(11), Value: 2, Labels: map[string]string{"env": "prod"}},
	}
	for i := range original {
		n, err := Normalize(original[i])
		if err != nil {
			t.Fatal(err)
		}
		original[i] = n
	}
	data, err := MarshalBatch(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded %d", len(decoded))
	}
	for i := range original {
		if !sameContent(original[i], decoded[i]) {
			t.Fatalf("round trip mismatch at %d: %+v vs %+v", i, original[i], decoded[i])
		}
	}
}
