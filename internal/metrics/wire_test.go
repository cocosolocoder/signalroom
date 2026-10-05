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

func TestDecodeBatchRejectsDuplicateFields(t *testing.T) {
	// A duplicate is a syntactic error, independent of the values carried: the
	// same value twice and a null first value are both rejected rather than
	// silently last-wins.
	sample := func(inner string) string {
		return `[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,` + inner + `}]`
	}
	// Names compare on their decoded text, so a \uXXXX escape of the same
	// letters is the same member name. Build the escape without writing it
	// literally.
	u := func(hexdigits, tail string) string {
		return string(rune(0x5C)) + "u" + hexdigits + tail // 0x5C == '\\'
	}
	bad := []string{
		// Outer request envelope (validated by the HTTP layer's strict
		// decoder) is covered in the httpapi tests; here DecodeBatch sees the
		// samples array, whose entries and labels must be unique-named.
		`[{"id":"a","id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1}]`, // identical values
		`[{"id":null,"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1}]`, // null first, legal second
		`[{"id":"a","` + u("0069", "d") + `":"b","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1}]`,          // escaped key == id
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"` + u("0076", "alue") + `":2}]`,          // escaped key == value
		sample(`"labels":{"env":"prod","env":"prod"}`),                             // identical label values
		sample(`"labels":{"env":"prod","env":"dev"}`),                              // differing label values still dup
		sample(`"labels":{"env":"prod","` + u("0065", "nv") + `":null}`),           // escaped label name == env
		sample(`"labels":null,"labels":{"env":"prod"}`),                            // labels member twice
	}
	for i, in := range bad {
		if _, err := DecodeBatch([]byte(in)); err == nil {
			t.Fatalf("duplicate input %d must fail: %s", i, in)
		}
	}
}

func TestDecodeBatchRepeatedNamesAcrossObjectsAreLegal(t *testing.T) {
	// Duplicate detection is scoped to one object: a field name recurring in
	// another sample, or env appearing in separate labels objects, is normal.
	in := `[
		{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"b","service":"gw","name":"n","at":"2026-10-01T10:00:01Z","value":2,"labels":{"env":"staging","region":"cn"}}
	]`
	batch, err := DecodeBatch([]byte(in))
	if err != nil {
		t.Fatalf("repeated names in distinct objects must decode: %v", err)
	}
	if len(batch) != 2 || batch[0].Labels["env"] != "prod" || batch[1].Labels["env"] != "staging" {
		t.Fatalf("batch: %+v", batch)
	}
}

func TestDecodeBatchEmptyLabelShapes(t *testing.T) {
	// No labels member, an explicit null, and {} all normalize to no labels.
	cases := []string{
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1}]`,
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"labels":null}]`,
		`[{"id":"a","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"labels":{}}]`,
	}
	for i, in := range cases {
		batch, err := DecodeBatch([]byte(in))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(batch[0].Labels) != 0 {
			t.Fatalf("case %d: labels must be empty, got %+v", i, batch[0].Labels)
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
