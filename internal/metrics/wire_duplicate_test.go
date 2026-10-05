package metrics

import "testing"

// TestDecodeBatchRejectsDuplicateSampleFields pins the duplicate-member
// rule a plain struct or map decode would hide: every sample object field
// may appear at most once within the same object. Equal values, a null first
// value, and \u0069-style spellings of a name are still repeats because names
// are compared after JSON decoding.
func TestDecodeBatchRejectsDuplicateSampleFields(t *testing.T) {
	const at = `"at":"2026-10-01T10:00:00Z"`
	cases := map[string]string{
		"duplicate id same value":      `[{"id":"s1","id":"s1","service":"gw","name":"n",` + at + `,"value":1}]`,
		"duplicate id differing":       `[{"id":"s1","id":"s2","service":"gw","name":"n",` + at + `,"value":1}]`,
		"duplicate value same literal": `[{"id":"s1","service":"gw","name":"n",` + at + `,"value":1,"value":1}]`,
		"duplicate at":                 `[{"id":"s1","service":"gw","name":"n",` + at + `,` + at + `}]`,
		"duplicate service":            `[{"id":"s1","service":"gw1","service":"gw2","name":"n",` + at + `,"value":1}]`,
		"duplicate name":               `[{"id":"s1","service":"gw","name":"n1","name":"n2",` + at + `,"value":1}]`,
		"duplicate labels field":       `[{"id":"s1","service":"gw","name":"n",` + at + `,"value":1,"labels":{"env":"prod"},"labels":{"env":"dev"}}]`,
		"null then valid id":           `[{"id":null,"id":"s1","service":"gw","name":"n",` + at + `,"value":1}]`,
		"null then valid value":        `[{"id":"s1","service":"gw","name":"n",` + at + `,"value":null,"value":1}]`,
		"null then valid at":           `[{"id":"s1","service":"gw","name":"n","at":null,` + at + `,"value":1}]`,
		"null then real labels":        `[{"id":"s1","service":"gw","name":"n",` + at + `,"value":1,"labels":null,"labels":{"env":"prod"}}]`,
		"bad labels then good labels":  `[{"id":"s1","service":"gw","name":"n",` + at + `,"value":1,"labels":{"env":1},"labels":{"env":"prod"}}]`,
		// \u0069d decodes to "id"; \u0061t decodes to "at". Comparison
		// runs on the decoded text, so these repeat the literal member.
		"unicode escape duplicates id": `[{"id":"s1","\u0069d":"s2","service":"gw","name":"n",` + at + `,"value":1}]`,
		"unicode escaped at repeats":   `[{"id":"s1","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","\u0061t":"2026-10-01T10:00:00Z","value":1}]`,
		"dup field in later sample": `[
			{"id":"s1","service":"gw","name":"n",` + at + `,"value":1},
			{"id":"s2","id":"s3","service":"gw","name":"n","at":"2026-10-01T10:00:01Z","value":2}
		]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBatch([]byte(body)); err == nil {
				t.Fatal("want decode error for duplicate field")
			}
		})
	}
}

// TestDecodeBatchRejectsDuplicateLabelNames pins the same rule inside a
// sample's labels object: a label name may repeat only across distinct label
// objects, never within one. The \u0065nv escape of the literal name env is
// the same name after decoding.
func TestDecodeBatchRejectsDuplicateLabelNames(t *testing.T) {
	const prefix = `{"id":"s1","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1,"labels":`
	cases := map[string]string{
		"duplicate label same value": `{"env":"prod","env":"prod"}`,
		"duplicate label differing":  `{"env":"prod","env":"dev"}`,
		"escape then literal":        `{"\u0065nv":"prod","env":"dev"}`,
		"literal then escape":        `{"env":"prod","\u0065nv":"dev"}`,
		"null then string":           `{"env":null,"env":"prod"}`,
		"name repeats third":         `{"a":"1","b":"2","a":"3"}`,
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBatch([]byte(prefix + labels + `}`)); err == nil {
				t.Fatalf("want decode error for labels %s", labels)
			}
		})
	}
}

// TestDecodeBatchSameFieldInDifferentObjectsIsFine establishes that duplicate
// detection is scoped to one JSON object: shared field names across samples,
// and shared label names across different samples' labels objects, remain
// valid input even though repeating them within one object would fail.
func TestDecodeBatchSameFieldInDifferentObjectsIsFine(t *testing.T) {
	body := `[
		{"id":"s1","service":"gw","name":"requests","at":"2026-10-01T10:00:00Z","value":1,"labels":{"env":"prod"}},
		{"id":"s2","service":"gw","name":"requests","at":"2026-10-01T10:00:01Z","value":2,"labels":{"env":"staging"}},
		{"id":"s3","service":"gw","name":"requests","at":"2026-10-01T10:00:02Z","value":3,"labels":{"env":"prod","team":"gw"}}
	]`
	batch, err := DecodeBatch([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 3 {
		t.Fatalf("want 3 samples, got %d", len(batch))
	}
	if batch[0].Labels["env"] != "prod" || batch[1].Labels["env"] != "staging" || batch[2].Labels["team"] != "gw" {
		t.Fatalf("independent label objects must keep their own values: %+v", batch)
	}
}

// TestDecodeBatchNoLabelsSpellings guarantees absent, null, and {} labels
// all decode as "no labels", matching the stored and aggregated shape.
func TestDecodeBatchNoLabelsSpellings(t *testing.T) {
	body := `[
		{"id":"s1","service":"gw","name":"n","at":"2026-10-01T10:00:00Z","value":1},
		{"id":"s2","service":"gw","name":"n","at":"2026-10-01T10:00:01Z","value":2,"labels":null},
		{"id":"s3","service":"gw","name":"n","at":"2026-10-01T10:00:02Z","value":3,"labels":{}}
	]`
	batch, err := DecodeBatch([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 3 {
		t.Fatalf("want 3 samples, got %d", len(batch))
	}
	for i, sample := range batch {
		if sample.Labels != nil {
			t.Fatalf("sample %d: absent/null/empty labels must decode nil, got %+v", i, sample.Labels)
		}
	}
}
