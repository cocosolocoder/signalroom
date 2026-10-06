package strictjson

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDecodeObjectRejectsDuplicateNames(t *testing.T) {
	// A repeat is structural regardless of the values carried: identical
	// values and a null first value are both rejected.
	cases := map[string]string{
		"identical values":    `{"a":1,"a":1}`,
		"differing values":    `{"a":1,"a":2}`,
		"null then value":     `{"a":null,"a":1}`,
		"value then null":     `{"a":1,"a":null}`,
		"unicode escape same": `{"a":1,"\u0061":2}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeObject([]byte(body), "test object", nil)
			var se *Error
			if !errors.As(err, &se) || se.Kind != KindDuplicateField || se.Field != "a" {
				t.Fatalf("want duplicate-field error for a, got %v", err)
			}
		})
	}
}

func TestDecodeObjectScopedToSingleObject(t *testing.T) {
	// Repeating a name across separate objects in an array is normal input.
	batch, err := DecodeArray([]byte(`[{"a":1},{"a":2}]`), "test array", func(dec *json.Decoder) (int, error) {
		var n int
		err := EachObjectField(dec, "test entry", nil, func(key string, raw json.RawMessage) error {
			if key != "a" {
				t.Fatalf("unexpected key %q", key)
			}
			return json.Unmarshal(raw, &n)
		})
		return n, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0] != 1 || batch[1] != 2 {
		t.Fatalf("names in distinct objects must not collide: %+v", batch)
	}
}

func TestDecodeObjectUnknownFields(t *testing.T) {
	allowed := map[string]struct{}{"a": {}}
	if _, err := DecodeObject([]byte(`{"a":1,"b":2}`), "test object", allowed); err == nil {
		t.Fatal("unknown field must be rejected")
	} else {
		var se *Error
		if !errors.As(err, &se) || se.Kind != KindUnknownField || se.Field != "b" {
			t.Fatalf("want unknown-field b, got %v", err)
		}
	}
	fields, err := DecodeObject([]byte(`{"a":1}`), "test object", allowed)
	if err != nil {
		t.Fatalf("allowed shape must decode: %v", err)
	}
	if string(fields["a"]) != "1" {
		t.Fatalf("raw value mismatch: %s", fields["a"])
	}
}

func TestDecodeObjectShapeFailures(t *testing.T) {
	for name, body := range map[string]string{
		"not object":     `[1,2]`,
		"broken json":    `{"a":`,
		"trailing value": `{"a":1} 2`,
		"trailing junk":  `{"a":1}garbage`,
		"empty":          ``,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeObject([]byte(body), "test object", nil); err == nil {
				t.Fatalf("%s must fail", name)
			}
		})
	}
}

func TestDecodeArrayShapeFailures(t *testing.T) {
	decode := func(dec *json.Decoder) (json.RawMessage, error) {
		var raw json.RawMessage
		err := dec.Decode(&raw)
		return raw, err
	}
	for name, body := range map[string]string{
		"not array":     `{"a":1}`,
		"broken json":   `[1,`,
		"entry not obj": `[{"a":1}, 3]`, // callback below decides; raw decode accepts 3
		"trailing":      `[1] {}`,
	} {
		t.Run(name, func(t *testing.T) {
			if name == "entry not obj" {
				// A permissive raw element decoder accepts the number; the
				// array itself is still structurally complete.
				got, err := DecodeArray([]byte(body), "test array", decode)
				if err != nil || len(got) != 2 {
					t.Fatalf("permissive element decode should succeed, got %v %v", got, err)
				}
				return
			}
			if _, err := DecodeArray([]byte(body), "test array", decode); err == nil {
				t.Fatalf("%s must fail", name)
			}
		})
	}
}

func TestDecodeEmptyArrayIsStructurallyLegal(t *testing.T) {
	// Emptiness is a caller policy: the structural scanner accepts [].
	batch, err := DecodeArray([]byte(`[]`), "test array", func(dec *json.Decoder) (int, error) {
		var n int
		err := dec.Decode(&n)
		return n, err
	})
	if err != nil || len(batch) != 0 {
		t.Fatalf("empty array must decode, got %v (%d entries)", err, len(batch))
	}
}

func TestVisitOrderMatchesStream(t *testing.T) {
	var order []string
	dec := json.NewDecoder(strings.NewReader(`{"z":1,"a":2,"m":3}`))
	err := EachObjectField(dec, "test object", nil, func(key string, _ json.RawMessage) error {
		order = append(order, key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"z", "a", "m"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("visit order %v, want %v", order, want)
	}
}

func TestVisitErrorAborts(t *testing.T) {
	want := errors.New("bad value")
	err := EachObjectField(json.NewDecoder(strings.NewReader(`{"a":1,"b":2}`)), "test object", nil,
		func(key string, raw json.RawMessage) error {
			if key == "a" {
				return want
			}
			return nil
		})
	if !errors.Is(err, want) {
		t.Fatalf("visitor error must pass through unwrapped, got %v", err)
	}
}

func TestNullValueStaysRaw(t *testing.T) {
	fields, err := DecodeObject([]byte(`{"a":null}`), "test object", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(fields["a"]); got != "null" {
		t.Fatalf("null must be preserved as a raw token, got %q", got)
	}
}
