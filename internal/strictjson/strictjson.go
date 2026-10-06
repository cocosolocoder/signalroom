// Package strictjson holds the structural checks shared by the batch
// ingestion decoders: a JSON array of record objects whose member names must
// be unique within each object and drawn from a known set. Field meaning and
// content validation stay with the callers; only the shape rules live here.
package strictjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// EachArrayElement decodes data as a JSON array, invoking decodeElement with
// the decoder positioned at each element, and rejects anything after the
// closing bracket. context prefixes the structural error messages (for
// example "decode events array").
func EachArrayElement(data []byte, context string, decodeElement func(dec *json.Decoder) error) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	open, err := dec.Token()
	if err != nil {
		return fmt.Errorf("%s: %w", context, err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("%s: must be an array", context)
	}

	for dec.More() {
		if err := decodeElement(dec); err != nil {
			return err
		}
	}

	if _, err := dec.Token(); err != nil { // closing ']'
		return fmt.Errorf("%s: %w", context, err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%s: unexpected trailing data", context)
		}
		return fmt.Errorf("%s: %w", context, err)
	}
	return nil
}

// ObjectMembers consumes one JSON object from dec. Each member name is
// checked against allowed and rejected when it repeats an earlier name of the
// same object — names are compared after JSON decoding, so a literal name
// and its \uXXXX escape of the same text count as a repeat. The check is
// scoped to this one object; other records are unaffected. decodeValue is
// called to consume each member's value. The returned set reports which
// member names were present, so callers can enforce required fields.
func ObjectMembers(dec *json.Decoder, context string, allowed map[string]struct{}, decodeValue func(key string) error) (map[string]struct{}, error) {
	open, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", context, err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("%s: each entry must be an object", context)
	}

	seen := make(map[string]struct{})
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", context, err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("%s: field names must be strings", context)
		}
		if _, known := allowed[key]; !known {
			return nil, fmt.Errorf("%s: unknown field %q", context, key)
		}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("%s: duplicate field %q", context, key)
		}
		seen[key] = struct{}{}

		if err := decodeValue(key); err != nil {
			return nil, err
		}
	}

	if _, err := dec.Token(); err != nil { // closing '}'
		return nil, fmt.Errorf("%s: %w", context, err)
	}
	return seen, nil
}
