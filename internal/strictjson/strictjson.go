// Package strictjson holds the structural JSON rules shared by the batch
// ingesters and request-body decoders: every object is read as a sequence of
// uniquely named members, where names are compared after JSON decoding (so a
// literal name and its \uXXXX escape of the same text repeat), unknown names
// are rejected, and each document is exactly one complete value with no
// tokens trailing it. Only the structure is shared: callers keep every
// domain meaning and type rule for the values they read.
package strictjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Kind classifies the structural failure an Error reports.
type Kind int

const (
	// KindSyntax wraps a tokenizer/decoder error such as malformed JSON.
	KindSyntax Kind = iota
	// KindNotObject means a value expected to be an object was not '{'.
	KindNotObject
	// KindNotArray means a value expected to be an array was not '['.
	KindNotArray
	// KindNameNotString means an object member name was not a JSON string.
	KindNameNotString
	// KindUnknownField means a member name was outside the allowed set.
	KindUnknownField
	// KindDuplicateField means a member name repeated within one object,
	// compared on the decoded name text.
	KindDuplicateField
	// KindTrailingData means another JSON value followed the document.
	KindTrailingData
)

// Error is a structural rejection located in the value named by What.
type Error struct {
	What  string
	Kind  Kind
	Field string // member name for unknown/duplicate-field errors
	Err   error  // underlying decoder error for KindSyntax
}

func (e *Error) Error() string {
	switch e.Kind {
	case KindSyntax:
		return fmt.Sprintf("%s: %v", e.What, e.Err)
	case KindNotObject:
		return fmt.Sprintf("%s: must be a JSON object", e.What)
	case KindNotArray:
		return fmt.Sprintf("%s: must be a JSON array", e.What)
	case KindNameNotString:
		return fmt.Sprintf("%s: member names must be strings", e.What)
	case KindUnknownField:
		return fmt.Sprintf("%s: unknown field %q", e.What, e.Field)
	case KindDuplicateField:
		return fmt.Sprintf("%s: duplicate field %q", e.What, e.Field)
	case KindTrailingData:
		return fmt.Sprintf("%s: unexpected trailing data", e.What)
	default:
		return e.What
	}
}

// Unwrap exposes the underlying decoder error.
func (e *Error) Unwrap() error { return e.Err }

func syntaxError(what string, err error) error {
	return &Error{What: what, Kind: KindSyntax, Err: err}
}

// EachObjectField reads exactly one JSON object from dec and invokes visit
// once per member in stream order with the member's decoded name and raw
// value. Names repeat on their decoded text, including across
// literal/Unicode-escape spellings, and (when allowed is non-nil) names
// outside allowed are unknown fields; both reject the whole object before
// visit sees the offender. Pass a nil allowed to accept any member name.
// Values are not examined, so null keeps its raw token for visit to
// interpret. A visit error aborts the read and is returned unwrapped.
func EachObjectField(
	dec *json.Decoder,
	what string,
	allowed map[string]struct{},
	visit func(key string, raw json.RawMessage) error,
) error {
	open, err := dec.Token()
	if err != nil {
		return syntaxError(what, err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		return &Error{What: what, Kind: KindNotObject}
	}
	seen := make(map[string]struct{})
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return syntaxError(what, err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return &Error{What: what, Kind: KindNameNotString}
		}
		if allowed != nil {
			if _, known := allowed[key]; !known {
				return &Error{What: what, Kind: KindUnknownField, Field: key}
			}
		}
		if _, dup := seen[key]; dup {
			return &Error{What: what, Kind: KindDuplicateField, Field: key}
		}
		seen[key] = struct{}{}

		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return syntaxError(what, err)
		}
		if err := visit(key, raw); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return syntaxError(what, err)
	}
	return nil
}

// DecodeObject strictly decodes data as exactly one JSON object, collecting
// every member's raw value and rejecting anything following the closing
// brace. Member names are already unique by the time they reach the map.
func DecodeObject(data []byte, what string, allowed map[string]struct{}) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	fields := make(map[string]json.RawMessage)
	err := EachObjectField(dec, what, allowed, func(key string, raw json.RawMessage) error {
		fields[key] = raw
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := NoTrailingData(dec, what); err != nil {
		return nil, err
	}
	return fields, nil
}

// DecodeArray strictly decodes data as exactly one JSON array, invoking
// decodeElement for each entry; the callback consumes exactly one element
// from dec. A non-array opener, a callback error, a malformed closing
// bracket, or content after the array aborts the whole decode.
func DecodeArray[T any](data []byte, what string, decodeElement func(*json.Decoder) (T, error)) ([]T, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	open, err := dec.Token()
	if err != nil {
		return nil, syntaxError(what, err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '[' {
		return nil, &Error{What: what, Kind: KindNotArray}
	}

	var batch []T
	for dec.More() {
		element, err := decodeElement(dec)
		if err != nil {
			return nil, err
		}
		batch = append(batch, element)
	}

	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, syntaxError(what, err)
	}
	if err := NoTrailingData(dec, what); err != nil {
		return nil, err
	}
	return batch, nil
}

// NoTrailingData verifies dec holds no further JSON value after the one just
// read. Whitespace is permitted; another token is trailing data.
func NoTrailingData(dec *json.Decoder, what string) error {
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return &Error{What: what, Kind: KindTrailingData}
		}
		return syntaxError(what, err)
	}
	return nil
}
