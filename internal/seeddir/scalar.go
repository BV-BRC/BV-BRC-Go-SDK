package seeddir

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Scalar is a JSON value that gets written back out as text.
//
// The genome typed object is not consistent about which of its scalars are
// numbers and which are strings -- a location's start is usually a string and
// its length a number, in the same tuple -- and the SEED directory is plain
// text, so the distinction only matters for how the value is rendered. Keeping
// the raw JSON and unquoting it at the end renders every one of them the way
// the input had it, which is what Perl's untyped scalars did for free.
type Scalar json.RawMessage

// UnmarshalJSON keeps the value's own bytes. A defined type does not inherit
// json.RawMessage's methods, so without this the decoder would try to read a
// number or an object into a byte slice.
func (s *Scalar) UnmarshalJSON(data []byte) error {
	*s = append((*s)[:0], data...)
	return nil
}

// String renders the value: a JSON string loses its quotes and its escapes,
// null and a missing value become empty, and everything else is passed through
// as it was written.
func (s Scalar) String() string {
	text := strings.TrimSpace(string(s))
	if text == "" || text == "null" {
		return ""
	}
	var str string
	if json.Unmarshal([]byte(text), &str) == nil {
		return str
	}
	return text
}

// Truthy reports whether Perl would have considered the value true, which is
// what several of write_seed_dir's `if` tests come down to: not missing, not
// null, not the empty string, not "0" and not a zero number.
func (s Scalar) Truthy() bool {
	str := s.String()
	if str == "" || str == "0" {
		return false
	}
	if f, err := strconv.ParseFloat(str, 64); err == nil && f == 0 {
		return false
	}
	return true
}

// Int reads the value as an integer, tolerating the numeric strings the GTO
// uses for coordinates and a float that happens to be whole.
func (s Scalar) Int() (int, bool) {
	str := s.String()
	if n, err := strconv.Atoi(str); err == nil {
		return n, true
	}
	if f, err := strconv.ParseFloat(str, 64); err == nil {
		return int(f), true
	}
	return 0, false
}
