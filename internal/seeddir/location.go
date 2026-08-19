package seeddir

import (
	"fmt"
	"strings"
)

// SeedString renders one location part the way BasicLocation::SeedString does:
// contig, start and end joined with underscores.
//
// The end is computed the way BasicLocation::new computes it from a
// (contig, begin, strand, length) tuple: begin+length-1 going forward,
// begin-length+1 going back. Anything that is not "+" is treated as the reverse
// strand, as the Perl's ternary does. The one other spelling BasicLocation
// accepts, the old-format "_" strand whose fourth field is the end rather than
// the length, is honored too.
//
// The start is printed as it arrived rather than as a reparsed number: a GTO
// writes it as a string about as often as it writes it as a number.
func SeedString(part []Scalar) (string, error) {
	if len(part) < 4 {
		return "", fmt.Errorf("location has %d fields, want contig, start, strand, length", len(part))
	}

	contig := part[0].String()
	beginText := part[1].String()
	strand := part[2].String()

	begin, ok := part[1].Int()
	if !ok {
		return "", fmt.Errorf("location start %q on contig %s is not a number", beginText, contig)
	}
	length, ok := part[3].Int()
	if !ok {
		return "", fmt.Errorf("location length %q on contig %s is not a number", part[3].String(), contig)
	}

	var end int
	switch strand {
	case "_":
		// Old format: the fourth field is the end, not the length.
		end = length
	case "+":
		end = begin + length - 1
	default:
		end = begin - length + 1
	}

	return fmt.Sprintf("%s_%s_%d", contig, beginText, end), nil
}

// SeedLocation renders a whole multi-part location, parts joined with commas.
func SeedLocation(location [][]Scalar) (string, error) {
	parts := make([]string, 0, len(location))
	for _, part := range location {
		s, err := SeedString(part)
		if err != nil {
			return "", err
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ","), nil
}
