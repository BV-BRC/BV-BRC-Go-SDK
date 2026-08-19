// Package seq reads and writes FASTA, matching the behaviour of the Perl
// gjoseqlib the rast-* tools have always used.
package seq

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Record is one FASTA entry.
type Record struct {
	ID   string
	Desc string
	Seq  string
}

// ReadFasta reads every record from r.
//
// It follows gjoseqlib::next_fasta: the id is the first whitespace-delimited
// word of the title and the rest is the description, spaces, tabs and carriage
// returns are stripped out of the sequence lines, and a header with no sequence
// under it yields no record.
func ReadFasta(r io.Reader) ([]Record, error) {
	var (
		records []Record
		cur     Record
		seq     strings.Builder
		open    bool
	)

	flush := func() {
		if open && seq.Len() > 0 {
			cur.Seq = seq.String()
			records = append(records, cur)
		}
		seq.Reset()
	}

	sc := bufio.NewScanner(r)
	// Contig lines are usually wrapped, but nothing requires them to be: a
	// whole chromosome can arrive as one line.
	sc.Buffer(make([]byte, 0, 64*1024), 512*1024*1024)

	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if strings.HasPrefix(line, ">") {
			flush()
			id, desc := parseTitle(line)
			cur = Record{ID: id, Desc: desc}
			open = true
			continue
		}
		if !open {
			// Leading junk before the first header, as gjoseqlib does,
			// is ignored rather than being an error.
			continue
		}
		seq.WriteString(strings.NewReplacer(" ", "", "\t", "").Replace(line))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading fasta: %w", err)
	}
	flush()

	return records, nil
}

// parseTitle splits a ">id description" line, mirroring
// gjoseqlib::parse_fasta_title.
func parseTitle(line string) (id, desc string) {
	rest := strings.TrimSpace(strings.TrimPrefix(line, ">"))
	if rest == "" {
		return "", ""
	}
	if i := strings.IndexAny(rest, " \t"); i >= 0 {
		return rest[:i], strings.TrimSpace(rest[i+1:])
	}
	return rest, ""
}

// LineWidth is how many residues gjoseqlib::print_seq_as_fasta puts on a line.
const LineWidth = 60

// WriteFasta writes one record, wrapped at LineWidth. An empty description is
// omitted from the title, as the Perl does.
func WriteFasta(w io.Writer, rec Record) error {
	title := ">" + rec.ID
	if strings.TrimSpace(rec.Desc) != "" {
		title += " " + rec.Desc
	}
	if _, err := fmt.Fprintln(w, title); err != nil {
		return err
	}

	// A record with no residues prints its title and nothing else. This is
	// the one deliberate divergence from print_seq_as_fasta, whose
	// join-then-newline emits a blank line for an empty sequence -- which no
	// reader wants, and which only an empty contig could produce.
	for i := 0; i < len(rec.Seq); i += LineWidth {
		end := i + LineWidth
		if end > len(rec.Seq) {
			end = len(rec.Seq)
		}
		if _, err := fmt.Fprintln(w, rec.Seq[i:end]); err != nil {
			return err
		}
	}
	return nil
}
