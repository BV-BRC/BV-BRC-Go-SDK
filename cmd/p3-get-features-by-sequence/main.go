// Command p3-get-features-by-sequence finds BV-BRC features by exact sequence match.
//
// Takes DNA or protein sequences from stdin (tab-delimited or FASTA) and
// returns the patric_id of every feature whose stored sequence has the same
// MD5 hash.
//
// The Perl implementation translated DNA to protein via genetic codes 4 and 11
// and looked up by aa_sequence_md5 because na_sequence_md5 was not yet indexed.
// Both fields are now indexed on feature records, so DNA sequences can be looked
// up directly without translation.
//
// Usage:
//
//	p3-get-features-by-sequence [options]
//
// Examples:
//
//	# Find features matching protein sequences from a tab-delimited file
//	p3-get-features-by-sequence --protein < seqs.tbl
//
//	# Find features matching DNA sequences (default)
//	p3-get-features-by-sequence < seqs.tbl
//
//	# Find features from a FASTA file
//	p3-get-features-by-sequence --fasta < seqs.fasta
package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	colOpts    cli.ColOptions
	ioOpts     cli.IOOptions
	proteinMode bool
	fastaMode   bool
	debug       bool
)

var rootCmd = &cobra.Command{
	Use:          "p3-get-features-by-sequence",
	Short:        "Find BV-BRC features by exact sequence match",
	SilenceUsage: true,
	Long: `This command reads DNA or protein sequences from the standard input and
finds BV-BRC features whose stored sequence exactly matches each input.

Lookup is performed via MD5 hash against the na_sequence_md5 (DNA) or
aa_sequence_md5 (protein) field on feature records, so only exact full-length
matches are returned.

Tab-delimited input: the sequence column is selected with --col (default: last).
FASTA input: use --fasta; output columns are fasta.id, fasta.comment, sequence.patric_id.

Examples:

  # DNA sequences from a tab-delimited file (default)
  p3-get-features-by-sequence < seqs.tbl

  # Protein sequences
  p3-get-features-by-sequence --protein < seqs.tbl

  # FASTA input
  p3-get-features-by-sequence --fasta < seqs.fasta`,
	RunE: run,
}

func init() {
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVar(&proteinMode, "protein", false, "input contains protein (amino acid) sequences")
	rootCmd.Flags().BoolVar(&fastaMode, "fasta", false, "input is a FASTA file")
	rootCmd.Flags().BoolVar(&debug, "debug", false, "enable debug output")
}

// seqEntry holds a sequence and its associated row data for output.
type seqEntry struct {
	seq string
	row []string // nil for FASTA input; holds input row for tab input
	id  string   // FASTA sequence ID
	cmt string   // FASTA comment/description
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	token, err := auth.GetToken()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}

	clientOpts := []api.ClientOption{}
	if token != nil {
		clientOpts = append(clientOpts, api.WithToken(token))
	}
	if debug {
		clientOpts = append(clientOpts, api.WithDebug(true))
	}
	client := api.NewClient(clientOpts...)

	inFile, err := cli.OpenInput(ioOpts.Input)
	if err != nil {
		return fmt.Errorf("opening input: %w", err)
	}
	defer inFile.Close()

	outFile, err := cli.OpenOutput(ioOpts.Output)
	if err != nil {
		return fmt.Errorf("opening output: %w", err)
	}
	defer outFile.Close()

	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// md5Field selects the correct feature field for lookup.
	md5Field := "na_sequence_md5"
	if proteinMode {
		md5Field = "aa_sequence_md5"
	}

	var entries []seqEntry
	var inputHeaders []string

	if fastaMode {
		entries, err = readFASTA(inFile)
		if err != nil {
			return fmt.Errorf("reading FASTA: %w", err)
		}
		if !colOpts.NoHead {
			if err := writer.WriteHeaders([]string{"fasta.id", "fasta.comment", "sequence.patric_id"}); err != nil {
				return err
			}
		}
	} else {
		reader := cli.NewTabReader(inFile, !colOpts.NoHead)
		inputHeaders, err = reader.Headers()
		if err != nil {
			return fmt.Errorf("reading headers: %w", err)
		}
		// Default to last column when no --col specified (matches Perl col_options default).
		col := colOpts.Col
		if col == "" {
			col = "0" // cli.TabReader treats "0" as last column
		}
		keyCol, err := reader.FindColumn(col)
		if err != nil {
			return fmt.Errorf("finding sequence column: %w", err)
		}
		// Slurp all rows.
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("reading input: %w", err)
			}
			idx := keyCol
			if idx < 0 {
				idx = len(row) - 1 // -1 means last column
			}
			if idx >= len(row) {
				continue
			}
			seq := strings.TrimSpace(row[idx])
			if seq == "" {
				continue
			}
			entries = append(entries, seqEntry{seq: seq, row: row})
		}
		if !colOpts.NoHead {
			headers := append(append([]string{}, inputHeaders...), "sequence.patric_id")
			if err := writer.WriteHeaders(headers); err != nil {
				return err
			}
		}
	}

	// Process each sequence individually (same as Perl: poor bulk performance,
	// but exact semantics — one query per sequence).
	for _, entry := range entries {
		seq := entry.seq
		// Normalise: uppercase for protein, lowercase for DNA (Solr is case-sensitive
		// on the stored sequences; match whichever case the input uses).
		hash := fmt.Sprintf("%x", md5.Sum([]byte(seq)))

		q := api.NewQuery().Eq(md5Field, hash).Select("patric_id")
		results, err := client.Query(ctx, "feature", q)
		if err != nil {
			return fmt.Errorf("querying %s=%s: %w", md5Field, hash, err)
		}

		for _, r := range results {
			patricID, _ := r["patric_id"].(string)
			if patricID == "" {
				continue
			}
			if fastaMode {
				if err := writer.WriteRow(entry.id, entry.cmt, patricID); err != nil {
					return err
				}
			} else {
				if err := writer.WriteRow(append(entry.row, patricID)...); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// readFASTA reads a FASTA file and returns entries. Sequences spanning
// multiple lines are concatenated. The comment is everything after the
// first space on the header line.
func readFASTA(r io.Reader) ([]seqEntry, error) {
	var entries []seqEntry
	var cur *seqEntry
	var seqBuf strings.Builder

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ">") {
			if cur != nil {
				cur.seq = seqBuf.String()
				entries = append(entries, *cur)
			}
			seqBuf.Reset()
			header := line[1:]
			id, cmt, _ := strings.Cut(header, " ")
			cur = &seqEntry{id: id, cmt: cmt}
		} else if cur != nil {
			seqBuf.WriteString(strings.TrimSpace(line))
		}
	}
	if cur != nil {
		cur.seq = seqBuf.String()
		entries = append(entries, *cur)
	}
	return entries, scanner.Err()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
