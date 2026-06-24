// Command p3-tbl-to-fasta converts a tab-delimited file to FASTA format.
//
// Usage:
//
//	p3-tbl-to-fasta [options] idCol seqCol
//
// Examples:
//
//	p3-tbl-to-fasta 1 2 < data.txt
//	p3-tbl-to-fasta genome_id sequence < data.txt
//	p3-tbl-to-fasta --comment description 1 2 < data.txt
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	ioOpts      cli.IOOptions
	commentCols []string
	noHead      bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-tbl-to-fasta [options] idCol seqCol",
	Short: "Convert a tab-delimited file to FASTA format",
	Long: `This script converts a tab-delimited file containing sequence data to a FASTA file.
The tab-delimited file is taken from the standard input; the FASTA file will be the standard output.

The positional parameters are the index (1-based) or name of the column containing the
sequence IDs and the index or name of the column containing the sequences.

Examples:

  # Convert using column indices
  p3-tbl-to-fasta 1 2 < data.txt

  # Convert using column names
  p3-tbl-to-fasta genome_id sequence < data.txt

  # Include a comment column
  p3-tbl-to-fasta --comment description genome_id sequence < data.txt

  # Multiple comment columns (concatenated with tab)
  p3-tbl-to-fasta --comment description --comment notes genome_id sequence < data.txt`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().StringArrayVarP(&commentCols, "comment", "k", nil,
		"index (1-based) or name of comment column (can be repeated)")
	rootCmd.Flags().BoolVar(&noHead, "nohead", false, "input has no headers")
}

func run(cmd *cobra.Command, args []string) error {
	idColSpec := args[0]
	seqColSpec := args[1]

	// Open input
	input, err := cli.OpenInput(ioOpts.Input)
	if err != nil {
		return fmt.Errorf("opening input: %w", err)
	}
	defer input.Close()

	// Open output
	output, err := cli.OpenOutput(ioOpts.Output)
	if err != nil {
		return fmt.Errorf("opening output: %w", err)
	}
	defer output.Close()

	reader := cli.NewTabReader(input, !noHead)

	// Read headers
	_, err = reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	// Find the id and sequence columns
	idCol, err := reader.FindColumn(idColSpec)
	if err != nil {
		return fmt.Errorf("finding id column: %w", err)
	}

	seqCol, err := reader.FindColumn(seqColSpec)
	if err != nil {
		return fmt.Errorf("finding sequence column: %w", err)
	}

	// Find comment columns
	commentIdxs := make([]int, 0, len(commentCols))
	for _, spec := range commentCols {
		idx, err := reader.FindColumn(spec)
		if err != nil {
			return fmt.Errorf("finding comment column %q: %w", spec, err)
		}
		commentIdxs = append(commentIdxs, idx)
	}

	// Build the ordered list of all columns we need (id, seq, comments...)
	// We read each row fully, then index into it.

	// Loop through the input, creating FASTA output
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		id := colValue(row, idCol)
		seq := colValue(row, seqCol)

		// Build comment string from all comment columns
		comments := make([]string, 0, len(commentIdxs))
		for _, idx := range commentIdxs {
			comments = append(comments, colValue(row, idx))
		}
		comment := strings.Join(comments, "\t")

		// Write FASTA header: ">id comment\n" (space before comment even if empty,
		// matching Perl: print ">$id $comment\n")
		if _, err := fmt.Fprintf(output, ">%s %s\n", id, comment); err != nil {
			return fmt.Errorf("writing FASTA header: %w", err)
		}

		// Write sequence in 60-character chunks
		for len(seq) > 0 {
			chunkLen := 60
			if chunkLen > len(seq) {
				chunkLen = len(seq)
			}
			if _, err := fmt.Fprintf(output, "%s\n", seq[:chunkLen]); err != nil {
				return fmt.Errorf("writing sequence: %w", err)
			}
			seq = seq[chunkLen:]
		}
	}

	return nil
}

// colValue returns the value at a given 0-based column index from a row.
// If idx is -1 (last column sentinel from FindColumn), returns the last element.
func colValue(row []string, idx int) string {
	if idx < 0 {
		// Last column
		if len(row) > 0 {
			return row[len(row)-1]
		}
		return ""
	}
	if idx < len(row) {
		return row[idx]
	}
	return ""
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
