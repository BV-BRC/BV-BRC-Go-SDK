// Command p3-collate outputs the first N rows for each value of a key column.
//
// Usage:
//
//	p3-collate N [options]
//
// Examples:
//
//	p3-collate 3 < data.txt
//	p3-collate 3 -c species < data.txt
//	p3-collate 5 --col genome_id < data.txt
package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	colOpts cli.ColOptions
	ioOpts  cli.IOOptions
)

var rootCmd = &cobra.Command{
	Use:   "p3-collate N [options]",
	Short: "Output the first N rows for each value of a key column",
	Long: `This command reads a tab-delimited file and outputs the first N rows
for each distinct value found in the key column. For example, if you have a set
of genomes sorted by quality and ask for a 3-row sample based on the species
column, it will extract the 3 best-quality genomes for each species.

The positional parameter N is the number of rows to extract per key value
(default: 1). Results are output sorted by key value.

Examples:

  # Output first 3 rows per key value (last column)
  p3-collate 3 < data.txt

  # Output first 3 rows per species (named column)
  p3-collate 3 -c species < data.txt

  # Output first 5 rows per genome_id
  p3-collate 5 --col genome_id < data.txt`,
	Args: cobra.MaximumNArgs(1),
	RunE: run,
}

func init() {
	cli.AddColFlags(rootCmd, &colOpts, 0)
	cli.AddIOFlags(rootCmd, &ioOpts)
}

func run(cmd *cobra.Command, args []string) error {
	// Get N from positional argument (default 1)
	N := 1
	if len(args) > 0 {
		var err error
		N, err = strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("invalid row count %q: %w", args[0], err)
		}
	}
	if N < 1 {
		return fmt.Errorf("collation specifies no output")
	}

	// Open input
	var inFile *os.File
	var err error
	if ioOpts.Input != "" && ioOpts.Input != "-" {
		inFile, err = os.Open(ioOpts.Input)
		if err != nil {
			return fmt.Errorf("opening input: %w", err)
		}
		defer inFile.Close()
	} else {
		inFile = os.Stdin
	}

	// Open output
	var outFile *os.File
	if ioOpts.Output != "" && ioOpts.Output != "-" {
		outFile, err = os.Create(ioOpts.Output)
		if err != nil {
			return fmt.Errorf("opening output: %w", err)
		}
		defer outFile.Close()
	} else {
		outFile = os.Stdout
	}

	reader := cli.NewTabReader(inFile, !colOpts.NoHead)

	// Read headers
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	// Find key column
	keyCol, err := reader.FindColumn(colOpts.Col)
	if err != nil {
		return fmt.Errorf("finding column: %w", err)
	}

	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Write headers if present
	if headers != nil {
		if err := writer.WriteHeaders(headers); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	// groups maps key value -> list of rows (up to N)
	groups := make(map[string][][]string)
	// keyOrder tracks insertion order for deterministic output
	var keyOrder []string

	// Read all rows and build groups
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		// Extract key value
		var key string
		if keyCol < 0 {
			// Last column
			if len(row) > 0 {
				key = row[len(row)-1]
			}
		} else if keyCol < len(row) {
			key = row[keyCol]
		}

		group, exists := groups[key]
		if !exists {
			groups[key] = [][]string{row}
			keyOrder = append(keyOrder, key)
		} else if len(group) < N {
			groups[key] = append(group, row)
		}
	}

	// Sort keys for output (matches Perl's sort keys behavior)
	sort.Strings(keyOrder)

	// Write output rows
	for _, key := range keyOrder {
		for _, row := range groups[key] {
			if err := writer.WriteRow(row...); err != nil {
				return fmt.Errorf("writing row: %w", err)
			}
		}
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
