// Command p3-file-filter filters a tab-delimited file against contents of a second file.
//
// Usage:
//
//	p3-file-filter [options] filterFile filterCol1 filterCol2 ...
//
// Examples:
//
//	p3-file-filter --col=feature.role aRoles.tbl feature.role <cRoles.tbl
//	p3-file-filter --reverse --col=genome_id ids.tbl genome_id <data.tbl
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
	reverse bool
	noHead  bool
	inCols  []string
	ioOpts  cli.IOOptions
)

var rootCmd = &cobra.Command{
	Use:   "p3-file-filter [options] filterFile filterCol1 filterCol2 ...",
	Short: "Filter a tab-delimited file against contents of a second file",
	Long: `Filter the standard input using the contents of a filter file.
The output will contain only those rows in the input file whose key value
matches a value from the specified column(s) of the filter file.

To have the output contain only those rows that do NOT match, use --reverse.

The positional parameters are the name of the filter file and the indices
(1-based) or names of the key columns in the filter file. If the filter
column(s) are absent, the value of --col is used for both files.

Multiple key columns are matched one-for-one between the input and filter files.

Examples:

  # Keep only rows whose feature.role appears in aRoles.tbl
  p3-file-filter --col=feature.role aRoles.tbl feature.role <cRoles.tbl

  # Keep rows whose feature.role does NOT appear in aRoles.tbl
  p3-file-filter --reverse --col=feature.role aRoles.tbl feature.role <cRoles.tbl`,
	Args: cobra.MinimumNArgs(1),
	RunE: run,
}

func init() {
	rootCmd.Flags().BoolVarP(&reverse, "reverse", "v", false, "only keep non-matching records")
	rootCmd.Flags().BoolVar(&noHead, "nohead", false, "file has no headers")
	rootCmd.Flags().StringArrayVarP(&inCols, "col", "c", []string{"0"}, "input file key column(s) (1-based index or name)")
	cli.AddIOFlags(rootCmd, &ioOpts)
}

// resolveColsBySpec resolves a list of column specs (name or 1-based index) to 0-based indices.
// When noHead is true, specs must be numeric (1-based).
func resolveColsBySpec(specs []string, headers []string, context string) ([]int, error) {
	indices := make([]int, 0, len(specs))
	for _, spec := range specs {
		// Try numeric first
		idx := 0
		isNum := false
		n := 0
		for _, ch := range spec {
			if ch >= '0' && ch <= '9' {
				n = n*10 + int(ch-'0')
				isNum = true
			} else {
				isNum = false
				break
			}
		}
		if isNum && len(spec) > 0 {
			if n < 1 {
				return nil, fmt.Errorf("%s: column index must be >= 1, got %d", context, n)
			}
			idx = n - 1 // convert to 0-based
		} else {
			// Search headers
			if headers == nil {
				return nil, fmt.Errorf("%s: cannot use column name %q when --nohead is set", context, spec)
			}
			found := false
			for i, h := range headers {
				if h == spec {
					idx = i
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("%s: column %q not found in headers", context, spec)
			}
		}
		indices = append(indices, idx)
	}
	return indices, nil
}

// getKey extracts and joins the values at the given 0-based column indices from a row.
func getKey(row []string, indices []int) string {
	parts := make([]string, 0, len(indices))
	for _, idx := range indices {
		if idx < len(row) {
			parts = append(parts, row[idx])
		} else {
			parts = append(parts, "")
		}
	}
	return strings.Join(parts, "\t")
}

func run(cmd *cobra.Command, args []string) error {
	// First positional arg is the filter file; remaining are filter columns.
	filterFile := args[0]
	filterColSpecs := args[1:]

	// If no filter columns specified, use the same as input columns.
	if len(filterColSpecs) == 0 {
		filterColSpecs = inCols
	}

	// Validate counts match.
	if len(inCols) != len(filterColSpecs) {
		return fmt.Errorf("filter column count (%d) does not match key column count (%d)", len(filterColSpecs), len(inCols))
	}

	// Validate filter file exists.
	if _, err := os.Stat(filterFile); err != nil {
		return fmt.Errorf("filter file %q invalid or not found", filterFile)
	}

	// Open filter file.
	fh, err := os.Open(filterFile)
	if err != nil {
		return fmt.Errorf("could not open filter file: %w", err)
	}
	defer fh.Close()

	filterReader := cli.NewTabReader(fh, !noHead)

	// Read filter headers.
	filterHeaders, err := filterReader.Headers()
	if err != nil {
		return fmt.Errorf("reading filter file headers: %w", err)
	}

	// Resolve filter column indices.
	filterColIndices, err := resolveColsBySpec(filterColSpecs, filterHeaders, "filter file")
	if err != nil {
		return err
	}

	// Build the filter hash.
	filterSet := make(map[string]bool)
	for {
		row, err := filterReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading filter file: %w", err)
		}
		key := getKey(row, filterColIndices)
		filterSet[key] = true
	}
	fh.Close()

	// Open the input.
	input, err := cli.OpenInput(ioOpts.Input)
	if err != nil {
		return fmt.Errorf("opening input: %w", err)
	}
	defer input.Close()

	inputReader := cli.NewTabReader(input, !noHead)

	// Read input headers.
	inputHeaders, err := inputReader.Headers()
	if err != nil {
		return fmt.Errorf("reading input headers: %w", err)
	}

	// Resolve input column indices.
	inColIndices, err := resolveColsBySpec(inCols, inputHeaders, "input file")
	if err != nil {
		return err
	}

	// Open output.
	output, err := cli.OpenOutput(ioOpts.Output)
	if err != nil {
		return fmt.Errorf("opening output: %w", err)
	}
	defer output.Close()

	writer := cli.NewTabWriter(output)
	defer writer.Flush()

	// Write output headers.
	if inputHeaders != nil {
		if err := writer.WriteHeaders(inputHeaders); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	// Process input rows.
	for {
		row, err := inputReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}

		key := getKey(row, inColIndices)
		inFilter := filterSet[key]

		// XOR: keep if (inFilter AND NOT reverse) OR (NOT inFilter AND reverse)
		if inFilter != reverse {
			if err := writer.WriteRow(row...); err != nil {
				return fmt.Errorf("writing output: %w", err)
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
