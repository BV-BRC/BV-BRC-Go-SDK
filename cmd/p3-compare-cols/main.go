// Command p3-compare-cols compares two columns in a tab-delimited file.
//
// Usage:
//
//	p3-compare-cols [options] col1 col2
//
// Examples:
//
//	p3-compare-cols 1 2 < data.txt
//	p3-compare-cols genome_id ref_genome_id < data.txt
//	p3-compare-cols --save mismatches.txt col1 col2 < data.txt
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
	inputFile  string
	saveFile   string
)

var rootCmd = &cobra.Command{
	Use:   "p3-compare-cols [options] col1 col2",
	Short: "Compare two columns in a tab-delimited file",
	Long: `Read a tab-delimited file and output a comparison matrix between two columns.
The output is a tab-delimited matrix showing how many times each value in the second
column occurs with each value in the first column. The number of distinct values in
the second column should be small.

The positional parameters are positions (1-based) or names of the two columns.
The first column forms the row keys; the second column forms the matrix columns.

Examples:

  # Compare columns 1 and 2
  p3-compare-cols 1 2 < data.txt

  # Compare named columns
  p3-compare-cols genome_id ref_genome_id < data.txt

  # Save mismatches to a file
  p3-compare-cols --save mismatches.txt col1 col2 < data.txt`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&inputFile, "input", "i", "", "input file (default: stdin)")
	rootCmd.Flags().StringVar(&saveFile, "save", "", "file in which to save mismatches")
}

func run(cmd *cobra.Command, args []string) error {
	col1Spec := args[0]
	col2Spec := args[1]

	// Open input
	var in *os.File
	var err error
	if inputFile != "" && inputFile != "-" {
		in, err = os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("opening input: %w", err)
		}
		defer in.Close()
	} else {
		in = os.Stdin
	}

	reader := cli.NewTabReader(in, true)

	// Read headers
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	// Find the two columns
	col1Idx, err := resolveColumn(col1Spec, headers)
	if err != nil {
		return fmt.Errorf("finding col1: %w", err)
	}
	col2Idx, err := resolveColumn(col2Spec, headers)
	if err != nil {
		return fmt.Errorf("finding col2: %w", err)
	}

	// Set up save file if requested
	var saveOut *os.File
	var saveWriter *cli.TabWriter
	if saveFile != "" {
		saveOut, err = os.Create(saveFile)
		if err != nil {
			return fmt.Errorf("opening save file: %w", err)
		}
		defer saveOut.Close()
		saveWriter = cli.NewTabWriter(saveOut)
		defer saveWriter.Flush()
		// Write headers to save file
		if err := saveWriter.WriteHeaders(headers); err != nil {
			return fmt.Errorf("writing save file headers: %w", err)
		}
	}

	// counts[val1][val2] = count
	counts := make(map[string]map[string]int)
	// values tracks distinct values in col2
	values := make(map[string]bool)
	// rawLines holds the original tab-joined lines for save output
	type rowRecord struct {
		raw  []string
		val1 string
		val2 string
	}

	var rows []rowRecord

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		var val1, val2 string
		if col1Idx >= 0 && col1Idx < len(row) {
			val1 = row[col1Idx]
		}
		if col2Idx >= 0 && col2Idx < len(row) {
			val2 = row[col2Idx]
		}

		if counts[val1] == nil {
			counts[val1] = make(map[string]int)
		}
		counts[val1][val2]++
		values[val2] = true

		if saveWriter != nil {
			rows = append(rows, rowRecord{raw: row, val1: val1, val2: val2})
		}
	}

	// Write mismatches to save file
	if saveWriter != nil {
		for _, r := range rows {
			if r.val1 != r.val2 {
				if err := saveWriter.WriteRow(r.raw...); err != nil {
					return fmt.Errorf("writing save row: %w", err)
				}
			}
		}
	}

	// Sort the col2 values for deterministic column order
	sortedValues := make([]string, 0, len(values))
	for v := range values {
		sortedValues = append(sortedValues, v)
	}
	sort.Strings(sortedValues)

	// Output the matrix
	writer := cli.NewTabWriter(os.Stdout)
	defer writer.Flush()

	// Header row: col1 name, then each col2 value
	headerRow := make([]string, 0, 1+len(sortedValues))
	headerRow = append(headerRow, col1Spec)
	headerRow = append(headerRow, sortedValues...)
	if err := writer.WriteHeaders(headerRow); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	// One row per col1 value
	sortedKeys := make([]string, 0, len(counts))
	for k := range counts {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		subCounts := counts[key]
		outRow := make([]string, 0, 1+len(sortedValues))
		outRow = append(outRow, key)
		for _, v := range sortedValues {
			outRow = append(outRow, strconv.Itoa(subCounts[v]))
		}
		if err := writer.WriteRow(outRow...); err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
	}

	return nil
}

// resolveColumn converts a column spec (name or 1-based index) to a 0-based index.
func resolveColumn(spec string, headers []string) (int, error) {
	// Try to parse as number (1-based)
	if idx, err := strconv.Atoi(spec); err == nil {
		if idx < 1 {
			return 0, fmt.Errorf("column index must be >= 1, got %d", idx)
		}
		return idx - 1, nil
	}

	// Search headers for matching name
	for i, h := range headers {
		if h == spec {
			return i, nil
		}
	}

	return 0, fmt.Errorf("column %q not found in headers", spec)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
