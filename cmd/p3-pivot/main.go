// Command p3-pivot performs a pivot analysis of two columns.
//
// Usage:
//
//	p3-pivot [options] col1 col2
//
// Examples:
//
//	p3-pivot 1 2 < data.txt
//	p3-pivot genome_id feature_type < data.txt
package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var ioOpts cli.IOOptions

var rootCmd = &cobra.Command{
	Use:   "p3-pivot [options] col1 col2",
	Short: "Create a pivot analysis of two columns",
	Long: `This script analyzes the frequency distribution of the values in one column
compared to the values in the other. The output is a five-column table:
  (0) value in the first column
  (1) value in the second column
  (2) number of times the pair occurred
  (3) percent of rows containing the first column's value that had the second column's value
  (4) percent of rows containing the second column's value that had the first column's value

The positional parameters are the column indices (1-based) or names of the two columns.

Examples:

  # Pivot on columns 1 and 2
  p3-pivot 1 2 < data.txt

  # Pivot on named columns
  p3-pivot genome_id feature_type < data.txt`,
	Args: cobra.ExactArgs(2),
	RunE: run,
}

func init() {
	cli.AddIOFlags(rootCmd, &ioOpts)
}

// nearest rounds x to the nearest multiple of unit (like Math::Round::nearest).
func nearest(unit, x float64) float64 {
	if unit == 0 {
		return x
	}
	return math.Round(x/unit) * unit
}

func run(cmd *cobra.Command, args []string) error {
	col1Spec := args[0]
	col2Spec := args[1]

	// Open input
	inFile, err := cli.OpenInput(ioOpts.Input)
	if err != nil {
		return fmt.Errorf("opening input: %w", err)
	}
	defer inFile.Close()

	// Open output
	outFile, err := cli.OpenOutput(ioOpts.Output)
	if err != nil {
		return fmt.Errorf("opening output: %w", err)
	}
	defer outFile.Close()

	reader := cli.NewTabReader(inFile, true)

	// Read headers
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}
	_ = headers

	// Find the two columns
	col1Idx, err := reader.FindColumn(col1Spec)
	if err != nil {
		return fmt.Errorf("finding column 1 %q: %w", col1Spec, err)
	}
	col2Idx, err := reader.FindColumn(col2Spec)
	if err != nil {
		return fmt.Errorf("finding column 2 %q: %w", col2Spec, err)
	}

	// Resolve "last column" index (-1) into an actual index using header length
	if col1Idx < 0 && len(headers) > 0 {
		col1Idx = len(headers) - 1
	}
	if col2Idx < 0 && len(headers) > 0 {
		col2Idx = len(headers) - 1
	}

	// Determine output column names: use header name if available, else the spec
	col1Name := col1Spec
	col2Name := col2Spec
	if col1Idx >= 0 && col1Idx < len(headers) {
		col1Name = headers[col1Idx]
	}
	if col2Idx >= 0 && col2Idx < len(headers) {
		col2Name = headers[col2Idx]
	}

	// Data structures mirroring the Perl script
	// counts[val1][val2] = count of pairs
	counts := make(map[string]map[string]int)
	// values[val2] = total rows containing val2
	values := make(map[string]int)
	// keys[val1] = total rows containing val1
	keys := make(map[string]int)

	// Helper to get a field from a row by index (-1 = last)
	getField := func(row []string, idx int) string {
		if idx < 0 {
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

	// Read all rows
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		val1 := getField(row, col1Idx)
		val2 := getField(row, col2Idx)

		if counts[val1] == nil {
			counts[val1] = make(map[string]int)
		}
		counts[val1][val2]++
		values[val2]++
		keys[val1]++
	}

	// Write output
	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Header row
	if err := writer.WriteHeaders([]string{col1Name, col2Name, "count", "%" + col1Name, "%" + col2Name}); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	// Sort keys for deterministic output
	sortedKeys := make([]string, 0, len(counts))
	for k := range counts {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		subCounts := counts[key]

		// Sort values for deterministic output
		sortedValues := make([]string, 0, len(subCounts))
		for v := range subCounts {
			sortedValues = append(sortedValues, v)
		}
		sort.Strings(sortedValues)

		for _, value := range sortedValues {
			count := subCounts[value]
			pct := float64(count) * 100.0
			pct1 := nearest(0.01, pct/float64(keys[key]))
			pct2 := nearest(0.01, pct/float64(values[value]))

			if err := writer.WriteRow(
				key,
				value,
				strconv.Itoa(count),
				strconv.FormatFloat(pct1, 'f', -1, 64),
				strconv.FormatFloat(pct2, 'f', -1, 64),
			); err != nil {
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
