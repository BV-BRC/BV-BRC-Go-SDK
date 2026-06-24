// Command p3-stats computes statistics for a numeric column grouped by a key column.
//
// Usage:
//
//	p3-stats [options] statCol
//
// Examples:
//
//	p3-stats value_col < data.txt
//	p3-stats --col genome_id value_col < data.txt
//	p3-stats --col none value_col < data.txt
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

var (
	col    string
	noHead bool
	ioOpts cli.IOOptions
)

var rootCmd = &cobra.Command{
	Use:   "p3-stats [options] statCol",
	Short: "Compute statistics for a numeric column grouped by a key column",
	Long: `This script divides the input into groups by the key column and analyzes
the values found in a second column (specified by the parameter).
It outputs the mean, standard deviation, minimum, maximum, and count.

Examples:

  # Compute stats on value_col, grouping by last column
  p3-stats value_col < data.txt

  # Compute stats on value_col, grouping by genome_id column
  p3-stats --col genome_id value_col < data.txt

  # Compute stats on value_col without grouping (all rows in one group)
  p3-stats --col none value_col < data.txt

  # Input file has no header; use 1-based column indices
  p3-stats --nohead --col 1 2 < data.txt`,
	Args: cobra.ExactArgs(1),
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&col, "col", "c", "0",
		`grouping column (1-based index or name, 0=last, "none"=no grouping)`)
	rootCmd.Flags().BoolVar(&noHead, "nohead", false, "input has no headers")
	rootCmd.Flags().StringVarP(&ioOpts.Input, "input", "i", "", "input file (default: stdin)")
	rootCmd.Flags().StringVarP(&ioOpts.Output, "output", "o", "", "output file (default: stdout)")
}

// tally holds running statistics for a group: [count, sum, min, max, sumOfSquares]
type tally struct {
	count   int
	sum     float64
	min     float64
	max     float64
	sumSqrs float64
}

func run(cmd *cobra.Command, args []string) error {
	statColSpec := args[0]

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

	reader := cli.NewTabReader(inFile, !noHead)
	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Read headers
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	// Determine key column and stat column
	// keyCol == -2 means "none" (no grouping, put all in one group)
	// keyCol == -1 means last column
	var keyCol int
	colName := "key"
	noneGrouping := (col == "none")

	if noneGrouping {
		keyCol = -2
	} else if headers == nil {
		// No header mode: col and statColSpec are 1-based indices
		if col == "0" {
			keyCol = -1 // last column
		} else {
			idx, err := strconv.Atoi(col)
			if err != nil || idx < 1 {
				return fmt.Errorf("invalid column index %q", col)
			}
			keyCol = idx - 1
		}
	} else {
		// Has headers: FindColumn handles name or 1-based index
		keyCol, err = reader.FindColumn(col)
		if err != nil {
			return fmt.Errorf("finding key column: %w", err)
		}
		if keyCol >= 0 && keyCol < len(headers) {
			colName = headers[keyCol]
		}
	}

	// Find stat column
	var statCol int
	if headers == nil {
		// No header: statColSpec is a 1-based index
		idx, err := strconv.Atoi(statColSpec)
		if err != nil || idx < 1 {
			return fmt.Errorf("invalid stat column index %q", statColSpec)
		}
		statCol = idx - 1
	} else {
		statCol, err = reader.FindColumn(statColSpec)
		if err != nil {
			return fmt.Errorf("finding stat column: %w", err)
		}
	}

	// Write output header (if input had headers)
	if headers != nil {
		outHeaders := []string{colName, "count", "average", "min", "max", "stdev"}
		if err := writer.WriteHeaders(outHeaders); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	// Tally map: key -> tally
	tallyMap := make(map[string]*tally)
	// Preserve insertion order for sorting (we'll sort keys at output time)
	// Actually we sort alphabetically like Perl's "sort keys %tally"

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		// Get the value to analyze
		var valStr string
		if statCol < 0 {
			// last column
			if len(row) > 0 {
				valStr = row[len(row)-1]
			}
		} else if statCol < len(row) {
			valStr = row[statCol]
		}

		value, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			// Skip non-numeric values (or treat as 0 like Perl would)
			continue
		}

		// Get the key
		var key string
		if keyCol == -2 {
			// none grouping
			key = "all"
		} else if keyCol < 0 {
			// last column
			if len(row) > 0 {
				key = row[len(row)-1]
			}
		} else if keyCol < len(row) {
			key = row[keyCol]
		}

		t, exists := tallyMap[key]
		if !exists {
			tallyMap[key] = &tally{
				count:   1,
				sum:     value,
				min:     value,
				max:     value,
				sumSqrs: value * value,
			}
		} else {
			t.count++
			t.sum += value
			if value < t.min {
				t.min = value
			}
			if value > t.max {
				t.max = value
			}
			t.sumSqrs += value * value
		}
	}

	// Sort keys alphabetically (matching Perl's sort keys %tally)
	keys := make([]string, 0, len(tallyMap))
	for k := range tallyMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Output results
	for _, key := range keys {
		t := tallyMap[key]
		avg := t.sum / float64(t.count)
		stdev := math.Sqrt(t.sumSqrs/float64(t.count) - avg*avg)

		if err := writer.WriteRow(
			key,
			strconv.Itoa(t.count),
			strconv.FormatFloat(avg, 'g', -1, 64),
			strconv.FormatFloat(t.min, 'g', -1, 64),
			strconv.FormatFloat(t.max, 'g', -1, 64),
			strconv.FormatFloat(stdev, 'g', -1, 64),
		); err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
