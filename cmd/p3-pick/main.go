// Command p3-pick randomly selects a specified number of rows from a tab-delimited file.
//
// Usage:
//
//	p3-pick [options] count
//
// Examples:
//
//	p3-pick 100 < data.txt
//	p3-pick --nohead 50 < data.txt
//	p3-pick -i data.txt 10
package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	noHead    bool
	inputFile string
)

var rootCmd = &cobra.Command{
	Use:   "p3-pick [options] count",
	Short: "Randomly select rows from a tab-delimited file",
	Long: `This command randomly selects the specified number of rows from the input
and copies them to the output. The header line (if present) is always included.

Examples:

  # Pick 100 random rows
  p3-pick 100 < data.txt

  # Pick 50 rows from a headerless file
  p3-pick --nohead 50 < data.txt

  # Pick 10 rows from a specific file
  p3-pick -i data.txt 10`,
	Args: cobra.ExactArgs(1),
	RunE: run,
}

func init() {
	rootCmd.Flags().BoolVar(&noHead, "nohead", false, "input file has no header row")
	rootCmd.Flags().StringVarP(&inputFile, "input", "i", "", "input file (default: stdin)")
}

func run(cmd *cobra.Command, args []string) error {
	// Parse the count argument
	countStr := args[0]
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return fmt.Errorf("count not numeric: %s", countStr)
	}
	if count <= 0 {
		return fmt.Errorf("count must be a positive integer, got %d", count)
	}

	// Open input
	var in *os.File
	if inputFile != "" && inputFile != "-" {
		in, err = os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("opening input: %w", err)
		}
		defer in.Close()
	} else {
		in = os.Stdin
	}

	reader := cli.NewTabReader(in, !noHead)
	writer := cli.NewTabWriter(os.Stdout)
	defer writer.Flush()

	// Read and output headers if present
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}
	if headers != nil {
		if err := writer.WriteHeaders(headers); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	// Read all data rows into memory
	var rows [][]string
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}
		rows = append(rows, row)
	}

	nlines := len(rows)

	// If we have more rows than requested, randomly select 'count' of them.
	// Use a partial Fisher-Yates shuffle (only 'count' iterations) to pick
	// 'count' indices, then sort them to preserve original order.
	if nlines > count {
		// Build index slice
		index := make([]int, nlines)
		for i := range index {
			index[i] = i
		}

		// Partial Fisher-Yates: swap count random elements to the front
		for i := 0; i < count; i++ {
			j := i + rand.Intn(nlines-i)
			index[i], index[j] = index[j], index[i]
		}

		// Take the first 'count' indices and sort them
		selected := index[:count]
		sort.Ints(selected)

		// Output selected rows
		for _, idx := range selected {
			if err := writer.WriteRow(rows[idx]...); err != nil {
				return fmt.Errorf("writing row: %w", err)
			}
		}
	} else {
		// Output all rows
		for _, row := range rows {
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
