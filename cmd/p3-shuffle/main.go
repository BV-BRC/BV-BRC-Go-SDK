// Command p3-shuffle scrambles the records in a tab-delimited file.
//
// Usage:
//
//	p3-shuffle [options]
//
// Examples:
//
//	p3-shuffle < data.txt
//	p3-shuffle --batchSize 100000 < data.txt
//	p3-shuffle -i input.tbl -o output.tbl
package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	inputFile  string
	outputFile string
	batchSize  int
	verbose    bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-shuffle [options]",
	Short: "Scramble the records in a tab-delimited file",
	Long: `This script reads a file in batches and writes them out in a shuffled order.
It is used to un-sort files for deep learning purposes. The header line is
preserved at the top of the output; only data rows are shuffled.

Examples:

  # Shuffle stdin to stdout
  p3-shuffle < data.txt

  # Shuffle with a custom batch size
  p3-shuffle --batchSize 100000 < data.txt

  # Use explicit input/output files
  p3-shuffle -i input.tbl -o output.tbl`,
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&inputFile, "input", "i", "", "input file (default: stdin)")
	rootCmd.Flags().StringVarP(&outputFile, "output", "o", "", "output file (default: stdout)")
	rootCmd.Flags().IntVar(&batchSize, "batchSize", 500000, "size of each batch to scramble")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "show progress on STDERR")
}

func run(cmd *cobra.Command, args []string) error {
	// Open input
	var inFile *os.File
	var err error
	if inputFile != "" && inputFile != "-" {
		inFile, err = os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("opening input: %w", err)
		}
		defer inFile.Close()
	} else {
		inFile = os.Stdin
	}

	// Open output
	var outFile *os.File
	if outputFile != "" && outputFile != "-" {
		outFile, err = os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("opening output: %w", err)
		}
		defer outFile.Close()
	} else {
		outFile = os.Stdout
	}

	reader := cli.NewTabReader(inFile, true)

	// Read and write headers
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	if headers != nil {
		if err := writer.WriteHeaders(headers); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	batchCount := 0
	batch := make([][]string, 0, batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		batchCount++
		if verbose {
			fmt.Fprintf(os.Stderr, "Shuffling batch %d.\n", batchCount)
		}
		// Fisher-Yates shuffle
		n := len(batch)
		for i := 0; i < n-1; i++ {
			j := i + rand.Intn(n-i)
			batch[i], batch[j] = batch[j], batch[i]
		}
		for _, row := range batch {
			if err := writer.WriteRow(row...); err != nil {
				return fmt.Errorf("writing row: %w", err)
			}
		}
		batch = batch[:0]
		return nil
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	// Flush remaining rows
	if err := flushBatch(); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
