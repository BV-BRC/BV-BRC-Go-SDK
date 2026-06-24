// Command p3-pick-by-class randomly selects rows so each class is equally represented.
//
// Usage:
//
//	p3-pick-by-class [options]
//
// Examples:
//
//	p3-pick-by-class < data.txt
//	p3-pick-by-class --col class_col < data.txt
//	p3-pick-by-class --fuzz 1.5 --max 1000 < data.txt
package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	colOpts cli.ColOptions
	ioOpts  cli.IOOptions
	verbose bool
	fuzz    float64
	maxOut  int
)

var rootCmd = &cobra.Command{
	Use:   "p3-pick-by-class [options]",
	Short: "Pick records for classification training",
	Long: `This script reads an entire file into memory and collates records by the key
column value. It then outputs randomly-selected records so that the number of
records with each value is roughly the same.

The --fuzz flag controls the maximum allowed ratio between the largest and
smallest class (default 1.2). A fuzz of 1.0 means all classes get exactly the
same number of records; 2.0 allows up to double the smallest class.

Examples:

  # Balance classes using the last column
  p3-pick-by-class < data.txt

  # Balance classes using a named column
  p3-pick-by-class --col class_label < data.txt

  # Allow up to 50% more records than the smallest class
  p3-pick-by-class --fuzz 1.5 < data.txt

  # Limit total output to 1000 lines
  p3-pick-by-class --max 1000 < data.txt`,
	RunE: run,
}

func init() {
	cli.AddColFlags(rootCmd, &colOpts, 1000)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "show progress on stderr")
	rootCmd.Flags().Float64Var(&fuzz, "fuzz", 1.2, "error multiplier (must be between 1 and 2)")
	rootCmd.Flags().IntVarP(&maxOut, "max", "m", -1, "maximum number of output lines")
}

func run(cmd *cobra.Command, args []string) error {
	if fuzz < 1.0 || fuzz > 2.0 {
		return fmt.Errorf("invalid fuzz number. Must be between 1 and 2")
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

	// Read headers and find key column
	headers, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	keyCol, err := reader.FindColumn(colOpts.Col)
	if err != nil {
		return fmt.Errorf("finding column: %w", err)
	}

	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Echo headers to output
	if headers != nil {
		if err := writer.WriteHeaders(headers); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	// Collate input by class
	classes := make(map[string][][]string)
	count := 0
	batchSize := colOpts.BatchSize

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		// Get class value from key column
		var class string
		if keyCol < 0 {
			// Last column
			if len(row) > 0 {
				class = row[len(row)-1]
			}
		} else if keyCol < len(row) {
			class = row[keyCol]
		}

		// Strip trailing newline/carriage return (already handled by TabReader
		// but match Perl behavior just in case)
		class = strings.TrimRight(class, "\r\n")

		classes[class] = append(classes[class], row)
		count++

		if verbose && batchSize > 0 && count%batchSize == 0 {
			fmt.Fprintf(os.Stderr, "%d records processed.\n", count)
		}
	}

	// Find the smallest class
	smallest := count
	for class, rows := range classes {
		size := len(rows)
		if verbose {
			fmt.Fprintf(os.Stderr, "%d records of type %s.\n", size, class)
		}
		if size < smallest {
			smallest = size
		}
	}

	max := int(float64(smallest) * fuzz)
	if verbose {
		fmt.Fprintf(os.Stderr, "Maximum records per class is %d.\n", max)
	}

	// Shuffle each class (partial Fisher-Yates up to max)
	for class, lines := range classes {
		if verbose {
			fmt.Fprintf(os.Stderr, "Shuffling records for %s.\n", class)
		}
		size := len(lines)
		used := max
		if size < used {
			used = size
		}
		for i := 0; i < used; i++ {
			j := rand.Intn(size-i) + i
			lines[i], lines[j] = lines[j], lines[i]
		}
		classes[class] = lines
	}

	// Write the output. We want to make sure that the extras are evenly
	// distributed. Compute xPos (where the extras start) and xSpace (how far
	// apart the extras are) for each class.
	type classInfo struct {
		name   string
		xPos   int
		xSpace int
	}
	classInfos := make(map[string]*classInfo)
	for class, rows := range classes {
		residual := len(rows)
		if residual > max {
			residual = max
		}
		residual -= smallest
		space := smallest
		if residual > 0 {
			space = smallest / residual
		}
		classInfos[class] = &classInfo{
			name:   class,
			xPos:   smallest,
			xSpace: space,
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Writing output.\n")
	}

	maxLines := maxOut
	lines := 0
	abort := false
	counts := make(map[string]int)

	for i := 0; i < smallest && !abort; i++ {
		var buffer [][]string
		for class, info := range classInfos {
			positions := []int{i}
			if info.xSpace > 0 && i%info.xSpace == 0 {
				positions = append(positions, info.xPos)
				info.xPos++
			}
			for _, j := range positions {
				if j < len(classes[class]) {
					row := classes[class][j]
					if row != nil {
						buffer = append(buffer, row)
						counts[class]++
						lines++
					}
				}
			}
		}
		if maxLines < 0 || lines <= maxLines {
			for _, row := range buffer {
				if err := writer.WriteRow(row...); err != nil {
					return fmt.Errorf("writing row: %w", err)
				}
			}
		} else {
			abort = true
		}
	}

	if verbose {
		total := 0
		// Sort keys for deterministic output
		for class, c := range counts {
			fmt.Fprintf(os.Stderr, "%d lines output for %s.\n", c, class)
			total += c
		}
		fmt.Fprintf(os.Stderr, "%d total lines output.\n", total)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
