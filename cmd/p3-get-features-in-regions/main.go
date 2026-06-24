// Command p3-get-features-in-regions retrieves features overlapping specified genome regions.
//
// This command reads genome coordinates from the standard input and returns features
// that overlap those coordinates from the BV-BRC database.
//
// Usage:
//
//	p3-get-features-in-regions [options] genomeCol contigCol startCol endCol
//
// The four positional parameters are the 1-based column indices or names of the
// genome ID column, contig/sequence ID column, start location column, and end
// location column in the input.
//
// A feature overlaps if it starts on or before the region end and ends on or
// after the region start.  Because multiple features may exist in a region, each
// input record may appear multiple times in the output.
//
// Examples:
//
//	# Get features in regions specified in a file (columns 1-4)
//	p3-get-features-in-regions 1 2 3 4 < regions.txt
//
//	# Get CDS features only
//	p3-get-features-in-regions --eq feature_type,CDS 1 2 3 4 < regions.txt
//
//	# Use named columns
//	p3-get-features-in-regions genome_id sequence_id start end < regions.txt
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	dataOpts cli.DataOptions
	ioOpts   cli.IOOptions
	noHead   bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-features-in-regions genomeCol contigCol startCol endCol",
	Short: "Return features overlapping specified genome regions",
	Long: `This script reads a list of genome coordinates from the standard input and
returns the features that overlap those coordinates from the BV-BRC database.

The four positional parameters are the 1-based column indices (or column
header names) of the genome ID column, the contig/sequence ID column, the
starting-location column, and the ending-location column in the input.

A feature overlaps a region if it starts on or before the region end point
and ends on or after the region start point.  Because multiple features may
exist in a region, each input record may appear multiple times in the output.

Examples:

  # Get features in regions specified in a file (columns 1-4)
  p3-get-features-in-regions 1 2 3 4 < regions.txt

  # Get only CDS features
  p3-get-features-in-regions --eq feature_type,CDS 1 2 3 4 < regions.txt

  # Use named columns from the header row
  p3-get-features-in-regions genome_id sequence_id start end < regions.txt`,
	Args:         cobra.ExactArgs(4),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVar(&noHead, "nohead", false,
		"input file has no header row")
}

// resolveColumn resolves a column specifier (1-based number or header name) to
// a 0-based index given the parsed header slice.  When hasHeader is false only
// numeric specs are accepted.
func resolveColumn(spec string, headers []string, hasHeader bool) (int, error) {
	// Try numeric first
	if n, err := strconv.Atoi(spec); err == nil {
		if n < 1 {
			return 0, fmt.Errorf("column index must be >= 1, got %d", n)
		}
		return n - 1, nil
	}
	// Named column — requires a header row
	if !hasHeader {
		return 0, fmt.Errorf("column %q: cannot resolve by name when --nohead is set", spec)
	}
	for i, h := range headers {
		if h == spec {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %q not found in headers", spec)
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	genomeColSpec := args[0]
	contigColSpec := args[1]
	startColSpec := args[2]
	endColSpec := args[3]

	// Build API client
	token, _ := auth.GetToken()
	clientOpts := []api.ClientOption{}
	if token != nil {
		clientOpts = append(clientOpts, api.WithToken(token))
	}
	if dataOpts.Debug {
		clientOpts = append(clientOpts, api.WithDebug(true))
	}
	if dataOpts.APIURL != "" {
		clientOpts = append(clientOpts, api.WithBaseURL(dataOpts.APIURL))
	}
	if dataOpts.MaxRetries > 0 {
		clientOpts = append(clientOpts, api.WithMaxRetries(dataOpts.MaxRetries))
	}
	if dataOpts.Verbose {
		clientOpts = append(clientOpts, api.WithVerbose(true))
	}
	if dataOpts.UserAgent != "" {
		clientOpts = append(clientOpts, api.WithUserAgent(dataOpts.UserAgent))
	}
	client := api.NewClient(clientOpts...)

	// Handle --fields option
	if dataOpts.Fields {
		fields, err := client.GetSchema(ctx, "feature")
		if err != nil {
			return fmt.Errorf("getting schema: %w", err)
		}
		for _, f := range fields {
			if f.MultiValued {
				fmt.Printf("%s (multi)\n", f.Name)
			} else {
				fmt.Println(f.Name)
			}
		}
		return nil
	}

	// Open input / output
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

	hasHeader := !noHead
	reader := cli.NewTabReader(inFile, hasHeader)
	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Read header row (may be nil when --nohead)
	inputHeaders, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	// Resolve positional column specifiers
	genomeCol, err := resolveColumn(genomeColSpec, inputHeaders, hasHeader)
	if err != nil {
		return fmt.Errorf("genomeCol: %w", err)
	}
	contigCol, err := resolveColumn(contigColSpec, inputHeaders, hasHeader)
	if err != nil {
		return fmt.Errorf("contigCol: %w", err)
	}
	startCol, err := resolveColumn(startColSpec, inputHeaders, hasHeader)
	if err != nil {
		return fmt.Errorf("startCol: %w", err)
	}
	endCol, err := resolveColumn(endColSpec, inputHeaders, hasHeader)
	if err != nil {
		return fmt.Errorf("endCol: %w", err)
	}

	// Determine selected feature fields
	defaultFields := api.GetDefaultFields("feature")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "feature."+f)
	}
	if err := writer.WriteHeaders(outputHeaders); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	delim := ioOpts.GetDelimiter()

	// Process row by row (no OR in the data API, must issue one query per row)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}

		// Extract coordinate values
		getCol := func(idx int) string {
			if idx < len(row) {
				return row[idx]
			}
			return ""
		}
		genomeID := getCol(genomeCol)
		contigID := getCol(contigCol)
		regionStart := getCol(startCol)
		regionEnd := getCol(endCol)

		// Build query with region overlap filters plus any user-supplied filters
		query, err := dataOpts.BuildQueryWithFields(fields)
		if err != nil {
			return fmt.Errorf("building query: %w", err)
		}
		query.Eq("genome_id", genomeID)
		query.Eq("sequence_id", contigID)
		// feature.start <= regionEnd  (feature starts before or at our end)
		query.Le("start", regionEnd)
		// feature.end >= regionStart  (feature ends at or after our start)
		query.Ge("end", regionStart)

		// Execute query and emit results
		results, err := client.Query(ctx, "feature", query)
		if err != nil {
			return fmt.Errorf("querying features for genome %s contig %s: %w", genomeID, contigID, err)
		}

		for _, result := range results {
			var outRow []string
			outRow = append(outRow, row...)
			for _, f := range fields {
				outRow = append(outRow, cli.FormatValue(result[f], delim))
			}
			if err := writer.WriteRow(outRow...); err != nil {
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
