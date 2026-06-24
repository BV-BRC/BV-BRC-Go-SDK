// Command p3-get-sf-data retrieves sequence feature data for sequence feature IDs from stdin.
//
// This command reads sequence feature IDs from the standard input and retrieves the
// corresponding sequence feature data from the BV-BRC database.
//
// A sequence feature describes a portion of a gene or coding region that is of
// particular interest to the biological community.
//
// Usage:
//
//	p3-get-sf-data [options] < sf_ids.txt
//
// Examples:
//
//	# Get sequence feature data for IDs in a file
//	p3-get-sf-data < sf_ids.txt
//
//	# Get specific fields
//	p3-get-sf-data -a sf_name -a sf_category -a gene < sf_ids.txt
//
//	# Use a specific column from input
//	p3-get-sf-data --col 2 < input.txt
//
//	# List available fields
//	p3-get-sf-data --fields
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	dataOpts cli.DataOptions
	colOpts  cli.ColOptions
	ioOpts   cli.IOOptions
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-sf-data",
	Short: "Return sequence feature data for sequence feature IDs from stdin",
	Long: `This script reads sequence feature IDs from the standard input and returns
corresponding sequence feature data from the BV-BRC database.

A sequence feature describes a portion of a gene or coding region that is of
particular interest to the biological community.

The input should be tab-delimited with sequence feature IDs in the specified column
(default: last column). The output includes the original input columns plus the
requested sequence feature data fields.

Examples:

  # Get sequence feature data for IDs in a file
  p3-get-sf-data < sf_ids.txt

  # Get specific fields
  p3-get-sf-data -a sf_name -a sf_category -a gene < sf_ids.txt

  # Use a specific column from input
  p3-get-sf-data --col 2 < input.txt

  # List available fields
  p3-get-sf-data --fields`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Get optional authentication token
	token, _ := auth.GetToken()

	// Create API client
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
		fields, err := client.GetSchema(ctx, "sf")
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

	// Create tab reader/writer
	reader := cli.NewTabReader(inFile, !colOpts.NoHead)
	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Read headers and find key column
	inputHeaders, err := reader.Headers()
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	keyCol, err := reader.FindColumn(colOpts.Col)
	if err != nil {
		return fmt.Errorf("finding key column: %w", err)
	}

	// Get default fields for sf object
	defaultFields := api.GetDefaultFields("sf")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Ensure sf_id is in the select list for result association
	hasSfID := false
	for _, f := range fields {
		if f == "sf_id" {
			hasSfID = true
			break
		}
	}
	selectFields := fields
	if !hasSfID {
		selectFields = append([]string{"sf_id"}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "sf."+f)
	}
	if err := writer.WriteHeaders(outputHeaders); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	// Get delimiter for multi-valued fields
	delim := ioOpts.GetDelimiter()

	// Process in batches
	for {
		keys, rows, err := reader.ReadBatch(colOpts.BatchSize, keyCol)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading batch: %w", err)
		}
		if len(keys) == 0 {
			break
		}

		// Build query with IN filter for the batch of keys
		query, err := dataOpts.BuildQueryWithFields(selectFields)
		if err != nil {
			return fmt.Errorf("building query: %w", err)
		}
		query.In("sf_id", keys...)

		// Execute query
		results, err := client.Query(ctx, "sf", query)
		if err != nil {
			return fmt.Errorf("querying sequence features: %w", err)
		}

		// Build lookup map from results
		resultMap := make(map[string]map[string]any)
		for _, r := range results {
			if id, ok := r["sf_id"].(string); ok {
				resultMap[id] = r
			}
		}

		// Output results in input order
		for i, key := range keys {
			var outRow []string
			outRow = append(outRow, rows[i]...)

			if result, ok := resultMap[key]; ok {
				for _, f := range fields {
					outRow = append(outRow, cli.FormatValue(result[f], delim))
				}
			} else {
				// No result found - add empty fields
				for range fields {
					outRow = append(outRow, "")
				}
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
