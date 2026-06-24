// Command p3-get-feature-protein-structures retrieves protein structures for feature IDs from stdin.
//
// This command reads BV-BRC feature IDs from the standard input and retrieves the
// corresponding protein structures associated with each feature from the BV-BRC database.
// Since each feature may have many identified protein structures, each input record
// can produce multiple output records.
//
// Usage:
//
//	p3-get-feature-protein-structures [options] < feature_ids.txt
//
// Examples:
//
//	# Get protein structures for feature IDs in a file
//	p3-get-feature-protein-structures < feature_ids.txt
//
//	# Get specific fields
//	p3-get-feature-protein-structures -a pdb_id -a title -a method < feature_ids.txt
//
//	# Use a specific column from input
//	p3-get-feature-protein-structures --col 2 < input.txt
//
//	# List available fields
//	p3-get-feature-protein-structures --fields
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
	Use:   "p3-get-feature-protein-structures",
	Short: "Return protein structures for feature IDs from stdin",
	Long: `This script reads BV-BRC feature IDs from the standard input and returns
the protein structures associated with each feature from the BV-BRC database.

The input should be tab-delimited with feature IDs (patric_id) in the specified
column (default: last column). The output includes the original input columns
plus the requested protein structure data fields. Since each feature may have
many identified protein structures, each input record can produce multiple
output records.

Examples:

  # Get protein structures for feature IDs in a file
  p3-get-feature-protein-structures < feature_ids.txt

  # Get specific fields
  p3-get-feature-protein-structures -a pdb_id -a title -a method < feature_ids.txt

  # Use a specific column from input
  p3-get-feature-protein-structures --col 2 < input.txt

  # List available fields
  p3-get-feature-protein-structures --fields`,
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
		fields, err := client.GetSchema(ctx, "protein_structure")
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

	// Get default fields for protein_structure object
	defaultFields := api.GetDefaultFields("protein_structure")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Ensure patric_id is in the select list for result association
	hasPatricID := false
	for _, f := range fields {
		if f == "patric_id" {
			hasPatricID = true
			break
		}
	}
	selectFields := fields
	if !hasPatricID {
		selectFields = append([]string{"patric_id"}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	} else {
		outputHeaders = append(outputHeaders, "feature.patric_id")
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "protein_structure."+f)
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

		// Build a map of patric_id -> input row for later association
		rowMap := make(map[string][]string)
		for i, key := range keys {
			rowMap[key] = rows[i]
		}

		// Query protein_structure objects for all keys in batch using IN filter
		query, err := dataOpts.BuildQueryWithFields(selectFields)
		if err != nil {
			return fmt.Errorf("building query: %w", err)
		}
		query.In("patric_id", keys...)

		// Execute query using callback to stream results
		queryErr := client.QueryCallback(ctx, "protein_structure", query, func(records []map[string]any, info *api.ChunkInfo) bool {
			for _, record := range records {
				patricID, _ := record["patric_id"].(string)
				inputRow := rowMap[patricID]
				if inputRow == nil {
					// No matching input row - use patric_id only
					inputRow = []string{patricID}
				}

				var outRow []string
				outRow = append(outRow, inputRow...)
				for _, f := range fields {
					outRow = append(outRow, cli.FormatValue(record[f], delim))
				}

				if err := writer.WriteRow(outRow...); err != nil {
					fmt.Fprintf(os.Stderr, "Error writing row: %v\n", err)
					return false
				}
			}
			return true
		})
		if queryErr != nil {
			return fmt.Errorf("querying protein structures: %w", queryErr)
		}
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
