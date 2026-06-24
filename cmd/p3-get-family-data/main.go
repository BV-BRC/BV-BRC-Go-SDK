// Command p3-get-family-data retrieves protein family data for family IDs from stdin.
//
// This command reads family IDs from the standard input and retrieves the
// corresponding family data from the BV-BRC database.
//
// Usage:
//
//	p3-get-family-data [options] < family_ids.txt
//
// Examples:
//
//	# Get family data for IDs in a file
//	p3-get-family-data < family_ids.txt
//
//	# Get specific fields
//	p3-get-family-data -a family_product -a family_type < family_ids.txt
//
//	# Use a specific column from input
//	p3-get-family-data --col 2 < input.txt
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
	Use:   "p3-get-family-data",
	Short: "Return protein family data for family IDs from stdin",
	Long: `This script reads family IDs from the standard input and returns
corresponding protein family data from the BV-BRC database.

The input should be tab-delimited with family IDs in the specified column
(default: last column). The output includes the original input columns
plus the requested family data fields.

Examples:

  # Get family data for IDs in a file
  p3-get-family-data < family_ids.txt

  # Get specific fields
  p3-get-family-data -a family_product -a family_type < family_ids.txt

  # Use a specific column from input
  p3-get-family-data --col 2 < input.txt`,
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
		fields, err := client.GetSchema(ctx, "protein_family_ref")
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

	// Get default fields for family object
	defaultFields := api.GetDefaultFields("family")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Ensure family_id is in the select list for result association
	hasFamilyID := false
	for _, f := range fields {
		if f == "family_id" {
			hasFamilyID = true
			break
		}
	}
	selectFields := fields
	if !hasFamilyID {
		selectFields = append([]string{"family_id"}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "family."+f)
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
		query.In("family_id", keys...)

		// Execute query
		results, err := client.Query(ctx, "protein_family_ref", query)
		if err != nil {
			return fmt.Errorf("querying families: %w", err)
		}

		// Build lookup map from results
		resultMap := make(map[string]map[string]any)
		for _, r := range results {
			if id, ok := r["family_id"].(string); ok {
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
