// Command p3-all-genomes retrieves all genomes from the BV-BRC database.
//
// This command queries the BV-BRC genome database and returns genome records
// matching the specified criteria. It supports standard data query options
// for filtering and field selection.
//
// Usage:
//
//	p3-all-genomes [options]
//
// Examples:
//
//	# Get all complete genomes
//	p3-all-genomes --eq genome_status,Complete
//
//	# Get genomes from a specific genus with selected fields
//	p3-all-genomes --eq genus,Streptomyces -a genome_id -a genome_name -a genome_length
//
//	# Count genomes matching criteria
//	p3-all-genomes --eq host_name,Human --count
//
//	# Limit results
//	p3-all-genomes --limit 100
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	dataOpts cli.DataOptions
	ioOpts   cli.IOOptions
)

var rootCmd = &cobra.Command{
	Use:   "p3-all-genomes",
	Short: "Return all genomes from BV-BRC",
	Long: `This script returns data for all the genomes in the BV-BRC database.
It supports standard filtering parameters to filter the output and column
options to select the columns to return.

    p3-all-genomes [options]

The output columns are defined by the --attr (-a) option. If no columns
are specified, a default set of columns is returned including genome_id,
genome_name, and genome_status.

Examples:

  # Get all complete genomes
  p3-all-genomes --eq genome_status,Complete

  # Get genomes from genus Streptomyces
  p3-all-genomes --eq genus,Streptomyces -a genome_id -a genome_name

  # Count human-associated genomes
  p3-all-genomes --eq host_name,Human --count`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
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
		fields, err := client.GetSchema(ctx, "genome")
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

	// Get default fields for genome object
	defaultFields := api.GetDefaultFields("genome")

	// Build query from options
	query, err := dataOpts.BuildQuery(defaultFields)
	if err != nil {
		return fmt.Errorf("building query: %w", err)
	}

	// Handle count mode
	if dataOpts.Count {
		count, err := client.Count(ctx, "genome", query)
		if err != nil {
			return fmt.Errorf("counting genomes: %w", err)
		}
		fmt.Println(count)
		return nil
	}

	// Open output
	outFile, err := cli.OpenOutput(ioOpts.Output)
	if err != nil {
		return fmt.Errorf("opening output: %w", err)
	}
	defer outFile.Close()

	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Get the fields we're selecting
	fields := dataOpts.GetSelectFields(defaultFields)

	// Write header
	if err := writer.WriteHeaders(fields); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	// Get delimiter for multi-valued fields
	delim := ioOpts.GetDelimiter()

	// Choose pagination method based on --cursor flag
	var queryFunc func() error
	if dataOpts.Cursor {
		// Use cursor-based pagination (more efficient for large result sets)
		// Note: cursor support requires alpha.bv-brc.org or a compatible API
		queryFunc = func() error {
			err := client.QueryCallbackWithCursor(ctx, "genome", query, func(records []map[string]any, info *api.ChunkInfo) bool {
				for _, record := range records {
					row := cli.FormatRecord(record, fields, delim)
					if err := writer.WriteRow(row...); err != nil {
						fmt.Fprintf(os.Stderr, "Error writing row: %v\n", err)
						return false
					}
				}
				return true // continue fetching
			})
			// Check if the error is due to cursor not being supported
			if err != nil && strings.Contains(err.Error(), "undefined field object") {
				return fmt.Errorf("%w\n\nNote: cursor-based pagination may not be supported by this API endpoint.\nTry using --api-url https://alpha.bv-brc.org/api or remove the --cursor flag", err)
			}
			return err
		}
	} else {
		// Use offset-based pagination (default)
		queryFunc = func() error {
			return client.QueryCallback(ctx, "genome", query, func(records []map[string]any, info *api.ChunkInfo) bool {
				for _, record := range records {
					row := cli.FormatRecord(record, fields, delim)
					if err := writer.WriteRow(row...); err != nil {
						fmt.Fprintf(os.Stderr, "Error writing row: %v\n", err)
						return false
					}
				}
				return true // continue fetching
			})
		}
	}

	// Execute query
	err = queryFunc()

	if err != nil {
		return fmt.Errorf("querying genomes: %w", err)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
