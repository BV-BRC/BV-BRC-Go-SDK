// Command p3-all-features retrieves all features from the BV-BRC database.
//
// This command queries the BV-BRC feature database and returns feature records
// matching the specified criteria. It supports standard data query options
// for filtering and field selection.
//
// Usage:
//
//	p3-all-features [options]
//
// Examples:
//
//	# Get all CDS features for a genome
//	p3-all-features --eq genome_id,83332.12 --eq feature_type,CDS
//
//	# Get features with specific product annotation
//	p3-all-features --eq product,"DNA polymerase" -a patric_id -a product
//
//	# Count features for a genome
//	p3-all-features --eq genome_id,83332.12 --count
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
	Use:   "p3-all-features",
	Short: "Return all features from BV-BRC",
	Long: `This script returns data for all the features in the BV-BRC database.
It supports standard filtering parameters to filter the output and column
options to select the columns to return.

    p3-all-features [options]

The output columns are defined by the --attr (-a) option. If no columns
are specified, a default set of columns is returned.

Examples:

  # Get all CDS features for a genome
  p3-all-features --eq genome_id,83332.12 --eq feature_type,CDS

  # Get features with specific product annotation
  p3-all-features --eq product,"DNA polymerase" -a patric_id -a product

  # Count features for a genome
  p3-all-features --eq genome_id,83332.12 --count`,
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

	// Get default fields for feature object
	defaultFields := api.GetDefaultFields("feature")

	// Build query from options
	query, err := dataOpts.BuildQuery(defaultFields)
	if err != nil {
		return fmt.Errorf("building query: %w", err)
	}

	// Handle count mode
	if dataOpts.Count {
		count, err := client.Count(ctx, "feature", query)
		if err != nil {
			return fmt.Errorf("counting features: %w", err)
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
		queryFunc = func() error {
			err := client.QueryCallbackWithCursor(ctx, "feature", query, func(records []map[string]any, info *api.ChunkInfo) bool {
				for _, record := range records {
					row := cli.FormatRecord(record, fields, delim)
					if err := writer.WriteRow(row...); err != nil {
						fmt.Fprintf(os.Stderr, "Error writing row: %v\n", err)
						return false
					}
				}
				return true
			})
			if err != nil && strings.Contains(err.Error(), "undefined field object") {
				return fmt.Errorf("%w\n\nNote: cursor-based pagination may not be supported by this API endpoint.\nTry using --api-url https://alpha.bv-brc.org/api or remove the --cursor flag", err)
			}
			return err
		}
	} else {
		// Use offset-based pagination (default)
		queryFunc = func() error {
			return client.QueryCallback(ctx, "feature", query, func(records []map[string]any, info *api.ChunkInfo) bool {
				for _, record := range records {
					row := cli.FormatRecord(record, fields, delim)
					if err := writer.WriteRow(row...); err != nil {
						fmt.Fprintf(os.Stderr, "Error writing row: %v\n", err)
						return false
					}
				}
				return true
			})
		}
	}

	// Execute query
	err = queryFunc()

	if err != nil {
		return fmt.Errorf("querying features: %w", err)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
