// Command p3-find-surveillance-data searches for surveillance data in the BV-BRC database.
//
// This command queries the BV-BRC surveillance database and returns records
// matching the specified criteria. At least one filtering parameter must be
// specified.
//
// Usage:
//
//	p3-find-surveillance-data [options]
//
// Examples:
//
//	# Find surveillance data for a specific country
//	p3-find-surveillance-data --eq collection_country,USA
//
//	# Find surveillance data with a specific pathogen test result
//	p3-find-surveillance-data --eq pathogen_test_result,Positive
//
//	# List available fields
//	p3-find-surveillance-data --fields
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
	Use:   "p3-find-surveillance-data",
	Short: "Return surveillance data from BV-BRC",
	Long: `This script returns surveillance data from the BV-BRC database.
It supports standard filtering parameters to filter the output and column
options to select the columns to return. At least one filtering parameter
must be specified.

    p3-find-surveillance-data [options]

The output columns are defined by the --attr (-a) option. If no columns
are specified, a default set of columns is returned.

Examples:

  # Find surveillance data for a specific country
  p3-find-surveillance-data --eq collection_country,USA

  # Find surveillance data with a specific pathogen test result
  p3-find-surveillance-data --eq pathogen_test_result,Positive -a sample_identifier -a host_species

  # List available fields
  p3-find-surveillance-data --fields`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddIOFlags(rootCmd, &ioOpts)
}

// hasFilters returns true if the user has specified at least one filtering parameter.
func hasFilters(opts *cli.DataOptions) bool {
	return len(opts.Equal) > 0 ||
		len(opts.Lt) > 0 ||
		len(opts.Le) > 0 ||
		len(opts.Gt) > 0 ||
		len(opts.Ge) > 0 ||
		len(opts.Ne) > 0 ||
		len(opts.In) > 0 ||
		len(opts.Required) > 0 ||
		opts.Keyword != ""
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
		fields, err := client.GetSchema(ctx, "surveillance")
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

	// Require at least one filtering parameter (mirrors the Perl behavior)
	if !hasFilters(&dataOpts) {
		return fmt.Errorf("at least one filtering parameter is required")
	}

	// Get default fields for surveillance object
	defaultFields := api.GetDefaultFields("surveillance")

	// Build query from options
	query, err := dataOpts.BuildQuery(defaultFields)
	if err != nil {
		return fmt.Errorf("building query: %w", err)
	}

	// Handle count mode
	if dataOpts.Count {
		count, err := client.Count(ctx, "surveillance", query)
		if err != nil {
			return fmt.Errorf("counting surveillance records: %w", err)
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
			err := client.QueryCallbackWithCursor(ctx, "surveillance", query, func(records []map[string]any, info *api.ChunkInfo) bool {
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
			return client.QueryCallback(ctx, "surveillance", query, func(records []map[string]any, info *api.ChunkInfo) bool {
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
	if err := queryFunc(); err != nil {
		return fmt.Errorf("querying surveillance data: %w", err)
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
