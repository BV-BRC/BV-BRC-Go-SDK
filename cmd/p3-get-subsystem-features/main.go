// Command p3-get-subsystem-features retrieves features for subsystem IDs from stdin.
//
// This command reads subsystem IDs from the standard input and retrieves the
// corresponding subsystem-item records from the BV-BRC database.  Each result
// describes a feature's relationship to the subsystem (patric_id, role_name,
// genome_id, etc.).  To obtain the feature data itself, pipe the output into
// p3-get-feature-data.
//
// Usage:
//
//	p3-get-subsystem-features [options] < subsystem_ids.txt
//
// Examples:
//
//	# Get subsystem features for IDs in a file
//	p3-get-subsystem-features < subsystem_ids.txt
//
//	# Input contains subsystem names (spaces) instead of IDs (underscores)
//	p3-get-subsystem-features --names < subsystem_names.txt
//
//	# Get specific fields
//	p3-get-subsystem-features -a patric_id -a role_name -a genome_id < subsystem_ids.txt
//
//	# Use a specific column from input
//	p3-get-subsystem-features --col 2 < input.txt
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	dataOpts cli.DataOptions
	colOpts  cli.ColOptions
	ioOpts   cli.IOOptions
	names    bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-subsystem-features",
	Short: "Return subsystem-item records for subsystem IDs from stdin",
	Long: `This script reads subsystem IDs from the standard input and returns
the subsystem-item records (features in those subsystems) from the BV-BRC
database.

Each returned record describes a feature's relationship to the subsystem
(patric_id, role_name, genome_id, etc.), not the feature itself.  To obtain
full feature data, pipe the output into p3-get-feature-data.

The input should be tab-delimited with subsystem IDs in the specified column
(default: last column).  Use --names if the input contains subsystem names
with spaces instead of IDs with underscores.

Examples:

  # Get subsystem features for IDs in a file
  p3-get-subsystem-features < subsystem_ids.txt

  # Input contains subsystem names (spaces) instead of IDs (underscores)
  p3-get-subsystem-features --names < subsystem_names.txt

  # Get specific fields
  p3-get-subsystem-features -a patric_id -a role_name -a genome_id < subsystem_ids.txt

  # Use a specific column from input
  p3-get-subsystem-features --col 2 < input.txt`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVarP(&names, "names", "N", false,
		"input contains subsystem names (spaces) instead of IDs (underscores)")
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
		fields, err := client.GetSchema(ctx, "subsystemItem")
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

	// Get default fields for subsystemItem object
	defaultFields := api.GetDefaultFields("subsystemItem")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Ensure subsystem_id is in the select list for result association
	hasSubsystemID := false
	for _, f := range fields {
		if f == "subsystem_id" {
			hasSubsystemID = true
			break
		}
	}
	selectFields := fields
	if !hasSubsystemID {
		selectFields = append([]string{"subsystem_id"}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	} else {
		outputHeaders = append(outputHeaders, "subsystem.subsystem_id")
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "subsystemItem."+f)
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

		// If --names is set, convert spaces to underscores (matching Perl behaviour)
		if names {
			for i, k := range keys {
				keys[i] = strings.ReplaceAll(k, " ", "_")
			}
		}

		// Build a map of subsystem_id -> input row for later association
		rowMap := make(map[string][]string)
		for i, key := range keys {
			rowMap[key] = rows[i]
		}

		// Query one subsystem at a time (one-to-many: each subsystem has many features)
		for _, key := range keys {
			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.Eq("subsystem_id", key)

			inputRow := rowMap[key]

			// Choose pagination method based on --cursor flag
			var queryErr error
			if dataOpts.Cursor {
				queryErr = client.QueryCallbackWithCursor(ctx, "subsystemItem", query, func(records []map[string]any, info *api.ChunkInfo) bool {
					for _, record := range records {
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
				if queryErr != nil && strings.Contains(queryErr.Error(), "undefined field object") {
					return fmt.Errorf("%w\n\nNote: cursor-based pagination may not be supported by this API endpoint.\nTry using --api-url https://alpha.bv-brc.org/api or remove the --cursor flag", queryErr)
				}
			} else {
				queryErr = client.QueryCallback(ctx, "subsystemItem", query, func(records []map[string]any, info *api.ChunkInfo) bool {
					for _, record := range records {
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
			}
			if queryErr != nil {
				return fmt.Errorf("querying subsystem features for %s: %w", key, queryErr)
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
