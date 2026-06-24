// Command p3-find-features finds features by matching a specified field against values from stdin.
//
// This command reads key values from the standard input and retrieves the
// corresponding features from the BV-BRC database by matching against the
// specified field. It supports standard filtering and field selection options.
//
// Usage:
//
//	p3-find-features [options] keyName
//
// The positional argument keyName specifies which feature field to match against.
// Valid key names are: gene, gene_id, refseq_locus_tag, protein_id, aa_sequence_md5, product
//
// Examples:
//
//	# Find features by gene name
//	p3-echo coaA | p3-find-features --attr patric_id,product gene
//
//	# Find features by gene name within a specific genome
//	p3-echo coaA | p3-find-features --attr patric_id,product --eq genome_id,210007.7 gene
//
//	# List valid key names
//	p3-find-features --keyNames
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

// validKeys maps key field names to their batch type:
//   - type 1 (common): use IN query batching (gene_id, refseq_locus_tag) — one result per key
//   - type 2 (uncommon): use individual per-key queries (gene, protein_id, aa_sequence_md5, product)
//     because these can return multiple results per key value
var validKeys = map[string]int{
	"gene":              2,
	"gene_id":           1,
	"refseq_locus_tag":  1,
	"protein_id":        2,
	"aa_sequence_md5":   2,
	"product":           2,
}

var (
	dataOpts cli.DataOptions
	colOpts  cli.ColOptions
	ioOpts   cli.IOOptions
	keyNames bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-find-features keyName",
	Short: "Find features by matching a specified field against values from stdin",
	Long: `This script finds features based on the value in one of several
feature-identifying fields (other than patric_id). It provides standard
filtering parameters to otherwise limit the output.

The positional argument keyName specifies which feature field to match against.
The following key names are permitted:

  refseq_locus_tag   The locus tag from REFSEQ
  protein_id         The REFSEQ protein ID
  gene               The common gene name (e.g. rpoA)
  gene_id            The standard gene number
  aa_sequence_md5    The protein sequence MD5 code
  product            The functional assignment of the feature

The input should be tab-delimited with key values in the specified column
(default: last column). The output includes the original input columns plus
the requested feature data fields.

Examples:

  # Find features by gene name
  p3-echo coaA | p3-find-features --attr patric_id,product gene

  # Find features by gene name within a specific genome
  p3-echo coaA | p3-find-features --attr patric_id,product --eq genome_id,210007.7 gene

  # List valid key names
  p3-find-features --keyNames`,
	Args:         cobra.MaximumNArgs(1),
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVar(&keyNames, "keyNames", false,
		"list valid key field names and exit")
	rootCmd.Flags().BoolVar(&keyNames, "keynames", false, "")
	_ = rootCmd.Flags().MarkHidden("keynames")
	rootCmd.Flags().BoolVar(&keyNames, "keys", false, "")
	_ = rootCmd.Flags().MarkHidden("keys")
}

func run(cmd *cobra.Command, args []string) error {
	// Handle --keyNames: list valid key names and exit
	if keyNames {
		for name := range validKeys {
			fmt.Println(name)
		}
		return nil
	}

	// Require keyName positional argument
	if len(args) == 0 {
		return fmt.Errorf("no key field name specified")
	}
	keyName := args[0]

	// Validate the key name
	batchType, ok := validKeys[keyName]
	if !ok {
		return fmt.Errorf("key field %s not supported", keyName)
	}

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

	// Get default fields for feature object
	defaultFields := api.GetDefaultFields("feature")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Ensure the search key field is in the select list for result association
	hasKeyField := false
	for _, f := range fields {
		if f == keyName {
			hasKeyField = true
			break
		}
	}
	selectFields := fields
	if !hasKeyField {
		selectFields = append([]string{keyName}, fields...)
	}

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

		if batchType == 1 {
			// Type 1 (common): use a single IN query for all keys in the batch.
			// Each key should match at most one feature (gene_id, refseq_locus_tag).
			// Build a lookup map from input key -> input row.
			rowMap := make(map[string][]string)
			for i, key := range keys {
				rowMap[key] = rows[i]
			}

			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.In(keyName, keys...)

			results, err := client.Query(ctx, "feature", query)
			if err != nil {
				return fmt.Errorf("querying features: %w", err)
			}

			// Output results; associate each result with its input row via the key field
			for _, result := range results {
				keyVal, _ := result[keyName].(string)
				inputRow := rowMap[keyVal]

				var outRow []string
				outRow = append(outRow, inputRow...)
				for _, f := range fields {
					outRow = append(outRow, cli.FormatValue(result[f], delim))
				}
				if err := writer.WriteRow(outRow...); err != nil {
					return fmt.Errorf("writing row: %w", err)
				}
			}
		} else {
			// Type 2 (uncommon): query per-key because each key can match multiple features.
			// (e.g. gene name, product, protein_id, aa_sequence_md5)
			for i, key := range keys {
				inputRow := rows[i]

				query, err := dataOpts.BuildQueryWithFields(selectFields)
				if err != nil {
					return fmt.Errorf("building query: %w", err)
				}

				// For product, the Perl uses a substring (SOLR) match; in the API
				// this is handled via Eq which generates a keyword/phrase query on
				// the field. Other type-2 keys use exact match.
				query.Eq(keyName, key)

				// Use streaming to handle large result sets
				var queryErr error
				if dataOpts.Cursor {
					queryErr = client.QueryCallbackWithCursor(ctx, "feature", query, func(records []map[string]any, info *api.ChunkInfo) bool {
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
					queryErr = client.QueryCallback(ctx, "feature", query, func(records []map[string]any, info *api.ChunkInfo) bool {
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
					return fmt.Errorf("querying features for key %s=%s: %w", keyName, key, queryErr)
				}
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
