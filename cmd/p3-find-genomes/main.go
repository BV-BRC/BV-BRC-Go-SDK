// Command p3-find-genomes finds genomes by filtering on a non-ID field.
//
// This command reads values from the standard input and finds genomes whose
// specified key field matches those values. It supports standard filtering
// parameters to further limit the output.
//
// Usage:
//
//	p3-find-genomes [options] keyName
//
// The positional parameter is the name of the field used to match the
// incoming keys. The following fields are permitted:
//
//   - genome_name
//   - genbank_accessions
//   - sra_accession
//   - assembly_accession
//   - genus
//   - species
//   - taxon_id
//   - family
//   - order
//   - class
//   - phylum
//
// Examples:
//
//	# Find genomes by genome name
//	echo -e "genome_name\nEscherichia coli" | p3-find-genomes genome_name
//
//	# Find genomes by genus with additional filter
//	echo -e "genus\nStreptomyces" | p3-find-genomes --eq genome_status,Complete genus
//
//	# List valid key names
//	p3-find-genomes --keyNames
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

// keyType indicates whether a key field is "common" (high-cardinality, one query per value)
// or "uncommon" (low-cardinality, batch IN queries).
// This mirrors the Perl KEYS constant: 1 = uncommon (batch), 2 = common (per-key query).
type keyType int

const (
	keyBatch  keyType = 1 // uncommon keys: use batch IN queries
	keyPerVal keyType = 2 // common keys: one Eq query per value
)

// validKeys maps each allowed key field to its query strategy.
var validKeys = map[string]keyType{
	"genome_name":         keyBatch,
	"genbank_accessions":  keyBatch,
	"assembly_accession":  keyBatch,
	"sra_accession":       keyBatch,
	"genus":               keyPerVal,
	"species":             keyPerVal,
	"taxon_id":            keyPerVal,
	"family":              keyPerVal,
	"order":               keyPerVal,
	"class":               keyPerVal,
	"phylum":              keyPerVal,
}

var (
	dataOpts cli.DataOptions
	colOpts  cli.ColOptions
	ioOpts   cli.IOOptions
	keyNames bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-find-genomes [options] keyName",
	Short: "Find genomes by filtering on a field",
	Long: `This script finds genomes based on the value in one of several genome-identifying
fields (other than genome_id). It provides standard filtering parameters to
otherwise limit the output.

The positional parameter is the name of the field used to match the incoming
keys. The following fields are permitted:

  genome_name, genbank_accessions, sra_accession, assembly_accession,
  genus, species, taxon_id, family, order, class, phylum

The standard input should be tab-delimited with the key values in the
specified column (default: last column). The output includes the original
input columns plus the requested genome data fields.

Examples:

  # Find genomes by genome name
  echo -e "genome_name\nEscherichia coli" | p3-find-genomes genome_name

  # Find genomes by genus, only complete genomes
  echo -e "genus\nStreptomyces" | p3-find-genomes --eq genome_status,Complete genus

  # List valid key names
  p3-find-genomes --keyNames`,
	Args:         cobra.MaximumNArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVar(&keyNames, "keyNames", false, "list valid key field names")
	rootCmd.Flags().BoolVar(&keyNames, "keynames", false, "")
	_ = rootCmd.Flags().MarkHidden("keynames")
	rootCmd.Flags().BoolVar(&keyNames, "keys", false, "")
	_ = rootCmd.Flags().MarkHidden("keys")
}

func run(cmd *cobra.Command, args []string) error {
	// Handle --keyNames: just print valid key names and exit
	if keyNames {
		names := make([]string, 0, len(validKeys))
		for k := range validKeys {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Println(name)
		}
		return nil
	}

	// Validate positional argument
	if len(args) == 0 {
		return fmt.Errorf("no key field name specified")
	}
	keyName := args[0]
	strategy, ok := validKeys[keyName]
	if !ok {
		return fmt.Errorf("key field %q not supported; use --keyNames to list valid fields", keyName)
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

	// Get default fields for genome object
	defaultFields := api.GetDefaultFields("genome")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Write output headers: input headers + genome fields
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "genome."+f)
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

		switch strategy {
		case keyBatch:
			// Uncommon keys: batch IN query, preserving input row association.
			// Build a map from key -> input rows (multiple rows may share a key).
			rowsByKey := make(map[string][][]string)
			for i, key := range keys {
				rowsByKey[key] = append(rowsByKey[key], rows[i])
			}

			query, err := dataOpts.BuildQueryWithFields(fields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.In(keyName, keys...)

			results, err := client.Query(ctx, "genome", query)
			if err != nil {
				return fmt.Errorf("querying genomes: %w", err)
			}

			// Output each result joined to its input row(s)
			for _, result := range results {
				// Determine which input key this result matches
				matchKey, _ := result[keyName].(string)
				inputRowList := rowsByKey[matchKey]
				if len(inputRowList) == 0 {
					// Key not found in input map (shouldn't happen); output one row without input
					var outRow []string
					for _, f := range fields {
						outRow = append(outRow, cli.FormatValue(result[f], delim))
					}
					if err := writer.WriteRow(outRow...); err != nil {
						return fmt.Errorf("writing row: %w", err)
					}
				} else {
					for _, inputRow := range inputRowList {
						var outRow []string
						outRow = append(outRow, inputRow...)
						for _, f := range fields {
							outRow = append(outRow, cli.FormatValue(result[f], delim))
						}
						if err := writer.WriteRow(outRow...); err != nil {
							return fmt.Errorf("writing row: %w", err)
						}
					}
				}
			}

		case keyPerVal:
			// Common keys: one Eq query per distinct key value.
			// Build map of key -> input rows
			rowsByKey := make(map[string][][]string)
			var keyOrder []string
			seen := make(map[string]bool)
			for i, key := range keys {
				rowsByKey[key] = append(rowsByKey[key], rows[i])
				if !seen[key] {
					seen[key] = true
					keyOrder = append(keyOrder, key)
				}
			}

			for _, key := range keyOrder {
				query, err := dataOpts.BuildQueryWithFields(fields)
				if err != nil {
					return fmt.Errorf("building query: %w", err)
				}
				query.Eq(keyName, key)

				inputRowList := rowsByKey[key]

				var queryErr error
				if dataOpts.Cursor {
					queryErr = client.QueryCallbackWithCursor(ctx, "genome", query, func(records []map[string]any, info *api.ChunkInfo) bool {
						for _, record := range records {
							for _, inputRow := range inputRowList {
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
						}
						return true
					})
					if queryErr != nil && strings.Contains(queryErr.Error(), "undefined field object") {
						return fmt.Errorf("%w\n\nNote: cursor-based pagination may not be supported by this API endpoint.\nTry using --api-url https://alpha.bv-brc.org/api or remove the --cursor flag", queryErr)
					}
				} else {
					queryErr = client.QueryCallback(ctx, "genome", query, func(records []map[string]any, info *api.ChunkInfo) bool {
						for _, record := range records {
							for _, inputRow := range inputRowList {
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
						}
						return true
					})
				}
				if queryErr != nil {
					return fmt.Errorf("querying genomes for %s=%q: %w", keyName, key, queryErr)
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
