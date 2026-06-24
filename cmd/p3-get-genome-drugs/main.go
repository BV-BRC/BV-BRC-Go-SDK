// Command p3-get-genome-drugs retrieves AMR drug data for genome IDs from stdin.
//
// This command reads genome IDs from the standard input and retrieves the
// corresponding anti-microbial resistance data from the BV-BRC database.
//
// Usage:
//
//	p3-get-genome-drugs [options] < genome_ids.txt
//
// Examples:
//
//	# Get all AMR data for genomes in a file
//	p3-get-genome-drugs < genome_ids.txt
//
//	# Filter for resistant drugs only
//	p3-get-genome-drugs --resistant < genome_ids.txt
//
//	# Filter for susceptible drugs only
//	p3-get-genome-drugs --susceptible < genome_ids.txt
//
//	# Get specific fields
//	p3-get-genome-drugs -a antibiotic -a resistant_phenotype < genome_ids.txt
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
	dataOpts    cli.DataOptions
	colOpts     cli.ColOptions
	ioOpts      cli.IOOptions
	resistant   bool
	susceptible bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-genome-drugs",
	Short: "Return AMR data for genome IDs from stdin",
	Long: `This script reads genome IDs from the standard input and returns
the anti-microbial resistance data for those genomes from the BV-BRC database.

The input should be tab-delimited with genome IDs in the specified column
(default: last column). The output includes the genome ID plus the
requested genome_drug data fields.

Examples:

  # Get all AMR data for genomes in a file
  p3-get-genome-drugs < genome_ids.txt

  # Filter for resistant drugs only
  p3-get-genome-drugs --resistant < genome_ids.txt

  # Filter for susceptible drugs only
  p3-get-genome-drugs --susceptible < genome_ids.txt

  # Get specific fields
  p3-get-genome-drugs -a antibiotic -a resistant_phenotype < genome_ids.txt`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVar(&resistant, "resistant", false,
		"filter for drugs to which the genome is resistant")
	rootCmd.Flags().BoolVar(&susceptible, "susceptible", false,
		"filter for drugs to which the genome is susceptible")
	// Aliases from Perl: --resist, --strong for resistant; --suscept, --weak for susceptible
	rootCmd.Flags().BoolVar(&resistant, "resist", false,
		"alias for --resistant")
	rootCmd.Flags().BoolVar(&resistant, "strong", false,
		"alias for --resistant")
	rootCmd.Flags().BoolVar(&susceptible, "suscept", false,
		"alias for --susceptible")
	rootCmd.Flags().BoolVar(&susceptible, "weak", false,
		"alias for --susceptible")
	// Mark aliases as hidden
	_ = rootCmd.Flags().MarkHidden("resist")
	_ = rootCmd.Flags().MarkHidden("strong")
	_ = rootCmd.Flags().MarkHidden("suscept")
	_ = rootCmd.Flags().MarkHidden("weak")
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
		fields, err := client.GetSchema(ctx, "genome_drug")
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

	// Get default fields for genome_drug object
	defaultFields := api.GetDefaultFields("genome_drug")
	fields := dataOpts.GetSelectFields(defaultFields)

	// Ensure genome_id is in the select list for output association
	hasGenomeID := false
	for _, f := range fields {
		if f == "genome_id" {
			hasGenomeID = true
			break
		}
	}
	selectFields := fields
	if !hasGenomeID {
		selectFields = append([]string{"genome_id"}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	} else {
		outputHeaders = append(outputHeaders, "genome.genome_id")
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "genome_drug."+f)
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

		// Build a map of genome_id -> input row for later association
		rowMap := make(map[string][]string)
		for i, key := range keys {
			rowMap[key] = rows[i]
		}

		// Standard query - one query per genome
		for _, key := range keys {
			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.Eq("genome_id", key)

			// Add special phenotype filters
			if resistant {
				query.Eq("resistant_phenotype", "Resistant")
			}
			if susceptible {
				query.Eq("resistant_phenotype", "Susceptible")
			}

			inputRow := rowMap[key]

			// Choose pagination method based on --cursor flag
			var queryErr error
			if dataOpts.Cursor {
				queryErr = client.QueryCallbackWithCursor(ctx, "genome_drug", query, func(records []map[string]any, info *api.ChunkInfo) bool {
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
				queryErr = client.QueryCallback(ctx, "genome_drug", query, func(records []map[string]any, info *api.ChunkInfo) bool {
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
				return fmt.Errorf("querying genome_drug for genome %s: %w", key, queryErr)
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
