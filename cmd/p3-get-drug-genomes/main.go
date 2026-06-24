// Command p3-get-drug-genomes retrieves AMR genome data for drug names from stdin.
//
// This command reads drug names from the standard input and retrieves the
// corresponding anti-microbial resistance data from the BV-BRC database.
//
// Usage:
//
//	p3-get-drug-genomes [options] < drug_names.txt
//
// Examples:
//
//	# Get all AMR data for drugs in a file
//	p3-get-drug-genomes < drug_names.txt
//
//	# Filter for resistant genomes only
//	p3-get-drug-genomes --resistant < drug_names.txt
//
//	# Filter for susceptible genomes only
//	p3-get-drug-genomes --susceptible < drug_names.txt
//
//	# Get specific fields
//	p3-get-drug-genomes -a genome_id -a resistant_phenotype < drug_names.txt
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
	Use:   "p3-get-drug-genomes",
	Short: "Return AMR genome data for drug names from stdin",
	Long: `This script reads drug names from the standard input and returns
the anti-microbial resistance data for those drugs from the BV-BRC database.

The input should be tab-delimited with drug names in the specified column
(default: last column). The output includes the drug name plus the
requested genome_drug data fields.

Examples:

  # Get all AMR data for drugs in a file
  p3-get-drug-genomes < drug_names.txt

  # Filter for resistant genomes only
  p3-get-drug-genomes --resistant < drug_names.txt

  # Filter for susceptible genomes only
  p3-get-drug-genomes --susceptible < drug_names.txt

  # Get specific fields
  p3-get-drug-genomes -a genome_id -a resistant_phenotype < drug_names.txt`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVar(&resistant, "resistant", false,
		"filter for genomes resistant to the drug")
	rootCmd.Flags().BoolVar(&susceptible, "susceptible", false,
		"filter for genomes susceptible to the drug")
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

	// Ensure antibiotic_name is in the select list for output association
	hasAntibioticName := false
	for _, f := range fields {
		if f == "antibiotic_name" {
			hasAntibioticName = true
			break
		}
	}
	selectFields := fields
	if !hasAntibioticName {
		selectFields = append([]string{"antibiotic_name"}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	} else {
		outputHeaders = append(outputHeaders, "drug.antibiotic_name")
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

		// Build a map of antibiotic_name -> input row for later association
		rowMap := make(map[string][]string)
		for i, key := range keys {
			rowMap[key] = rows[i]
		}

		// Standard query - one query per drug name
		for _, key := range keys {
			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.Eq("antibiotic_name", key)

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
				return fmt.Errorf("querying genome_drug for drug %s: %w", key, queryErr)
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
