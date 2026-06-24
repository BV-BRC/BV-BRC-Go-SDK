// Command p3-get-genome-sp-genes retrieves specialty genes for genome IDs from stdin.
//
// This command reads genome IDs from the standard input and retrieves the
// corresponding specialty gene data from the BV-BRC database, filtered by the
// specified specialty gene type.
//
// Usage:
//
//	p3-get-genome-sp-genes [options] property < genome_ids.txt
//
// Examples:
//
//	# Get antibiotic resistance genes for a genome
//	p3-echo 83332.12 | p3-get-genome-sp-genes amr
//
//	# Get virulence factor genes
//	p3-get-genome-sp-genes virulence < genome_ids.txt
//
//	# List available specialty types
//	p3-get-genome-sp-genes --typeNames
//
//	# List available fields
//	p3-get-genome-sp-genes --fields
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	dataOpts  cli.DataOptions
	colOpts   cli.ColOptions
	ioOpts    cli.IOOptions
	typeNames bool
)

// spGeneTypes maps short type names to their full "property" field values in BV-BRC.
var spGeneTypes = map[string]string{
	"amr":         "Antibiotic Resistance",
	"human":       "Human Homolog",
	"target":      "Drug Target",
	"transporter": "Transporter",
	"virulence":   "Virulence Factor",
}

var rootCmd = &cobra.Command{
	Use:   "p3-get-genome-sp-genes property",
	Short: "Return specialty genes for genome IDs from stdin",
	Long: `This script reads genome IDs from the standard input and returns
specialty gene data for those genomes from the BV-BRC database,
filtered by the specified specialty gene type.

The positional argument is the type of specialty gene desired.
Use --typeNames to list available types.

The input should be tab-delimited with genome IDs in the specified column
(default: last column). The output includes the original input columns
plus the requested specialty gene data fields.

Specialty gene types:
  amr         Antibiotic Resistance
  human       Human Homolog
  target      Drug Target
  transporter Transporter
  virulence   Virulence Factor

Examples:

  # Get antibiotic resistance genes for a genome
  p3-echo 83332.12 | p3-get-genome-sp-genes amr

  # Get virulence factor genes with specific fields
  p3-get-genome-sp-genes -a patric_id -a gene -a product virulence < genome_ids.txt`,
	Args:         cobra.MaximumNArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVarP(&typeNames, "typeNames", "t", false,
		"list available specialty gene types and exit")
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
		fields, err := client.GetSchema(ctx, "sp_gene")
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

	// Handle --typeNames option
	if typeNames {
		keys := make([]string, 0, len(spGeneTypes))
		for k := range spGeneTypes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("%s\t%s\n", k, spGeneTypes[k])
		}
		return nil
	}

	// Require the positional argument (property type)
	if len(args) == 0 {
		return fmt.Errorf("a specialty gene type is required (use --typeNames to list them)")
	}
	typeName := args[0]
	propertyValue, ok := spGeneTypes[typeName]
	if !ok {
		return fmt.Errorf("invalid specialty type %q (use --typeNames to list valid types)", typeName)
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

	// Get default fields for sp_gene object
	defaultFields := api.GetDefaultFields("sp_gene")
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

	// Always fetch "property" so we can filter by type; add it if not already present
	hasProperty := false
	for _, f := range selectFields {
		if f == "property" {
			hasProperty = true
			break
		}
	}
	if !hasProperty {
		selectFields = append(selectFields, "property")
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	} else {
		outputHeaders = append(outputHeaders, "genome.genome_id")
	}
	for _, f := range fields {
		outputHeaders = append(outputHeaders, "sp_gene."+f)
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

		// Query one genome at a time (better for large result counts)
		for _, key := range keys {
			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.Eq("genome_id", key)

			inputRow := rowMap[key]

			queryErr := client.QueryCallback(ctx, "sp_gene", query, func(records []map[string]any, info *api.ChunkInfo) bool {
				for _, record := range records {
					// Filter by property value (specialty gene type)
					prop, _ := record["property"].(string)
					if prop != propertyValue {
						continue
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
				return fmt.Errorf("querying sp_genes for genome %s: %w", key, queryErr)
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
