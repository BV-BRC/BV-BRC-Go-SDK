// Command p3-get-family-features retrieves features for protein family IDs from stdin.
//
// This command reads protein family IDs from the standard input and retrieves the
// corresponding features from the BV-BRC database. The family type determines
// which ID field is used for filtering (plfam_id, pgfam_id, or figfam_id).
//
// Usage:
//
//	p3-get-family-features [options] < family_ids.txt
//
// Examples:
//
//	# Get features for local protein family IDs
//	p3-get-family-features < family_ids.txt
//
//	# Get features for global protein families
//	p3-get-family-features --ftype global < family_ids.txt
//
//	# Filter results to specific genomes
//	p3-get-family-features --gFile genomes.txt < family_ids.txt
//
//	# Get specific fields
//	p3-get-family-features -a patric_id -a product -a aa_length < family_ids.txt
package main

import (
	"bufio"
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

// famTypeMap maps family type names to the feature field used for filtering.
var famTypeMap = map[string]string{
	"local":   "plfam_id",
	"plfam":   "plfam_id",
	"global":  "pgfam_id",
	"pgfam":   "pgfam_id",
	"figfam":  "figfam_id",
	"fig":     "figfam_id",
}

var (
	dataOpts  cli.DataOptions
	colOpts   cli.ColOptions
	ioOpts    cli.IOOptions
	gFile     string
	gCol      string
	ftype     string
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-family-features",
	Short: "Return features for protein family IDs from stdin",
	Long: `This script reads protein family IDs from the standard input and returns
the features belonging to those families from the BV-BRC database.

The input should be tab-delimited with family IDs in the specified column
(default: last column). The output includes the family ID plus the
requested feature data fields.

The --ftype option controls which family ID field is used:
  local   (default) - plfam_id (BV-BRC local protein families)
  global  or pgfam  - pgfam_id (BV-BRC global protein families)
  figfam  or fig    - figfam_id (FIGfam families)

Examples:

  # Get features for local protein family IDs
  p3-get-family-features < family_ids.txt

  # Get features for global protein families
  p3-get-family-features --ftype global < family_ids.txt

  # Filter results to specific genomes
  p3-get-family-features --gFile genomes.txt < family_ids.txt

  # Get specific fields
  p3-get-family-features -a patric_id -a product -a aa_length < family_ids.txt`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().StringVar(&gFile, "gFile", "",
		"name of a file containing genome IDs to filter results")
	rootCmd.Flags().StringVar(&gCol, "gCol", "genome.genome_id",
		"index (1-based) or header name of the genome ID column in the genome file")
	rootCmd.Flags().StringVar(&ftype, "ftype", "local",
		"type of protein family: local (plfam_id), global/pgfam (pgfam_id), figfam/fig (figfam_id)")
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

	// Resolve family type to filter field name
	famField, ok := famTypeMap[strings.ToLower(ftype)]
	if !ok {
		return fmt.Errorf("invalid protein family type %q (valid: local, global, pgfam, figfam, fig, plfam)", ftype)
	}

	// If a genome file was specified, load the genome IDs from it
	var genomeIDs []string
	if gFile != "" {
		var err error
		genomeIDs, err = readGenomeFile(gFile, gCol)
		if err != nil {
			return fmt.Errorf("reading genome file: %w", err)
		}
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

	// Ensure the family field is in the select list for result association
	hasFamField := false
	for _, f := range fields {
		if f == famField {
			hasFamField = true
			break
		}
	}
	selectFields := fields
	if !hasFamField {
		selectFields = append([]string{famField}, fields...)
	}

	// Write output headers
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	} else {
		outputHeaders = append(outputHeaders, "family."+famField)
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

		// Build a map of family_id -> input row for later association
		rowMap := make(map[string][]string)
		for i, key := range keys {
			rowMap[key] = rows[i]
		}

		// Build query filtering by the batch of family IDs
		query, err := dataOpts.BuildQueryWithFields(selectFields)
		if err != nil {
			return fmt.Errorf("building query: %w", err)
		}
		query.In(famField, keys...)

		// If genome IDs were specified, add a filter for them
		if len(genomeIDs) > 0 {
			query.In("genome_id", genomeIDs...)
		}

		// Execute the query (use cursor or standard pagination)
		var queryErr error
		if dataOpts.Cursor {
			queryErr = client.QueryCallbackWithCursor(ctx, "feature", query, func(records []map[string]any, info *api.ChunkInfo) bool {
				for _, record := range records {
					famID, _ := record[famField].(string)
					inputRow := rowMap[famID]
					if inputRow == nil {
						inputRow = []string{famID}
					}

					var outRow []string
					outRow = append(outRow, inputRow...)
					for _, f := range fields {
						outRow = append(outRow, cli.FormatValue(record[f], delim))
					}

					if writeErr := writer.WriteRow(outRow...); writeErr != nil {
						fmt.Fprintf(os.Stderr, "Error writing row: %v\n", writeErr)
						return false
					}
				}
				return true
			})
			if queryErr != nil && strings.Contains(queryErr.Error(), "undefined field object") {
				return fmt.Errorf("%w\n\nNote: cursor-based pagination may not be supported by this API endpoint.\nTry using --api-url https://alpha.bv-brc.org/api or remove the --cursor flag", queryErr)
			}
		} else {
			results, err := client.Query(ctx, "feature", query)
			if err != nil {
				return fmt.Errorf("querying features: %w", err)
			}

			for _, record := range results {
				famID, _ := record[famField].(string)
				inputRow := rowMap[famID]
				if inputRow == nil {
					inputRow = []string{famID}
				}

				var outRow []string
				outRow = append(outRow, inputRow...)
				for _, f := range fields {
					outRow = append(outRow, cli.FormatValue(record[f], delim))
				}

				if writeErr := writer.WriteRow(outRow...); writeErr != nil {
					return fmt.Errorf("writing row: %w", writeErr)
				}
			}
		}
		if queryErr != nil {
			return fmt.Errorf("querying features: %w", queryErr)
		}
	}

	return nil
}

// readGenomeFile reads genome IDs from a tab-delimited file.
// colSpec can be a 1-based column index or a header name.
func readGenomeFile(path string, colSpec string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open genome file %q: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	// Read first line to determine column
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		return nil, nil // empty file
	}
	firstLine := scanner.Text()
	headers := strings.Split(firstLine, "\t")

	// Resolve column index
	colIdx := -1
	// Try numeric first
	if len(colSpec) > 0 {
		n := 0
		isNum := true
		for _, c := range colSpec {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			} else {
				isNum = false
				break
			}
		}
		if isNum && n > 0 {
			colIdx = n - 1 // convert 1-based to 0-based
		} else {
			// Search headers for matching name
			for i, h := range headers {
				if h == colSpec {
					colIdx = i
					break
				}
			}
			if colIdx == -1 {
				// Default column name may include prefix like "genome.genome_id"
				// Try stripping prefix
				bare := colSpec
				if idx := strings.LastIndex(colSpec, "."); idx >= 0 {
					bare = colSpec[idx+1:]
				}
				for i, h := range headers {
					if h == bare {
						colIdx = i
						break
					}
				}
			}
			if colIdx == -1 {
				// If no header match, treat as last column of first data row
				colIdx = len(headers) - 1
			}
		}
	} else {
		colIdx = len(headers) - 1
	}

	// Collect IDs: if first line looks like a header (not a genome ID), skip it.
	// Heuristic: if the value in the resolved column contains a dot, it's a genome ID;
	// otherwise treat the first line as a header and skip it.
	var ids []string
	firstVal := ""
	if colIdx < len(headers) {
		firstVal = headers[colIdx]
	}
	// A genome ID typically looks like "83332.12" (has a dot)
	// A header name like "genome.genome_id" also has a dot, so check if it starts
	// with a digit as a stronger heuristic.
	if len(firstVal) > 0 && firstVal[0] >= '0' && firstVal[0] <= '9' {
		ids = append(ids, firstVal)
	}
	// else: first line is a header, already consumed — continue reading data

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if colIdx < len(parts) && parts[colIdx] != "" {
			ids = append(ids, parts[colIdx])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
