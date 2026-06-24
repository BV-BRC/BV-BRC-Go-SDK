// Command p3-get-subsystem-roles retrieves the roles of subsystems from the BV-BRC database.
//
// This command reads subsystem IDs (or names) from the standard input and appends
// the subsystem's roles. There will be multiple output rows per input row since each
// subsystem has multiple roles.
//
// Usage:
//
//	p3-get-subsystem-roles [options] < subsystem_ids.txt
//
// Examples:
//
//	# Get roles for subsystem IDs
//	p3-get-subsystem-roles < subsystem_ids.txt
//
//	# Input contains subsystem names (spaces) instead of IDs (underscores)
//	p3-get-subsystem-roles --names < subsystem_names.txt
//
//	# List available fields
//	p3-get-subsystem-roles --fields
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
	useNames bool
)

var rootCmd = &cobra.Command{
	Use:   "p3-get-subsystem-roles",
	Short: "Return the roles of subsystems from the BV-BRC database",
	Long: `This script reads subsystem IDs or names from the standard input and appends
the subsystem's roles. There will always be multiple roles per subsystem, so each
input line will produce more than one output line.

The input should be tab-delimited with subsystem IDs (or names if --names is
specified) in the specified column (default: last column).

Examples:

  # Get roles for subsystem IDs
  p3-get-subsystem-roles < subsystem_ids.txt

  # Input contains subsystem names (spaces) instead of IDs (underscores)
  p3-get-subsystem-roles --names < subsystem_names.txt

  # List available fields
  p3-get-subsystem-roles --fields`,
	RunE:         run,
	SilenceUsage: true, // Don't print usage on runtime errors
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().BoolVarP(&useNames, "names", "N", false,
		"input contains subsystem names (with spaces) instead of IDs (with underscores)")
}

// encodeSubsystemID encodes special characters in a subsystem ID for use in a query,
// mirroring the Perl script's %encode table.
func encodeSubsystemID(id string) string {
	var buf strings.Builder
	for _, ch := range id {
		switch ch {
		case '<':
			buf.WriteString("%60")
		case '=':
			buf.WriteString("%61")
		case '>':
			buf.WriteString("%62")
		case '"':
			buf.WriteString("%34")
		case '#':
			buf.WriteString("%35")
		case '%':
			buf.WriteString("%37")
		case '+':
			buf.WriteString("%43")
		case '/':
			buf.WriteString("%47")
		case ':':
			buf.WriteString("%58")
		case '{':
			buf.WriteString("%7B")
		case '|':
			buf.WriteString("%7C")
		case '}':
			buf.WriteString("%7D")
		case '^':
			buf.WriteString("%94")
		case '`':
			buf.WriteString("%96")
		case '&':
			buf.WriteString("%26")
		case '\'':
			buf.WriteString("%27")
		case '(':
			buf.WriteString("%28")
		case ')':
			buf.WriteString("%29")
		case ',':
			buf.WriteString("%2C")
		default:
			buf.WriteRune(ch)
		}
	}
	return buf.String()
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
		fields, err := client.GetSchema(ctx, "subsystem")
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

	// Write output headers: original input headers + "role"
	var outputHeaders []string
	if inputHeaders != nil {
		outputHeaders = append(outputHeaders, inputHeaders...)
	}
	outputHeaders = append(outputHeaders, "role")
	if err := writer.WriteHeaders(outputHeaders); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	// Process in batches
	for {
		keys, rows, err := reader.ReadBatch(colOpts.BatchSize, keyCol)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading batch: %w", err)
		}
		if len(keys) == 0 {
			break
		}

		// Convert names to IDs if --names flag is set (spaces -> underscores)
		if useNames {
			for i, key := range keys {
				keys[i] = strings.ReplaceAll(key, " ", "_")
			}
		}

		// Encode special characters in the subsystem IDs
		encodedKeys := make([]string, len(keys))
		for i, key := range keys {
			encodedKeys[i] = encodeSubsystemID(key)
		}

		// Build a map from encoded key -> original input row
		rowMap := make(map[string][]string)
		for i, key := range encodedKeys {
			rowMap[key] = rows[i]
		}

		// Query each subsystem individually to preserve input order
		for i, key := range encodedKeys {
			inputRow := rows[i]

			// Build query requesting role_name field
			selectFields := []string{"subsystem_id", "role_name"}
			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.Eq("subsystem_id", key)

			results, err := client.Query(ctx, "subsystem", query)
			if err != nil {
				return fmt.Errorf("querying subsystem %s: %w", key, err)
			}

			// Each subsystem result has a role_name array; expand into one row per role
			for _, result := range results {
				roles, ok := result["role_name"]
				if !ok {
					continue
				}

				switch rv := roles.(type) {
				case []interface{}:
					for _, role := range rv {
						roleStr, _ := role.(string)
						var outRow []string
						outRow = append(outRow, inputRow...)
						outRow = append(outRow, roleStr)
						if err := writer.WriteRow(outRow...); err != nil {
							return fmt.Errorf("writing row: %w", err)
						}
					}
				case []string:
					for _, roleStr := range rv {
						var outRow []string
						outRow = append(outRow, inputRow...)
						outRow = append(outRow, roleStr)
						if err := writer.WriteRow(outRow...); err != nil {
							return fmt.Errorf("writing row: %w", err)
						}
					}
				case string:
					// Single value returned as string
					var outRow []string
					outRow = append(outRow, inputRow...)
					outRow = append(outRow, rv)
					if err := writer.WriteRow(outRow...); err != nil {
						return fmt.Errorf("writing row: %w", err)
					}
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
