// Command p3-role-features retrieves features for role descriptions from stdin.
//
// This command reads role descriptions (functional role text) from the standard
// input and retrieves matching feature records from the BV-BRC database. It can
// optionally accept a file of genome IDs to which the features must belong.
//
// Because a feature's product field may contain multiple roles (e.g. "RoleA / RoleB"),
// the command performs checksum-based verification to ensure only features with
// the exact queried role are returned.
//
// Usage:
//
//	p3-role-features [options] < roles.txt
//
// Examples:
//
//	# Get features for roles listed in a file
//	p3-role-features < roles.txt
//
//	# Filter to specific genomes
//	p3-role-features --genomes genomes.txt < roles.txt
//
//	# Get specific feature fields
//	p3-role-features -a patric_id -a product -a genome_id < roles.txt
package main

import (
	"bufio"
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	dataOpts   cli.DataOptions
	colOpts    cli.ColOptions
	ioOpts     cli.IOOptions
	genomesFile string
	verboseFlag bool
)

// EC number pattern: (E.C. d.d.d.d) with optional spaces/colons
var ecPattern = regexp.MustCompile(`\(\s*E\.?C\.?(?:\s+|:)(\d\.(?:\d+|-)\.(?:\d+|-)\.(?:n?\d+|-))\s*\)`)

// TC number pattern: (T.C. d.A.d.d.d)
var tcPattern = regexp.MustCompile(`\(\s*T\.?C\.?(?:\s+|:)(\d\.[A-Z]\.(?:\d+|-)\.(?:\d+|-)\.(?:\d+|-)\s*)\)`)

// roleFixupPattern fixes misspellings like "hyphothetical"
var roleFixupPattern = regexp.MustCompile(`(?i)^\d{7}[a-z]\d{2}rik\b|\b(?:hyphothetical|hyothetical)\b`)

// extraSpacePattern compresses multiple whitespace/punctuation runs
var extraSpacePunct = regexp.MustCompile(`[\s,.:]{2,}`)

var rootCmd = &cobra.Command{
	Use:   "p3-role-features",
	Short: "Return features for role descriptions from stdin",
	Long: `This script reads role descriptions from the standard input and returns
matching feature records from the BV-BRC database.

The input should be tab-delimited with role descriptions in the specified column
(default: last column). The output includes the original input columns plus
the requested feature data fields.

Because a feature's product field may contain multiple roles, the command performs
checksum-based verification to ensure only features possessing the exact queried
role are returned.

Examples:

  # Get features for roles listed in a file
  p3-role-features < roles.txt

  # Filter to specific genomes
  p3-role-features --genomes genomes.txt < roles.txt

  # Get specific feature fields
  p3-role-features -a patric_id -a product -a genome_id < roles.txt`,
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddDataFlags(rootCmd, &dataOpts)
	cli.AddColFlags(rootCmd, &colOpts, 100)
	cli.AddIOFlags(rootCmd, &ioOpts)
	rootCmd.Flags().StringVarP(&genomesFile, "genomes", "G", "",
		"name of a file containing genome IDs in the first column")
	// Note: --verbose/-v is already added by AddDataFlags via dataOpts.Verbose.
	// We use dataOpts.Verbose directly rather than adding a duplicate flag.
}

// fixupRole applies spelling and whitespace normalizations to a role string.
// Mirrors RoleParse::FixupRole in Perl.
func fixupRole(role string) string {
	// Fix known misspellings
	role = roleFixupPattern.ReplaceAllStringFunc(role, func(m string) string {
		lm := strings.ToLower(m)
		if lm == "hyphothetical" || lm == "hyothetical" {
			return "hypothetical"
		}
		return "hypothetical"
	})
	// Replace carriage returns with spaces
	role = strings.ReplaceAll(role, "\r", " ")
	// Trim leading/trailing whitespace
	role = strings.TrimSpace(role)
	// Remove surrounding quotes
	role = strings.TrimPrefix(role, "\"")
	role = strings.TrimSuffix(role, "\"")
	// Compress whitespace runs
	role = strings.Join(strings.Fields(role), " ")
	return role
}

// textJoin joins two phrases: if the second starts with punctuation, concatenate
// directly; otherwise join with a space. Mirrors RoleParse::TextJoin.
func textJoin(a, b string) string {
	b = strings.TrimSpace(b)
	if b == "" {
		return strings.TrimSpace(a)
	}
	a = strings.TrimSpace(a)
	if len(b) > 0 && (b[0] == ',' || b[0] == '.' || b[0] == ';' || b[0] == ':') {
		return a + b
	}
	return a + " " + b
}

// parseRole extracts the main role text, stripping EC/TC numbers.
// Mirrors RoleParse::Parse in Perl.
func parseRole(role string) string {
	var roleText string
	if m := ecPattern.FindStringSubmatchIndex(role); m != nil {
		// m[0],m[1] = full match; m[2],m[3] = capture group 1 (EC number)
		before := role[:m[0]]
		after := role[m[1]:]
		roleText = textJoin(before, after)
	} else if m := tcPattern.FindStringSubmatchIndex(role); m != nil {
		before := role[:m[0]]
		after := role[m[1]:]
		roleText = textJoin(before, after)
	} else {
		roleText = role
	}
	roleText = fixupRole(roleText)
	return roleText
}

// normalizeRole normalizes a role for checksum computation.
// Mirrors RoleParse::Normalize in Perl.
func normalizeRole(role string) string {
	// Compress runs of whitespace/punctuation to a single space
	role = extraSpacePunct.ReplaceAllString(role, " ")
	// Translate unusual whitespace to regular space (Go strings.Map)
	role = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, role)
	return strings.ToLower(role)
}

// roleChecksum computes the MD5-base64 checksum of a role.
// Mirrors RoleParse::Checksum in Perl.
func roleChecksum(role string) string {
	roleText := parseRole(role)
	normalized := normalizeRole(roleText)
	// Perl uses encode_utf8 which is effectively UTF-8 encoding of the string.
	// Go strings are already UTF-8.
	sum := md5.Sum([]byte(normalized)) //nolint:gosec
	// Perl uses md5_base64 which is standard base64 without padding.
	return base64.StdEncoding.EncodeToString(sum[:])[:22] // drop trailing '=='
}

// rolesOfFunction splits a functional assignment string into individual roles.
// Mirrors SeedUtils::roles_of_function in Perl.
// Splits on " / ", " @ ", or "; " and removes comments (# or !).
func rolesOfFunction(function string) []string {
	// Remove comment (everything after # or !)
	commentFree := function
	for i, c := range function {
		if c == '#' || c == '!' {
			commentFree = strings.TrimRight(function[:i], " \t")
			break
		}
	}
	if commentFree == "" {
		return nil
	}
	// Split on " / ", " @ ", or "; " (with surrounding whitespace)
	splitRe := regexp.MustCompile(`\s+[/@]\s+|\s*;\s+`)
	parts := splitRe.Split(commentFree, -1)
	var roles []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			roles = append(roles, p)
		}
	}
	return roles
}

// cleanValue escapes special characters in a role for the BV-BRC query API.
// Mirrors P3Utils::clean_value in Perl (escapes Solr special chars).
func cleanValue(val string) string {
	// Characters that need escaping in Solr queries
	special := `\+-&|!(){}[]^"~*?:/`
	var sb strings.Builder
	for _, c := range val {
		if strings.ContainsRune(special, c) {
			sb.WriteRune('\\')
		}
		sb.WriteRune(c)
	}
	return sb.String()
}

// readGenomeIDs reads genome IDs from the first column of a tab-delimited file,
// skipping the header line. Mirrors the genome-file reading logic in the Perl script.
func readGenomeIDs(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open genome file %q: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	genomeH := make(map[string]bool)

	// Skip the header line
	if scanner.Scan() {
		// first line consumed as header
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) >= 1 && parts[0] != "" {
			genomeH[parts[0]] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return genomeH, nil
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	debug := dataOpts.Verbose

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
		schemaFields, err := client.GetSchema(ctx, "feature")
		if err != nil {
			return fmt.Errorf("getting schema: %w", err)
		}
		for _, f := range schemaFields {
			if f.MultiValued {
				fmt.Printf("%s (multi)\n", f.Name)
			} else {
				fmt.Println(f.Name)
			}
		}
		return nil
	}

	// Load genome filter if requested
	var genomeH map[string]bool
	if genomesFile != "" {
		var err error
		genomeH, err = readGenomeIDs(genomesFile)
		if err != nil {
			return err
		}
		if debug {
			fmt.Fprintf(os.Stderr, "%d genome IDs found in filter file.\n", len(genomeH))
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

	// We need genome_id and product in the select list for filtering/checksum.
	// Build selectFields that always includes genome_id and product.
	hasGenomeID := false
	hasProduct := false
	for _, f := range fields {
		if f == "genome_id" {
			hasGenomeID = true
		}
		if f == "product" {
			hasProduct = true
		}
	}
	selectFields := make([]string, 0, len(fields)+2)
	// Always prepend genome_id and product so we can filter; they won't appear
	// in the output columns unless the user asked for them.
	if !hasGenomeID {
		selectFields = append(selectFields, "genome_id")
	}
	if !hasProduct {
		selectFields = append(selectFields, "product")
	}
	selectFields = append(selectFields, fields...)

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

	// Process in batches from the reader, but query each role individually
	// (matching Perl: "we process the roles one at a time")
	for {
		keys, rows, err := reader.ReadBatch(colOpts.BatchSize, keyCol)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading batch: %w", err)
		}
		if len(keys) == 0 {
			break
		}

		if debug {
			fmt.Fprintf(os.Stderr, "%d roles found in batch.\n", len(keys))
		}

		for i, role := range keys {
			inputRow := rows[i]

			// Compute the role checksum for verification
			checksum := roleChecksum(role)

			// Clean the role for query
			role2 := cleanValue(role)
			if debug {
				fmt.Fprintf(os.Stderr, "Query for: %s.\n", role)
			}

			// Build query: eq product <role>, plus any user-specified filters.
			// We always select genome_id and product for filtering.
			query, err := dataOpts.BuildQueryWithFields(selectFields)
			if err != nil {
				return fmt.Errorf("building query: %w", err)
			}
			query.Eq("product", role2)

			// Execute query - collect all results
			results, err := client.Query(ctx, "feature", query)
			if err != nil {
				return fmt.Errorf("querying features for role %q: %w", role, err)
			}

			if debug {
				fmt.Fprintf(os.Stderr, "%d found for %s.\n", len(results), role)
			}

			count := 0
			gCount := 0
			rCount := 0

			for _, result := range results {
				genomeID, _ := result["genome_id"].(string)
				function, _ := result["product"].(string)

				// Filter by genome ID if requested
				if genomeH != nil && !genomeH[genomeID] {
					continue
				}
				gCount++

				// Check all roles within the function for checksum match
				foundRoles := rolesOfFunction(function)
				for _, foundR := range foundRoles {
					rCount++
					fcheck := roleChecksum(foundR)
					if fcheck == checksum {
						// Build output row: input row + feature fields
						var outRow []string
						outRow = append(outRow, inputRow...)
						for _, f := range fields {
							outRow = append(outRow, cli.FormatValue(result[f], delim))
						}
						if err := writer.WriteRow(outRow...); err != nil {
							return fmt.Errorf("writing row: %w", err)
						}
						count++
						break // only count this feature once even if role appears multiple times
					}
				}
			}

			if debug {
				fmt.Fprintf(os.Stderr, "%d features kept. %d found in genomes, %d roles checked.\n",
					count, gCount, rCount)
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
