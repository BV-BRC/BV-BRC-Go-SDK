// Command p3-genus-species produces a genus/species list from the BV-BRC database.
//
// This command queries all public genomes and produces a two-column table listing
// each genus/species pair along with a count of how many genomes belong to each.
// Pseudo-species (those that begin with "sp.") are excluded, as are genera that
// start with a lowercase letter, contain "Candidatus", or start with "SAR".
//
// Usage:
//
//	p3-genus-species [options]
//
// Examples:
//
//	# List all genus/species pairs with counts
//	p3-genus-species
//
//	# Write output to a file
//	p3-genus-species -o genus_species.txt
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var ioOpts cli.IOOptions

var rootCmd = &cobra.Command{
	Use:   "p3-genus-species",
	Short: "List genus/species pairs with genome counts from BV-BRC",
	Long: `This script produces a two-column table listing each genus/species pair
in the BV-BRC database along with how many genomes belong to each.

Pseudo-species (those that begin with "sp.") are excluded. Genera that start
with a lowercase letter, contain "Candidatus", or start with "SAR" are also
excluded. The output is an orthodox list suitable for exhaustive species-by-species
analysis.

Output columns: genus, species, count

Examples:

  # List all genus/species pairs with counts
  p3-genus-species

  # Write output to a file
  p3-genus-species -o genus_species.txt`,
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	cli.AddIOFlags(rootCmd, &ioOpts)
}

// isValidGenus returns true if the genus name should be included.
// Mirrors the Perl logic:
//   - skip if genus starts with a lowercase letter
//   - skip if genus contains "Candidatus"
//   - skip if genus starts with "SAR"
func isValidGenus(genus string) bool {
	if genus == "" {
		return false
	}
	if strings.Contains(genus, "Candidatus") {
		return false
	}
	if strings.HasPrefix(genus, "SAR") {
		return false
	}
	// Skip genera that start with a lowercase letter
	runes := []rune(genus)
	if len(runes) > 0 && unicode.IsLower(runes[0]) {
		return false
	}
	return true
}

// isValidSpecies returns true if the species epithet should be included.
// Mirrors the Perl logic: skip if species contains "sp."
func isValidSpecies(species string) bool {
	if species == "" {
		return false
	}
	if strings.Contains(species, "sp.") {
		return false
	}
	return true
}

// cleanGenus removes non-word characters from the genus name (punctuation, etc.),
// mirroring the Perl: $genus =~ s/\W//g
func cleanGenus(genus string) string {
	var b strings.Builder
	for _, r := range genus {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
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
	client := api.NewClient(clientOpts...)

	// Build query: all public genomes, retrieve genome_name and genome_id
	query := api.NewQuery()
	query.Select("genome_name", "genome_id")
	query.Eq("public", "1")
	query.Required("genome_id")

	// Open output
	outFile, err := cli.OpenOutput(ioOpts.Output)
	if err != nil {
		return fmt.Errorf("opening output: %w", err)
	}
	defer outFile.Close()

	writer := cli.NewTabWriter(outFile)
	defer writer.Flush()

	// Write header
	if err := writer.WriteHeaders([]string{"genus", "species", "count"}); err != nil {
		return fmt.Errorf("writing headers: %w", err)
	}

	// Accumulate genus -> species -> count
	counts := make(map[string]map[string]int)
	processed := 0

	err = client.QueryCallback(ctx, "genome", query, func(records []map[string]any, info *api.ChunkInfo) bool {
		for _, record := range records {
			genomeName, _ := record["genome_name"].(string)
			processed++
			if processed%1000 == 0 {
				fmt.Fprintf(os.Stderr, "%d genomes processed.\n", processed)
			}
			if genomeName == "" {
				continue
			}

			// Split genome name into genus and species (first two words)
			parts := strings.Fields(genomeName)
			if len(parts) < 2 {
				continue
			}
			genus := parts[0]
			species := parts[1]

			// Remove punctuation from genus
			genus = cleanGenus(genus)

			// Validate genus and species
			if !isValidGenus(genus) {
				continue
			}
			if !isValidSpecies(species) {
				continue
			}

			// Accumulate count
			if counts[genus] == nil {
				counts[genus] = make(map[string]int)
			}
			counts[genus][species]++
		}
		return true // continue fetching
	})
	if err != nil {
		return fmt.Errorf("querying genomes: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Genomes retrieved.\n")

	// Output results sorted by genus then species
	genera := make([]string, 0, len(counts))
	for g := range counts {
		genera = append(genera, g)
	}
	sort.Strings(genera)

	for _, genus := range genera {
		speciesMap := counts[genus]
		speciesList := make([]string, 0, len(speciesMap))
		for s := range speciesMap {
			speciesList = append(speciesList, s)
		}
		sort.Strings(speciesList)

		for _, species := range speciesList {
			count := speciesMap[species]
			if err := writer.WriteRow(genus, species, fmt.Sprintf("%d", count)); err != nil {
				return fmt.Errorf("writing row: %w", err)
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
