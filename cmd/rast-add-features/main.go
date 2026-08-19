// Command rast-add-features adds features listed in a tab-delimited file.
//
// A port of genome_annotation/scripts/rast-add-features.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var common *rastcli.Common

	rootCmd := &cobra.Command{
		Use:   "rast-add-features [options] features-file < input > output",
		Short: "Add a set of features to the genome",
		Long: `Add a set of features to the genome.

The features file is tab-delimited with one feature per line and five columns:

    id    location    feature-type    function    aliases

The location is a comma-separated list of contig_begin_strand_length parts.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			features, err := readFeatures(args[0])
			if err != nil {
				return err
			}

			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).AddFeatures(genome, features)
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}

// readFeatures parses the tab-delimited feature file.
//
// Columns beyond the fifth are ignored and missing ones are empty, matching a
// Perl list assignment from split -- except that Perl leaves a missing column
// undef, which encodes as JSON null. The service declares all five as strings,
// so an empty string is sent instead.
func readFeatures(path string) ([]genomeannotation.CompactFeature, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", path, err)
	}
	defer f.Close()

	var features []genomeannotation.CompactFeature

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		fields := strings.Split(strings.TrimRight(scanner.Text(), "\r\n"), "\t")
		var col [5]string
		copy(col[:], fields)

		features = append(features, genomeannotation.CompactFeature{
			ID:       col[0],
			Location: col[1],
			Type:     col[2],
			Function: col[3],
			Aliases:  col[4],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	return features, nil
}
