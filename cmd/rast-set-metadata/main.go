// Command rast-set-metadata sets metadata fields in a genome.
//
// A port of genome_annotation/scripts/rast-set-metadata.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
//
// Only the options actually given on the command line are sent, so an option
// left off is absent from the request rather than sent as a zero. That is the
// Perl `if defined` behaviour, and it is what decides which service-side
// defaults apply.
package main

import (
	"os"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common *rastcli.Common
		opts   *rastcli.GenomeMetadata
	)

	rootCmd := &cobra.Command{
		Use:   "rast-set-metadata [options] < input > output",
		Short: "Set metadata fields in the genome",
		Long:  `Set the given metadata fields in the genome object.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).SetMetadata(genome, opts.Params())
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	opts = rastcli.AddGenomeMetadataFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
