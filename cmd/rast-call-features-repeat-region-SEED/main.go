// Command rast-call-features-repeat-region-SEED calls repeat regions.
//
// A port of genome_annotation/scripts/rast-call-features-repeat-region-SEED.pl:
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
		opts   *rastcli.RepeatRegion
	)

	rootCmd := &cobra.Command{
		Use:   "rast-call-features-repeat-region-SEED [options] < input > output",
		Short: "Call repeat regions",
		Long:  `Call repeat regions in the contigs of the given genome.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).CallFeaturesRepeatRegionSEED(genome, opts.Params())
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	opts = rastcli.AddRepeatRegionFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
