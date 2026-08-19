// Command rast-call-features-CDS-glimmer3 calls CDS features with the Glimmer3 gene caller.
//
// A port of genome_annotation/scripts/rast-call-features-CDS-glimmer3.pl:
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
		opts   *rastcli.Glimmer3
	)

	rootCmd := &cobra.Command{
		Use:   "rast-call-features-CDS-glimmer3 [options] < input > output",
		Short: "Call CDS features using the Glimmer3 gene caller",
		Long:  `Call features from the contigs in the given genome using the Glimmer3 gene caller.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).CallFeaturesCDSGlimmer3(genome, opts.Params())
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	opts = rastcli.AddGlimmer3Flags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
