// Command rast-resolve-overlapping-features resolves overlapping features.
//
// A port of genome_annotation/scripts/rast-resolve-overlapping-features.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
//
// The Perl declares no options for this step and sends an empty parameter
// object; so does this, rather than sending null, because the service reads
// fields out of it.
package main

import (
	"os"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var common *rastcli.Common

	rootCmd := &cobra.Command{
		Use:   "rast-resolve-overlapping-features [options] < input > output",
		Short: "Resolve overlapping features",
		Long:  `Resolve overlapping features in this genome.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).ResolveOverlappingFeatures(genome, genomeannotation.Params{})
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
