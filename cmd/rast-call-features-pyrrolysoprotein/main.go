// Command rast-call-features-pyrrolysoprotein calls pyrrolysoproteins.
//
// A port of genome_annotation/scripts/rast-call-features-pyrrolysoprotein.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
package main

import (
	"os"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var common *rastcli.Common

	rootCmd := &cobra.Command{
		Use:   "rast-call-features-pyrrolysoprotein [options] < input > output",
		Short: "Call pyrrolysoproteins",
		Long:  `Call pyrrolysoproteins in the given genome.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).CallFeaturesPyrrolysoprotein(genome)
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
