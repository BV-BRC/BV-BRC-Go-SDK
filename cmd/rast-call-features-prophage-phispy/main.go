// Command rast-call-features-prophage-phispy calls prophage features with PhiSpy.
//
// A port of genome_annotation/scripts/rast-call-features-prophage-phispy.pl:
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
		Use:   "rast-call-features-prophage-phispy [options] < input > output",
		Short: "Call prophage features using PhiSpy",
		Long:  `Call prophage features in the given genome using PhiSpy.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).CallFeaturesProphagePhispy(genome)
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
