// Command rast-call-features-rRNA-SEED calls rRNA features.
//
// A port of genome_annotation/scripts/rast-call-features-rRNA-SEED.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
//
// Selecting no RNA type means all of them: the Perl turns an empty selection
// into the single type "ALL" rather than sending an empty list, which the
// service would read as "call nothing".
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
		opts   *rastcli.RRNA
	)

	rootCmd := &cobra.Command{
		Use:   "rast-call-features-rRNA-SEED [options] < input > output",
		Short: "Call rRNA features",
		Long: `Call rRNA features in the contigs of the given genome.

If none of --call-5S, --call-SSU and --call-LSU is given, all rRNA types are
called.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).CallFeaturesRRNASEED(genome, opts.Types())
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	opts = rastcli.AddRRNAFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
