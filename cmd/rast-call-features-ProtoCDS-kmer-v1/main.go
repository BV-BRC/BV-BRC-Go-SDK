// Command rast-call-features-ProtoCDS-kmer-v1 calls ProtoCDS features using the SEED version-1 kmers.
//
// A port of genome_annotation/scripts/rast-call-features-ProtoCDS-kmer-v1.pl:
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
		opts   *rastcli.KmerV1
	)

	rootCmd := &cobra.Command{
		Use:   "rast-call-features-ProtoCDS-kmer-v1 [options] < input > output",
		Short: "Call ProtoCDS features using the SEED version-1 kmers",
		Long:  `Call ProtoCDS features from the contigs in the given genome using the SEED version-1 kmers.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).CallFeaturesProtoCDSKmerV1(genome, opts.Params())
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	opts = rastcli.AddKmerV1Flags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
