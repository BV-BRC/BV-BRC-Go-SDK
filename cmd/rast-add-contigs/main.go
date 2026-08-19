// Command rast-add-contigs adds contigs from a FASTA file to a genome.
//
// A port of genome_annotation/scripts/rast-add-contigs.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
package main

import (
	"errors"
	"os"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common  *rastcli.Common
		contigs *string
	)

	rootCmd := &cobra.Command{
		Use:   "rast-add-contigs [options] --contigs contigs.fa < input > output",
		Short: "Add contig data to the genome",
		Long:  `Add the given contig data to the genome object.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if *contigs == "" {
				return errors.New("a value for the --contigs flag must be provided")
			}

			data, err := rastcli.ReadContigsFasta(*contigs)
			if err != nil {
				return err
			}

			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			out, err := rastcli.NewClient(common).AddContigs(genome, data)
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	contigs = rastcli.AddContigsFlag(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
