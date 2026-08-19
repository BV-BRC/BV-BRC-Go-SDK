// Command rast-create-genome-from-RAST creates a genome object from a RAST job.
//
// A port of genome_annotation/scripts/rast-create-genome-from-RAST.pl:
// make one GenomeAnnotation service call and write the resulting genome typed
// object. It reads no genome, so it has --output but no --input.
package main

import (
	"os"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	common := &rastcli.Common{}

	rootCmd := &cobra.Command{
		Use:   "rast-create-genome-from-RAST [options] job-number-or-genome-id > output",
		Short: "Create a genome object from a RAST job",
		Long:  `Create a genome object based on a RAST job.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out, err := rastcli.NewClient(common).CreateGenomeFromRAST(args[0])
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	rastcli.AddOutputFlag(rootCmd, common)
	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
