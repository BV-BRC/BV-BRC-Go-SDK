// Command rast-get-default-workflow prints the default annotation workflow.
//
// A port of genome_annotation/scripts/rast-get-default-workflow.pl: one
// GenomeAnnotation service call, pretty-printed JSON out. Edit what it prints
// and hand it back with rast-process-genome --workflow to run a pipeline of
// your own. It reads no genome, so it has --output but no --input.
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
		Use:   "rast-get-default-workflow [options] > output",
		Short: "Retrieve the default annotation workflow",
		Long:  `Retrieve the default RAST2 annotation workflow.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			workflow, err := rastcli.NewClient(common).DefaultWorkflow()
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(workflow, common)
		},
	}

	rastcli.AddOutputFlag(rootCmd, common)
	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
