// Command rast-process-genome runs a whole annotation pipeline on a genome.
//
// A port of genome_annotation/scripts/rast-process-genome.pl: fetch (or read) a
// workflow, hand the genome and the workflow to run_pipeline, and write the
// result -- as a genome typed object, or exported to another format.
//
// Two departures from the Perl, both of which it documented but never
// implemented:
//
//   - --batch-input-directory and --batch-input-file are not accepted. The Perl
//     declares them and never reads them, so a batch invocation silently ran in
//     immediate mode on standard input instead. Failing on an unknown flag says
//     so.
//   - --timeout is accepted and works. The Perl declares it and never reads it;
//     here it sets the HTTP timeout for the pipeline call, which is what a
//     pipeline long enough to outlive the 30-minute default needs. CDMI_TIMEOUT
//     still sets it for every rast-* command.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common        *rastcli.Common
		workflowPath  *string
		outputFormats []string
		timeout       int
	)

	rootCmd := &cobra.Command{
		Use:   "rast-process-genome [options] < input > output",
		Short: "Annotate a genome with the RAST2 pipeline",
		Long: `Annotate a bacterial genome using the RAST2 pipeline. Eukaryotic genomes are
not supported.

The genome is processed in real time; the command does not return until the
pipeline has completed. Its input is a genome typed object, which
rast-create-genome will build from contigs in FASTA format.

Unless --workflow names a file of your own, the service's default workflow is
used. rast-get-default-workflow prints it, so editing that output is the way to
build one.

` + rastcli.ExportFormatHelp(),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var extra []genomeannotation.Option
			if cmd.Flags().Changed("timeout") {
				extra = append(extra, genomeannotation.WithTimeout(time.Duration(timeout)*time.Second))
			}
			client := rastcli.NewClient(common, extra...)

			var workflow json.RawMessage
			if *workflowPath != "" {
				data, err := os.ReadFile(*workflowPath)
				if err != nil {
					return fmt.Errorf("could not read workflow file %s: %w", *workflowPath, err)
				}
				if !json.Valid(data) {
					return fmt.Errorf("workflow file %s does not contain valid JSON", *workflowPath)
				}
				workflow = json.RawMessage(data)
			} else {
				var err error
				workflow, err = client.DefaultWorkflow()
				if err != nil {
					return err
				}
			}

			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			result, err := client.RunPipeline(genome, workflow)
			if err != nil {
				return err
			}

			// --output-format is repeatable, but the Perl's loop over it ends
			// with an unconditional `last`, so only the first is ever written
			// -- and there is one output stream to write it to. Same here.
			format := "genome_object"
			if len(outputFormats) > 0 {
				format = outputFormats[0]
			}
			if format == "genome_object" {
				return rastcli.WriteOutput(result, common)
			}

			text, err := client.ExportGenome(result, format, nil)
			if err != nil {
				return err
			}
			return rastcli.WriteText(text, common)
		},
	}

	rootCmd.Flags().StringArrayVar(&outputFormats, "output-format", nil,
		"Output format. Defaults to genome_object")
	workflowPath = rastcli.AddWorkflowFlag(rootCmd)
	rootCmd.Flags().IntVar(&timeout, "timeout", 0,
		"Maximum time in seconds to wait for the pipeline to complete")
	common = rastcli.AddCommonFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
