// Command rast-export-genome exports a genome in another format.
//
// A port of genome_annotation/scripts/rast-export-genome.pl: read a genome
// typed object as JSON, make one GenomeAnnotation service call, and write the
// formatted text the service returns -- text, not a genome, so the output is
// not re-indented.
package main

import (
	"fmt"
	"os"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common       *rastcli.Common
		featureTypes *[]string
	)

	rootCmd := &cobra.Command{
		Use:   "rast-export-genome [options] format < input > output",
		Short: "Export the genome in the given format",
		Long: `Export the given genome using the specified format.

` + rastcli.ExportFormatHelp(),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// The Perl lowercases the format and turns dashes into
			// underscores, so "GFF" and "feature-data" both work.
			format := rastcli.NormalizeExportFormat(args[0])
			if !rastcli.IsExportFormat(format) {
				return fmt.Errorf("invalid format %s; valid formats are %s",
					format, rastcli.ExportFormatNames())
			}

			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			text, err := rastcli.NewClient(common).ExportGenome(genome, format, *featureTypes)
			if err != nil {
				return err
			}

			return rastcli.WriteText(text, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	featureTypes = rastcli.AddExportFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
