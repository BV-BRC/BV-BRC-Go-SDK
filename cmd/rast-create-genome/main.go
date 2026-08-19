// Command rast-create-genome creates a new genome object.
//
// A port of genome_annotation/scripts/rast-create-genome.pl: make one or two
// GenomeAnnotation service calls and write the resulting genome typed object.
// It reads no genome, so it has --output but no --input.
package main

import (
	"fmt"
	"os"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common      = &rastcli.Common{}
		metadata    *rastcli.GenomeMetadata
		contigs     *string
		fromGenbank string
	)

	rootCmd := &cobra.Command{
		Use:   "rast-create-genome [options] > output",
		Short: "Create a new genome object",
		Long: `Create a new empty genome object.

If a GenBank file is given with --from-genbank, the genome object's data and
metadata are initialized from that file and the metadata options are not used.

Otherwise the RAST2 pipeline needs a minimum of metadata to work with:

  --scientific-name  minimally "Genus species", optionally with a strain. Some
                     pipeline components produce better results given an
                     accurate name.
  --domain           Bacteria or Archaea. Some components are more accurate
                     when the domain is right.
  --genetic-code     the DNA-to-protein translation table: 11 for Archaea, most
                     Bacteria, most viruses and some mitochondria; 4 for
                     Mycoplasma, Spiroplasma, Ureaplasma and fungal
                     mitochondria.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client := rastcli.NewClient(common)

			var (
				genome genomeannotation.GTO
				err    error
			)

			if fromGenbank != "" {
				data, readErr := os.ReadFile(fromGenbank)
				if readErr != nil {
					return fmt.Errorf("cannot open genbank file %s: %w", fromGenbank, readErr)
				}
				genome, err = client.CreateGenomeFromGenbank(string(data))
				if err != nil {
					return err
				}
			} else {
				genome, err = client.CreateGenome(metadata.Params())
				if err != nil {
					return err
				}

				// Only the metadata path adds contigs. The Perl checks
				// --contigs inside this branch, so --from-genbank --contigs
				// silently ignores the contigs file; keep that.
				if *contigs != "" {
					data, readErr := rastcli.ReadContigsFasta(*contigs)
					if readErr != nil {
						return readErr
					}
					genome, err = client.AddContigs(genome, data)
					if err != nil {
						return err
					}
				}
			}

			return rastcli.WriteOutput(genome, common)
		},
	}

	rootCmd.Flags().StringVar(&fromGenbank, "from-genbank", "", "Create from this genbank file")
	rastcli.AddOutputFlag(rootCmd, common)
	metadata = rastcli.AddGenomeMetadataFlags(rootCmd)
	contigs = rastcli.AddContigsFlag(rootCmd)
	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
