// Command rast-export-SEED writes a genome out as a SEED directory.
//
// A port of genome_annotation/scripts/rast-export-SEED.pl. It is the only
// rast-* tool that calls no service: the whole of the work is
// GenomeTypeObject::write_seed_dir, ported in internal/seeddir. --url is
// accepted because CmdHelper's option list gives it to every rast-* script, and
// dropping it would break a command line that has always been legal; nothing
// reads it here.
//
// The Perl's POD lists the options of rast-export-genome -- --output,
// --feature-type and a list of formats. That is a copy-paste error in the
// script: its actual option list is options_genome_in() + options_help(), which
// is what is implemented here.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/seeddir"
	"github.com/spf13/cobra"
)

func main() {
	common := &rastcli.Common{}

	rootCmd := &cobra.Command{
		Use:   "rast-export-SEED [options] output-directory < input",
		Short: "Export a genome as a SEED directory",
		Long: `Export the given genome as a SEED directory.

The directory is created if it does not exist. It is written with CDS features
renamed to peg, as the SEED expects.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			raw, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			var genome seeddir.Genome
			if err := json.Unmarshal(raw, &genome); err != nil {
				return fmt.Errorf("input is not a genome typed object: %w", err)
			}

			return seeddir.Write(args[0], &genome, seeddir.Options{MapCDSToPeg: true})
		},
	}

	rastcli.AddInputFlag(rootCmd, common)
	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
