// Command rast-compute-special-proteins finds specialty proteins in a genome.
//
// A port of genome_annotation/scripts/rast-compute-special-proteins.pl: read a
// genome typed object as JSON, make one GenomeAnnotation service call, and
// write the hits as tab-delimited text. It writes no genome, so the input
// genome is left untouched.
package main

import (
	"fmt"
	"os"
	"strings"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common = &rastcli.Common{}
		dbs    []string
	)

	rootCmd := &cobra.Command{
		Use:   "rast-compute-special-proteins [options] < input > output",
		Short: "Compute specialty protein instances",
		Long: `Compute the instances of the specialty proteins in the given genome.

Output is tab-delimited, one hit per line. Use rast-enumerate-special-protein-databases
to list the databases --db accepts.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			hits, err := rastcli.NewClient(common).ComputeSpecialProteins(genome, dbs)
			if err != nil {
				return err
			}

			out, closeOut, err := rastcli.OutputWriter(common)
			if err != nil {
				return err
			}
			for _, hit := range hits {
				if _, err := fmt.Fprintln(out, strings.Join(hit.Fields(), "\t")); err != nil {
					closeOut()
					return err
				}
			}
			return closeOut()
		},
	}

	rootCmd.Flags().StringArrayVar(&dbs, "db", nil,
		"Database name to search (option may be repeated). Defaults to all available databases")
	rastcli.AddInputFlag(rootCmd, common)
	rastcli.AddOutputFlag(rootCmd, common)
	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
