// Command rast-enumerate-special-protein-databases lists the specialty protein
// databases the service can search.
//
// A port of
// genome_annotation/scripts/rast-enumerate-special-protein-databases.pl:
// one GenomeAnnotation service call, one database name per line. It reads no
// genome and writes no genome, so --url is its only option.
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
	common := &rastcli.Common{}

	rootCmd := &cobra.Command{
		Use:   "rast-enumerate-special-protein-databases [options]",
		Short: "List the available specialty protein databases",
		Long: `Enumerate the available specialty protein databases.

The names printed here are what rast-compute-special-proteins --db accepts.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbs, err := rastcli.NewClient(common).EnumerateSpecialProteinDatabases()
			if err != nil {
				return err
			}

			for _, db := range dbs {
				fmt.Println(db)
			}
			return nil
		},
	}

	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
