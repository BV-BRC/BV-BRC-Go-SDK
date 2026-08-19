// Command rast-enumerate-classifiers lists the available kmer classifiers.
//
// A port of genome_annotation/scripts/rast-enumerate-classifiers.pl: one
// GenomeAnnotation service call, one classifier name per line. It reads no
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
		Use:   "rast-enumerate-classifiers [options]",
		Short: "List the available kmer classifiers",
		Long: `Enumerate the available kmer classifiers.

The names printed here are what rast-classify and rast-query-classifier-groups
take as their argument.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			classifiers, err := rastcli.NewClient(common).EnumerateClassifiers()
			if err != nil {
				return err
			}

			for _, c := range classifiers {
				fmt.Println(c)
			}
			return nil
		},
	}

	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
