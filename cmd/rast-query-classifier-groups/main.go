// Command rast-query-classifier-groups lists the groups a classifier assigns to.
//
// A port of genome_annotation/scripts/rast-query-classifier-groups.pl: one
// GenomeAnnotation service call, one group per line, tab-delimited. It reads no
// genome and writes no genome, so --url is its only option.
package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

// groupNumber matches the digits in a name like "group12", which is how the
// group names sort: numerically when both names carry a number, by name
// otherwise.
var groupNumber = regexp.MustCompile(`group(\d+)`)

func main() {
	common := &rastcli.Common{}

	rootCmd := &cobra.Command{
		Use:   "rast-query-classifier-groups [options] classifier-name",
		Short: "List the groups a kmer classifier can assign to",
		Long: `List the groups defined by the given kmer classifier, and the genomes in each.

Each line is the group name followed by its genome IDs, tab-delimited. Use
rast-enumerate-classifiers for the classifier names.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			groups, err := rastcli.NewClient(common).QueryClassifierGroups(args[0])
			if err != nil {
				return err
			}

			names := make([]string, 0, len(groups))
			for name := range groups {
				names = append(names, name)
			}
			sort.Slice(names, func(i, j int) bool {
				a, aok := number(names[i])
				b, bok := number(names[j])
				if aok && bok {
					return a < b
				}
				return names[i] < names[j]
			})

			for _, name := range names {
				fields := append([]string{name}, groups[name]...)
				fmt.Println(strings.Join(fields, "\t"))
			}
			return nil
		},
	}

	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}

func number(name string) (int, bool) {
	m := groupNumber.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}
