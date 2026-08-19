// Command rast-update-functions updates feature functions from a file.
//
// A port of genome_annotation/scripts/rast-update-functions.pl:
// read a genome typed object as JSON, make one GenomeAnnotation service call,
// write the resulting genome typed object.
package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

func main() {
	var common *rastcli.Common

	rootCmd := &cobra.Command{
		Use:   "rast-update-functions [options] functions-file < input > output",
		Short: "Update feature functions",
		Long: `Update the functions of features in the genome.

The functions file has one assignment per line: a feature ID, whitespace, and
the function to assign to it. A line that does not have that form is reported
on standard error and skipped.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			functions, err := readFunctions(args[0])
			if err != nil {
				return err
			}

			genome, err := rastcli.LoadInput(common)
			if err != nil {
				return err
			}

			// The Perl sends an empty analysis event; the service fills in the
			// tool and timestamp itself.
			out, err := rastcli.NewClient(common).UpdateFunctions(genome, functions, genomeannotation.Params{})
			if err != nil {
				return err
			}

			return rastcli.WriteOutput(out, common)
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}

// assignment matches "<id><whitespace><function>", the Perl /^(\S+)\s+(.*)$/.
var assignment = regexp.MustCompile(`^(\S+)\s+(.*)$`)

func readFunctions(path string) ([]genomeannotation.FunctionAssignment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", path, err)
	}
	defer f.Close()

	var functions []genomeannotation.FunctionAssignment

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		m := assignment.FindStringSubmatch(scanner.Text())
		if m == nil {
			fmt.Fprintf(os.Stderr, "Cannot parse line %d\n", line)
			continue
		}
		functions = append(functions, genomeannotation.FunctionAssignment{
			FeatureID: m[1],
			Function:  m[2],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	return functions, nil
}
