// Command rast-classify assigns DNA reads to bins with a kmer classifier.
//
// A port of genome_annotation/scripts/rast-classify.pl: read FASTA (not a
// genome typed object) from --input or standard input, make one
// GenomeAnnotation service call, and write "bin<TAB>count" lines ordered by
// descending count.
//
// Asking for either of the two extra output files switches the call from
// classify_into_bins to classify_full, which returns the detailed report and
// the unclassified read IDs alongside the bins.
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/seq"
	"github.com/spf13/cobra"
)

func main() {
	var (
		common *rastcli.Common
		opts   *rastcli.Classifier
	)

	rootCmd := &cobra.Command{
		Use:   "rast-classify [options] classifier < input > output",
		Short: "Classify DNA reads into bins",
		Long: `Classify the DNA sequences in the input FASTA using the given kmer classifier.

The output is one line per bin, "bin<TAB>count", ordered by descending count.
Use rast-enumerate-classifiers for the classifier names.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			classifier := args[0]

			// Open the extra output files first, as the Perl does: a
			// classification is slow, and an unwritable path should be
			// reported before rather than after it.
			var detailed, unclassified *os.File
			if opts.DetailedOutputFile != "" {
				f, err := os.Create(opts.DetailedOutputFile)
				if err != nil {
					return fmt.Errorf("cannot write %s: %w", opts.DetailedOutputFile, err)
				}
				defer f.Close()
				detailed = f
			}
			if opts.UnclassifiedOutputFile != "" {
				f, err := os.Create(opts.UnclassifiedOutputFile)
				if err != nil {
					return fmt.Errorf("cannot write %s: %w", opts.UnclassifiedOutputFile, err)
				}
				defer f.Close()
				unclassified = f
			}

			in, closeIn, err := rastcli.InputReader(common)
			if err != nil {
				return err
			}
			records, err := seq.ReadFasta(in)
			if err != nil {
				closeIn()
				return err
			}
			if err := closeIn(); err != nil {
				return err
			}

			dna := make([]genomeannotation.DNAInput, 0, len(records))
			for _, r := range records {
				dna = append(dna, genomeannotation.DNAInput{ID: r.ID, DNA: r.Seq})
			}

			client := rastcli.NewClient(common)

			var bins map[string]int
			if opts.Full() {
				var (
					raw  string
					unas []string
				)
				bins, raw, unas, err = client.ClassifyFull(classifier, dna)
				if err != nil {
					return err
				}
				if detailed != nil {
					if _, err := detailed.WriteString(raw); err != nil {
						return err
					}
				}
				if unclassified != nil {
					for _, id := range unas {
						if _, err := fmt.Fprintln(unclassified, id); err != nil {
							return err
						}
					}
				}
			} else {
				bins, err = client.ClassifyIntoBins(classifier, dna)
				if err != nil {
					return err
				}
			}

			names := make([]string, 0, len(bins))
			for name := range bins {
				names = append(names, name)
			}
			// Descending count, as in the Perl. The name breaks ties, which
			// the Perl leaves to hash order; equal counts otherwise come out
			// in a different order on every run.
			sort.Slice(names, func(i, j int) bool {
				if bins[names[i]] != bins[names[j]] {
					return bins[names[i]] > bins[names[j]]
				}
				return names[i] < names[j]
			})

			out, closeOut, err := rastcli.OutputWriter(common)
			if err != nil {
				return err
			}
			for _, name := range names {
				if _, err := fmt.Fprintf(out, "%s\t%d\n", name, bins[name]); err != nil {
					closeOut()
					return err
				}
			}
			return closeOut()
		},
	}

	common = rastcli.AddCommonFlags(rootCmd)
	opts = rastcli.AddClassifierFlags(rootCmd)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}
