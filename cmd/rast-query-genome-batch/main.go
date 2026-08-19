// Command rast-query-genome-batch reports on a submitted batch annotation job.
//
// A port of genome_annotation/scripts/rast-query-genome-batch.pl: one
// GenomeAnnotation service call, formatted as a table, as a human-readable
// report, as a summary, or as the raw JSON the service returned.
//
// The Perl loads Bio::KBase::HandleService but never calls it: the stdout,
// stderr and output handles are only ever formatted as a download URL, which is
// string work. So this needs no Shock client either.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliroot"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/rastcli"
	"github.com/spf13/cobra"
)

// handle is a Bio::KBase::HandleService handle, of which only the two fields
// that make up a download URL are read.
type handle struct {
	URL string `json:"url"`
	ID  string `json:"id"`
}

// URLString is Perl get_url: a handle becomes a download URL, and a missing
// handle becomes the empty string.
func (h *handle) URLString() string {
	if h == nil {
		return ""
	}
	return fmt.Sprintf("%s/node/%s?download", h.URL, h.ID)
}

type entry struct {
	GenomeID       string  `json:"genome_id"`
	Status         string  `json:"status"`
	CreationDate   string  `json:"creation_date"`
	StartDate      string  `json:"start_date"`
	CompletionDate string  `json:"completion_date"`
	Stdout         *handle `json:"stdout"`
	Stderr         *handle `json:"stderr"`
	Output         *handle `json:"output"`
	Filename       string  `json:"filename"`
}

type batchStatus struct {
	SubmitDate     string  `json:"submit_date"`
	StartDate      string  `json:"start_date"`
	CompletionDate string  `json:"completion_date"`
	Details        []entry `json:"details"`
}

func main() {
	var (
		common   = &rastcli.Common{}
		list     bool
		readable bool
		summary  bool
		raw      bool
	)

	rootCmd := &cobra.Command{
		Use:   "rast-query-genome-batch [options] [batch-id] [< batch-id-file]",
		Short: "Query the status of a batch annotation job",
		Long: `Query the status of a batch annotation job. If the batch-id is not given on the
command line, it is read from standard input.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := rastcli.NewClient(common)

			if list {
				batches, err := client.PipelineBatchEnumerateBatches()
				if err != nil {
					return err
				}
				for _, b := range batches {
					fmt.Printf("%s\t%s\n", b.ID, b.SubmitTime)
				}
				return nil
			}

			batchID, err := readBatchID(args)
			if err != nil {
				return err
			}

			data, err := client.PipelineBatchStatus(batchID)
			if err != nil {
				return err
			}

			if raw {
				pretty, err := rastcli.Pretty(data)
				if err != nil {
					return err
				}
				fmt.Println(string(pretty))
				return nil
			}

			var status batchStatus
			if err := json.Unmarshal(data, &status); err != nil {
				return fmt.Errorf("cannot interpret the status of batch %s: %w", batchID, err)
			}

			if readable || summary {
				printCounts(status)
			}
			if summary {
				return nil
			}

			for _, e := range status.Details {
				if readable {
					printReadable(e)
				} else {
					fmt.Println(strings.Join([]string{
						e.GenomeID, e.Status, e.CreationDate, e.CompletionDate,
						e.Stdout.URLString(), e.Stderr.URLString(), e.Output.URLString(),
						e.Filename,
					}, "\t"))
				}
			}
			return nil
		},
	}

	f := rootCmd.Flags()
	f.BoolVarP(&list, "list", "l", false, "List this user's submitted batches")
	f.BoolVarP(&readable, "readable", "r", false, "Show output in human-readable form")
	f.BoolVar(&summary, "summary", false, "Show summary of job status only")
	f.BoolVar(&raw, "raw", false, "Print the raw AWE status")
	rastcli.AddURLFlag(rootCmd, common)

	if err := cliroot.Execute(rootCmd); err != nil {
		os.Exit(1)
	}
}

// readBatchID takes the batch id from the command line, or from standard input
// when it is not there.
func readBatchID(args []string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("cannot read the batch id from standard input: %w", err)
	}
	line, _, _ := strings.Cut(string(data), "\n")
	id := strings.TrimSpace(line)
	if id == "" {
		return "", fmt.Errorf("no batch id given on the command line or on standard input")
	}
	return id, nil
}

func printCounts(status batchStatus) {
	counts := map[string]int{}
	for _, e := range status.Details {
		counts[e.Status]++
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Println("Status counts:")
	for _, name := range names {
		fmt.Printf("    %-14s %4d\n", name+":", counts[name])
	}
	fmt.Println()

	fmt.Printf("Batch submit time:     %s\n", status.SubmitDate)
	fmt.Printf("Batch start time:      %s\n", status.StartDate)
	fmt.Printf("Batch completion time: %s\n", status.CompletionDate)
	fmt.Println()
}

func printReadable(e entry) {
	fmt.Printf("%s:\n", e.GenomeID)
	fmt.Printf("    input filename:  %s\n", e.Filename)
	fmt.Printf("    status:          %s\n", e.Status)
	fmt.Printf("    creation date:   %s\n", e.CreationDate)
	fmt.Printf("    start date:      %s\n", e.StartDate)
	fmt.Printf("    completion date: %s\n", e.CompletionDate)
	if e.Stdout != nil {
		fmt.Printf("    stdout:          %s\n", e.Stdout.URLString())
	}
	if e.Stderr != nil {
		fmt.Printf("    stderr:          %s\n", e.Stderr.URLString())
	}
	if e.Output != nil {
		fmt.Printf("    output:          %s\n", e.Output.URLString())
	}
	fmt.Println()
}
