// Command p3-fasta-md5 computes the whole-sequence MD5 checksum from a FASTA file.
//
// Usage:
//
//	p3-fasta-md5 [options]
//
// Examples:
//
//	p3-fasta-md5 < genome.fasta
//	p3-fasta-md5 -i genome.fasta
package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

var (
	inputFile  string
	outputFile string
)

var rootCmd = &cobra.Command{
	Use:   "p3-fasta-md5 [options]",
	Short: "Compute whole-sequence MD5 checksum from a FASTA file",
	Long: `This script computes the whole-genome MD5 checksum from a genome's FASTA file.
This can be used to determine if two genomes have identical DNA.

The algorithm:
  1. For each contig, compute MD5 of the upper-cased concatenated sequence.
  2. Sort all contig MD5 values, join with commas, compute MD5 of the result.

The single genome MD5 is written to standard output.

Examples:

  # Compute whole-genome MD5 from stdin
  p3-fasta-md5 < genome.fasta

  # Compute whole-genome MD5 from a file
  p3-fasta-md5 -i genome.fasta`,
	Args: cobra.NoArgs,
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&inputFile, "input", "i", "", "input FASTA file (default: stdin)")
	rootCmd.Flags().StringVarP(&outputFile, "output", "o", "", "output file (default: stdout)")
}

// md5Hex returns the hex-encoded MD5 of the given byte slice.
func md5Hex(data []byte) string {
	h := md5.Sum(data)
	return hex.EncodeToString(h[:])
}

func run(cmd *cobra.Command, args []string) error {
	// Open input
	var in io.Reader
	if inputFile != "" && inputFile != "-" {
		f, err := os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("opening input: %w", err)
		}
		defer f.Close()
		in = f
	} else {
		in = os.Stdin
	}

	// Open output
	var out io.Writer
	if outputFile != "" && outputFile != "-" {
		f, err := os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("opening output: %w", err)
		}
		defer f.Close()
		out = f
	} else {
		out = os.Stdout
	}

	// contigs maps contig ID -> MD5 of its uppercase sequence
	contigs := make(map[string]string)

	var currentID string
	var seqBuf strings.Builder

	closeContig := func() {
		if currentID == "" {
			return
		}
		seqMD5 := md5Hex([]byte(seqBuf.String()))
		contigs[currentID] = seqMD5
		currentID = ""
		seqBuf.Reset()
	}

	scanner := bufio.NewScanner(in)
	// Allow long sequence lines (default 64KB may be too small for some FASTA files)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		// Strip carriage return if present (Windows line endings)
		line = strings.TrimRight(line, "\r")

		if strings.HasPrefix(line, ">") {
			// Header line: close previous contig, start new one
			closeContig()
			// Extract ID: first non-whitespace token after ">"
			rest := line[1:]
			fields := strings.Fields(rest)
			if len(fields) > 0 {
				currentID = fields[0]
			} else {
				currentID = ""
			}
		} else {
			// Sequence data: accumulate uppercase
			if currentID != "" {
				seqBuf.WriteString(strings.ToUpper(line))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	// Close final contig
	closeContig()

	// Compute genome MD5: sort contig MD5 values, join with comma, MD5 the result
	contigMD5s := make([]string, 0, len(contigs))
	for _, v := range contigs {
		contigMD5s = append(contigMD5s, v)
	}
	sort.Strings(contigMD5s)

	contigString := strings.Join(contigMD5s, ",")
	genomeMD5 := md5Hex([]byte(contigString))

	fmt.Fprint(out, genomeMD5)

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
