// Command p3-merge merges two or more tab-delimited files by whole-line deduplication.
//
// Usage:
//
//	p3-merge [options] file1 file2 ... fileN
//
// Examples:
//
//	p3-merge file1.txt file2.txt
//	p3-merge --and file1.txt file2.txt
//	p3-merge --diff file1.txt file2.txt
//	p3-merge --or file1.txt - file3.txt
package main

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	noHead    bool
	modeAnd   bool
	modeOr    bool
	modeDiff  bool
	inputFile string
)

var rootCmd = &cobra.Command{
	Use:   "p3-merge [options] file1 file2 ... fileN",
	Short: "Merge tab-delimited files by union, intersection, or difference",
	Long: `This script reads one or more files and outputs a new one containing whole
lines from those files. The output file can be the union (all lines from all
files), intersection (all lines present in all files), or difference (all
lines in the first but not the others). All files must have the same header
line.

Duplicate lines will be removed. A line that occurs in multiple files or
occurs more than once in any file will only appear once in the output.

Any one file can be replaced by the standard input using a minus sign (-).

Examples:

  # Union (default): all lines from either file
  p3-merge file1.txt file2.txt

  # Intersection: only lines in both files
  p3-merge --and file1.txt file2.txt

  # Difference: lines in file1 but not file2
  p3-merge --diff file1.txt file2.txt`,
	Args: cobra.ArbitraryArgs,
	RunE: run,
}

func init() {
	rootCmd.Flags().BoolVar(&noHead, "nohead", false, "input files do not have headers")
	rootCmd.Flags().BoolVar(&modeAnd, "and", false, "output lines found in all files (intersection)")
	rootCmd.Flags().BoolVar(&modeOr, "or", false, "output lines from any file (union, default)")
	rootCmd.Flags().BoolVar(&modeDiff, "diff", false, "output lines only in first file (difference)")
	rootCmd.Flags().StringVarP(&inputFile, "input", "i", "", "name of file containing input file names (first column)")
}

// md5Key computes the MD5 base64 key of a line, matching Perl's Digest::MD5::md5_base64.
func md5Key(line string) string {
	h := md5.Sum([]byte(line))
	return base64.StdEncoding.EncodeToString(h[:])
}

// openFile opens a file handle for the given path. "-" means stdin.
// The opt stdin is passed to use when "-" is encountered.
func openFile(path string, stdinUsed *bool) (io.ReadCloser, error) {
	if path == "-" {
		if *stdinUsed {
			return nil, fmt.Errorf("the standard input (-) can only be specified once")
		}
		*stdinUsed = true
		return io.NopCloser(os.Stdin), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open %s: %w", path, err)
	}
	return f, nil
}

// readHeader reads and returns the header line (without newline) from a scanner.
// Returns "" and false if the file is empty.
func readHeader(scanner *bufio.Scanner) (string, bool) {
	if scanner.Scan() {
		return scanner.Text(), true
	}
	return "", false
}

// readIntoSeen reads all lines from a scanner and records their MD5 keys in seen.
func readIntoSeen(scanner *bufio.Scanner, seen map[string]bool) {
	for scanner.Scan() {
		line := scanner.Text()
		seen[md5Key(line)] = true
	}
}

// printUnseen reads lines from scanner and prints those not already in seen,
// adding them to seen to prevent duplicates.
func printUnseen(scanner *bufio.Scanner, seen map[string]bool, w *bufio.Writer) error {
	for scanner.Scan() {
		line := scanner.Text()
		key := md5Key(line)
		if !seen[key] {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
			seen[key] = true
		}
	}
	return nil
}

// printNewSeen reads lines from scanner and prints those that appear in seen,
// using a local dedup set to avoid printing duplicates.
func printNewSeen(scanner *bufio.Scanner, seen map[string]bool, w *bufio.Writer) error {
	local := make(map[string]bool)
	for scanner.Scan() {
		line := scanner.Text()
		key := md5Key(line)
		if seen[key] && !local[key] {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
			local[key] = true
		}
	}
	return nil
}

func run(cmd *cobra.Command, args []string) error {
	// Validate mode flags: at most one can be set
	modeCount := 0
	if modeAnd {
		modeCount++
	}
	if modeOr {
		modeCount++
	}
	if modeDiff {
		modeCount++
	}
	if modeCount > 1 {
		return fmt.Errorf("--and, --or, and --diff are mutually exclusive")
	}

	// Determine mode string
	mode := "or"
	if modeAnd {
		mode = "and"
	} else if modeDiff {
		mode = "diff"
	}

	// Collect file names
	files := append([]string{}, args...)

	// Read additional file names from --input file
	if inputFile != "" {
		f, err := os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("could not open file-name input: %w", err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			// Take only the first column (tab-delimited)
			fields := strings.SplitN(line, "\t", 2)
			if len(fields) > 0 && fields[0] != "" {
				files = append(files, fields[0])
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("reading input file: %w", err)
		}
	}

	if len(files) == 0 {
		return fmt.Errorf("at least one file name must be specified")
	}

	// Check stdin used at most once
	stdinCount := 0
	for _, f := range files {
		if f == "-" {
			stdinCount++
		}
	}
	if stdinCount > 1 {
		return fmt.Errorf("the standard input (-) can only be specified once")
	}

	// Open all files and read headers
	type fileEntry struct {
		scanner *bufio.Scanner
		closer  io.Closer
	}

	var entries []fileEntry
	var header string
	stdinUsed := false

	// For cleanup on error
	defer func() {
		for _, e := range entries {
			e.closer.Close()
		}
	}()

	for _, filePath := range files {
		rc, err := openFile(filePath, &stdinUsed)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(rc)
		// Increase default buffer for long lines
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

		if !noHead {
			headLine, ok := readHeader(scanner)
			if !ok {
				// Empty file — still add it (no lines to process)
				// just use current header (or set it if first)
				if header == "" {
					// Can't determine header from empty file; that's ok
				}
				entries = append(entries, fileEntry{scanner: scanner, closer: rc})
				continue
			}
			if header == "" {
				header = headLine
			} else if headLine != header {
				rc.Close()
				return fmt.Errorf("file %s has an incompatible header", filePath)
			}
		}
		entries = append(entries, fileEntry{scanner: scanner, closer: rc})
	}

	// Set up buffered writer for output
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	// Write header if applicable
	if !noHead && header != "" {
		if _, err := fmt.Fprintln(w, header); err != nil {
			return err
		}
	}

	// Perform the merge
	seen := make(map[string]bool)

	switch mode {
	case "or":
		// Union: print lines from all files that haven't been seen
		for _, e := range entries {
			if err := printUnseen(e.scanner, seen, w); err != nil {
				return err
			}
		}

	case "diff":
		// Difference: read remaining files into seen, then print first file's unseen lines
		if len(entries) < 1 {
			break
		}
		first := entries[0]
		rest := entries[1:]
		// Read rest into seen hash
		for _, e := range rest {
			readIntoSeen(e.scanner, seen)
		}
		// Print lines from first file not in seen
		if err := printUnseen(first.scanner, seen, w); err != nil {
			return err
		}

	case "and":
		// Intersection: find lines that appear in all files, print from first file
		if len(entries) < 1 {
			break
		}
		first := entries[0]
		rest := entries[1:]

		if len(rest) == 0 {
			// Only one file: just print it (deduped)
			if err := printUnseen(first.scanner, seen, w); err != nil {
				return err
			}
			break
		}

		// Read second file into seen
		readIntoSeen(rest[0].scanner, seen)

		// For each additional file (3rd, 4th, ...), compute intersection with seen
		for _, e := range rest[1:] {
			newSeen := make(map[string]bool)
			scannerScanner := e.scanner
			for scannerScanner.Scan() {
				line := scannerScanner.Text()
				key := md5Key(line)
				newSeen[key] = true
			}
			// Keep only keys in both seen and newSeen
			for k := range seen {
				if !newSeen[k] {
					delete(seen, k)
				}
			}
		}

		// Print lines from first file that are in seen (all other files)
		if err := printNewSeen(first.scanner, seen, w); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
