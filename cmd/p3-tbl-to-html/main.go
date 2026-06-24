// Command p3-tbl-to-html converts a tab-delimited file to an HTML table.
//
// Usage:
//
//	p3-tbl-to-html [options]
//
// Examples:
//
//	p3-tbl-to-html < data.tsv > table.html
//	p3-tbl-to-html --class mytable --border 1 < data.tsv > table.html
//	p3-tbl-to-html --nohead < data.tsv > table.html
package main

import (
	"fmt"
	"html"
	"io"
	"os"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
	"github.com/spf13/cobra"
)

var (
	noHead      bool
	tableClass  string
	tableBorder string
	inputFile   string
	outputFile  string
)

var rootCmd = &cobra.Command{
	Use:   "p3-tbl-to-html [options]",
	Short: "Convert a tab-delimited file to an HTML table",
	Long: `This script converts a P3 tab-delimited file to an HTML table.
The header row is converted into an actual table header row.

Examples:

  # Convert stdin to HTML table
  p3-tbl-to-html < data.tsv > table.html

  # Add a CSS class and border
  p3-tbl-to-html --class mytable --border 1 < data.tsv > table.html

  # File has no headers
  p3-tbl-to-html --nohead < data.tsv > table.html`,
	Args: cobra.NoArgs,
	RunE: run,
}

func init() {
	rootCmd.Flags().BoolVar(&noHead, "nohead", false, "file does not have headers")
	rootCmd.Flags().StringVar(&tableClass, "class", "", "if specified, style class for the table")
	rootCmd.Flags().StringVar(&tableBorder, "border", "", "if specified, border style for the table")
	rootCmd.Flags().StringVarP(&inputFile, "input", "i", "", "input file (default: stdin)")
	rootCmd.Flags().StringVarP(&outputFile, "output", "o", "", "output file (default: stdout)")
}

func run(cmd *cobra.Command, args []string) error {
	// Open input
	var in *os.File
	var err error
	if inputFile != "" && inputFile != "-" {
		in, err = os.Open(inputFile)
		if err != nil {
			return fmt.Errorf("opening input: %w", err)
		}
		defer in.Close()
	} else {
		in = os.Stdin
	}

	// Open output
	var out *os.File
	if outputFile != "" && outputFile != "-" {
		out, err = os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("opening output: %w", err)
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	reader := cli.NewTabReader(in, !noHead)

	// Build table opening tag attributes
	tableAttrs := ""
	var attrParts []string
	if tableClass != "" {
		attrParts = append(attrParts, fmt.Sprintf(` class="%s"`, html.EscapeString(tableClass)))
	}
	if tableBorder != "" {
		attrParts = append(attrParts, fmt.Sprintf(` border="%s"`, html.EscapeString(tableBorder)))
	}
	tableAttrs = strings.Join(attrParts, "")

	// Write HTML preamble
	fmt.Fprintln(out, "<html>")
	fmt.Fprintln(out, "<body>")
	fmt.Fprintf(out, "<table%s>\n", tableAttrs)

	// Write header row if present
	if !noHead {
		headers, err := reader.Headers()
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("reading headers: %w", err)
			}
			// Empty file - just close the table
			fmt.Fprintln(out, "</table>")
			fmt.Fprintln(out, "</body>")
			fmt.Fprintln(out, "</html>")
			return nil
		}
		fmt.Fprintf(out, "<tr>%s</tr>\n", thCells(headers))
	}

	// Process data rows
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}
		fmt.Fprintf(out, "<tr>%s</tr>\n", tdCells(row))
	}

	// Write HTML postamble
	fmt.Fprintln(out, "</table>")
	fmt.Fprintln(out, "</body>")
	fmt.Fprintln(out, "</html>")

	return nil
}

// thCells renders a slice of strings as <th> elements.
func thCells(fields []string) string {
	var sb strings.Builder
	for _, f := range fields {
		sb.WriteString("<th>")
		sb.WriteString(html.EscapeString(f))
		sb.WriteString("</th>")
	}
	return sb.String()
}

// tdCells renders a slice of strings as <td> elements.
func tdCells(fields []string) string {
	var sb strings.Builder
	for _, f := range fields {
		sb.WriteString("<td>")
		sb.WriteString(html.EscapeString(f))
		sb.WriteString("</td>")
	}
	return sb.String()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
