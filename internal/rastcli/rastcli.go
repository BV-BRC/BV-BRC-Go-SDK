// Package rastcli holds what the rast-* commands share: the common flags, the
// stdin/stdout plumbing, and the option groups that build a service parameter
// object.
//
// It is the Go counterpart of Bio::KBase::GenomeAnnotation::CmdHelper, and is
// deliberately organized the same way, so a Perl script and its port can be
// read side by side.
package rastcli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/seq"
	"github.com/spf13/cobra"
)

// Common holds the flags every rast-* command has: CmdHelper's
// options_genome_in, options_genome_out and options_help. (--help itself is
// cobra's.)
type Common struct {
	Input  string
	Output string
	URL    string
}

// AddCommonFlags registers -i/--input, -o/--output and --url.
func AddCommonFlags(cmd *cobra.Command) *Common {
	c := &Common{}
	AddInputFlag(cmd, c)
	AddOutputFlag(cmd, c)
	AddURLFlag(cmd, c)
	return c
}

// AddInputFlag registers only -i/--input, for the commands that read a genome
// but write something other than one.
func AddInputFlag(cmd *cobra.Command, c *Common) {
	cmd.Flags().StringVarP(&c.Input, "input", "i", "", "file from which the input is to be read")
}

// AddOutputFlag registers only -o/--output.
func AddOutputFlag(cmd *cobra.Command, c *Common) {
	cmd.Flags().StringVarP(&c.Output, "output", "o", "", "file to which the output is to be written")
}

// AddURLFlag registers only --url, for the commands that take no genome at all.
func AddURLFlag(cmd *cobra.Command, c *Common) {
	cmd.Flags().StringVar(&c.URL, "url", "", "URL for the genome annotation service")
}

// NewClient builds a service client honoring --url. The login token is used
// when there is one: several methods answer an unauthenticated caller, and the
// rast-* tools have never required a login for those.
//
// extra is applied last, so a command with its own timeout flag can override
// the default.
func NewClient(c *Common, extra ...genomeannotation.Option) *genomeannotation.Client {
	opts := []genomeannotation.Option{genomeannotation.WithURL(c.URL)}
	if token, err := auth.GetToken(); err == nil && token != nil {
		opts = append(opts, genomeannotation.WithToken(token))
	}
	return genomeannotation.New(append(opts, extra...)...)
}

// LoadInput reads the genome from --input, or from standard input.
//
// The bytes are returned as they were read: this is the front half of the
// pass-through described in the genomeannotation package comment. The only
// check is that the input is JSON at all, so a mistyped filename or a truncated
// pipe is reported here rather than as a service-side error.
func LoadInput(c *Common) (json.RawMessage, error) {
	var (
		data []byte
		err  error
	)
	if c.Input != "" {
		data, err = os.ReadFile(c.Input)
		if err != nil {
			return nil, fmt.Errorf("cannot read input file %s: %w", c.Input, err)
		}
	} else {
		data, err = io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("cannot read standard input: %w", err)
		}
	}

	if !json.Valid(data) {
		where := "standard input"
		if c.Input != "" {
			where = c.Input
		}
		return nil, fmt.Errorf("%s does not contain valid JSON", where)
	}

	return json.RawMessage(data), nil
}

// InputReader returns the source for --input, or standard input, and a close
// function. The caller must call the close function; it is a no-op for standard
// input. This is CmdHelper::get_input_fh, for the one command whose input is
// not JSON.
func InputReader(c *Common) (io.Reader, func() error, error) {
	if c.Input == "" {
		return os.Stdin, func() error { return nil }, nil
	}
	f, err := os.Open(c.Input)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open input file %s: %w", c.Input, err)
	}
	return f, f.Close, nil
}

// Indent is the indentation JSON::XS->new->pretty uses, and so what the Perl
// rast-* tools have always written.
const Indent = "   "

// WriteOutput writes a genome to --output, or to standard output, pretty-printed.
func WriteOutput(raw json.RawMessage, c *Common) error {
	pretty, err := Pretty(raw)
	if err != nil {
		return err
	}
	return WriteText(string(pretty)+"\n", c)
}

// Pretty re-indents JSON without decoding it, so key order, numeric precision
// and every field we do not know about survive.
func Pretty(raw json.RawMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", Indent); err != nil {
		return nil, fmt.Errorf("formatting output: %w", err)
	}
	return buf.Bytes(), nil
}

// WriteText writes arbitrary text to --output, or to standard output.
func WriteText(text string, c *Common) error {
	if c.Output == "" {
		_, err := io.WriteString(os.Stdout, text)
		return err
	}

	f, err := os.Create(c.Output)
	if err != nil {
		return fmt.Errorf("cannot open output file %s: %w", c.Output, err)
	}
	if _, err := io.WriteString(f, text); err != nil {
		f.Close()
		return fmt.Errorf("writing %s: %w", c.Output, err)
	}
	return f.Close()
}

// OutputWriter returns the destination for --output, and a close function. The
// caller must call the close function; it is a no-op for standard output.
func OutputWriter(c *Common) (io.Writer, func() error, error) {
	if c.Output == "" {
		return os.Stdout, func() error { return nil }, nil
	}
	f, err := os.Create(c.Output)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open output file %s: %w", c.Output, err)
	}
	return f, f.Close, nil
}

// ReadContigsFasta loads a contigs FASTA into the form add_contigs takes,
// mirroring CmdHelper::get_params_for_contigs.
func ReadContigsFasta(path string) ([]genomeannotation.Contig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open contigs data file %s: %w", path, err)
	}
	defer f.Close()

	records, err := seq.ReadFasta(f)
	if err != nil {
		return nil, fmt.Errorf("reading contigs from %s: %w", path, err)
	}

	contigs := make([]genomeannotation.Contig, 0, len(records))
	for _, r := range records {
		contigs = append(contigs, genomeannotation.Contig{ID: r.ID, DNA: r.Seq})
	}
	return contigs, nil
}
