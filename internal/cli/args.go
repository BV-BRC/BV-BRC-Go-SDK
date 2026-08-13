package cli

import (
	"fmt"
	"strings"
)

const (
	pairedEndLibFlag  = "--paired-end-lib"
	pairedEndLibAlias = "--paired-end-libs"
)

// PairedEndLibUsage is the help text for --paired-end-lib. Every submit command
// uses it so the accepted forms are described identically everywhere.
const PairedEndLibUsage = "paired-end read library, as two files: --paired-end-lib read1 read2 (or read1,read2)"

// NormalizePairedEndLibArgs pre-processes os.Args so that both spellings of a
// paired-end library are accepted by every command:
//
//	--paired-end-lib read1.fq read2.fq   (Perl ReadSpec =s{2} style)
//	--paired-end-lib read1.fq,read2.fq   (original Go style)
//
// When two consecutive non-flag arguments follow the flag, they are joined with
// a comma and rewritten as a single argument before cobra parses the flag set.
// The comma form and --paired-end-lib=f1,f2 are passed through unchanged.
//
// The Perl plural alias --paired-end-libs (ReadSpec.pm:255) is rewritten to the
// singular so it works here too, without every command having to register a
// second flag.
//
// Everything after a bare "--" is positional and is left alone.
//
// Call this as the first line of main() before rootCmd.Execute():
//
//	os.Args = cli.NormalizePairedEndLibArgs(os.Args)
func NormalizePairedEndLibArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]

		// End of flags: the rest is positional.
		if arg == "--" {
			out = append(out, args[i:]...)
			break
		}

		// Accept the plural spelling wherever the singular works.
		switch {
		case arg == pairedEndLibAlias:
			arg = pairedEndLibFlag
		case strings.HasPrefix(arg, pairedEndLibAlias+"="):
			arg = pairedEndLibFlag + strings.TrimPrefix(arg, pairedEndLibAlias)
		}

		if arg != pairedEndLibFlag {
			out = append(out, arg)
			continue
		}

		// Peek at the next two arguments. If both look like filenames (not
		// flags), consume them as a pair.
		if i+2 < len(args) &&
			!strings.HasPrefix(args[i+1], "-") &&
			!strings.HasPrefix(args[i+2], "-") &&
			!strings.Contains(args[i+1], ",") { // already comma-joined: pass through on next iteration
			out = append(out, arg, args[i+1]+","+args[i+2])
			i += 2
			continue
		}

		// One arg or already comma form: cobra handles it normally.
		out = append(out, arg)
	}
	return out
}

// SplitPairedEndLib splits one --paired-end-lib value into its two read files.
// NormalizePairedEndLibArgs has already joined the two-argument form with a
// comma, so both spellings arrive here identically; whitespace around the comma
// is tolerated. Commands use this rather than splitting themselves so that the
// accepted syntax and the error text stay the same across the suite.
func SplitPairedEndLib(value string) (read1, read2 string, err error) {
	parts := strings.Split(value, ",")
	if len(parts) == 2 {
		read1, read2 = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if read1 != "" && read2 != "" {
			return read1, read2, nil
		}
	}
	return "", "", fmt.Errorf("paired-end library needs two files, given as "+
		"--paired-end-lib read1 read2 or --paired-end-lib read1,read2: %q", value)
}
