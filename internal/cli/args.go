package cli

import "strings"

// NormalizePairedEndLibArgs pre-processes os.Args so that the Perl-compatible
// two-argument form of --paired-end-lib is accepted in addition to the
// comma-joined form:
//
//	--paired-end-lib read1.fq read2.fq   (Perl ReadSpec =s{2} style)
//	--paired-end-lib read1.fq,read2.fq   (original Go style, still accepted)
//
// When two consecutive non-flag arguments follow --paired-end-lib, they are
// joined with a comma and rewritten as a single argument before cobra parses
// the flag set.  The comma form and --paired-end-lib=f1,f2 are passed through
// unchanged.
//
// Call this as the first line of main() before rootCmd.Execute():
//
//	os.Args = cli.NormalizePairedEndLibArgs(os.Args)
func NormalizePairedEndLibArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		// Match --paired-end-lib and the alias --paired-end-libs
		if arg == "--paired-end-lib" || arg == "--paired-end-libs" {
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
			continue
		}
		out = append(out, arg)
	}
	return out
}
