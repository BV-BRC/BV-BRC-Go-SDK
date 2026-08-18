// Package cliroot holds the setup every p3-* root command shares: the
// --version and --debug-http flags. main calls Execute instead of
// rootCmd.Execute(), which is the one seam that reaches all 101 commands.
//
// It is a leaf package -- cobra, version and httpdiag, none of which pull in
// anything beyond the standard library -- so the commands that make no requests
// (p3-echo, p3-fasta-md5, p3-merge) do not link the API client just to report a
// version, which is what internal/cli would drag in.
package cliroot

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/httpdiag"
	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
	"github.com/spf13/cobra"
)

// buildAnnotation keys the second line of --version output in the command's
// annotations, so that text reaches the template as data. Interpolating it
// into the template string instead would let a "{{" in P3_USER_AGENT be parsed
// as a template action.
const buildAnnotation = "bvbrc.build"

// Register gives root the flags every command shares. Call it before Execute --
// or just use Execute, which does both.
func Register(root *cobra.Command) {
	registerVersion(root)
	registerDebugHTTP(root)
}

// registerVersion adds a --version flag that prints
//
//	p3-ls 2.0.14
//	bvbrc-cli-go/2.0.14 linux/amd64 go1.25.6
//
// The first line is the conventional "<name> <version>". The second is the
// exact User-Agent the binary sends plus the build platform: the User-Agent
// names the product, carries the same version, and reflects a P3_USER_AGENT
// override, which is precisely what a report about a Cloudflare rejection
// needs to state.
func registerVersion(root *cobra.Command) {
	// version.Get never returns "", and cobra ignores the flag when Version is
	// empty, so this is also what arms the printing below.
	root.Version = version.Get()

	// Declare the flag rather than leaving it to cobra's
	// InitDefaultVersionFlag, which claims -v as a shorthand whenever that is
	// free. Eight commands already bind -v (to --verbose, and in three cases to
	// --reverse), so letting cobra have it would make -v mean "version" in most
	// of the suite and something else in the rest. cobra's own handling only
	// looks up a bool flag named "version", so it still does the printing.
	if root.Flags().Lookup("version") == nil {
		root.Flags().Bool("version", false, "print version information and exit")
	}

	if root.Annotations == nil {
		root.Annotations = make(map[string]string)
	}
	root.Annotations[buildAnnotation] = fmt.Sprintf("%s %s/%s %s",
		version.UserAgent(), runtime.GOOS, runtime.GOARCH, runtime.Version())

	root.SetVersionTemplate(
		`{{.Name}} {{.Version}}` + "\n" +
			`{{index .Annotations "` + buildAnnotation + `"}}` + "\n")
}

// registerDebugHTTP adds --debug-http, which dumps the headers of any failed
// HTTP exchange: the same switch as P3_DEBUG_HTTP, which remains the documented
// one because it needs no per-command plumbing.
//
// It belongs on every command, not just the ones that query the data API. Only
// 40 commands take --debug (it rides on DataOptions), so before this the other
// 61 -- every p3-submit-*, p3-ls, p3-cp, p3-job-status -- had no flag at all
// for the case the diagnostics exist to serve.
func registerDebugHTTP(root *cobra.Command) {
	// A command that declares the flag itself keeps its own wording:
	// registering a duplicate name panics in pflag.
	if root.Flags().Lookup("debug-http") != nil {
		return
	}

	var v debugHTTP
	root.Flags().Var(&v, "debug-http",
		"dump the HTTP headers of failed requests (same as setting P3_DEBUG_HTTP)")
	// Without this the flag would demand an argument; it is a boolean.
	root.Flags().Lookup("debug-http").NoOptDefVal = "true"
}

// debugHTTP turns diagnostics on as the flag is parsed. Reading a bool variable
// later would mean reading it from somewhere -- PersistentPreRun, which commands
// define for themselves, or a cobra.OnInitialize closure, which is global state
// that accumulates across calls. Acting in Set keeps it local, and parsing
// happens before any command body runs.
type debugHTTP bool

func (d *debugHTTP) Set(s string) error {
	on, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*d = debugHTTP(on)
	if on {
		httpdiag.SetEnabled(true)
	}
	return nil
}

func (d *debugHTTP) String() string { return strconv.FormatBool(bool(*d)) }

// Type reports "bool" so pflag renders the flag as a boolean in --help.
func (d *debugHTTP) Type() string { return "bool" }

// Execute registers the shared flags on root and runs it. Every p3-* command
// calls this from main instead of root.Execute(), so that a new command cannot
// quietly ship without them; TestEveryCommandUsesTheSharedRoot enforces it.
func Execute(root *cobra.Command) error {
	Register(root)
	return root.Execute()
}
