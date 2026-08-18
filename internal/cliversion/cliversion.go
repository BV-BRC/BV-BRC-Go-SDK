// Package cliversion gives every p3-* command an identical --version flag.
//
// It is a leaf package -- it depends on cobra and on the version package and
// nothing else -- so the commands that make no requests (p3-echo, p3-fasta-md5,
// p3-merge) can report their version without linking the API client, which is
// what internal/cli would drag in.
package cliversion

import (
	"fmt"
	"runtime"

	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
	"github.com/spf13/cobra"
)

// buildAnnotation keys the second line of --version output in the command's
// annotations, so that text reaches the template as data. Interpolating it
// into the template string instead would let a "{{" in P3_USER_AGENT be parsed
// as a template action.
const buildAnnotation = "bvbrc.build"

// Register gives root a --version flag that prints
//
//	p3-ls 2.0.14
//	bvbrc-cli-go/2.0.14 linux/amd64 go1.25.6
//
// The first line is the conventional "<name> <version>". The second is the
// exact User-Agent the binary sends plus the build platform: the User-Agent
// names the product, carries the same version, and reflects a P3_USER_AGENT
// override, which is precisely what a report about a Cloudflare rejection
// needs to state.
//
// Call it before Execute -- or just use Execute, which does both.
func Register(root *cobra.Command) {
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

// Execute registers the --version flag on root and runs it. Every p3-* command
// calls this from main instead of root.Execute(), so that the flag cannot be
// forgotten by a new command; TestEveryCommandSupportsVersion enforces it.
func Execute(root *cobra.Command) error {
	Register(root)
	return root.Execute()
}
