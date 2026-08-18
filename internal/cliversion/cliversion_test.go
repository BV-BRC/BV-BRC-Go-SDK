package cliversion_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliversion"
	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
	"github.com/spf13/cobra"
)

// newCmd builds a command shaped like a real p3-* root command.
func newCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "p3-thing [options] arg",
		Short: "test command",
		RunE:  func(cmd *cobra.Command, args []string) error { return nil },
	}
}

// runVersion executes cmd with --version and returns what it printed.
func runVersion(t *testing.T, cmd *cobra.Command) string {
	t.Helper()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--version"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("executing with --version: %v", err)
	}
	return out.String()
}

func TestVersionOutput(t *testing.T) {
	cmd := newCmd()
	cliversion.Register(cmd)

	got := runVersion(t, cmd)
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("--version printed %d lines, want 2:\n%s", len(lines), got)
	}

	// First line is the conventional "<name> <version>" -- the name comes from
	// the first word of Use, not the whole usage string.
	if want := "p3-thing " + version.Get(); lines[0] != want {
		t.Errorf("first line = %q, want %q", lines[0], want)
	}
	// Second line leads with the exact User-Agent the binary would send.
	if !strings.HasPrefix(lines[1], version.UserAgent()+" ") {
		t.Errorf("second line = %q, want it to lead with the user-agent %q", lines[1], version.UserAgent())
	}
}

func TestRegisterDoesNotClaimTheVShorthand(t *testing.T) {
	// cobra's InitDefaultVersionFlag binds -v to --version when the shorthand
	// is free. Most of the suite leaves it free, but eight commands bind -v to
	// --verbose or --reverse, so -v must not mean "version" anywhere.
	cmd := newCmd()
	cliversion.Register(cmd)
	if err := cmd.Execute(); err != nil { // triggers cobra's flag initialization
		t.Fatalf("executing: %v", err)
	}

	if f := cmd.Flags().ShorthandLookup("v"); f != nil {
		t.Errorf("-v is bound to %q; it must stay free for commands that use it themselves", f.Name)
	}
	if cmd.Flags().Lookup("version") == nil {
		t.Error("no --version flag was registered")
	}
}

func TestRegisterLeavesAnExistingVShorthandAlone(t *testing.T) {
	cmd := newCmd()
	var reverse bool
	cmd.Flags().BoolVarP(&reverse, "reverse", "v", false, "output non-matching rows instead")
	cliversion.Register(cmd)

	cmd.SetArgs([]string{"-v"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("executing with -v: %v", err)
	}
	if !reverse {
		t.Error("-v did not set --reverse")
	}
	if out.Len() != 0 {
		t.Errorf("-v printed %q; it must not be treated as --version", out.String())
	}
}

func TestUserAgentIsNotTreatedAsATemplate(t *testing.T) {
	// P3_USER_AGENT is arbitrary text from the environment. It reaches the
	// output through the command's annotations rather than the template source,
	// so a template action in it must come out verbatim.
	t.Setenv("P3_USER_AGENT", "weird/{{.Name}}")

	cmd := newCmd()
	cliversion.Register(cmd)
	got := runVersion(t, cmd)

	if !strings.Contains(got, "weird/{{.Name}}") {
		t.Errorf("--version output = %q, want it to contain the user-agent verbatim", got)
	}
}

func TestExecuteRegistersTheFlag(t *testing.T) {
	cmd := newCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--version"})

	if err := cliversion.Execute(cmd); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.HasPrefix(out.String(), "p3-thing ") {
		t.Errorf("Execute did not handle --version; output = %q", out.String())
	}
}

// TestEveryCommandSupportsVersion is the counterpart of
// TestEveryCommandDeclaresAProduct in internal/cliproduct: a command that calls
// rootCmd.Execute() directly still builds and still works, it just silently has
// no --version flag. Only a test catches that.
func TestEveryCommandSupportsVersion(t *testing.T) {
	const (
		pkg     = "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliversion"
		cmdRoot = "../../cmd"
	)

	entries, err := os.ReadDir(cmdRoot)
	if err != nil {
		t.Fatalf("reading %s: %v", cmdRoot, err)
	}

	var checked int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cmd := e.Name()
		imports, callsHelper, callsExecuteDirectly := scanCommand(t, filepath.Join(cmdRoot, cmd))

		if !imports[pkg] || !callsHelper {
			t.Errorf("%s: main must call cliversion.Execute(rootCmd) so the command has a --version flag", cmd)
		}
		if callsExecuteDirectly {
			t.Errorf("%s: calls rootCmd.Execute() directly, bypassing the --version flag", cmd)
		}
		checked++
	}

	if checked == 0 {
		t.Fatalf("found no commands under %s", cmdRoot)
	}
}

// scanCommand returns the packages the command imports, whether it calls
// cliversion.Execute or cliversion.Register, and whether it still calls
// rootCmd.Execute directly.
func scanCommand(t *testing.T, dir string) (imports map[string]bool, callsHelper, callsExecuteDirectly bool) {
	t.Helper()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}

	imports = make(map[string]bool)
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, imp := range file.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					continue
				}
				imports[path] = true
			}
			ast.Inspect(file, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				ident, ok := sel.X.(*ast.Ident)
				if !ok {
					return true
				}
				switch {
				case ident.Name == "cliversion" && (sel.Sel.Name == "Execute" || sel.Sel.Name == "Register"):
					callsHelper = true
				case ident.Name == "rootCmd" && sel.Sel.Name == "Execute":
					callsExecuteDirectly = true
				}
				return true
			})
		}
	}
	return imports, callsHelper, callsExecuteDirectly
}
