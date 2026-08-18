package cliproduct_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
)

func TestImportDeclaresTheCLIProduct(t *testing.T) {
	// This test file blank-imports the package, so its init has run.
	if got, want := version.Product(), version.CLIProduct; got != want {
		t.Errorf("version.Product() = %q, want %q", got, want)
	}
	if ua := version.UserAgent(); !strings.HasPrefix(ua, version.CLIProduct+"/") {
		t.Errorf("version.UserAgent() = %q, want it to lead with %q", ua, version.CLIProduct)
	}
}

// Nothing forces a new command to declare an identity, and a command that
// forgets quietly announces itself as the library instead -- which is exactly
// the ambiguity the product name exists to remove. So check every command.
//
// The exceptions are listed rather than inferred: a command that gains a
// service call should have to come here and remove itself from the list.
func TestEveryCommandDeclaresAProduct(t *testing.T) {
	const (
		cliPkg  = "github.com/BV-BRC/BV-BRC-Go-SDK/internal/cliproduct"
		verPkg  = "github.com/BV-BRC/BV-BRC-Go-SDK/version"
		cmdRoot = "../../cmd"
	)

	// These make no requests at all: they filter, format or hash files.
	offline := map[string]bool{
		"p3-echo": true, "p3-fasta-md5": true, "p3-merge": true,
	}
	// p3-login declares version.AuthProduct itself; see its main.go.
	ownIdentity := map[string]bool{"p3-login": true}

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
		imports, calls := scanCommand(t, filepath.Join(cmdRoot, cmd))

		switch {
		case ownIdentity[cmd]:
			if !imports[verPkg] || !calls {
				t.Errorf("%s: expected it to call version.SetProduct itself", cmd)
			}
			if imports[cliPkg] {
				t.Errorf("%s: imports %s as well as setting its own product; one identity wins and it is not obvious which", cmd, cliPkg)
			}
		case offline[cmd]:
			if imports[cliPkg] {
				t.Errorf("%s: listed as making no requests, but imports %s -- update the list", cmd, cliPkg)
			}
		default:
			if !imports[cliPkg] {
				t.Errorf("%s: does not import %s, so it would announce itself as %q rather than %q",
					cmd, cliPkg, version.Name, version.CLIProduct)
			}
		}
		checked++
	}

	if checked == 0 {
		t.Fatalf("found no commands under %s", cmdRoot)
	}
}

// scanCommand returns the set of packages the command imports and whether any
// of its files calls version.SetProduct.
func scanCommand(t *testing.T, dir string) (map[string]bool, bool) {
	t.Helper()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}

	imports := make(map[string]bool)
	var setsProduct bool
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
				if !ok || sel.Sel.Name != "SetProduct" {
					return true
				}
				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "version" {
					setsProduct = true
				}
				return true
			})
		}
	}
	return imports, setsProduct
}
