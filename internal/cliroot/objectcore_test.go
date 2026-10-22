package cliroot_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/BV-BRC/BV-BRC-Go-SDK/v2/api"
)

var (
	defaultFieldsRE = regexp.MustCompile(`GetDefaultFields\("([A-Za-z_]+)"\)`)
	queryObjectRE   = regexp.MustCompile(`(?:QueryCallback|QueryWithCursor|Query|Stream|GetSchema|Count)\(ctx, "([A-Za-z_]+)"`)
)

// resolveCore maps a name as written in a command to the Solr core it reaches.
// api.Objects is the user-friendly-name -> core table; a name already spelled
// as a core is not in it and passes through unchanged. Both spellings are in
// use across cmd/ and both are fine -- what matters is where they land.
func resolveCore(name string) string {
	if core, ok := api.Objects[name]; ok {
		return core
	}
	return name
}

// A command must query the same Solr core whose default fields it prints.
//
// This guards a failure that is silent in review and loud only at runtime:
// p3-get-feature-subsystems asked api.GetDefaultFields for "subsystemItem"
// (core "subsystem", the per-feature membership rows, which carry patric_id)
// while querying "subsystem" -- whose mapping is "subsystem_ref", the
// subsystem *definition* core, which has no patric_id. The header row named
// subsystemItem fields, so the output looked right up until Solr rejected the
// filter with `undefined field patric_id`. The Perl counterpart was correct,
// so the two toolkits silently disagreed.
//
// The comparison is on the resolved core, not the spelling. Several commands
// legitimately pass an already-resolved core name (p3-get-family-data writes
// "protein_family_ref" where the object is "family"); that is equivalent and
// must keep passing.
func TestCommandQueriesTheCoreItPrintsFieldsFor(t *testing.T) {
	const cmdRoot = "../../cmd"

	entries, err := os.ReadDir(cmdRoot)
	if err != nil {
		t.Fatalf("reading %s: %v", cmdRoot, err)
	}

	var checked int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		src, err := os.ReadFile(filepath.Join(cmdRoot, e.Name(), "main.go"))
		if err != nil {
			continue // not every cmd dir is laid out this way
		}
		text := string(src)

		fieldCores := distinctCores(defaultFieldsRE, text)
		if len(fieldCores) == 0 {
			continue // command prints no object's default fields
		}
		queryCores := distinctCores(queryObjectRE, text)
		if len(queryCores) == 0 {
			continue // no literal object name to compare against
		}
		checked++

		for _, qc := range queryCores {
			if !contains(fieldCores, qc) {
				t.Errorf("%s: queries core %q but prints default fields for core(s) %v -- "+
					"a command must read the core whose fields it declares",
					e.Name(), qc, fieldCores)
			}
		}
	}

	if checked < 20 {
		t.Fatalf("only compared %d commands; the layout changed and this guard "+
			"is not looking at anything", checked)
	}
}

func distinctCores(re *regexp.Regexp, text string) []string {
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(text, -1) {
		seen[resolveCore(m[1])] = true
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if strings.EqualFold(h, needle) {
			return true
		}
	}
	return false
}
