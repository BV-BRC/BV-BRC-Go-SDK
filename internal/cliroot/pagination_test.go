package cliroot_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Pagination is cursor-only. Nothing in cmd/ should branch on a pagination
// mode, call a *WithCursor entry point, or build an offset limit clause --
// api.Query, api.QueryCallback and api.Stream all page by cursor now, so a
// command gets the right behaviour by doing nothing special.
//
// This is a guard rather than a style check: offset paging makes every Solr
// shard collect and rank start+rows documents per page, work that grows with
// depth and is paid by the shared tier. A single page at offset 1,000,000
// measured 96.7s against genome_feature. Reintroducing it in one command
// would be invisible in review and expensive in production.
func TestNoCommandUsesOffsetPagination(t *testing.T) {
	const cmdRoot = "../../cmd"

	banned := map[string]string{
		"dataOpts.Cursor":         "pagination is not selectable any more; delete the branch",
		"QueryWithCursor":         "deprecated alias; call api.Query",
		"QueryCallbackWithCursor": "deprecated alias; call api.QueryCallback",
		"StreamWithCursor":        "deprecated alias; call api.Stream",
		"limit(%d,%d)":            "offset pagination; the cursor loop in api handles paging",
		"alpha.bv-brc.org":        "cursor works on every production endpoint; the advice is stale",
	}

	entries, err := os.ReadDir(cmdRoot)
	if err != nil {
		t.Fatalf("reading %s: %v", cmdRoot, err)
	}

	var checked int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(cmdRoot, e.Name(), "main.go")
		src, err := os.ReadFile(path)
		if err != nil {
			continue // not every cmd dir is laid out this way
		}
		for needle, why := range banned {
			if strings.Contains(string(src), needle) {
				t.Errorf("%s: contains %q -- %s", e.Name(), needle, why)
			}
		}
		checked++
	}

	if checked < 100 {
		t.Fatalf("only scanned %d commands; the layout changed and this guard is not looking at anything", checked)
	}
}
