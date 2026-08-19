package p3tests

// Guard tests for the packaging scripts.
//
// Every command in cmd/ has to reach the release archives, the .deb/.rpm file
// lists and the conda package. Nothing enforced that: the build scripts
// enumerated the toolkit with a `cmd/p3-*/` glob, which was correct only for as
// long as every command started with p3-. Adding the rast-* family would have
// shipped a release quietly missing 37 tools -- the build succeeds, the archive
// is a plausible size, and only a user notices.
//
// These tests are hermetic: they read the scripts as text rather than running
// them, so they pass on any platform and need no toolchain beyond Go.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// commandPrefixes returns the distinct "p3", "rast", ... prefixes in cmd/.
func commandPrefixes(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir("cmd")
	if err != nil {
		t.Fatalf("reading cmd/: %v", err)
	}

	seen := map[string]bool{}
	var prefixes []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		i := strings.Index(name, "-")
		if i <= 0 {
			t.Errorf("cmd/%s does not have a <family>-<name> form; "+
				"the packaging globs assume one", name)
			continue
		}
		if p := name[:i]; !seen[p] {
			seen[p] = true
			prefixes = append(prefixes, p)
		}
	}

	if len(prefixes) == 0 {
		t.Fatal("found no commands in cmd/")
	}
	return prefixes
}

// TestBuildScriptsEnumerateEveryCommand checks that the four build scripts
// select commands with a glob that matches all of cmd/, not one family of it.
func TestBuildScriptsEnumerateEveryCommand(t *testing.T) {
	scripts := []string{
		"build-linux.sh",
		"build-macos.sh",
		"build-windows.sh",
		"build-macos-pkg.sh",
	}

	// The one glob that cannot go stale.
	want := regexp.MustCompile(`COMMANDS=\$\(ls -d cmd/\*/ \|`)
	// Anything narrower, e.g. the cmd/p3-*/ this replaced.
	narrow := regexp.MustCompile(`COMMANDS=\$\(ls -d cmd/[^*]`)

	for _, script := range scripts {
		t.Run(script, func(t *testing.T) {
			data, err := os.ReadFile(script)
			if err != nil {
				t.Fatalf("reading %s: %v", script, err)
			}
			body := string(data)

			if narrow.MatchString(body) {
				t.Errorf("%s selects commands with a name-prefix glob; "+
					"use `ls -d cmd/*/` so a new command family is not silently dropped", script)
			}
			if !want.MatchString(body) {
				t.Errorf("%s does not enumerate commands with `ls -d cmd/*/`", script)
			}
		})
	}
}

// TestPackageFileListsCoverEveryFamily checks the places that must name each
// command family explicitly, because they glob installed files rather than
// source directories.
func TestPackageFileListsCoverEveryFamily(t *testing.T) {
	prefixes := commandPrefixes(t)

	// The rpm %files section: a family missing here is left out of the rpm.
	rpmSpec := readFile(t, "build-linux.sh")
	for _, p := range prefixes {
		if !strings.Contains(rpmSpec, "/usr/local/bin/"+p+"-*") {
			t.Errorf("build-linux.sh rpm %%files section is missing /usr/local/bin/%s-*", p)
		}
	}

	// conda's build.sh installs by prefix, from a PREFIXES array.
	condaBuild := readFile(t, filepath.Join("conda-recipe", "build.sh"))
	declared := regexp.MustCompile(`PREFIXES=\(([^)]*)\)`).FindStringSubmatch(condaBuild)
	if declared == nil {
		t.Fatal("conda-recipe/build.sh no longer declares a PREFIXES array")
	}
	for _, p := range prefixes {
		if !containsField(declared[1], p) {
			t.Errorf("conda-recipe/build.sh PREFIXES is missing %q; "+
				"those commands would not be installed into the conda package", p)
		}
	}
}

// TestReadmeCountsAreNotHardCoded guards the tool count in the shipped README,
// which was wrong every time a command was added until it was noticed.
func TestReadmeCountsAreNotHardCoded(t *testing.T) {
	body := readFile(t, filepath.Join("scripts", "make-readme.sh"))

	if !strings.Contains(body, "total_count") {
		t.Error("scripts/make-readme.sh no longer derives the tool count from cmd/")
	}
	// A three-digit literal in the prose is almost certainly a stale count.
	if m := regexp.MustCompile(`collection of \d+`).FindString(body); m != "" {
		t.Errorf("scripts/make-readme.sh hard-codes a tool count (%q); derive it from cmd/", m)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(data)
}

// containsField reports whether a whitespace-separated list contains value.
func containsField(list, value string) bool {
	for _, f := range strings.Fields(list) {
		if strings.Trim(f, `"'`) == value {
			return true
		}
	}
	return false
}
