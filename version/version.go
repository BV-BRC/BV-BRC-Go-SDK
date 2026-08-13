// Package version reports the build version of this SDK and the User-Agent
// derived from it.
package version

import (
	"os"
	"runtime/debug"
	"sync"
)

// Name is the library name reported in the User-Agent.
const Name = "bvbrc-go-sdk"

// Version is stamped in at build time:
//
//	go build -ldflags "-X github.com/BV-BRC/BV-BRC-Go-SDK/version.Version=$(scripts/version.sh)"
//
// scripts/version.sh yields the tag when HEAD is exactly tagged and the short
// commit hash otherwise; the release workflow passes the pushed tag instead.
// The build scripts and the Makefile do this for you.
var Version string

// fromBuildInfo is only consulted when nothing was stamped in, and the answer
// cannot change during a run, so compute it once.
var fromBuildInfo = sync.OnceValue(func() string {
	// Fall back to whatever the toolchain recorded.
	// Builds in this tree pass -buildvcs=false (the git+svn mix in the
	// dev_container breaks VCS stamping), so in practice this path is for
	// `go install github.com/BV-BRC/BV-BRC-Go-SDK/cmd/...@v2.0.12` users,
	// where Main.Version carries the module version.
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var rev string
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "unknown"
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	if dirty {
		rev += "-dirty"
	}
	return rev
})

// Get returns the build version: a tag like "2.0.12" for a release build, a
// short commit hash for a build off an untagged checkout, or "unknown" when
// neither was recorded.
func Get() string {
	if Version != "" {
		return Version
	}
	return fromBuildInfo()
}

// UserAgent returns the User-Agent every HTTP client in this SDK sends, e.g.
// "bvbrc-go-sdk/2.0.12". P3_USER_AGENT overrides it wholesale.
//
// Do not report a library user-agent (Go's default "Go-http-client/1.1", or
// Perl's "libwww-perl") to the BV-BRC data API: it sits behind Cloudflare,
// which rejects some of those outright with error 1010.
func UserAgent() string {
	if ua := os.Getenv("P3_USER_AGENT"); ua != "" {
		return ua
	}
	return Name + "/" + Get()
}
