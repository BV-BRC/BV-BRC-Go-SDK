// Package version reports the build version of this SDK, the identity of the
// product using it, and the User-Agent derived from the two.
package version

import (
	"os"
	"runtime/debug"
	"strings"
	"sync"
)

// Name is reported in the User-Agent by a program that has not declared a
// product identity of its own -- i.e. by anything that links this SDK as a
// library. The p3-* tools in this repository all declare one; see SetProduct.
const Name = "bvbrc-go-sdk"

// The product identities used by the programs in this repository.
//
// Naming the product rather than the library is what makes CLI traffic
// distinguishable in an access log, and in a Cloudflare block report, from a
// third-party program that merely imports these packages. The login tool gets
// its own identity because the login path is where an edge rejection is
// hardest to tell apart from a bad password. These mirror the Perl side's
// bvbrc-cli-perl / bvbrc-auth-perl (p3_auth's P3ClientUA).
const (
	// CLIProduct is declared by every p3-* command that talks to a service;
	// see the internal/cliproduct package.
	CLIProduct = "bvbrc-cli-go"
	// AuthProduct is declared by p3-login.
	AuthProduct = "bvbrc-auth-go"
)

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

var (
	productMu sync.RWMutex
	product   string
)

// SetProduct declares which product this process is, for the User-Agent.
//
// Call it from an init function, before anything makes a request. Unlike the
// version -- which cannot be known from the source and so is stamped in by the
// build -- the product is a fixed property of the program, so it is declared
// in the program: a binary built with a plain "go build" or "go install" still
// identifies itself correctly, and only its version is missing.
func SetProduct(name string) {
	productMu.Lock()
	defer productMu.Unlock()
	product = headerSafe(name)
}

// Product returns the declared product, or Name when none was declared.
func Product() string {
	productMu.RLock()
	defer productMu.RUnlock()
	if product != "" {
		return product
	}
	return Name
}

// UserAgent returns the User-Agent every HTTP client in this SDK sends: the
// product and the build version, e.g. "bvbrc-cli-go/2.0.13", or
// "bvbrc-go-sdk/2.0.13" from a program that declared no product.
// P3_USER_AGENT overrides it wholesale.
//
// Do not report a library user-agent (Go's default "Go-http-client/1.1", or
// Perl's "libwww-perl") to the BV-BRC data API: it sits behind Cloudflare,
// which rejects some of those outright with error 1010.
//
// The Perl clients also honor P3_CLIENT_PRODUCT, which their generated
// wrappers export. That is deliberately not read here: a Perl p3-* tool that
// shells out to one of these would otherwise pass its own identity down, and
// unlike Perl this side has a compile-time seam and does not need the
// environment one.
func UserAgent() string {
	if ua := headerSafe(os.Getenv("P3_USER_AGENT")); ua != "" {
		return ua
	}
	return Product() + "/" + Get()
}

// headerSafe strips what would make a string an invalid -- or forged -- header
// value. P3_USER_AGENT comes from the environment, so a newline in it is
// header injection rather than a typo; net/http would reject the request
// outright, which is a confusing way to learn that.
func headerSafe(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	return strings.TrimSpace(s)
}
