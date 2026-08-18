package version

import (
	"strings"
	"testing"
)

func TestGetPrefersStampedVersion(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })

	Version = "2.0.12"
	if got := Get(); got != "2.0.12" {
		t.Errorf("Get() = %q, want the stamped version", got)
	}
}

// Nothing is stamped into a `go test` binary, so this exercises the fallback.
func TestGetFallbackIsUsable(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })

	Version = ""
	got := Get()
	if got == "" || strings.ContainsAny(got, " \t\n") {
		t.Errorf("Get() = %q, want a single non-empty token", got)
	}
}

func TestUserAgent(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })
	Version = "2.0.12"

	t.Setenv("P3_USER_AGENT", "")
	if got, want := UserAgent(), "bvbrc-go-sdk/2.0.12"; got != want {
		t.Errorf("UserAgent() = %q, want %q", got, want)
	}

	// The data API rejects some default library user-agents, so the SDK must
	// never fall back to one.
	for _, bad := range []string{"Go-http-client", "libwww-perl"} {
		if strings.Contains(UserAgent(), bad) {
			t.Errorf("UserAgent() = %q, must not look like %s", UserAgent(), bad)
		}
	}
}

func TestUserAgentEnvOverride(t *testing.T) {
	t.Setenv("P3_USER_AGENT", "something else")
	if got := UserAgent(); got != "something else" {
		t.Errorf("UserAgent() = %q, want the P3_USER_AGENT value", got)
	}
}

// Every case below starts from a clean slate: whichever binary these run in,
// some other package's init may already have declared a product.
func withNoProduct(t *testing.T) {
	t.Helper()

	productMu.Lock()
	old := product
	product = ""
	productMu.Unlock()

	t.Cleanup(func() {
		productMu.Lock()
		product = old
		productMu.Unlock()
	})
}

func TestProductDefaultsToTheLibraryName(t *testing.T) {
	withNoProduct(t)

	if got := Product(); got != Name {
		t.Errorf("Product() = %q, want the library name %q when none was declared", got, Name)
	}
}

func TestSetProductChangesTheUserAgent(t *testing.T) {
	withNoProduct(t)
	old := Version
	t.Cleanup(func() { Version = old })
	Version = "2.0.13"
	t.Setenv("P3_USER_AGENT", "")

	SetProduct(CLIProduct)
	if got, want := UserAgent(), "bvbrc-cli-go/2.0.13"; got != want {
		t.Errorf("UserAgent() = %q, want %q", got, want)
	}

	// p3-login declares its own, so a login can be picked out of an access log.
	SetProduct(AuthProduct)
	if got, want := UserAgent(), "bvbrc-auth-go/2.0.13"; got != want {
		t.Errorf("UserAgent() = %q, want %q", got, want)
	}
}

func TestUserAgentEnvOverridesTheProduct(t *testing.T) {
	withNoProduct(t)
	SetProduct(CLIProduct)

	t.Setenv("P3_USER_AGENT", "libwww-perl/6.67")
	if got := UserAgent(); got != "libwww-perl/6.67" {
		t.Errorf("UserAgent() = %q, want P3_USER_AGENT to outrank the product", got)
	}
}

// P3_CLIENT_PRODUCT is the Perl wrappers' seam. A Perl p3-* tool that shells
// out to one of these must not hand down its identity.
func TestClientProductEnvIsIgnored(t *testing.T) {
	withNoProduct(t)
	SetProduct(CLIProduct)

	t.Setenv("P3_USER_AGENT", "")
	t.Setenv("P3_CLIENT_PRODUCT", "bvbrc-cli-perl/1.2.3")
	if got := UserAgent(); strings.Contains(got, "perl") {
		t.Errorf("UserAgent() = %q, want the inherited Perl identity to be ignored", got)
	}
}

// The agent string becomes a header value and comes from the environment, so a
// newline in it is header injection rather than a typo -- and net/http would
// reject the whole request over it.
func TestUserAgentStripsControlCharacters(t *testing.T) {
	withNoProduct(t)

	t.Setenv("P3_USER_AGENT", "bad\r\nX-Injected: yes")
	if got := UserAgent(); strings.ContainsAny(got, "\r\n") {
		t.Errorf("UserAgent() = %q, want control characters stripped", got)
	}
}
