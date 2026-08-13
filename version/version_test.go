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
