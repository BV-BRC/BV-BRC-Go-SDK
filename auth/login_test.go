package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
)

const cfBlockPage = `<!DOCTYPE html><html><head><title>Access denied</title></head>
<body><h1>Access denied</h1><h2>Error 1010</h2>
<div class="cf-error-details">Cloudflare Ray ID: 8f2c1d4e5a6b7890</div></body></html>`

// withAuthURL points a login URL variable at a test server for the duration of
// the test.
func withAuthURL(t *testing.T, target *string, h http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(h)
	orig := *target
	*target = srv.URL
	t.Cleanup(func() { *target = orig; srv.Close() })
}

func TestLoginPatricCloudflareBlock(t *testing.T) {
	withAuthURL(t, &PatricAuthURL, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("CF-Ray", "8f2c1d4e5a6b7890-ORD")
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, cfBlockPage)
	})

	_, err := LoginPatric("someone", "hunter2")
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"login failed", "blocked by Cloudflare", "1010", "8f2c1d4e5a6b7890-ORD"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, missing %q", err, want)
		}
	}
}

func TestLoginPatricBadPassword(t *testing.T) {
	// Our own service answering: proxied by Cloudflare, but not a block.
	withAuthURL(t, &PatricAuthURL, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("CF-Ray", "abc123-ORD")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"message":"Invalid username, email, or password"}`)
	})

	_, err := LoginPatric("someone", "wrong")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "blocked by Cloudflare") {
		t.Errorf("error = %q, a wrong password must not be reported as a Cloudflare block", err)
	}
	if !strings.Contains(err.Error(), "Invalid username") {
		t.Errorf("error = %q, should quote the service message", err)
	}
}

func TestLoginPatricSuccess(t *testing.T) {
	withAuthURL(t, &PatricAuthURL, func(w http.ResponseWriter, r *http.Request) {
		// Whatever the program declared itself to be -- "bvbrc-auth-go/…"
		// from p3-login, the library's own name from a test binary -- never
		// Go's default, which Cloudflare may reject.
		if got, want := r.Header.Get("User-Agent"), version.UserAgent(); got != want {
			t.Errorf("User-Agent = %q, want %q", got, want)
		}
		io.WriteString(w, "un=someone@patricbrc.org|tokenid=x|sig=y")
	})

	token, err := LoginPatric("someone@patricbrc.org", "hunter2")
	if err != nil {
		t.Fatalf("LoginPatric: %v", err)
	}
	if !strings.Contains(token, "un=someone@patricbrc.org") {
		t.Errorf("token = %q", token)
	}
}

func TestLoginRastCloudflareBlock(t *testing.T) {
	withAuthURL(t, &RastAuthURL, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("CF-Ray", "8f2c1d4e5a6b7890-ORD")
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, cfBlockPage)
	})

	_, err := LoginRast("someone", "hunter2")
	if err == nil {
		t.Fatal("expected an error")
	}
	// The regression guarded here: LoginRast used to check the status before
	// reading the body, so the page naming the rule was thrown away.
	if !strings.Contains(err.Error(), "blocked by Cloudflare") {
		t.Errorf("error = %q, want the block named", err)
	}
	if !strings.Contains(err.Error(), "1010") {
		t.Errorf("error = %q, want the error number from the body", err)
	}
}
