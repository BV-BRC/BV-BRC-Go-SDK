package httpdiag

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A trimmed Cloudflare 1010 block page, of the kind served when the presented
// user-agent is not allowed.
const cf1010Body = `<!DOCTYPE html><html><head><title>Access denied | www.patricbrc.org used Cloudflare to restrict access</title></head>
<body><h1>Access denied</h1><h2>Error code 1010</h2>
<p>The owner of this website has banned your access based on your browser's signature.</p>
<div class="cf-error-details">Cloudflare Ray ID: <strong>8f2c1d4e5a6b7890</strong></div></body></html>`

// The same rejection as served to a client that asked for JSON — captured from
// www.patricbrc.org/api in August 2026. Note it is application/json, so the
// HTML heuristic does not apply; the structured markers are what identify it.
const cf1010JSON = `{"type":"https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-1xxx-errors/error-1010/","title":"Error 1010: Access denied","status":403,"detail":"The site owner has blocked access based on your browser's signature.","instance":"a2cb01bfa85fa599","error_code":1010,"error_name":"browser_signature_banned","error_category":"access_denied","ray_id":"a2cb01bfa85fa599","cloudflare_error":true,"retryable":false}`

// And the terse third form, served as text/plain to a client that asked for
// neither — this is what the Perl solr query path gets from www.patricbrc.org.
const cf1010Text = "error code: 1010"

// What our own login service returns for a wrong password: JSON, from the
// origin, but still proxied by Cloudflare so it carries a CF-Ray.
const originAuthBody = `{"message":"Invalid username, email, or password","error":{"status":401}}`

func serve(t *testing.T, status int, headers map[string]string, body string) (*http.Response, []byte) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)

	req, err := http.NewRequest("POST", srv.URL, strings.NewReader("username=someone&password=hunter2"))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.SetBasicAuth("someone", "hunter2")
	req.Header.Set("User-Agent", "bvbrc-go-sdk/test")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	// The response we hand back must still know its request, as a real one does.
	resp.Request = req
	return resp, got
}

func TestIsCloudflareBlock(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		headers map[string]string
		body    string
		want    bool
	}{
		{
			name:    "1010 block page",
			status:  http.StatusForbidden,
			headers: map[string]string{"Server": "cloudflare", "CF-Ray": "8f2c1d4e5a6b7890-ORD", "Content-Type": "text/html; charset=UTF-8"},
			body:    cf1010Body,
			want:    true,
		},
		{
			name:    "1010 as json",
			status:  http.StatusForbidden,
			headers: map[string]string{"Server": "cloudflare", "CF-Ray": "a2cb01bfa85fa599-ORD", "Content-Type": "application/json; charset=utf-8"},
			body:    cf1010JSON,
			want:    true,
		},
		{
			name:    "1010 as text/plain",
			status:  http.StatusForbidden,
			headers: map[string]string{"Server": "cloudflare", "CF-Ray": "a2cb01bfa85fa599-ORD", "Content-Type": "text/plain; charset=UTF-8"},
			body:    cf1010Text,
			want:    true,
		},
		{
			name:    "managed challenge",
			status:  http.StatusForbidden,
			headers: map[string]string{"Server": "cloudflare", "CF-Mitigated": "challenge", "CF-Ray": "8f2c1d4e5a6b7890-ORD"},
			body:    "",
			want:    true,
		},
		{
			// The regression this rule exists for: a CF-Ray alone must not be
			// read as a block, or every wrong password looks like one.
			name:    "origin 401 proxied by cloudflare",
			status:  http.StatusUnauthorized,
			headers: map[string]string{"Server": "cloudflare", "CF-Ray": "8f2c1d4e5a6b7890-ORD", "Content-Type": "application/json; charset=utf-8"},
			body:    originAuthBody,
			want:    false,
		},
		{
			name:    "origin 500",
			status:  http.StatusInternalServerError,
			headers: map[string]string{"Content-Type": "application/json"},
			body:    `{"error":"database is down"}`,
			want:    false,
		},
		{
			name:    "success",
			status:  http.StatusOK,
			headers: map[string]string{"Server": "cloudflare", "CF-Ray": "8f2c1d4e5a6b7890-ORD"},
			body:    `{"ok":true}`,
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := serve(t, tc.status, tc.headers, tc.body)
			if got := IsCloudflareBlock(resp, body); got != tc.want {
				t.Errorf("IsCloudflareBlock = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDescribeCloudflareBlock(t *testing.T) {
	resp, body := serve(t, http.StatusForbidden,
		map[string]string{"Server": "cloudflare", "CF-Ray": "8f2c1d4e5a6b7890-ORD", "Content-Type": "text/html"},
		cf1010Body)

	got := Describe(resp, body)
	for _, want := range []string{"403", "blocked by Cloudflare", "1010", "user-agent not allowed", "CF-Ray 8f2c1d4e5a6b7890-ORD"} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe() = %q, missing %q", got, want)
		}
	}
}

func TestDescribeCloudflareBlockJSON(t *testing.T) {
	resp, body := serve(t, http.StatusForbidden,
		map[string]string{"Server": "cloudflare", "CF-Ray": "a2cb01bfa85fa599-ORD", "Content-Type": "application/json"},
		cf1010JSON)

	got := Describe(resp, body)
	for _, want := range []string{"blocked by Cloudflare", "error 1010", "user-agent not allowed"} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe() = %q, missing %q", got, want)
		}
	}
}

func TestDescribeCloudflareBlockText(t *testing.T) {
	resp, body := serve(t, http.StatusForbidden,
		map[string]string{"Server": "cloudflare", "CF-Ray": "a2cb01bfa85fa599-ORD", "Content-Type": "text/plain"},
		cf1010Text)

	got := Describe(resp, body)
	for _, want := range []string{"blocked by Cloudflare", "error 1010", "user-agent not allowed"} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe() = %q, missing %q", got, want)
		}
	}
}

func TestDescribeOriginError(t *testing.T) {
	resp, body := serve(t, http.StatusUnauthorized,
		map[string]string{"Server": "cloudflare", "CF-Ray": "abc123-ORD", "Content-Type": "application/json"},
		originAuthBody)

	got := Describe(resp, body)
	if strings.Contains(got, "Cloudflare") && !strings.Contains(got, "CF-Ray") {
		t.Errorf("Describe() = %q, should not call an origin 401 a Cloudflare block", got)
	}
	if strings.Contains(got, "blocked by Cloudflare") {
		t.Errorf("Describe() = %q, should not report a block", got)
	}
	if !strings.Contains(got, "Invalid username") {
		t.Errorf("Describe() = %q, should quote the service message", got)
	}
	// The Ray id is still worth reporting: it identifies the request at the edge.
	if !strings.Contains(got, "CF-Ray abc123-ORD") {
		t.Errorf("Describe() = %q, should still carry the Ray id", got)
	}
}

func TestReportRedactsCredentials(t *testing.T) {
	resp, body := serve(t, http.StatusForbidden,
		map[string]string{"Server": "cloudflare", "CF-Ray": "8f2c1d4e5a6b7890-ORD", "Content-Type": "text/html",
			"Set-Cookie": "__cf_bm=secretcookievalue; path=/"},
		cf1010Body)

	var buf bytes.Buffer
	Report(&buf, resp.Request, resp, body)
	out := buf.String()

	// The request carries Basic c29tZW9uZTpodW50ZXIy, which decodes to the
	// password; the response sets a cookie. Neither may appear.
	for _, secret := range []string{"hunter2", "c29tZW9uZTpodW50ZXIy", "secretcookievalue"} {
		if strings.Contains(out, secret) {
			t.Errorf("Report() leaked %q:\n%s", secret, out)
		}
	}
	// Header names come back in Go's canonical casing ("Cf-Ray"), not the wire's.
	for _, want := range []string{"<redacted,", "Authorization", "Set-Cookie", "Cf-Ray", "403", "Error code 1010"} {
		if !strings.Contains(out, want) {
			t.Errorf("Report() missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "request body not shown") {
		t.Errorf("Report() should say the request body was withheld:\n%s", out)
	}
}

func TestReportTruncatesBody(t *testing.T) {
	big := strings.Repeat("x", BodyLimit*3)
	resp, body := serve(t, http.StatusInternalServerError, nil, big)

	var buf bytes.Buffer
	Report(&buf, resp.Request, resp, body)
	out := buf.String()

	if !strings.Contains(out, "first 2048 shown") {
		t.Errorf("Report() should note the truncation:\n%s", out[:min(len(out), 400)])
	}
	if len(out) > BodyLimit*2 {
		t.Errorf("Report() emitted %d bytes for a %d-byte body; should be capped", len(out), len(big))
	}
}

func TestEnabledToggle(t *testing.T) {
	orig := Enabled()
	t.Cleanup(func() { SetEnabled(orig) })

	SetEnabled(false)
	if Enabled() {
		t.Fatal("Enabled() true after SetEnabled(false)")
	}
	SetEnabled(true)
	if !Enabled() {
		t.Fatal("Enabled() false after SetEnabled(true)")
	}
}

func TestDescribeHintOnlyWhenDisabled(t *testing.T) {
	orig := Enabled()
	t.Cleanup(func() { SetEnabled(orig) })

	resp, body := serve(t, http.StatusForbidden, map[string]string{"Content-Type": "application/json"}, `{"error":"nope"}`)

	SetEnabled(false)
	if !strings.Contains(Describe(resp, body), "P3_DEBUG_HTTP=1") {
		t.Error("Describe() should suggest the debug switch when diagnostics are off")
	}
	SetEnabled(true)
	if strings.Contains(Describe(resp, body), "P3_DEBUG_HTTP=1") {
		t.Error("Describe() should not suggest the debug switch when it is already on")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
