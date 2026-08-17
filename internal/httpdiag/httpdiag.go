// Package httpdiag diagnoses failed HTTP requests to the BV-BRC services.
//
// The BV-BRC sites sit behind Cloudflare, which sometimes refuses a request
// before it ever reaches the origin (error 1010 for a disallowed user-agent, a
// managed challenge, a rate limit). When that happens the client used to report
// nothing but a status code, which is not enough to tell a bad password from an
// edge rejection.
//
// Two things are offered here: Describe, which folds the essentials — including
// the CF-Ray id Cloudflare support asks for — into an error message, and Report,
// which dumps the whole exchange when diagnostics are enabled.
//
// Diagnostics are enabled by setting P3_DEBUG_HTTP in the environment, or by
// calling SetEnabled (the --debug-http and --debug flags do this).
package httpdiag

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
)

// BodyLimit is how many bytes of a failing response body we are willing to
// print or quote.
const BodyLimit = 2048

var enabled atomic.Bool

func init() {
	if v := os.Getenv("P3_DEBUG_HTTP"); v != "" && v != "0" {
		enabled.Store(true)
	}
}

// Enabled reports whether HTTP diagnostics are on.
func Enabled() bool { return enabled.Load() }

// SetEnabled turns HTTP diagnostics on or off for the rest of the process.
func SetEnabled(v bool) { enabled.Store(v) }

// redacted names the headers whose values may carry credentials. They are never
// printed: LoginRast puts the user's password in a Basic Authorization header,
// and the API and JSONRPC clients send the login token in the same place.
var redacted = map[string]bool{
	"authorization":       true,
	"cookie":              true,
	"set-cookie":          true,
	"proxy-authorization": true,
	"x-auth-token":        true,
}

// cfStatus lists the statuses Cloudflare itself generates when it refuses a
// request or cannot reach the origin. 403 covers the WAF and user-agent blocks.
var cfStatus = map[int]bool{
	403: true, 429: true, 503: true,
	520: true, 521: true, 522: true, 523: true,
	524: true, 525: true, 526: true, 527: true, 530: true,
}

// Cloudflare serves the same rejection in three shapes, depending on what the
// request asked for: an HTML error page, a JSON document carrying
// "cloudflare_error":true and "error_code":1010, or a bare text/plain body
// reading "error code: 1010". All three were observed against BV-BRC hosts in
// August 2026, so match all of them — and all the spellings of the number.
var cfBodyRE = regexp.MustCompile(`(?i)Error\s+(?:code)?\s*:?\s*10\d\d|Attention Required|Cloudflare Ray ID|cf-error-details|__cf_chl|"cloudflare_error"\s*:\s*true|"error_code"\s*:\s*10\d\d`)

var cfErrorNumREs = []*regexp.Regexp{
	regexp.MustCompile(`(?i)"error_code"\s*:\s*(10\d\d)`),
	regexp.MustCompile(`(?i)Error\s+(?:code)?\s*:?\s*(10\d\d)`),
}

// cfErrorNum returns the Cloudflare error number named in a body, or "".
func cfErrorNum(body []byte) string {
	for _, re := range cfErrorNumREs {
		if m := re.FindSubmatch(truncate(body)); m != nil {
			return string(m[1])
		}
	}
	return ""
}

// Ray returns the CF-Ray identifier Cloudflare stamps on everything it proxies,
// or "" if there is none. It identifies this exact request at the edge.
func Ray(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	return resp.Header.Get("CF-Ray")
}

// IsCloudflareBlock reports whether the response came from Cloudflare's edge
// rather than from our own service.
//
// A CF-Ray header is not evidence of a block: Cloudflare stamps one on
// everything it proxies, including an ordinary 401 from the login service. What
// distinguishes a block is CF-Mitigated, the text of a Cloudflare error page, or
// one of Cloudflare's own statuses served as HTML.
func IsCloudflareBlock(resp *http.Response, body []byte) bool {
	if resp == nil || resp.StatusCode < 400 {
		return false
	}

	if resp.Header.Get("CF-Mitigated") != "" {
		return true
	}

	if cfBodyRE.Match(truncate(body)) {
		return true
	}

	// An error page from the edge is HTML; our services answer in JSON.
	return strings.Contains(strings.ToLower(resp.Header.Get("Server")), "cloudflare") &&
		cfStatus[resp.StatusCode] &&
		strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html")
}

// Describe returns a one-line summary of a failed response, suitable for
// wrapping into an error. It names a Cloudflare block as such, and always
// reports the CF-Ray id when there is one.
func Describe(resp *http.Response, body []byte) string {
	if resp == nil {
		return "no response"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "status %s", resp.Status)

	if IsCloudflareBlock(resp, body) {
		b.WriteString(" - blocked by Cloudflare")
		if code := cfErrorNum(body); code != "" {
			if code == "1010" {
				fmt.Fprintf(&b, " (error %s: user-agent not allowed)", code)
			} else {
				fmt.Fprintf(&b, " (error %s)", code)
			}
		}
	} else if line := firstTextLine(body); line != "" {
		fmt.Fprintf(&b, " - %s", line)
	}

	if ray := Ray(resp); ray != "" {
		fmt.Fprintf(&b, " [CF-Ray %s]", ray)
	}

	if !Enabled() {
		b.WriteString(". Re-run with P3_DEBUG_HTTP=1 for the full HTTP headers")
	}

	return b.String()
}

// Report writes the request and response headers of a failed exchange to w, so
// they can be pasted into a support ticket. Credential-bearing headers are
// redacted and the request body is never printed: for a login it is the user's
// password.
//
// Callers should gate this on Enabled (or their own debug flag); Report itself
// does not check.
func Report(w io.Writer, req *http.Request, resp *http.Response, body []byte) {
	fmt.Fprintln(w, "---- BV-BRC HTTP diagnostics ----")

	if req != nil {
		fmt.Fprintf(w, "Request: %s %s\n", req.Method, req.URL)
		writeHeaders(w, req.Header, "  ")
		if req.ContentLength != 0 {
			fmt.Fprintln(w, "  (request body not shown; it may contain credentials)")
		}
	}

	if resp == nil {
		fmt.Fprintln(w, "No response received.")
		fmt.Fprintln(w, "---- end HTTP diagnostics ----")
		return
	}

	fmt.Fprintf(w, "Response: %s\n", resp.Status)
	writeHeaders(w, resp.Header, "  ")

	switch {
	case len(body) == 0:
		fmt.Fprintln(w, "Response body: (empty)")
	case len(body) > BodyLimit:
		fmt.Fprintf(w, "Response body (%d bytes, first %d shown):\n%s\n", len(body), BodyLimit, body[:BodyLimit])
	default:
		fmt.Fprintf(w, "Response body (%d bytes):\n%s\n", len(body), body)
	}

	fmt.Fprintln(w, "---- end HTTP diagnostics ----")
}

// ReportIfEnabled is the common case: dump the exchange only when diagnostics
// are on. force lets a caller add its own flag (the api client's Debug field).
func ReportIfEnabled(force bool, req *http.Request, resp *http.Response, body []byte) {
	if force || Enabled() {
		Report(os.Stderr, req, resp, body)
	}
}

func writeHeaders(w io.Writer, h http.Header, indent string) {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		for _, value := range h[name] {
			if redacted[strings.ToLower(name)] {
				value = fmt.Sprintf("<redacted, %d bytes>", len(value))
			}
			fmt.Fprintf(w, "%s%s: %s\n", indent, name, value)
		}
	}
}

// truncate caps a body for pattern matching so a huge response cannot make the
// regexp scan expensive. The Cloudflare markers all appear early.
func truncate(body []byte) []byte {
	if len(body) > BodyLimit {
		return body[:BodyLimit]
	}
	return body
}

// firstTextLine returns the first non-blank line of a body when it looks like a
// service message rather than markup, truncated for a one-line error.
func firstTextLine(body []byte) string {
	s := strings.TrimSpace(string(truncate(body)))
	if s == "" || strings.HasPrefix(s, "<") {
		return ""
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return strings.TrimSpace(s)
}
