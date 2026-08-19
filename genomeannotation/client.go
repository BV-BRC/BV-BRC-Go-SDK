// Package genomeannotation provides a client for the BV-BRC GenomeAnnotation
// service.
//
// The service is the back end of the rast-* command-line tools: it takes a
// genome typed object (a "GTO"), runs one annotation step on it, and hands the
// GTO back. A whole annotation is built by piping one call into the next.
//
// GTOs pass through this package as json.RawMessage rather than as a Go struct.
// The GTO schema is large, versioned by the service rather than by us, and
// carries fields we have no business touching; decoding one into
// map[string]interface{} would also renumber every integer as a float and
// reorder the keys on the way out. Passing the bytes along keeps a step that
// only means to add features from silently rewriting the rest of the genome.
package genomeannotation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BV-BRC/BV-BRC-Go-SDK/auth"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/httpdiag"
	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
)

const (
	// DefaultURL is the default GenomeAnnotation service URL.
	DefaultURL = "https://p3.theseed.org/services/genome_annotation"

	// DefaultTimeout is the HTTP client timeout. Annotation steps are long:
	// the Perl client waits the same half hour, overridable the same way.
	DefaultTimeout = 30 * time.Minute
)

// Client is a client for the GenomeAnnotation service.
//
// A token is optional. Several methods -- default_workflow,
// enumerate_special_protein_databases, enumerate_classifiers -- answer an
// unauthenticated caller, and the rast-* tools have never required a login for
// them, so the client must not insist on one.
type Client struct {
	URL     string
	Token   string
	Timeout time.Duration
	client  *http.Client
}

// New creates a GenomeAnnotation client.
func New(opts ...Option) *Client {
	c := &Client{
		URL:     DefaultURL,
		Timeout: envTimeout(DefaultTimeout),
	}

	for _, opt := range opts {
		opt(c)
	}

	c.client = &http.Client{Timeout: c.Timeout}

	return c
}

// envTimeout honors CDMI_TIMEOUT, in seconds, as the Perl client does
// (Bio::KBase::GenomeAnnotation::Client). A value that does not parse, or is
// not positive, leaves the default alone rather than failing the command.
func envTimeout(def time.Duration) time.Duration {
	v := os.Getenv("CDMI_TIMEOUT")
	if v == "" {
		return def
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs <= 0 {
		return def
	}
	return time.Duration(secs) * time.Second
}

// Option is a functional option for configuring the client.
type Option func(*Client)

// WithURL sets a custom service URL. An empty string keeps the default, so a
// caller can pass an unset --url flag straight through.
func WithURL(url string) Option {
	return func(c *Client) {
		if url != "" {
			c.URL = url
		}
	}
}

// WithToken sets the authentication token.
func WithToken(token interface{}) Option {
	return func(c *Client) {
		switch t := token.(type) {
		case string:
			c.Token = t
		case *auth.Token:
			if t != nil {
				c.Token = t.String()
			}
		}
	}
}

// WithTimeout sets a custom timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.Timeout = timeout
	}
}

// rpcRequest represents a JSON-RPC request.
type rpcRequest struct {
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	Version string        `json:"version"`
	ID      string        `json:"id"`
}

// rpcResponse represents a JSON-RPC response. The service returns the method's
// return values as a list, so Result is decoded a second time by the callers
// below.
type rpcResponse struct {
	Result []json.RawMessage `json:"result"`
	Error  *rpcError         `json:"error,omitempty"`
	ID     string            `json:"id"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func (e *rpcError) String() string {
	if len(e.Error) == 0 {
		return e.Message
	}
	// The service usually puts a plain string here; decoding it turns the
	// escaped newlines of a Perl die message back into real ones.
	var s string
	if err := json.Unmarshal(e.Error, &s); err == nil {
		return s
	}
	return string(e.Error)
}

// callN makes a JSON-RPC call and returns all of the method's return values.
// Most methods return one; classify_full returns three.
func (c *Client) callN(method string, params ...interface{}) ([]json.RawMessage, error) {
	if params == nil {
		params = []interface{}{}
	}

	req := rpcRequest{
		Method:  "GenomeAnnotation." + method,
		Params:  params,
		Version: "1.1",
		ID:      "1",
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	httpReq, err := http.NewRequest("POST", c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", version.UserAgent())
	if c.Token != "" {
		httpReq.Header.Set("Authorization", c.Token)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var rpcResp rpcResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		// The service answers JSON-RPC even for its own errors, so an
		// unparseable body behind a failing status came from something in
		// front of it -- a Cloudflare block page, a proxy error. Say so,
		// rather than reporting a JSON parse error against HTML.
		if resp.StatusCode >= 400 {
			httpdiag.ReportIfEnabled(false, httpReq, resp, respBody)
			return nil, fmt.Errorf("%s failed: %s", method, httpdiag.Describe(resp, respBody))
		}
		return nil, fmt.Errorf("parsing response: %w (body: %s)", err, string(respBody))
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("%s: %s", method, cleanErrorMessage(rpcResp.Error.String()))
	}

	return rpcResp.Result, nil
}

// call is callN for the usual case of a single return value.
func (c *Client) call(method string, params ...interface{}) (json.RawMessage, error) {
	res, err := c.callN(method, params...)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("%s: service returned no result", method)
	}
	return res[0], nil
}

// cleanErrorMessage unwraps the _ERROR_..._ERROR_ envelope the service puts
// around a message from the annotation code itself.
func cleanErrorMessage(msg string) string {
	if start := strings.Index(msg, "_ERROR_"); start != -1 {
		if end := strings.LastIndex(msg, "_ERROR_"); end > start {
			return strings.TrimSpace(msg[start+len("_ERROR_") : end])
		}
	}
	return msg
}
