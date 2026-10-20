// Package api provides a client for the BV-BRC Data API.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BV-BRC/BV-BRC-Go-SDK/v2/internal/httpdiag"
	"github.com/BV-BRC/BV-BRC-Go-SDK/v2/version"
)

const (
	// DefaultBaseURL is the default BV-BRC API endpoint.
	DefaultBaseURL = "https://www.bv-brc.org/api"
	// DefaultChunkSize is the default number of records to fetch per request.
	DefaultChunkSize = 25000
	// DefaultMaxRetries is the default number of retry attempts for failed requests.
	DefaultMaxRetries = 3
)

// DefaultUserAgent is the library's own name and build version, e.g.
// "bvbrc-go-sdk/2.0.13". The data API sits behind Cloudflare, which bans some
// default library user-agents (error 1010), so a request must always name us.
//
// This is only what a program that declares no product identity sends: a
// client built by NewClient reports version.UserAgent() instead, which is
// "bvbrc-cli-go/<version>" from the p3-* tools and honors P3_USER_AGENT. It is
// resolved per request rather than cached here, because a product declared in
// an init function is set after this package is initialized.
var DefaultUserAgent = version.Name + "/" + version.Get()

// Client provides access to the BV-BRC Data API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Token      string
	ChunkSize  int
	MaxRetries int
	Debug      bool
	Verbose    bool   // Print retry messages to stderr
	UserAgent  string // Sent as the User-Agent header (Cloudflare allowlist)
}

// ChunkInfo contains information about a response chunk from Content-Range header.
type ChunkInfo struct {
	Start      int
	Next       int
	Count      int
	IsLast     bool
	CursorMark string // Cursor mark from X-Cursor-Mark header for cursor-based pagination
}

// ClientOption is a function that configures a Client.
type ClientOption func(*Client)

// WithBaseURL sets the base URL for the API client.
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) {
		c.BaseURL = strings.TrimSuffix(baseURL, "/")
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.HTTPClient = httpClient
	}
}

// WithToken sets the authentication token.
// Accepts either a string or a *auth.Token.
func WithToken(token any) ClientOption {
	return func(c *Client) {
		switch t := token.(type) {
		case string:
			c.Token = t
		case interface{ String() string }:
			c.Token = t.String()
		default:
			if token != nil {
				c.Token = fmt.Sprintf("%v", token)
			}
		}
	}
}

// WithChunkSize sets the number of records to fetch per request.
func WithChunkSize(size int) ClientOption {
	return func(c *Client) {
		c.ChunkSize = size
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(retries int) ClientOption {
	return func(c *Client) {
		c.MaxRetries = retries
	}
}

// WithDebug enables debug output. It also turns on HTTP diagnostics process-wide,
// so that the Workspace and AppService calls a command makes alongside its data
// queries dump their failures too; --debug is the one switch a user reaches for.
func WithDebug(debug bool) ClientOption {
	return func(c *Client) {
		c.Debug = debug
		if debug {
			httpdiag.SetEnabled(true)
		}
	}
}

// WithVerbose enables verbose output (retry messages to stderr).
func WithVerbose(verbose bool) ClientOption {
	return func(c *Client) {
		c.Verbose = verbose
	}
}

// WithUserAgent sets the User-Agent header sent on every request.
func WithUserAgent(ua string) ClientOption {
	return func(c *Client) {
		if ua != "" {
			c.UserAgent = ua
		}
	}
}

// NewClient creates a new BV-BRC API client with the given options.
func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		BaseURL:    DefaultBaseURL,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		ChunkSize:  DefaultChunkSize,
		MaxRetries: DefaultMaxRetries,
		UserAgent:  version.UserAgent(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// cursorPages walks a query with Solr's cursorMark, handing each page to fn.
// Returning false from fn stops the walk. It is the single pagination path
// behind Query, QueryCallback and Stream.
//
// Cursor is the only mode this client offers. Offset paging (start=N&rows=M)
// makes every shard collect and rank N+M documents and the aggregator merge
// them -- work proportional to the offset, repeated per page and thrown away,
// paid by the shared Solr tier rather than by the client that asked for it. A
// single page at offset 1,000,000 against genome_feature measured 96.7s
// (QTime 96,573ms) where a cursor page of the same size takes ~5s at any
// depth. It is also the more correct mechanism: without a total order Solr's
// ranking of equal documents across shards is not stable between requests, so
// offset paging can repeat or skip rows even on a static index.
func (c *Client) cursorPages(ctx context.Context, objectType string, q *Query,
	fn func([]map[string]any, *ChunkInfo) bool) error {

	resolvedType := GetObjectType(objectType)

	// Ensure query has at least one filter (BV-BRC API requirement)
	if !q.HasFilters() {
		// Add a wildcard filter using the ID column for this object type
		idCol := GetIDColumn(resolvedType)
		if idCol == "" {
			idCol = "id" // fallback
		}
		q = q.Clone()
		q.Eq(idCol, "*")
	}

	// Clone for cursor manipulation so the caller's query is left alone.
	cursorQuery := q.Clone()

	// Determine chunk size
	chunkSize := c.ChunkSize
	if q.LimitValue > 0 && q.LimitValue < chunkSize {
		chunkSize = q.LimitValue
	}

	// A caller may supply a mark to resume from; "*" starts at the beginning.
	cursorMark := q.CursorMark
	if cursorMark == "" {
		cursorMark = "*"
	}

	reqURL := fmt.Sprintf("%s/%s/", c.BaseURL, resolvedType)
	totalFetched := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		cursorQuery.CursorMark = cursorMark
		body := appendLimitClause(cursorQuery.Build(), chunkSize)

		if c.Debug {
			fmt.Printf("DEBUG: POST %s (cursor)\n", reqURL)
			fmt.Printf("DEBUG: Body: %s\n", body)
		}

		results, chunkInfo, err := c.doQueryRequest(ctx, reqURL, body)
		if err != nil {
			return err
		}

		// Trim results if we would exceed the limit
		if q.LimitValue > 0 && totalFetched+len(results) > q.LimitValue {
			results = results[:q.LimitValue-totalFetched]
		}
		totalFetched += len(results)

		if !fn(results, chunkInfo) {
			return nil
		}

		// End conditions:
		// 1. No cursor returned
		// 2. Cursor unchanged (Solr's signal that this was the last page)
		// 3. No results returned
		if chunkInfo.CursorMark == "" ||
			chunkInfo.CursorMark == cursorMark ||
			len(results) == 0 {
			return nil
		}

		// Check if we've reached the requested limit
		if q.LimitValue > 0 && totalFetched >= q.LimitValue {
			return nil
		}

		cursorMark = chunkInfo.CursorMark
	}
}

// Query executes a query against the specified object type and returns the results.
// It handles automatic pagination to fetch all matching records.
//
// Note this buffers the entire result set. For a large one prefer
// QueryCallback or Stream, which hand back a page at a time.
func (c *Client) Query(ctx context.Context, objectType string, q *Query) ([]map[string]any, error) {
	var allResults []map[string]any

	err := c.cursorPages(ctx, objectType, q, func(results []map[string]any, _ *ChunkInfo) bool {
		allResults = append(allResults, results...)
		return true
	})
	if err != nil {
		return nil, err
	}

	return allResults, nil
}

// doQueryRequest executes a single query request with retry logic.
func (c *Client) doQueryRequest(ctx context.Context, url, body string) ([]map[string]any, *ChunkInfo, error) {
	var lastErr error

	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := time.Duration(1<<uint(attempt-1)) * time.Second
			if c.Verbose {
				fmt.Fprintf(os.Stderr, "Retry %d/%d after %v: %v\n", attempt, c.MaxRetries, delay, lastErr)
			}
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			return nil, nil, fmt.Errorf("creating request: %w", err)
		}

		c.setHeaders(req)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/rqlquery+x-www-form-urlencoded")

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("executing request: %w", err)
			continue
		}

		// Check for server errors (retry-able). Cloudflare's own 5xx (520-527)
		// land here, so diagnose before discarding the body.
		if resp.StatusCode >= 500 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			httpdiag.ReportIfEnabled(c.Debug, req, resp, bodyBytes)
			resp.Body.Close()
			lastErr = fmt.Errorf("server error: %s", httpdiag.Describe(resp, bodyBytes))
			continue
		}

		// Check for client errors (not retry-able)
		if resp.StatusCode >= 400 {
			err := c.apiError(req, resp)
			resp.Body.Close()
			return nil, nil, err
		}

		// Parse Content-Range header
		chunkInfo := parseContentRange(resp.Header.Get("Content-Range"))

		// Extract cursor mark for cursor-based pagination
		chunkInfo.CursorMark = resp.Header.Get("X-Cursor-Mark")

		// Parse response body
		var results []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			resp.Body.Close()
			lastErr = fmt.Errorf("decoding response: %w", err)
			continue
		}
		resp.Body.Close()

		return results, chunkInfo, nil
	}

	return nil, nil, lastErr
}

// Count returns the count of records matching the query.
func (c *Client) Count(ctx context.Context, objectType string, q *Query) (int, error) {
	resolvedType := GetObjectType(objectType)

	// Ensure query has at least one filter (BV-BRC API requirement)
	if !q.HasFilters() {
		idCol := GetIDColumn(resolvedType)
		if idCol == "" {
			idCol = "id"
		}
		q = q.Clone()
		q.Eq(idCol, "*")
	}

	queryStr := q.Build()

	reqURL := fmt.Sprintf("%s/%s/", c.BaseURL, resolvedType)
	body := queryStr
	if body != "" {
		body += "&"
	}
	body += "limit(1)"

	if c.Debug {
		fmt.Printf("DEBUG: POST %s (count)\n", reqURL)
		fmt.Printf("DEBUG: Body: %s\n", body)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("creating request: %w", err)
	}

	c.setHeaders(req)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/rqlquery+x-www-form-urlencoded")
	req.Header.Set("Range", "items=0-0")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return 0, c.apiError(req, resp)
	}

	chunkInfo := parseContentRange(resp.Header.Get("Content-Range"))
	return chunkInfo.Count, nil
}

// Stream returns results via channels for efficient processing of large datasets.
func (c *Client) Stream(ctx context.Context, objectType string, q *Query) (<-chan map[string]any, <-chan error) {
	results := make(chan map[string]any, 100)
	errs := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errs)

		err := c.cursorPages(ctx, objectType, q, func(batch []map[string]any, _ *ChunkInfo) bool {
			for _, record := range batch {
				select {
				case results <- record:
				case <-ctx.Done():
					errs <- ctx.Err()
					return false
				}
			}
			return true
		})
		if err != nil {
			errs <- err
		}
	}()

	return results, errs
}

// GetByID retrieves a single record by its ID.
func (c *Client) GetByID(ctx context.Context, objectType, id string) (map[string]any, error) {
	resolvedType := GetObjectType(objectType)
	reqURL := fmt.Sprintf("%s/%s/%s", c.BaseURL, resolvedType, c.urlEncode(id))

	if c.Debug {
		fmt.Printf("DEBUG: GET %s\n", reqURL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	c.setHeaders(req)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode >= 400 {
		return nil, c.apiError(req, resp)
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

// apiError turns a >= 400 response into an error, naming a Cloudflare rejection
// as such rather than reporting the block page as the service's answer. It
// consumes the body, and dumps the whole exchange to stderr when --debug or
// P3_DEBUG_HTTP is set.
func (c *Client) apiError(req *http.Request, resp *http.Response) error {
	bodyBytes, _ := io.ReadAll(resp.Body)
	httpdiag.ReportIfEnabled(c.Debug, req, resp, bodyBytes)

	if httpdiag.IsCloudflareBlock(resp, bodyBytes) {
		return fmt.Errorf("API error: %s", httpdiag.Describe(resp, bodyBytes))
	}
	return fmt.Errorf("API error: %s - %s", resp.Status, string(bodyBytes))
}

// setHeaders sets common headers for API requests.
func (c *Client) setHeaders(req *http.Request) {
	if c.Token != "" {
		req.Header.Set("Authorization", c.Token)
	}
	ua := c.UserAgent
	if ua == "" {
		ua = version.UserAgent()
	}
	req.Header.Set("User-Agent", ua)
}

// urlEncode encodes a string for use in URLs, using PATRIC-specific encoding.
// This matches the encoding used by the Perl P3DataAPI.
func (c *Client) urlEncode(s string) string {
	// Replace special characters with encoded versions
	// Note: PATRIC uses non-standard encoding for some characters
	replacements := map[string]string{
		"<":  "%60",
		">":  "%62",
		"=":  "%61",
		`"`:  "%22",
		"&":  "%26",
		"#":  "%23",
		" ":  "%20",
		"\t": "%09",
	}

	result := s
	for old, new := range replacements {
		result = strings.ReplaceAll(result, old, new)
	}
	return result
}

// parseContentRange parses a Content-Range header value.
// Format: "items START-END/TOTAL"
func parseContentRange(header string) *ChunkInfo {
	info := &ChunkInfo{}

	if header == "" {
		return info
	}

	// Match pattern: items START-END/TOTAL
	re := regexp.MustCompile(`items\s+(\d+)-(\d+)/(\d+)`)
	matches := re.FindStringSubmatch(header)
	if len(matches) != 4 {
		return info
	}

	info.Start, _ = strconv.Atoi(matches[1])
	info.Next, _ = strconv.Atoi(matches[2])
	info.Count, _ = strconv.Atoi(matches[3])
	info.IsLast = info.Next >= info.Count

	return info
}

// GetObjectType returns the internal object type name for a given alias.
// If no alias is found, returns the input unchanged.
func GetObjectType(name string) string {
	if mapped, ok := Objects[name]; ok {
		return mapped
	}
	return name
}

// GetIDColumn returns the primary ID column for an object type.
// Returns empty string if the object type is unknown.
func GetIDColumn(objectType string) string {
	return IDColumns[objectType]
}

// GetDefaultFields returns the default fields for an object type.
// Returns nil if no defaults are defined.
func GetDefaultFields(objectType string) []string {
	return DefaultFields[objectType]
}

// FieldInfo describes a field in an object schema.
type FieldInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	MultiValued bool   `json:"multiValued"`
}

// GetSchema returns the schema (field definitions) for an object type.
func (c *Client) GetSchema(ctx context.Context, objectType string) ([]FieldInfo, error) {
	resolvedType := GetObjectType(objectType)
	reqURL := fmt.Sprintf("%s/%s/schema?http_content-type=application/solrquery+x-www-form-urlencoded&http_accept=application/solr+json",
		c.BaseURL, resolvedType)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	c.setHeaders(req)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, c.apiError(req, resp)
	}

	var schema struct {
		Schema struct {
			Fields []FieldInfo `json:"fields"`
		} `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("decoding schema: %w", err)
	}

	return schema.Schema.Fields, nil
}

// QueryCallback executes a query and calls the callback function with each batch of results.
// The callback receives the records and chunk information. Return false to stop fetching.
//
// The ChunkInfo passed to the callback carries the CursorMark for the *next*
// page, delivered once the current page's records have been handed over -- so
// a caller that wants to checkpoint its progress can record it there.
func (c *Client) QueryCallback(ctx context.Context, objectType string, q *Query, callback func([]map[string]any, *ChunkInfo) bool) error {
	return c.cursorPages(ctx, objectType, q, callback)
}

// appendLimitClause appends a limit(n) clause to an already-built query body.
//
// Query.Build does not emit a limit clause of its own, so the cursor loop has
// to add one by hand. Without it the API applies its default page size of 25,
// which means hundreds of round trips for a large result set.
func appendLimitClause(body string, n int) string {
	if n <= 0 {
		return body
	}
	if body != "" {
		body += "&"
	}
	return body + fmt.Sprintf("limit(%d)", n)
}

// QueryWithCursor executes a query using cursor-based pagination.
//
// Deprecated: pagination is always cursor-based now; call Query. Kept so
// existing callers keep compiling.
func (c *Client) QueryWithCursor(ctx context.Context, objectType string, q *Query) ([]map[string]any, error) {
	return c.Query(ctx, objectType, q)
}

// StreamWithCursor returns results via channels using cursor-based pagination.
//
// Deprecated: pagination is always cursor-based now; call Stream.
func (c *Client) StreamWithCursor(ctx context.Context, objectType string, q *Query) (<-chan map[string]any, <-chan error) {
	return c.Stream(ctx, objectType, q)
}

// QueryCallbackWithCursor executes a query using cursor-based pagination,
// calling the callback with each page.
//
// Deprecated: pagination is always cursor-based now; call QueryCallback.
func (c *Client) QueryCallbackWithCursor(ctx context.Context, objectType string, q *Query, callback func([]map[string]any, *ChunkInfo) bool) error {
	return c.QueryCallback(ctx, objectType, q, callback)
}
