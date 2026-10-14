package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// pageServer serves `pages` pages of one record each, recording every request
// body it was sent. It mimics the real API: a Content-Range on every response
// and an X-Cursor-Mark that repeats itself on the last page.
func pageServer(t *testing.T, pages int, bodies *[]string) *httptest.Server {
	t.Helper()
	n := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		*bodies = append(*bodies, string(buf))

		n++
		mark := fmt.Sprintf("mark%d", n)
		if n >= pages {
			// Repeating the mark we were sent is how Solr says "last page".
			mark = lastSentMark(string(buf))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("items 0-1/%d", pages))
		w.Header().Set("X-Cursor-Mark", mark)
		_ = json.NewEncoder(w).Encode([]map[string]any{{"genome_id": fmt.Sprintf("1.%d", n)}})
	}))
}

// lastSentMark returns the cursor mark in a request body, decoded. The client
// sends the mark percent-encoded but holds it decoded, so a server echoing one
// back (which is how Solr signals the last page) has to decode it first --
// otherwise the client never sees its own mark returned and pages forever.
func lastSentMark(body string) string {
	i := strings.Index(body, "cursor(")
	if i < 0 {
		return ""
	}
	rest := body[i+len("cursor("):]
	j := strings.Index(rest, ")")
	if j < 0 {
		return ""
	}
	decoded, err := url.QueryUnescape(rest[:j])
	if err != nil {
		return rest[:j]
	}
	return decoded
}

// Offset pagination is gone on purpose: start=N&rows=M makes every shard
// collect and rank N+M documents and the aggregator merge them, work
// proportional to the offset and repeated on every page, paid by the shared
// Solr tier. A page at offset 1,000,000 measured 96.7s against genome_feature.
// Every entry point must therefore page by cursor -- including Query, which
// buffers its results but must not get them by walking offsets.
func TestEveryQueryPathPagesByCursor(t *testing.T) {
	ctx := context.Background()

	paths := map[string]func(c *Client, q *Query) error{
		"Query": func(c *Client, q *Query) error {
			_, err := c.Query(ctx, "genome", q)
			return err
		},
		"QueryCallback": func(c *Client, q *Query) error {
			return c.QueryCallback(ctx, "genome", q, func([]map[string]any, *ChunkInfo) bool { return true })
		},
		"Stream": func(c *Client, q *Query) error {
			results, errs := c.Stream(ctx, "genome", q)
			for range results {
			}
			select {
			case err := <-errs:
				return err
			default:
				return nil
			}
		},
		// The deprecated aliases must stay behaviourally identical.
		"QueryWithCursor": func(c *Client, q *Query) error {
			_, err := c.QueryWithCursor(ctx, "genome", q)
			return err
		},
		"QueryCallbackWithCursor": func(c *Client, q *Query) error {
			return c.QueryCallbackWithCursor(ctx, "genome", q, func([]map[string]any, *ChunkInfo) bool { return true })
		},
	}

	for name, run := range paths {
		t.Run(name, func(t *testing.T) {
			var bodies []string
			srv := pageServer(t, 3, &bodies)
			defer srv.Close()

			c := NewClient(WithBaseURL(srv.URL), WithChunkSize(1))
			if err := run(c, NewQuery().Select("genome_id")); err != nil {
				t.Fatalf("%s: %v", name, err)
			}

			if len(bodies) < 2 {
				t.Fatalf("%s: sent %d requests, expected pagination", name, len(bodies))
			}
			for i, b := range bodies {
				if !strings.Contains(b, "cursor(") {
					t.Errorf("%s: request %d has no cursor clause: %s", name, i, b)
				}
				// limit(n,offset) is the offset form; limit(n) is fine.
				if strings.Contains(b, ",") && offsetLimit(b) {
					t.Errorf("%s: request %d uses offset pagination: %s", name, i, b)
				}
			}
			// Pages after the first must carry the mark the server handed back.
			if !strings.Contains(bodies[1], "mark1") {
				t.Errorf("%s: second request did not resume from the returned mark: %s", name, bodies[1])
			}
		})
	}
}

// offsetLimit reports whether the body contains a two-argument limit clause.
func offsetLimit(body string) bool {
	i := strings.Index(body, "limit(")
	for i >= 0 {
		rest := body[i+len("limit("):]
		if j := strings.Index(rest, ")"); j >= 0 && strings.Contains(rest[:j], ",") {
			return true
		}
		next := strings.Index(rest, "limit(")
		if next < 0 {
			return false
		}
		i = i + len("limit(") + next
	}
	return false
}

// The API percent-decodes the request body, and a base64 cursor mark can
// contain '+', '/' and '='. Sending one raw works today only because the
// decoder happens not to apply the form rule that '+' means space.
func TestCursorMarkIsPercentEncodedOnTheWire(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(buf))
		w.Header().Set("Content-Range", "items 0-0/0")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()

	mark := "AoE/El+BBVFJJQy4x="
	c := NewClient(WithBaseURL(srv.URL))
	if _, err := c.Query(context.Background(), "genome", NewQuery().Eq("genome_id", "*").Cursor(mark)); err != nil {
		t.Fatalf("Query: %v", err)
	}

	if len(bodies) != 1 {
		t.Fatalf("got %d requests, want 1", len(bodies))
	}
	onWire := bodies[0][strings.Index(bodies[0], "cursor(")+len("cursor("):]
	onWire = onWire[:strings.Index(onWire, ")")]
	for _, raw := range []string{"+", "/", "="} {
		if strings.Contains(onWire, raw) {
			t.Errorf("mark reached the wire with a raw %q: %s", raw, bodies[0])
		}
	}
	if got := lastSentMark(bodies[0]); got != mark {
		t.Errorf("mark does not survive a decode round trip: got %q, want %q", got, mark)
	}
	if !strings.Contains(bodies[0], "%2B") || !strings.Contains(bodies[0], "%2F") || !strings.Contains(bodies[0], "%3D") {
		t.Errorf("mark was not percent-encoded: %s", bodies[0])
	}
}

// A caller that has checkpointed a mark must be able to start from it rather
// than from the beginning. This is what resumable dumps are built on.
func TestQueryStartsFromASuppliedCursorMark(t *testing.T) {
	var bodies []string
	srv := pageServer(t, 1, &bodies)
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	q := NewQuery().Eq("genome_id", "*").Cursor("resumeHere")
	if _, err := c.Query(context.Background(), "genome", q); err != nil {
		t.Fatalf("Query: %v", err)
	}

	if got := lastSentMark(bodies[0]); got != "resumeHere" {
		t.Errorf("first request sent cursor(%s), want cursor(resumeHere)", got)
	}
}

// An empty mark still means "start at the beginning".
func TestQueryDefaultsToTheOpeningCursorMark(t *testing.T) {
	var bodies []string
	srv := pageServer(t, 1, &bodies)
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	if _, err := c.Query(context.Background(), "genome", NewQuery().Eq("genome_id", "*")); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := lastSentMark(bodies[0]); got != "*" {
		t.Errorf("first request sent cursor(%s), want cursor(*)", got)
	}
}
