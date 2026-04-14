package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClient(t *testing.T) {
	// Test default client
	c := NewClient()
	if c.BaseURL != DefaultBaseURL {
		t.Errorf("BaseURL = %q, want %q", c.BaseURL, DefaultBaseURL)
	}
	if c.ChunkSize != DefaultChunkSize {
		t.Errorf("ChunkSize = %d, want %d", c.ChunkSize, DefaultChunkSize)
	}
	if c.MaxRetries != DefaultMaxRetries {
		t.Errorf("MaxRetries = %d, want %d", c.MaxRetries, DefaultMaxRetries)
	}

	// Test with options
	c = NewClient(
		WithBaseURL("https://example.com/api"),
		WithChunkSize(1000),
		WithDebug(true),
	)
	if c.BaseURL != "https://example.com/api" {
		t.Errorf("BaseURL = %q, want %q", c.BaseURL, "https://example.com/api")
	}
	if c.ChunkSize != 1000 {
		t.Errorf("ChunkSize = %d, want %d", c.ChunkSize, 1000)
	}
	if !c.Debug {
		t.Error("Debug should be true")
	}
}

func TestClient_Query(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check request method
		if r.Method != "POST" {
			t.Errorf("Method = %q, want POST", r.Method)
		}

		// Check headers
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("Accept header = %q, want application/json", r.Header.Get("Accept"))
		}

		// Set content-range header
		w.Header().Set("Content-Range", "items 0-2/2")

		// Return mock data
		data := []map[string]any{
			{"genome_id": "123.456", "genome_name": "Test Genome 1"},
			{"genome_id": "789.012", "genome_name": "Test Genome 2"},
		}
		json.NewEncoder(w).Encode(data)
	}))
	defer server.Close()

	// Create client with test server
	c := NewClient(WithBaseURL(server.URL))

	// Execute query
	ctx := context.Background()
	results, err := c.Query(ctx, "genome", NewQuery().Select("genome_id", "genome_name"))
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("len(results) = %d, want 2", len(results))
	}

	if results[0]["genome_id"] != "123.456" {
		t.Errorf("results[0][genome_id] = %v, want 123.456", results[0]["genome_id"])
	}
}

func TestClient_Count(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set content-range header with total count
		w.Header().Set("Content-Range", "items 0-1/12345")
		json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer server.Close()

	c := NewClient(WithBaseURL(server.URL))

	ctx := context.Background()
	count, err := c.Count(ctx, "genome", NewQuery().Eq("genus", "Streptomyces"))
	if err != nil {
		t.Fatalf("Count() error = %v", err)
	}

	if count != 12345 {
		t.Errorf("count = %d, want 12345", count)
	}
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   *ChunkInfo
	}{
		{
			name:   "normal range",
			header: "items 0-100/500",
			want:   &ChunkInfo{Start: 0, Next: 100, Count: 500, IsLast: false},
		},
		{
			name:   "last chunk",
			header: "items 400-500/500",
			want:   &ChunkInfo{Start: 400, Next: 500, Count: 500, IsLast: true},
		},
		{
			name:   "with spaces",
			header: "items  50-100/200",
			want:   &ChunkInfo{Start: 50, Next: 100, Count: 200, IsLast: false},
		},
		{
			name:   "empty header",
			header: "",
			want:   &ChunkInfo{Start: 0, Next: 0, Count: 0, IsLast: false},
		},
		{
			name:   "invalid format",
			header: "not a valid header",
			want:   &ChunkInfo{Start: 0, Next: 0, Count: 0, IsLast: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseContentRange(tt.header)
			if got.Start != tt.want.Start {
				t.Errorf("Start = %d, want %d", got.Start, tt.want.Start)
			}
			if got.Next != tt.want.Next {
				t.Errorf("Next = %d, want %d", got.Next, tt.want.Next)
			}
			if got.Count != tt.want.Count {
				t.Errorf("Count = %d, want %d", got.Count, tt.want.Count)
			}
			if got.IsLast != tt.want.IsLast {
				t.Errorf("IsLast = %v, want %v", got.IsLast, tt.want.IsLast)
			}
		})
	}
}

func TestClient_urlEncode(t *testing.T) {
	c := NewClient()

	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"eq(name,test)", "eq(name,test)"},
		{"field<value", "field%60value"},
		{"field>value", "field%62value"},
		{"field=value", "field%61value"},
		{`field"value`, "field%22value"},
		{"a&b", "a%26b"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := c.urlEncode(tt.input)
			if got != tt.want {
				t.Errorf("urlEncode(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetObjectType(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"genome", "genome"},
		{"feature", "genome_feature"},
		{"contig", "genome_sequence"},
		{"unknown", "unknown"}, // passes through unchanged
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetObjectType(tt.name)
			if got != tt.want {
				t.Errorf("GetObjectType(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestGetIDColumn(t *testing.T) {
	tests := []struct {
		objectType string
		want       string
	}{
		{"genome", "genome_id"},
		{"feature", "patric_id"},
		{"contig", "sequence_id"},
		{"unknown", ""}, // returns empty for unknown
	}

	for _, tt := range tests {
		t.Run(tt.objectType, func(t *testing.T) {
			got := GetIDColumn(tt.objectType)
			if got != tt.want {
				t.Errorf("GetIDColumn(%q) = %q, want %q", tt.objectType, got, tt.want)
			}
		})
	}
}

func TestChunkInfo_CursorMark(t *testing.T) {
	// Test that CursorMark field exists and works
	info := &ChunkInfo{
		Start:      0,
		Next:       100,
		Count:      500,
		IsLast:     false,
		CursorMark: "AoEoODMzMzIuMTI=",
	}

	if info.CursorMark != "AoEoODMzMzIuMTI=" {
		t.Errorf("CursorMark = %q, want %q", info.CursorMark, "AoEoODMzMzIuMTI=")
	}
}

func TestClient_QueryWithCursor(t *testing.T) {
	// Create a test server that simulates cursor-based pagination
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Check request method
		if r.Method != "POST" {
			t.Errorf("Method = %q, want POST", r.Method)
		}

		// Check headers
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("Accept header = %q, want application/json", r.Header.Get("Accept"))
		}

		// Simulate cursor-based pagination
		var data []map[string]any
		var cursorMark string

		switch requestCount {
		case 1:
			// First request
			w.Header().Set("Content-Range", "items 0-2/5")
			w.Header().Set("X-Cursor-Mark", "cursor1")
			data = []map[string]any{
				{"genome_id": "1.1", "genome_name": "Genome 1"},
				{"genome_id": "1.2", "genome_name": "Genome 2"},
			}
		case 2:
			// Second request
			w.Header().Set("Content-Range", "items 0-2/5")
			w.Header().Set("X-Cursor-Mark", "cursor2")
			data = []map[string]any{
				{"genome_id": "1.3", "genome_name": "Genome 3"},
				{"genome_id": "1.4", "genome_name": "Genome 4"},
			}
		case 3:
			// Third request - last page (cursor unchanged)
			w.Header().Set("Content-Range", "items 0-1/5")
			w.Header().Set("X-Cursor-Mark", "cursor2") // Same as sent, signals end
			data = []map[string]any{
				{"genome_id": "1.5", "genome_name": "Genome 5"},
			}
		default:
			// Should not reach here
			cursorMark = "unexpected"
			data = []map[string]any{}
		}

		_ = cursorMark
		json.NewEncoder(w).Encode(data)
	}))
	defer server.Close()

	// Create client with test server
	c := NewClient(WithBaseURL(server.URL), WithChunkSize(2))

	// Execute cursor-based query
	ctx := context.Background()
	results, err := c.QueryWithCursor(ctx, "genome", NewQuery().Select("genome_id", "genome_name"))
	if err != nil {
		t.Fatalf("QueryWithCursor() error = %v", err)
	}

	if len(results) != 5 {
		t.Errorf("len(results) = %d, want 5", len(results))
	}

	// Verify the server was called multiple times (pagination worked)
	if requestCount < 2 {
		t.Errorf("requestCount = %d, expected at least 2 requests for pagination", requestCount)
	}
}

func TestClient_QueryWithCursor_SinglePage(t *testing.T) {
	// Create a test server that returns all results in one page
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "items 0-2/2")
		w.Header().Set("X-Cursor-Mark", "*") // Same as initial, signals end
		data := []map[string]any{
			{"genome_id": "1.1", "genome_name": "Genome 1"},
			{"genome_id": "1.2", "genome_name": "Genome 2"},
		}
		json.NewEncoder(w).Encode(data)
	}))
	defer server.Close()

	c := NewClient(WithBaseURL(server.URL))

	ctx := context.Background()
	results, err := c.QueryWithCursor(ctx, "genome", NewQuery().Eq("genus", "Test"))
	if err != nil {
		t.Fatalf("QueryWithCursor() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("len(results) = %d, want 2", len(results))
	}
}

func TestClient_QueryCallbackWithCursor(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		var data []map[string]any
		var nextCursor string

		switch requestCount {
		case 1:
			w.Header().Set("Content-Range", "items 0-2/4")
			nextCursor = "cursor1"
			data = []map[string]any{
				{"id": "1"}, {"id": "2"},
			}
		case 2:
			w.Header().Set("Content-Range", "items 0-2/4")
			nextCursor = "cursor1" // Same as sent, signals end
			data = []map[string]any{
				{"id": "3"}, {"id": "4"},
			}
		}

		w.Header().Set("X-Cursor-Mark", nextCursor)
		json.NewEncoder(w).Encode(data)
	}))
	defer server.Close()

	c := NewClient(WithBaseURL(server.URL), WithChunkSize(2))

	callbackCount := 0
	totalRecords := 0

	err := c.QueryCallbackWithCursor(context.Background(), "test", NewQuery().Eq("field", "value"),
		func(records []map[string]any, info *ChunkInfo) bool {
			callbackCount++
			totalRecords += len(records)
			return true
		})

	if err != nil {
		t.Fatalf("QueryCallbackWithCursor() error = %v", err)
	}

	if callbackCount != 2 {
		t.Errorf("callbackCount = %d, want 2", callbackCount)
	}

	if totalRecords != 4 {
		t.Errorf("totalRecords = %d, want 4", totalRecords)
	}
}
