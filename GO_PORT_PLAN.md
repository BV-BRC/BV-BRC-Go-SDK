# BV-BRC CLI Go Port Plan

## Executive Summary

This document outlines the plan to port the BV-BRC Perl CLI (~139 scripts, ~16,000 lines) to Go, producing:
1. **Portable CLI binaries** - Single static binaries for easy distribution
2. **Go library** - A reusable BV-BRC Data API client library for third-party tool development

## Decision: Go is Appropriate

**Advantages of Go for this project:**
- **Single binary distribution** - No runtime dependencies, unlike Perl (requires perl + many CPAN modules)
- **Cross-platform builds** - Trivial to produce Linux, macOS, and Windows binaries from one codebase
- **Strong HTTP/JSON support** - Standard library has excellent support for REST APIs
- **Concurrency** - Goroutines enable efficient parallel API queries and batch processing
- **Type safety** - Compile-time checks catch many errors the Perl code cannot
- **Performance** - Faster startup and execution than Perl scripts

**Potential challenges (mitigated by good design):**
- Go is more verbose than Perl for text processing - but we can create helper libraries
- No equivalent to Perl's flexible regex integration - but Go's `regexp` package is sufficient
- Stricter typing requires more upfront design - but improves long-term maintainability

**Recommendation:** Proceed with Go. The distribution benefits alone justify the effort.

---

## Phase 1: Foundation Library (`bvbrc` Go Module)

**Duration estimate:** Core foundation work
**Goal:** Create a robust, well-documented Go library for BV-BRC API access

### 1.1 Project Structure

```
p3_cli/
├── go/
│   ├── go.mod                     # Module: github.com/BV-BRC/bvbrc
│   ├── go.sum
│   ├── pkg/
│   │   ├── api/                   # Core API client
│   │   │   ├── client.go          # P3DataAPI equivalent
│   │   │   ├── client_test.go
│   │   │   ├── query.go           # Query builder (select, filter, etc.)
│   │   │   ├── query_test.go
│   │   │   ├── objects.go         # Object type definitions
│   │   │   └── fields.go          # Field mappings (OBJECTS, FIELDS, IDCOL)
│   │   ├── auth/                  # Authentication
│   │   │   ├── token.go           # P3AuthToken equivalent
│   │   │   ├── token_test.go
│   │   │   └── sources.go         # Token source chain
│   │   ├── workspace/             # Workspace client
│   │   │   ├── client.go          # WorkspaceClient equivalent
│   │   │   ├── client_test.go
│   │   │   ├── operations.go      # ls, cp, mkdir, etc.
│   │   │   └── paths.go           # Path handling
│   │   ├── appservice/            # Job submission
│   │   │   ├── client.go          # AppService client
│   │   │   └── jobs.go            # Job types and submission
│   │   ├── cli/                   # CLI utilities (P3Utils equivalent)
│   │   │   ├── options.go         # Standard option groups
│   │   │   ├── tabular.go         # Tab-delimited I/O
│   │   │   ├── headers.go         # Header processing
│   │   │   ├── batch.go           # Batch processing utilities
│   │   │   └── output.go          # Output formatting
│   │   └── types/                 # Common types
│   │       ├── genome.go          # Genome type
│   │       ├── feature.go         # Feature type
│   │       ├── contig.go          # Contig type
│   │       └── ...
│   ├── cmd/                       # CLI commands (one per script)
│   │   ├── p3-all-genomes/
│   │   │   └── main.go
│   │   ├── p3-get-genome-data/
│   │   │   └── main.go
│   │   └── ...
│   └── internal/                  # Internal utilities
│       └── testutil/              # Test helpers
```

### 1.2 Core API Client (`pkg/api`)

```go
// client.go - Main API client

package api

import (
    "context"
    "net/http"
)

const (
    DefaultBaseURL = "https://www.bv-brc.org/api"
    DefaultChunkSize = 25000
)

// Client provides access to the BV-BRC Data API
type Client struct {
    BaseURL    string
    HTTPClient *http.Client
    Token      string
    ChunkSize  int
    Debug      bool
}

// NewClient creates a new BV-BRC API client
func NewClient(opts ...ClientOption) *Client

// Query executes a query against the specified object type
func (c *Client) Query(ctx context.Context, objectType string, q *Query) ([]map[string]interface{}, error)

// QueryTyped executes a query and unmarshals into typed structs
func (c *Client) QueryTyped(ctx context.Context, objectType string, q *Query, result interface{}) error

// Count returns the count of matching records
func (c *Client) Count(ctx context.Context, objectType string, q *Query) (int, error)

// Stream returns results via channel for large datasets
func (c *Client) Stream(ctx context.Context, objectType string, q *Query) (<-chan map[string]interface{}, <-chan error)
```

```go
// query.go - Query builder

package api

// Query represents a BV-BRC query with filters and field selection
type Query struct {
    Select   []string
    Filters  []Filter
    Keyword  string
    Limit    int
    Sort     []SortSpec
}

// Filter represents a query filter condition
type Filter struct {
    Op    FilterOp  // Eq, Ne, Lt, Le, Gt, Ge, In
    Field string
    Value interface{}
}

type FilterOp string

const (
    OpEq FilterOp = "eq"
    OpNe FilterOp = "ne"
    OpLt FilterOp = "lt"
    OpLe FilterOp = "le"
    OpGt FilterOp = "gt"
    OpGe FilterOp = "ge"
    OpIn FilterOp = "in"
)

// NewQuery creates a new query builder
func NewQuery() *Query

// Select adds fields to retrieve
func (q *Query) Select(fields ...string) *Query

// Eq adds an equality filter
func (q *Query) Eq(field string, value interface{}) *Query

// In adds an "in" filter for multiple values
func (q *Query) In(field string, values ...interface{}) *Query

// Keyword adds a keyword search
func (q *Query) Keyword(phrase string) *Query

// Limit sets maximum results
func (q *Query) Limit(n int) *Query

// Build generates the URL-encoded query string
func (q *Query) Build() string
```

### 1.3 Authentication (`pkg/auth`)

```go
// token.go - Token resolution (P3AuthToken equivalent)

package auth

import (
    "os"
    "path/filepath"
)

// TokenSource represents a source of authentication tokens
type TokenSource interface {
    Token() (string, error)
    Name() string
}

// DefaultTokenChain returns the standard token resolution chain
func DefaultTokenChain() []TokenSource {
    return []TokenSource{
        EnvSource("P3_AUTH_TOKEN"),
        EnvSource("KB_AUTH_TOKEN"),
        FileSource(filepath.Join(os.Getenv("HOME"), ".patric_token")),
        KBaseConfigSource(filepath.Join(os.Getenv("HOME"), ".kbase_config")),
    }
}

// GetToken resolves a token using the default chain
func GetToken() (string, error)

// RequireToken returns token or exits with error message
func RequireToken() string
```

### 1.4 CLI Utilities (`pkg/cli`)

```go
// options.go - Standard option groups (equivalent to P3Utils option functions)

package cli

import (
    "github.com/spf13/cobra"
)

// DataOptions defines standard data query options
type DataOptions struct {
    Attr     []string  // --attr, -a
    Count    bool      // --count, -K
    Equal    []string  // --equal, --eq, -e (field,value pairs)
    Lt       []string  // --lt
    Le       []string  // --le
    Gt       []string  // --gt
    Ge       []string  // --ge
    Ne       []string  // --ne
    In       []string  // --in
    Required []string  // --required, -r
    Keyword  string    // --keyword
    Limit    int       // --limit
    Debug    bool      // --debug
}

// AddDataFlags adds standard data query flags to a command
func AddDataFlags(cmd *cobra.Command, opts *DataOptions)

// ColOptions defines column selection options
type ColOptions struct {
    Col       string  // --col, -c (column name or 1-based index)
    BatchSize int     // --batchSize, -b
    NoHead    bool    // --nohead
}

// AddColFlags adds column selection flags to a command
func AddColFlags(cmd *cobra.Command, opts *ColOptions)

// IOOptions defines input/output options
type IOOptions struct {
    Input  string  // --input, -i
    Output string  // --output, -o
    Delim  string  // --delim
}

// AddIOFlags adds I/O flags to a command
func AddIOFlags(cmd *cobra.Command, opts *IOOptions)

// BuildFilters converts DataOptions to API Query filters
func (d *DataOptions) BuildFilters() ([]api.Filter, error)

// BuildSelect converts DataOptions to select clause
func (d *DataOptions) BuildSelect(defaultFields []string) []string
```

```go
// tabular.go - Tab-delimited file handling

package cli

import (
    "bufio"
    "io"
)

// TabReader reads tab-delimited files with header support
type TabReader struct {
    reader    *bufio.Reader
    headers   []string
    delimiter string
    hasHeader bool
}

// NewTabReader creates a reader for tab-delimited input
func NewTabReader(r io.Reader, hasHeader bool) *TabReader

// Headers returns the header row (or generated headers if none)
func (t *TabReader) Headers() []string

// Read reads the next row as a slice of strings
func (t *TabReader) Read() ([]string, error)

// ReadBatch reads up to n rows, returning key column values and full rows
func (t *TabReader) ReadBatch(n int, keyCol int) (keys []string, rows [][]string, err error)

// FindColumn finds a column by name or 1-based index
func (t *TabReader) FindColumn(col string) (int, error)

// TabWriter writes tab-delimited output
type TabWriter struct {
    writer    *bufio.Writer
    delimiter string
}

// NewTabWriter creates a writer for tab-delimited output
func NewTabWriter(w io.Writer) *TabWriter

// WriteRow writes a single row
func (t *TabWriter) WriteRow(fields ...string) error

// WriteHeaders writes the header row
func (t *TabWriter) WriteHeaders(headers []string) error
```

### 1.5 Object Type Definitions (`pkg/types`)

```go
// genome.go - Genome type definition

package types

// Genome represents a BV-BRC genome record
type Genome struct {
    GenomeID       string   `json:"genome_id"`
    GenomeName     string   `json:"genome_name"`
    GenomeStatus   string   `json:"genome_status"`
    GenomeLength   int      `json:"genome_length"`
    Contigs        int      `json:"contigs"`
    TaxonID        int      `json:"taxon_id"`
    Kingdom        string   `json:"kingdom"`
    Phylum         string   `json:"phylum"`
    Class          string   `json:"class"`
    Order          string   `json:"order"`
    Family         string   `json:"family"`
    Genus          string   `json:"genus"`
    Species        string   `json:"species"`
    // ... additional fields
}

// Feature represents a BV-BRC genome feature
type Feature struct {
    PatricID     string `json:"patric_id"`
    GenomeID     string `json:"genome_id"`
    FeatureType  string `json:"feature_type"`
    Product      string `json:"product"`
    Start        int    `json:"start"`
    End          int    `json:"end"`
    Strand       string `json:"strand"`
    NaLength     int    `json:"na_length"`
    AaLength     int    `json:"aa_length"`
    // ... additional fields
}
```

---

## Phase 2: CLI Command Framework

**Goal:** Establish the command structure and implement representative commands from each category

### 2.1 Command Framework

Use [Cobra](https://github.com/spf13/cobra) for CLI structure:

```go
// cmd/root.go - Root command (optional, for unified binary)

package main

import (
    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "p3",
    Short: "BV-BRC Command Line Interface",
}

func init() {
    rootCmd.AddCommand(allGenomesCmd)
    rootCmd.AddCommand(getGenomeDataCmd)
    // ... add all subcommands
}
```

### 2.2 Implementation Priority

Commands are prioritized by frequency of use and complexity:

#### Tier 1: Core Data Query Commands (Implement First)
These establish the patterns for all other commands:

| Command | Perl Script | Complexity | Notes |
|---------|-------------|------------|-------|
| p3-all-genomes | p3-all-genomes.pl | Low | Enumerate all genomes |
| p3-get-genome-data | p3-get-genome-data.pl | Medium | Key-based lookup pattern |
| p3-get-genome-features | p3-get-genome-features.pl | Medium | Related object lookup |
| p3-all-features | p3-all-features.pl | Low | Enumerate features |
| p3-get-feature-data | p3-get-feature-data.pl | Medium | Feature lookup |
| p3-get-feature-sequence | p3-get-feature-sequence.pl | Medium | Sequence retrieval |

#### Tier 2: Data Manipulation Commands
Pure data transformation, no API calls:

| Command | Perl Script | Complexity | Notes |
|---------|-------------|------------|-------|
| p3-extract | p3-extract.pl | Low | Column selection |
| p3-join | p3-join.pl | Medium | File joining |
| p3-sort | p3-sort.pl | Low | Sorting |
| p3-match | p3-match.pl | Low | Pattern matching |
| p3-count | p3-count.pl | Low | Row counting |
| p3-head | p3-head.pl | Low | First N lines |
| p3-tail | p3-tail.pl | Low | Last N lines |

#### Tier 3: Workspace Commands
Requires workspace client implementation:

| Command | Perl Script | Complexity | Notes |
|---------|-------------|------------|-------|
| p3-ls | p3-ls.pl | Medium | List workspace |
| p3-cp | p3-cp.pl | High | Copy files |
| p3-mkdir | p3-mkdir.pl | Low | Create directory |
| p3-rm | p3-rm.pl | Low | Remove files |
| p3-cat | p3-cat.pl | Low | Display file |

#### Tier 4: Job Submission Commands
Requires AppService client implementation:

| Command | Perl Script | Complexity | Notes |
|---------|-------------|------------|-------|
| p3-submit-genome-annotation | p3-submit-genome-annotation.pl | High | Complex options |
| p3-submit-BLAST | p3-submit-BLAST.pl | Medium | Common use case |
| p3-job-status | p3-job-status.pl | Low | Status check |

#### Tier 5: Specialized Analysis Commands
Complex logic, implement last:

| Command | Perl Script | Complexity | Notes |
|---------|-------------|------------|-------|
| p3-blast | p3-blast.pl | High | Local BLAST |
| p3-genome-distance | p3-genome-distance.pl | High | Distance calculation |
| p3-role-matrix | p3-role-matrix.pl | Medium | Matrix generation |

### 2.3 Example Command Implementation

```go
// cmd/p3-all-genomes/main.go

package main

import (
    "context"
    "fmt"
    "os"

    "github.com/BV-BRC/bvbrc/pkg/api"
    "github.com/BV-BRC/bvbrc/pkg/auth"
    "github.com/BV-BRC/bvbrc/pkg/cli"
    "github.com/spf13/cobra"
)

var (
    dataOpts cli.DataOptions
    ioOpts   cli.IOOptions
)

var cmd = &cobra.Command{
    Use:   "p3-all-genomes",
    Short: "Return all genomes from BV-BRC",
    Long: `This script returns data for all the genomes in the BV-BRC database.
It supports standard filtering parameters to filter the output and column
options to select the columns to return.

    p3-all-genomes [options]

The output columns are defined by the --attr (-a) option. If no columns
are specified, a default set of columns is returned including genome_id,
genome_name, and genome_status.`,
    RunE: run,
}

func init() {
    cli.AddDataFlags(cmd, &dataOpts)
    cli.AddIOFlags(cmd, &ioOpts)
}

func run(cmd *cobra.Command, args []string) error {
    ctx := context.Background()

    // Initialize client with optional auth
    token, _ := auth.GetToken()
    client := api.NewClient(api.WithToken(token))

    // Build query from options
    query := api.NewQuery()

    // Add filters
    filters, err := dataOpts.BuildFilters()
    if err != nil {
        return fmt.Errorf("invalid filter: %w", err)
    }
    for _, f := range filters {
        query.AddFilter(f)
    }

    // Add field selection
    defaultFields := []string{"genome_id", "genome_name", "genome_status"}
    query.Select(dataOpts.BuildSelect(defaultFields)...)

    if dataOpts.Limit > 0 {
        query.Limit(dataOpts.Limit)
    }

    // Handle count mode
    if dataOpts.Count {
        count, err := client.Count(ctx, "genome", query)
        if err != nil {
            return err
        }
        fmt.Println(count)
        return nil
    }

    // Open output
    out, err := cli.OpenOutput(ioOpts.Output)
    if err != nil {
        return err
    }
    defer out.Close()

    writer := cli.NewTabWriter(out)

    // Write headers
    writer.WriteHeaders(query.SelectFields())

    // Stream results
    results, errs := client.Stream(ctx, "genome", query)
    for result := range results {
        row := make([]string, len(query.SelectFields()))
        for i, field := range query.SelectFields() {
            row[i] = fmt.Sprint(result[field])
        }
        writer.WriteRow(row...)
    }

    // Check for errors
    if err := <-errs; err != nil {
        return err
    }

    return nil
}

func main() {
    if err := cmd.Execute(); err != nil {
        os.Exit(1)
    }
}
```

---

## Phase 3: Build and Distribution

### 3.1 Build System

```makefile
# go/Makefile

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
VERSION ?= $(shell git describe --tags --always --dirty)
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"

# All CLI commands
COMMANDS := $(shell ls cmd/)

.PHONY: all build test clean

all: build

build: $(COMMANDS)

$(COMMANDS):
	go build $(LDFLAGS) -o bin/$@ ./cmd/$@

# Cross-compilation targets
build-linux:
	GOOS=linux GOARCH=amd64 $(MAKE) build

build-darwin:
	GOOS=darwin GOARCH=amd64 $(MAKE) build
	GOOS=darwin GOARCH=arm64 $(MAKE) build

build-windows:
	GOOS=windows GOARCH=amd64 $(MAKE) build

build-all: build-linux build-darwin build-windows

test:
	go test -v ./...

clean:
	rm -rf bin/
```

### 3.2 Release Packaging

```yaml
# .github/workflows/release.yml (example)

name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        goos: [linux, darwin, windows]
        goarch: [amd64, arm64]
        exclude:
          - goos: windows
            goarch: arm64
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - run: make build-all
      - uses: actions/upload-artifact@v3
        with:
          name: p3-cli-${{ matrix.goos }}-${{ matrix.goarch }}
          path: bin/
```

### 3.3 Installation Options

1. **Binary download**: Pre-built binaries for each platform
2. **Go install**: `go install github.com/BV-BRC/bvbrc/cmd/...@latest`
3. **Package managers**: Homebrew formula, apt/yum packages
4. **Docker image**: Containerized CLI

---

## Phase 4: Testing Strategy

### 4.1 Unit Tests

```go
// pkg/api/client_test.go

func TestClient_Query(t *testing.T) {
    // Mock HTTP server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Verify request format
        assert.Equal(t, "/genome", r.URL.Path)

        // Return mock response
        json.NewEncoder(w).Encode([]map[string]interface{}{
            {"genome_id": "123.456", "genome_name": "Test Genome"},
        })
    }))
    defer server.Close()

    client := api.NewClient(api.WithBaseURL(server.URL))

    results, err := client.Query(context.Background(), "genome",
        api.NewQuery().Eq("genome_id", "123.456"))

    assert.NoError(t, err)
    assert.Len(t, results, 1)
    assert.Equal(t, "123.456", results[0]["genome_id"])
}
```

### 4.2 Integration Tests

```go
// tests/integration/cli_test.go

func TestAllGenomesCommand(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    cmd := exec.Command("p3-all-genomes", "--limit", "10", "-a", "genome_id,genome_name")
    output, err := cmd.Output()

    require.NoError(t, err)

    lines := strings.Split(string(output), "\n")
    assert.GreaterOrEqual(t, len(lines), 2) // Header + at least 1 result

    // Verify header
    assert.Equal(t, "genome_id\tgenome_name", lines[0])
}
```

### 4.3 Compatibility Tests

Compare Perl and Go output for identical inputs:

```bash
#!/bin/bash
# tests/compat/test_compatibility.sh

compare_output() {
    local cmd="$1"
    local args="$2"

    perl_out=$(p3-$cmd.pl $args)
    go_out=$(p3-$cmd $args)

    if diff <(echo "$perl_out") <(echo "$go_out"); then
        echo "PASS: p3-$cmd $args"
    else
        echo "FAIL: p3-$cmd $args"
        return 1
    fi
}

# Test basic commands
compare_output "all-genomes" "--limit 5"
compare_output "get-genome-data" "-a genome_length" < test_genomes.txt
```

---

## Phase 5: Migration Path

### 5.1 Parallel Deployment

1. Install Go binaries alongside Perl scripts
2. Create wrapper scripts that check for Go version first
3. Gradually enable Go versions per command

```bash
# bin/p3-all-genomes (wrapper)
#!/bin/bash

if command -v p3-all-genomes-go &> /dev/null; then
    exec p3-all-genomes-go "$@"
else
    exec p3-all-genomes.pl "$@"
fi
```

### 5.2 Feature Parity Tracking

| Command | Perl | Go | Status |
|---------|------|-----|--------|
| p3-all-genomes | ✓ | ✓ | Complete |
| p3-get-genome-data | ✓ | ✓ | Complete |
| p3-extract | ✓ | ✓ | Complete |
| ... | | | |

### 5.3 Documentation

1. **API Documentation**: Go package documentation with examples
2. **CLI Documentation**: Man pages and `--help` output matching Perl
3. **Migration Guide**: Notes for users transitioning from Perl CLI
4. **Developer Guide**: How to contribute new commands

---

## Resource Estimates

### Code Volume

| Component | Estimated Lines | Notes |
|-----------|----------------|-------|
| pkg/api | ~800 | Core client, query builder |
| pkg/auth | ~200 | Token handling |
| pkg/workspace | ~600 | Workspace client |
| pkg/appservice | ~400 | Job submission |
| pkg/cli | ~600 | CLI utilities |
| pkg/types | ~500 | Type definitions |
| cmd/* (139 commands) | ~8,000 | Average ~60 lines each |
| Tests | ~3,000 | Unit + integration |
| **Total** | ~14,000 | |

### Dependencies

```
github.com/spf13/cobra      # CLI framework
github.com/spf13/pflag      # POSIX flags
github.com/stretchr/testify # Testing assertions
```

Minimal external dependencies - prefer standard library where possible.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| API behavior differences | High | Extensive compatibility testing |
| Edge cases in Perl regex | Medium | Document differences, add tests |
| Authentication complexity | Medium | Test all token sources |
| Performance regression | Low | Benchmark critical paths |
| Missing Perl module features | Medium | Implement Go equivalents |

---

## Success Criteria

1. **100% command coverage** - All 139+ scripts have Go equivalents
2. **Option compatibility** - Identical CLI flags and behavior
3. **Output compatibility** - Identical output format for same inputs
4. **Performance** - Equal or better than Perl for all commands
5. **Distribution** - Single binary download for each platform
6. **Documentation** - Complete API docs and user guide
7. **Test coverage** - >80% code coverage, all commands integration tested

---

## Appendix A: Complete Command List

### p3_cli/scripts (139 scripts)

Data Query:
- p3-all-contigs.pl, p3-all-drugs.pl, p3-all-features.pl, p3-all-genomes.pl
- p3-all-serology.pl, p3-all-structures.pl, p3-all-subsystems.pl
- p3-all-surveillance.pl, p3-all-taxonomies.pl
- p3-echo.pl, p3-find-couples.pl, p3-find-features.pl, p3-find-in-clusters.pl
- p3-get-contig-data.pl, p3-get-drug-data.pl, p3-get-family-data.pl
- p3-get-family-features.pl, p3-get-feature-data.pl, p3-get-feature-group.pl
- p3-get-feature-regions.pl, p3-get-feature-sequence.pl
- p3-get-feature-subsystems.pl, p3-get-genome-contigs.pl
- p3-get-genome-data.pl, p3-get-genome-drugs.pl, p3-get-genome-features.pl
- p3-get-genome-group.pl, p3-get-subsystem-data.pl, p3-get-subsystem-features.pl
- p3-get-subsystem-roles.pl, p3-get-surveillance-data.pl
- p3-get-taxonomy-data.pl

Data Manipulation:
- p3-aggregates-to-feature.pl, p3-closest-seqs.pl, p3-co-occur.pl
- p3-collate.pl, p3-compare-cols.pl, p3-count.pl, p3-extract.pl
- p3-file-filter.pl, p3-format-results.pl, p3-function-to-role.pl
- p3-head.pl, p3-join.pl, p3-match.pl, p3-merge.pl
- p3-pick-by-class.pl, p3-pick.pl, p3-pivot.pl
- p3-project-subsystems.pl, p3-put-feature-group.pl, p3-put-genome-group.pl
- p3-related-by-clusters.pl, p3-rep-genomes.pl, p3-role-features.pl
- p3-shuffle.pl, p3-sort.pl, p3-stats.pl, p3-tail.pl
- p3-tbl-to-fasta.pl, p3-tbl-to-html.pl

Analysis:
- p3-blast.pl, p3-build-kmer-db.pl, p3-cluster-pairs.pl
- p3-clusters-for-genomes.pl, p3-feature-gap.pl
- p3-generate-close-roles.pl, p3-genome-amr.pl, p3-genome-distance.pl
- p3-genome-fasta.pl, p3-genus-species.pl, p3-group-by-cluster.pl
- p3-identify-clusters.pl, p3-kmer-compare.pl, p3-nucleon-runs.pl
- p3-parse-ec.pl, p3-role-fasta.pl, p3-role-matrix.pl
- p3-sequence-profile.pl, p3-signature-families.pl, p3-signature-peginfo.pl
- p3-signature-regions.pl, p3-uni-options.pl, p3-write-kmers.pl

GTO Operations:
- p3-gto-dna.pl, p3-gto-fasta.pl, p3-gto-fetch.pl, p3-gto-scan.pl, p3-gto.pl

Job Submission:
- p3-submit-BLAST.pl, p3-submit-CGA.pl, p3-submit-comparative-systems.pl
- p3-submit-codon-tree.pl, p3-submit-comprehensive-genome-analysis.pl
- p3-submit-fastqutils.pl, p3-submit-gene-tree.pl
- p3-submit-genome-annotation.pl, p3-submit-genome-assembly.pl
- p3-submit-genome-comparison.pl, p3-submit-homology-search.pl
- p3-submit-id-mapper.pl, p3-submit-metagenomic-binning.pl
- p3-submit-metagenomic-read-mapping.pl, p3-submit-model-reconstruction.pl
- p3-submit-MSA.pl, p3-submit-phylogenetic-tree.pl
- p3-submit-primer-design.pl, p3-submit-proteome-comparison.pl
- p3-submit-rnaseq.pl, p3-submit-similar-genome-finder.pl
- p3-submit-sra-import.pl, p3-submit-taxonomic-classification.pl
- p3-submit-tn-seq.pl, p3-submit-variation-analysis.pl
- p3-submit-wastewater-analysis.pl

Utility:
- p3-fasta-md5.pl, p3-file-md5.pl, p3-generate-clusters.pl
- p3-job-status.pl, p3-list-feature-groups.pl, p3-list-genome-groups.pl
- p3-user-genomes.pl

### Workspace/scripts (p3-* scripts)

- p3-cat.pl, p3-config.pl, p3-cp.pl, p3-du.pl, p3-exists.pl
- p3-less.pl, p3-ls.pl, p3-mkdir.pl, p3-mount-ws.pl
- p3-rm.pl, p3-rmdir.pl
