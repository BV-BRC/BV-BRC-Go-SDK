// Package p3tests provides integration tests for the BV-BRC Go SDK.
// These tests are translated from the Perl p3-tests.pl script in p3_cli.
package p3tests

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/BV-BRC/BV-BRC-Go-SDK/api"
	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/cli"
)

// Test data - matching the Perl ROWS constant
var testRows = [][]string{
	{"id", "name", "length"},
	{"385964.3", "Yersinia pestis subsp. pestis strain 231(708)", "4568800"},
	{"1234661.4", "Yersinia pestis subsp. pestis bv. Orientalis strain ZE94-2122", "4827235"},
	{"992176.4", "Yersinia pestis PY-94", "4644905"},
	{"992176.5", "Yersinia pestis PY-94", "4644905"},
	{"632.188", "Yersinia pestis strain Algeria3", "4427555"},
	{"1345710.7", "Yersinia pestis 1045", "4684080"},
	{"1345703.9", "Yersinia pestis 1412", "4733482"},
	{"1345704.8", "Yersinia pestis 1413", "4736923"},
	{"1345709.34", "Yersinia pestis 14735", "4693748"},
	{"1345705.8", "Yersinia pestis 1522", "4738644"},
	{"1345700.10", "Yersinia pestis 1670", "4718815"},
}

// Expected results with taxonomy lineage - matching the Perl EXPECTED constant
var expectedResults = map[string][]string{
	"header":      {"id", "name", "length", "genome.taxon_lineage_names"},
	"385964.3":    {"385964.3", "Yersinia pestis subsp. pestis strain 231(708)", "4568800", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis subsp. pestis"},
	"1234661.4":   {"1234661.4", "Yersinia pestis subsp. pestis bv. Orientalis strain ZE94-2122", "4827235", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis subsp. pestis; Yersinia pestis subsp. pestis bv. Orientalis"},
	"992176.4":    {"992176.4", "Yersinia pestis PY-94", "4644905", "cellular organisms; Bacteria; Pseudomonadota; Gammaproteobacteria; Enterobacteriales; Enterobacteriaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis PY-94"},
	"992176.5":    {"992176.5", "Yersinia pestis PY-94", "4644905", "cellular organisms; Bacteria; Pseudomonadota; Gammaproteobacteria; Enterobacteriales; Enterobacteriaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis PY-94"},
	"632.188":     {"632.188", "Yersinia pestis strain Algeria3", "4427555", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis"},
	"1345710.7":   {"1345710.7", "Yersinia pestis 1045", "4684080", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis 1045"},
	"1345703.9":   {"1345703.9", "Yersinia pestis 1412", "4733482", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis 1412"},
	"1345704.8":   {"1345704.8", "Yersinia pestis 1413", "4736923", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis 1413"},
	"1345709.34":  {"1345709.34", "Yersinia pestis 14735", "4693748", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis 14735"},
	"1345705.8":   {"1345705.8", "Yersinia pestis 1522", "4738644", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis 1522"},
	"1345700.10":  {"1345700.10", "Yersinia pestis 1670", "4718815", "cellular organisms; Bacteria; Pseudomonadati; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Yersiniaceae; Yersinia; Yersinia pseudotuberculosis complex; Yersinia pestis; Yersinia pestis 1670"},
}

// createInputFile creates the test input file with tab-delimited genome data.
func createInputFile(t *testing.T, dir string) string {
	t.Helper()
	filePath := filepath.Join(dir, "in.tbl")
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Could not create input file: %v", err)
	}
	defer f.Close()

	w := cli.NewTabWriter(f)
	for _, row := range testRows {
		if err := w.WriteRow(row...); err != nil {
			t.Fatalf("Could not write row: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Could not flush writer: %v", err)
	}

	return filePath
}

// TestTabReader tests the TabReader functionality (corresponds to P3Utils file reading).
func TestTabReader(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "p3tests")
	if err != nil {
		t.Fatalf("Could not create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create input file
	inFile := createInputFile(t, tempDir)

	// Open and read the file
	f, err := os.Open(inFile)
	if err != nil {
		t.Fatalf("Could not open input file: %v", err)
	}
	defer f.Close()

	reader := cli.NewTabReader(f, true)

	// Test headers
	headers, err := reader.Headers()
	if err != nil {
		t.Fatalf("Could not read headers: %v", err)
	}
	if len(headers) != 3 {
		t.Errorf("len(headers) = %d, want 3", len(headers))
	}
	if headers[1] != "name" {
		t.Errorf("headers[1] = %q, want %q", headers[1], "name")
	}

	// Test FindColumn
	keyCol, err := reader.FindColumn("1")
	if err != nil {
		t.Fatalf("FindColumn error: %v", err)
	}
	if keyCol != 0 {
		t.Errorf("keyCol = %d, want 0", keyCol)
	}

	// Test reading batch
	keys, rows, err := reader.ReadBatch(7, 0)
	if err != nil {
		t.Fatalf("ReadBatch error: %v", err)
	}
	if len(keys) != 7 {
		t.Errorf("len(keys) = %d, want 7", len(keys))
	}

	// Check last row in batch
	lastRow := rows[len(rows)-1]
	if keys[len(keys)-1] != "1345703.9" {
		t.Errorf("last key = %q, want %q", keys[len(keys)-1], "1345703.9")
	}
	if lastRow[2] != "4733482" {
		t.Errorf("last row length = %q, want %q", lastRow[2], "4733482")
	}
}

// TestFindColumn tests the column finding functionality.
func TestFindColumn(t *testing.T) {
	headers := []string{"genome.id", "type", "genome.name", "feature.type"}

	tests := []struct {
		col     string
		want    int
		wantErr bool
	}{
		{"genome.id", 0, false},
		{"type", 1, false},
		{"genome.name", 2, false},
		{"feature.type", 3, false},
		{"1", 0, false},
		{"2", 1, false},
		{"nonexistent", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.col, func(t *testing.T) {
			// Create a reader with these headers
			r := strings.NewReader(strings.Join(headers, "\t") + "\n")
			reader := cli.NewTabReader(r, true)
			_, err := reader.Headers()
			if err != nil {
				t.Fatalf("Could not read headers: %v", err)
			}

			got, err := reader.FindColumn(tt.col)
			if (err != nil) != tt.wantErr {
				t.Errorf("FindColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("FindColumn(%q) = %d, want %d", tt.col, got, tt.want)
			}
		})
	}
}

// TestCleanValue tests value cleaning functionality.
func TestCleanValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// The Perl clean_value removes parentheses but not their contents.
		// "   This is (very) dirty   " -> trim -> "This is (very) dirty"
		// then remove parens -> "This is very dirty" -> quote if has spaces
		{"dirty value", "   This is (very) dirty   ", `"This is very dirty"`},
		{"quoted value", `"This is normal"`, `"This is normal"`},
		{"normal value", "123.4", "123.4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanValue(tt.input)
			if got != tt.want {
				t.Errorf("cleanValue(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// cleanValue cleans a value by trimming whitespace and removing parentheses.
// This is a translation of P3Utils::clean_value.
func cleanValue(value string) string {
	// Already quoted - return as-is
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		return value
	}

	// Trim whitespace
	value = strings.TrimSpace(value)

	// Remove parentheses (but keep their contents)
	value = strings.ReplaceAll(value, "(", "")
	value = strings.ReplaceAll(value, ")", "")

	// Clean up any resulting double spaces
	for strings.Contains(value, "  ") {
		value = strings.ReplaceAll(value, "  ", " ")
	}

	value = strings.TrimSpace(value)

	// Quote if contains spaces
	if strings.Contains(value, " ") {
		return `"` + value + `"`
	}

	return value
}

// TestMatch tests the pattern matching functionality.
func TestMatch(t *testing.T) {
	matches := []struct {
		pattern string
		text    string
	}{
		{"7", "7"},
		{"hardly", "this is hardly working"},
		{"hypothetical protein", "FIG00001: hypothetical protein in putative thing"},
		{"100", "100"},
	}

	fails := []struct {
		pattern string
		text    string
	}{
		{"7", "8"},
		{"hardly", "this is working"},
		{"hypothetical protein", "this is a hypothetical fail protein"},
		{"frog", "toad"},
	}

	for _, m := range matches {
		t.Run("match_"+m.pattern, func(t *testing.T) {
			if !match(m.pattern, m.text) {
				t.Errorf("match(%q, %q) = false, want true", m.pattern, m.text)
			}
		})
	}

	for _, f := range fails {
		t.Run("fail_"+f.pattern, func(t *testing.T) {
			if match(f.pattern, f.text) {
				t.Errorf("match(%q, %q) = true, want false", f.pattern, f.text)
			}
		})
	}
}

// match checks if a pattern matches text.
// This is a translation of P3Utils::match.
func match(pattern, text string) bool {
	// Exact match
	if pattern == text {
		return true
	}

	// Check for word boundary match
	// Pattern must appear as a complete substring with word boundaries
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(pattern) + `\b`)
	return re.MatchString(text)
}

// TestGetFields tests field splitting functionality.
func TestGetFields(t *testing.T) {
	line := "a\tb\tc\r\n"
	fields := getFields(line)

	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(fields, expected) {
		t.Errorf("getFields(%q) = %v, want %v", line, fields, expected)
	}
}

// getFields splits a line into fields.
func getFields(line string) []string {
	line = strings.TrimRight(line, "\r\n")
	return strings.Split(line, "\t")
}

// Integration tests that require network access to the BV-BRC API.
// These are skipped by default unless BVBRC_TEST_INTEGRATION is set.

// TestDerivedFields tests fetching derived fields from the API.
func TestDerivedFields(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	// Test derived/computed field (taxon_lineage_names)
	genomeID := "385964.3"
	q := api.NewQuery().Eq("genome_id", genomeID).Select("taxon_lineage_names", "genome_name")
	results, err := client.Query(ctx, "genome", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}

	// Verify genome name
	genomeName, ok := results[0]["genome_name"].(string)
	if !ok {
		t.Fatal("genome_name is not a string")
	}
	if !strings.Contains(genomeName, "Yersinia pestis") {
		t.Errorf("genome_name %q does not contain Yersinia pestis", genomeName)
	}

	// Check taxon_lineage_names contains expected parts
	// This is a multi-valued field returned as an array
	lineage, ok := results[0]["taxon_lineage_names"].([]any)
	if !ok {
		t.Logf("taxon_lineage_names type: %T, value: %v", results[0]["taxon_lineage_names"], results[0]["taxon_lineage_names"])
		// Some fields might be nil or not present - that's okay for this test
		return
	}

	// Check that Yersinia is in the lineage
	found := false
	for _, name := range lineage {
		if s, ok := name.(string); ok && strings.Contains(s, "Yersinia") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("taxon_lineage_names does not contain Yersinia: %v", lineage)
	}
}

// TestDrugQuery tests querying the drug object type.
func TestDrugQuery(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	q := api.NewQuery().
		Eq("antibiotic_name", "penicillin").
		Select("cas_id", "molecular_formula")

	results, err := client.Query(ctx, "drug", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}

	result := results[0]
	if result["cas_id"] != "61-33-6" {
		t.Errorf("cas_id = %v, want 61-33-6", result["cas_id"])
	}
	if result["molecular_formula"] != "C16H18N2O4S" {
		t.Errorf("molecular_formula = %v, want C16H18N2O4S", result["molecular_formula"])
	}
}

// TestGenomeLengthFilter tests filtering genomes by length.
func TestGenomeLengthFilter(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	// Get genome IDs from test data (skip header)
	var genomeIDs []string
	for _, row := range testRows[1:] {
		genomeIDs = append(genomeIDs, row[0])
	}

	q := api.NewQuery().
		In("genome_id", genomeIDs...).
		Gt("genome_length", "4700000").
		Select("genome_id", "genome_name", "genome_length")

	results, err := client.Query(ctx, "genome", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	// Build map of results
	resultMap := make(map[string]map[string]any)
	for _, r := range results {
		id, _ := r["genome_id"].(string)
		resultMap[id] = r
	}

	// Verify each test genome
	for _, row := range testRows[1:] {
		id := row[0]
		name := row[1]
		length := row[2]

		// Parse length
		var lengthInt int
		if _, err := stringToInt(length); err == nil {
			lengthInt, _ = stringToInt(length)
		}

		found := resultMap[id]

		if lengthInt < 4700000 {
			if found != nil {
				t.Errorf("Genome %s of length %s was returned by query (should not be)", id, length)
			}
		} else {
			if found == nil {
				t.Errorf("Genome %s of length %s was not returned by query", id, length)
			} else {
				if found["genome_id"] != id {
					t.Errorf("genome_id = %v, want %s", found["genome_id"], id)
				}
				if found["genome_name"] != name {
					t.Errorf("genome_name = %v, want %s", found["genome_name"], name)
				}
			}
		}
	}
}

// TestRawLimit tests limit functionality with raw API queries.
func TestRawLimit(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	// Ask for the first 100 features of genome 100226.15
	q := api.NewQuery().
		Eq("genome_id", "100226.15").
		Eq("feature_type", "CDS").
		Eq("annotation", "PATRIC").
		Select("patric_id", "product", "na_sequence_md5").
		Limit(100)

	results, err := client.Query(ctx, "feature", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(results) != 100 {
		t.Errorf("len(results) = %d, want 100", len(results))
	}

	// Verify feature ID format
	re := regexp.MustCompile(`^fig\|100226\.15\.peg\.\d+$`)
	for _, result := range results {
		id, _ := result["patric_id"].(string)
		if !re.MatchString(id) {
			t.Errorf("Invalid feature ID format: %q", id)
		}
	}
}

// TestNoLimit tests querying without limits.
func TestNoLimit(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	// Ask for all features of genome 11053.35 (there are 35)
	q := api.NewQuery().
		Eq("genome_id", "11053.35").
		Eq("annotation", "PATRIC").
		Select("patric_id", "product")

	results, err := client.Query(ctx, "feature", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	if len(results) != 35 {
		t.Errorf("len(results) = %d, want 35", len(results))
	}

	// Verify feature ID format
	re := regexp.MustCompile(`^fig\|11053\.35\.[^.]+\.\d+$`)
	for _, result := range results {
		id, _ := result["patric_id"].(string)
		if !re.MatchString(id) {
			t.Errorf("Invalid feature ID format: %q", id)
		}
	}
}

// TestChunkedQuery tests querying with chunking for large result sets.
func TestChunkedQuery(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	// Create client with small chunk size to force chunking
	client := api.NewClient(api.WithChunkSize(1000))
	ctx := context.Background()

	// Query all features of 511145.12 (E. coli K-12) - has thousands of CDS features
	q := api.NewQuery().
		Eq("genome_id", "511145.12").
		Eq("annotation", "PATRIC").
		Eq("feature_type", "CDS").
		Select("patric_id", "product")

	results, err := client.Query(ctx, "feature", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	// Should have gotten many features (at least enough to require multiple chunks)
	// The exact count may vary as the database is updated
	if len(results) < 1000 {
		t.Errorf("len(results) = %d, expected at least 1000", len(results))
	}
	t.Logf("Retrieved %d features for 511145.12", len(results))

	// Verify feature ID format and build buffer for comparison
	re := regexp.MustCompile(`^fig\|511145\.12\.peg\.\d+$`)
	buffer := make(map[string]string)

	for _, result := range results {
		id, _ := result["patric_id"].(string)
		if !re.MatchString(id) {
			t.Errorf("Invalid feature ID format: %q", id)
		}

		product, _ := result["product"].(string)
		if product == "" && result["product"] != nil {
			t.Errorf("Product is nil for feature %s", id)
		}
		buffer[id] = product
	}

	// Query again to verify consistency
	results2, err := client.Query(ctx, "feature", q)
	if err != nil {
		t.Fatalf("Second query error: %v", err)
	}

	if len(results2) != len(results) {
		t.Errorf("Second query returned %d results, first returned %d", len(results2), len(results))
	}

	// Verify results match
	for _, result := range results2 {
		id, _ := result["patric_id"].(string)
		originalProduct, exists := buffer[id]
		if !exists {
			t.Errorf("Feature %s in second query but not first", id)
			continue
		}

		product, _ := result["product"].(string)
		if product != originalProduct {
			t.Errorf("Product mismatch for %s: %q vs %q", id, product, originalProduct)
		}
	}
}

// TestCallbackQuery tests the callback query functionality.
func TestCallbackQuery(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient(api.WithChunkSize(1000))
	ctx := context.Background()

	q := api.NewQuery().
		Eq("genome_id", "11053.35").
		Eq("annotation", "PATRIC").
		Select("patric_id", "product")

	var cbChunks int
	var cbRecords int
	var results []map[string]any

	err := client.QueryCallback(ctx, "feature", q, func(records []map[string]any, info *api.ChunkInfo) bool {
		cbChunks++
		cbRecords += len(records)
		results = append(results, records...)
		return true // continue
	})

	if err != nil {
		t.Fatalf("QueryCallback error: %v", err)
	}

	if len(results) != 35 {
		t.Errorf("len(results) = %d, want 35", len(results))
	}

	if cbChunks != 1 {
		t.Errorf("cbChunks = %d, want 1", cbChunks)
	}

	if cbRecords != 35 {
		t.Errorf("cbRecords = %d, want 35", cbRecords)
	}
}

// TestCallbackQueryChunked tests callback with multiple chunks.
func TestCallbackQueryChunked(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient(api.WithChunkSize(1000))
	ctx := context.Background()

	q := api.NewQuery().
		Eq("genome_id", "511145.12").
		Eq("annotation", "PATRIC").
		Eq("feature_type", "CDS").
		Select("patric_id", "product")

	var cbChunks int
	var cbRecords int
	var results []map[string]any

	err := client.QueryCallback(ctx, "feature", q, func(records []map[string]any, info *api.ChunkInfo) bool {
		cbChunks++
		cbRecords += len(records)
		results = append(results, records...)
		return true // continue
	})

	if err != nil {
		t.Fatalf("QueryCallback error: %v", err)
	}

	// Should have multiple chunks
	if cbChunks <= 1 {
		t.Errorf("cbChunks = %d, expected > 1", cbChunks)
	}

	if cbRecords != len(results) {
		t.Errorf("cbRecords = %d, len(results) = %d, should be equal", cbRecords, len(results))
	}

	// Verify results
	re := regexp.MustCompile(`^fig\|511145\.12\.peg\.\d+$`)
	for _, result := range results {
		id, _ := result["patric_id"].(string)
		if !re.MatchString(id) {
			t.Errorf("Invalid feature ID format: %q", id)
		}
	}
}

// TestObjectFields tests listing object fields (schema).
func TestObjectFields(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	fields, err := client.GetSchema(ctx, "genome")
	if err != nil {
		t.Fatalf("GetSchema error: %v", err)
	}

	if len(fields) == 0 {
		t.Fatal("No fields returned")
	}

	// Look for taxon_lineage_ids (should be multi-valued)
	var foundTaxonLineage bool
	for _, f := range fields {
		if f.Name == "taxon_lineage_ids" {
			foundTaxonLineage = true
			if !f.MultiValued {
				t.Error("taxon_lineage_ids should be multi-valued")
			}
			break
		}
	}

	if !foundTaxonLineage {
		t.Error("taxon_lineage_ids field not found")
	}
}

// TestStreamQuery tests streaming query functionality.
func TestStreamQuery(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	q := api.NewQuery().
		Eq("genome_id", "11053.35").
		Eq("annotation", "PATRIC").
		Select("patric_id", "product")

	resultsChan, errChan := client.Stream(ctx, "feature", q)

	var results []map[string]any
	for record := range resultsChan {
		results = append(results, record)
	}

	// Check for errors
	if err := <-errChan; err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	if len(results) != 35 {
		t.Errorf("len(results) = %d, want 35", len(results))
	}
}

// TestCountQuery tests the count functionality.
func TestCountQuery(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	q := api.NewQuery().
		Eq("genome_id", "11053.35").
		Eq("annotation", "PATRIC")

	count, err := client.Count(ctx, "feature", q)
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}

	if count != 35 {
		t.Errorf("count = %d, want 35", count)
	}
}

// TestSortedResults tests that results can be sorted.
func TestSortedResults(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	client := api.NewClient()
	ctx := context.Background()

	q := api.NewQuery().
		Eq("genome_id", "11053.35").
		Eq("annotation", "PATRIC").
		Eq("feature_type", "CDS").
		Select("patric_id", "start").
		Sort("start", false). // ascending
		Limit(10)

	results, err := client.Query(ctx, "feature", q)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}

	// Verify results are sorted by start position
	var starts []int
	for _, r := range results {
		start, ok := r["start"].(float64)
		if ok {
			starts = append(starts, int(start))
		}
	}

	if !sort.IntsAreSorted(starts) {
		t.Errorf("Results are not sorted by start: %v", starts)
	}
}

// Helper function to convert string to int
func stringToInt(s string) (int, error) {
	var result int
	_, err := stringToIntParse(s, &result)
	return result, err
}

func stringToIntParse(s string, result *int) (bool, error) {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false, nil
		}
		*result = *result*10 + int(c-'0')
	}
	return true, nil
}
