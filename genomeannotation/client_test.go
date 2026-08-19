package genomeannotation

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// capture is what a test server records about the one request it received.
type capture struct {
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
	Version string            `json:"version"`
	ID      string            `json:"id"`

	header http.Header
}

// serve starts a test server that records the request and replies with the
// given JSON-RPC result values, and returns a client pointed at it.
func serve(t *testing.T, results ...string) (*Client, *capture) {
	t.Helper()
	got := &capture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, got); err != nil {
			t.Errorf("request body is not JSON-RPC: %v (%s)", err, body)
		}
		got.header = r.Header.Clone()

		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"1","result":[`+strings.Join(results, ",")+`]}`)
	}))
	t.Cleanup(srv.Close)

	return New(WithURL(srv.URL)), got
}

// serveError starts a test server that replies with a JSON-RPC error.
func serveError(t *testing.T, message string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, _ := json.Marshal(map[string]interface{}{
			"id":    "1",
			"error": map[string]interface{}{"code": -32603, "message": "server error", "error": message},
		})
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return New(WithURL(srv.URL))
}

func TestRequestEnvelope(t *testing.T) {
	c, got := serve(t, `{"id":"83333.1"}`)

	if _, err := c.CallFeaturesCDSProdigal(GTO(`{"id":"83333.1"}`)); err != nil {
		t.Fatalf("CallFeaturesCDSProdigal: %v", err)
	}

	if got.Method != "GenomeAnnotation.call_features_CDS_prodigal" {
		t.Errorf("method = %q", got.Method)
	}
	if got.Version != "1.1" {
		t.Errorf("version = %q, want 1.1", got.Version)
	}
	if len(got.Params) != 1 {
		t.Fatalf("got %d params, want 1", len(got.Params))
	}
	if ct := got.header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if ua := got.header.Get("User-Agent"); ua == "" || strings.HasPrefix(ua, "Go-http-client") {
		t.Errorf("User-Agent = %q; the SDK must name itself", ua)
	}
}

func TestNoTokenNoAuthorizationHeader(t *testing.T) {
	// Several methods answer unauthenticated, and the rast-* tools have never
	// required a login for them.
	c, got := serve(t, `{}`)
	if _, err := c.DefaultWorkflow(); err != nil {
		t.Fatalf("DefaultWorkflow: %v", err)
	}
	if _, ok := got.header["Authorization"]; ok {
		t.Error("Authorization header sent though no token was configured")
	}
}

func TestTokenIsSent(t *testing.T) {
	got := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.header = r.Header.Clone()
		io.WriteString(w, `{"id":"1","result":[{}]}`)
	}))
	defer srv.Close()

	c := New(WithURL(srv.URL), WithToken("un=bob|sig=abc"))
	if _, err := c.DefaultWorkflow(); err != nil {
		t.Fatalf("DefaultWorkflow: %v", err)
	}
	if got.header.Get("Authorization") != "un=bob|sig=abc" {
		t.Errorf("Authorization = %q", got.header.Get("Authorization"))
	}
}

func TestGTOPassesThroughUnchanged(t *testing.T) {
	// A large integer and a specific key order: both survive only because the
	// GTO is never decoded.
	in := `{"zebra":1,"n":12345678901234567,"apple":2}`
	c, got := serve(t, in)

	out, err := c.CallFeaturesCrispr(GTO(in))
	if err != nil {
		t.Fatalf("CallFeaturesCrispr: %v", err)
	}
	if string(got.Params[0]) != in {
		t.Errorf("request sent %s, want %s", got.Params[0], in)
	}
	if string(out) != in {
		t.Errorf("response returned %s, want %s", out, in)
	}
}

func TestParamsNeverNil(t *testing.T) {
	// The Perl service dereferences the parameter object; a JSON null would
	// take it down rather than producing a useful error.
	c, got := serve(t, `{}`)
	if _, err := c.AnnotateProteinsKmerV2(GTO(`{}`), nil); err != nil {
		t.Fatalf("AnnotateProteinsKmerV2: %v", err)
	}
	if len(got.Params) != 2 {
		t.Fatalf("got %d params, want 2", len(got.Params))
	}
	if string(got.Params[1]) != "{}" {
		t.Errorf("params[1] = %s, want {}", got.Params[1])
	}
}

func TestRRNATypesAreSentAsAList(t *testing.T) {
	c, got := serve(t, `{}`)
	if _, err := c.CallFeaturesRRNASEED(GTO(`{}`), []string{"5S", "SSU"}); err != nil {
		t.Fatalf("CallFeaturesRRNASEED: %v", err)
	}
	if string(got.Params[1]) != `["5S","SSU"]` {
		t.Errorf("params[1] = %s", got.Params[1])
	}
}

func TestCompactFeatureIsATuple(t *testing.T) {
	c, got := serve(t, `{}`)
	f := CompactFeature{ID: "fig|1.1.peg.1", Location: "c1_1_99", Type: "CDS", Function: "hypothetical protein", Aliases: "x"}
	if _, err := c.AddFeatures(GTO(`{}`), []CompactFeature{f}); err != nil {
		t.Fatalf("AddFeatures: %v", err)
	}
	want := `[["fig|1.1.peg.1","c1_1_99","CDS","hypothetical protein","x"]]`
	if string(got.Params[1]) != want {
		t.Errorf("params[1] = %s, want %s", got.Params[1], want)
	}
}

func TestFunctionAssignmentIsATuple(t *testing.T) {
	c, got := serve(t, `{}`)
	fns := []FunctionAssignment{{FeatureID: "fig|1.1.peg.1", Function: "a function"}}
	if _, err := c.UpdateFunctions(GTO(`{}`), fns, nil); err != nil {
		t.Fatalf("UpdateFunctions: %v", err)
	}
	if want := `[["fig|1.1.peg.1","a function"]]`; string(got.Params[1]) != want {
		t.Errorf("params[1] = %s, want %s", got.Params[1], want)
	}
	if string(got.Params[2]) != "{}" {
		t.Errorf("params[2] = %s, want the empty analysis event {}", got.Params[2])
	}
}

func TestContigsAreObjects(t *testing.T) {
	c, got := serve(t, `{}`)
	if _, err := c.AddContigs(GTO(`{}`), []Contig{{ID: "c1", DNA: "ACGT"}}); err != nil {
		t.Fatalf("AddContigs: %v", err)
	}
	if want := `[{"id":"c1","dna":"ACGT"}]`; string(got.Params[1]) != want {
		t.Errorf("params[1] = %s, want %s", got.Params[1], want)
	}
}

func TestDNAInputIsATuple(t *testing.T) {
	c, got := serve(t, `{"bin":3}`)
	if _, err := c.ClassifyIntoBins("kmer", []DNAInput{{ID: "r1", DNA: "ACGT"}}); err != nil {
		t.Fatalf("ClassifyIntoBins: %v", err)
	}
	if want := `[["r1","ACGT"]]`; string(got.Params[1]) != want {
		t.Errorf("params[1] = %s, want %s", got.Params[1], want)
	}
}

func TestClassifyFullReturnsThreeValues(t *testing.T) {
	// The one method whose Perl caller uses list context.
	c, _ := serve(t, `{"binA":7}`, `"raw output"`, `["r9","r10"]`)

	bins, raw, unassigned, err := c.ClassifyFull("kmer", []DNAInput{{ID: "r1", DNA: "ACGT"}})
	if err != nil {
		t.Fatalf("ClassifyFull: %v", err)
	}
	if bins["binA"] != 7 {
		t.Errorf("bins = %v", bins)
	}
	if raw != "raw output" {
		t.Errorf("raw = %q", raw)
	}
	if len(unassigned) != 2 || unassigned[0] != "r9" {
		t.Errorf("unassigned = %v", unassigned)
	}
}

func TestClassifyFullNeedsThreeValues(t *testing.T) {
	c, _ := serve(t, `{"binA":7}`)
	if _, _, _, err := c.ClassifyFull("kmer", nil); err == nil {
		t.Fatal("want an error when the service returns fewer than 3 values")
	}
}

func TestEnumerateReturnsStringList(t *testing.T) {
	c, _ := serve(t, `["card","vfdb"]`)
	dbs, err := c.EnumerateSpecialProteinDatabases()
	if err != nil {
		t.Fatalf("EnumerateSpecialProteinDatabases: %v", err)
	}
	if len(dbs) != 2 || dbs[0] != "card" {
		t.Errorf("got %v", dbs)
	}
}

func TestExportGenomeReturnsText(t *testing.T) {
	c, got := serve(t, `"LOCUS       contig1\n"`)
	text, err := c.ExportGenome(GTO(`{}`), "genbank", []string{"CDS"})
	if err != nil {
		t.Fatalf("ExportGenome: %v", err)
	}
	if !strings.HasPrefix(text, "LOCUS") {
		t.Errorf("got %q", text)
	}
	if string(got.Params[1]) != `"genbank"` {
		t.Errorf("format param = %s", got.Params[1])
	}
	if string(got.Params[2]) != `["CDS"]` {
		t.Errorf("feature-type param = %s", got.Params[2])
	}
}

func TestExportGenomeSendsEmptyFeatureTypeList(t *testing.T) {
	// Not null: the service iterates the list.
	c, got := serve(t, `""`)
	if _, err := c.ExportGenome(GTO(`{}`), "gff", nil); err != nil {
		t.Fatalf("ExportGenome: %v", err)
	}
	if string(got.Params[2]) != `[]` {
		t.Errorf("feature-type param = %s, want []", got.Params[2])
	}
}

func TestRPCErrorIsReported(t *testing.T) {
	c := serveError(t, "something went wrong")
	if _, err := c.CallFeaturesCrispr(GTO(`{}`)); err == nil {
		t.Fatal("want an error")
	} else if !strings.Contains(err.Error(), "something went wrong") {
		t.Errorf("got %v", err)
	}
}

func TestErrorEnvelopeIsUnwrapped(t *testing.T) {
	c := serveError(t, "_ERROR_the genome has no contigs_ERROR_")
	_, err := c.CallFeaturesCrispr(GTO(`{}`))
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), "_ERROR_") {
		t.Errorf("envelope not stripped: %v", err)
	}
	if !strings.Contains(err.Error(), "the genome has no contigs") {
		t.Errorf("got %v", err)
	}
}

func TestMultilineErrorSurvivesDecoding(t *testing.T) {
	// A Perl die message arrives as a JSON string with escaped newlines; it
	// should be printed with real ones, not as a quoted blob.
	c := serveError(t, "line one\nline two")
	_, err := c.CallFeaturesCrispr(GTO(`{}`))
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "line one\nline two") {
		t.Errorf("got %q", err.Error())
	}
}

func TestNonJSONErrorBodyIsDiagnosed(t *testing.T) {
	// What a Cloudflare block looks like: an HTML body behind a 403, where the
	// service itself would have answered JSON-RPC.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("CF-Ray", "abc123-ORD")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "<html><head><title>Error 1010</title></head></html>")
	}))
	defer srv.Close()

	c := New(WithURL(srv.URL))
	_, err := c.DefaultWorkflow()
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), "parsing response") {
		t.Errorf("an edge rejection was reported as a JSON parse failure: %v", err)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("error should carry the status: %v", err)
	}
}

func TestEmptyResultIsAnError(t *testing.T) {
	c, _ := serve(t)
	if _, err := c.CallFeaturesCrispr(GTO(`{}`)); err == nil {
		t.Fatal("want an error when the service returns no result")
	}
}

func TestWithURLEmptyKeepsDefault(t *testing.T) {
	// So a command can pass an unset --url straight through.
	if c := New(WithURL("")); c.URL != DefaultURL {
		t.Errorf("URL = %q, want the default", c.URL)
	}
	if c := New(WithURL("http://example.com/x")); c.URL != "http://example.com/x" {
		t.Errorf("URL = %q", c.URL)
	}
}

func TestEnvTimeout(t *testing.T) {
	tests := []struct {
		value string
		want  time.Duration
	}{
		{"", DefaultTimeout},
		{"60", time.Minute},
		{"1", time.Second},
		// A bad value keeps the default rather than failing the command.
		{"abc", DefaultTimeout},
		{"0", DefaultTimeout},
		{"-5", DefaultTimeout},
	}
	for _, tt := range tests {
		t.Setenv("CDMI_TIMEOUT", tt.value)
		if got := envTimeout(DefaultTimeout); got != tt.want {
			t.Errorf("CDMI_TIMEOUT=%q: got %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestCleanErrorMessage(t *testing.T) {
	tests := []struct{ in, want string }{
		{"_ERROR_boom_ERROR_", "boom"},
		{"_ERROR_ boom \n_ERROR_", "boom"},
		{"plain message", "plain message"},
		{"", ""},
		// A lone marker is not a complete envelope; leave it alone.
		{"_ERROR_unterminated", "_ERROR_unterminated"},
	}
	for _, tt := range tests {
		if got := cleanErrorMessage(tt.in); got != tt.want {
			t.Errorf("cleanErrorMessage(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
