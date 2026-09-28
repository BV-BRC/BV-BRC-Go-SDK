package sra

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// testdata/docset.xml is a trimmed capture of a real
// efetch?db=sra&rettype=docset response for SRR40145022 and SRR40145023,
// with a second run added to the first experiment package so the
// multiple-runs-per-package path is covered.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return data
}

// serve returns a client pointed at a stub that answers every request with the
// given handler, plus a pointer to the recorded query values of the last request.
func serve(t *testing.T, h http.HandlerFunc) (*Client, *[]string) {
	t.Helper()
	var ids []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids = append(ids, r.URL.Query().Get("id"))
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	c := New(WithBaseURL(srv.URL), WithHTTPClient(srv.Client()))
	return c, &ids
}

const studyTitle = "SARS-CoV-2 Colorado wastewater genomic surveillance Raw sequence reads"

func TestLookupStudyTitle(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "docset.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRR40145022", "SRR40145023"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(missing) != 0 {
		t.Errorf("missing = %v, want none", missing)
	}
	for _, acc := range []string{"SRR40145022", "SRR40145023"} {
		rec, ok := found[acc]
		if !ok {
			t.Fatalf("%s not found", acc)
		}
		if rec.StudyTitle != studyTitle {
			t.Errorf("%s StudyTitle = %q, want %q", acc, rec.StudyTitle, studyTitle)
		}
		if rec.StudyAccession != "SRP393881" {
			t.Errorf("%s StudyAccession = %q", acc, rec.StudyAccession)
		}
	}
	// The experiment title is distinct from the study title; the web UI
	// records the study title, so make sure we do not confuse the two.
	if got := found["SRR40145022"].ExperimentTitle; !strings.HasPrefix(got, "SARS-CoV-2: wastewater surveillance sample") {
		t.Errorf("ExperimentTitle = %q", got)
	}
	if got := found["SRR40145022"].ExperimentAccession; got != "SRX34779904" {
		t.Errorf("ExperimentAccession = %q, want SRX34779904", got)
	}
}

// A run that shares an experiment package with another run must still resolve,
// and must report every run in that package.
func TestLookupSecondRunInPackage(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "docset.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRR40145099"})
	if err != nil || len(missing) != 0 {
		t.Fatalf("Lookup: err=%v missing=%v", err, missing)
	}
	rec := found["SRR40145099"]
	if rec.StudyTitle != studyTitle {
		t.Errorf("StudyTitle = %q", rec.StudyTitle)
	}
	if len(rec.RunAccessions) != 2 {
		t.Errorf("RunAccessions = %v, want both runs", rec.RunAccessions)
	}
}

// Experiment and study accessions resolve too: sra_tools.py accepts them, so
// --srr-id SRX… or SRP… must not be reported as invalid.
func TestLookupExperimentAndStudyAccessions(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "docset.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRX34779903", "SRP393881"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}
	if found["SRX34779903"].StudyTitle != studyTitle || found["SRP393881"].StudyTitle != studyTitle {
		t.Errorf("study title not resolved for experiment/study accession: %+v", found)
	}
}

// An accession NCBI omits from the response is missing, not an error.
func TestLookupMissingAccession(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "docset.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRR40145022", "SRR99999999999"})
	if err != nil {
		t.Fatalf("Lookup returned error for an unknown accession: %v", err)
	}
	if len(missing) != 1 || missing[0] != "SRR99999999999" {
		t.Errorf("missing = %v, want [SRR99999999999]", missing)
	}
	if _, ok := found["SRR40145022"]; !ok {
		t.Error("the valid accession should still be found")
	}
}

// A wholly unrecognized id list yields eutils' error document. Every accession
// is missing and there is no error, so the caller reports all bad IDs at once.
func TestLookupEmptyIDList(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "empty_id_list.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRRBOGUS", "NOPE"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(found) != 0 {
		t.Errorf("found = %v, want none", found)
	}
	if len(missing) != 2 {
		t.Errorf("missing = %v, want both accessions", missing)
	}
}

// Same document, delivered with a 400 status, which eutils also does.
func TestLookupEmptyIDListWith400(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(fixture(t, "empty_id_list.xml"))
	})

	_, missing, err := c.Lookup(context.Background(), []string{"SRRBOGUS"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(missing) != 1 {
		t.Errorf("missing = %v, want one accession", missing)
	}
}

func TestLookupRetriesOnThrottle(t *testing.T) {
	var calls int
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Write(fixture(t, "docset.xml"))
	})

	found, _, err := c.Lookup(context.Background(), []string{"SRR40145022"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2 (one throttled, one retried)", calls)
	}
	if found["SRR40145022"].StudyTitle != studyTitle {
		t.Error("retry did not return the record")
	}
}

// An outage is an error, distinct from a missing accession, so callers can
// warn and continue rather than rejecting the user's input.
func TestLookupServerErrorIsError(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c.MaxRetries = 1

	if _, _, err := c.Lookup(context.Background(), []string{"SRR40145022"}); err == nil {
		t.Fatal("expected an error when NCBI is failing")
	}
}

func TestLookupBatchesAndDeduplicates(t *testing.T) {
	c, ids := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "docset.xml"))
	})
	c.BatchSize = 2

	_, _, err := c.Lookup(context.Background(), []string{
		"SRR40145022", "SRR40145022", " ", "SRR40145023", "SRR40145099"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	want := []string{"SRR40145022,SRR40145023", "SRR40145099"}
	if len(*ids) != len(want) {
		t.Fatalf("requests = %v, want %v", *ids, want)
	}
	for i := range want {
		if (*ids)[i] != want[i] {
			t.Errorf("request %d id = %q, want %q", i, (*ids)[i], want[i])
		}
	}
}

func TestLookupNoAccessions(t *testing.T) {
	c, ids := serve(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request should be made for an empty accession list")
	})

	found, missing, err := c.Lookup(context.Background(), nil)
	if err != nil || len(found) != 0 || len(missing) != 0 || len(*ids) != 0 {
		t.Errorf("Lookup(nil) = %v, %v, %v", found, missing, err)
	}
}

// An ILLUMINA/PAIRED experiment package reports Platform, LibraryLayout, and
// per-run metadata (Runs) on the resulting Record.
func TestLookupIlluminaPairedPlatformAndLayout(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "illumina_paired.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRR50000001"})
	if err != nil || len(missing) != 0 {
		t.Fatalf("Lookup: err=%v missing=%v", err, missing)
	}
	rec := found["SRR50000001"]
	if rec.Platform != "ILLUMINA" {
		t.Errorf("Platform = %q, want ILLUMINA", rec.Platform)
	}
	if rec.LibraryLayout != "PAIRED" {
		t.Errorf("LibraryLayout = %q, want PAIRED", rec.LibraryLayout)
	}
	if len(rec.Runs) != 1 {
		t.Fatalf("Runs = %+v, want exactly one", rec.Runs)
	}
	run := rec.Runs[0]
	if run.Accession != "SRR50000001" {
		t.Errorf("Runs[0].Accession = %q, want SRR50000001", run.Accession)
	}
	if run.TotalBases != 150000000 || run.TotalSpots != 500000 || run.SizeBytes != 95000000 {
		t.Errorf("Runs[0] = %+v, unexpected size fields", run)
	}
	if run.NReads != 2 {
		t.Errorf("Runs[0].NReads = %d, want 2", run.NReads)
	}
	if run.ReadLength != 150 {
		t.Errorf("Runs[0].ReadLength = %v, want 150", run.ReadLength)
	}
}

// A PACBIO_SMRT/SINGLE experiment package reports those fields too, and a
// single-read Statistics block yields NReads == 1.
func TestLookupPacbioSingleLayout(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "pacbio_single.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRR50000002"})
	if err != nil || len(missing) != 0 {
		t.Fatalf("Lookup: err=%v missing=%v", err, missing)
	}
	rec := found["SRR50000002"]
	if rec.Platform != "PACBIO_SMRT" {
		t.Errorf("Platform = %q, want PACBIO_SMRT", rec.Platform)
	}
	if rec.LibraryLayout != "SINGLE" {
		t.Errorf("LibraryLayout = %q, want SINGLE", rec.LibraryLayout)
	}
	if len(rec.Runs) != 1 || rec.Runs[0].NReads != 1 {
		t.Errorf("Runs = %+v, want one run with NReads=1", rec.Runs)
	}
}

// A study (SRP) accession spans multiple experiment packages. LookupAll must
// return every one of them (the multi-run gap matchAccession used to have);
// Lookup, now a thin first-match wrapper over LookupAll, must still resolve
// to exactly one, matching its behavior before LookupAll existed.
func TestLookupAllReturnsEveryPackageInAStudy(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "study_multi.xml"))
	})

	all, missing, err := c.LookupAll(context.Background(), []string{"SRP60000000"})
	if err != nil {
		t.Fatalf("LookupAll: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}
	recs := all["SRP60000000"]
	if len(recs) != 3 {
		t.Fatalf("LookupAll returned %d records, want 3", len(recs))
	}
	seen := make(map[string]bool, len(recs))
	for _, rec := range recs {
		seen[rec.ExperimentAccession] = true
	}
	for _, want := range []string{"SRX60000001", "SRX60000002", "SRX60000003"} {
		if !seen[want] {
			t.Errorf("LookupAll did not return experiment package %s", want)
		}
	}

	found, missing, err := c.Lookup(context.Background(), []string{"SRP60000000"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}
	rec, ok := found["SRP60000000"]
	if !ok {
		t.Fatal("Lookup did not find the study accession")
	}
	if !seen[rec.ExperimentAccession] {
		t.Errorf("Lookup returned experiment accession %q, not one of the study's packages", rec.ExperimentAccession)
	}
}

// SRR6263255-style lying nreads: the Statistics element's own "nreads"
// attribute understates the real read count (sra_tools.py:142's "nreads
// might lie" comment). NReads must come from counting Statistics children
// whose own count attribute is > 0, never from that summary attribute.
func TestLookupLyingNReadsAttribute(t *testing.T) {
	c, _ := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture(t, "lying_nreads.xml"))
	})

	found, missing, err := c.Lookup(context.Background(), []string{"SRR62630001"})
	if err != nil || len(missing) != 0 {
		t.Fatalf("Lookup: err=%v missing=%v", err, missing)
	}
	rec := found["SRR62630001"]
	if len(rec.Runs) != 1 {
		t.Fatalf("Runs = %+v, want exactly one", rec.Runs)
	}
	// The fixture's Statistics nreads="1" attribute is deliberately wrong;
	// both Read children carry count > 0, so the correct answer is 2.
	if got := rec.Runs[0].NReads; got != 2 {
		t.Errorf("NReads = %d, want 2 (from counting Statistics children, not the lying nreads attribute)", got)
	}
}

// TestLookupAllLiveEutils is a real network test against NCBI eutils, gated
// behind BVBRC_TEST_INTEGRATION=1 -- the existing SDK convention (see
// p3_test.go's TestDerivedFields). It is the only test here that can confirm
// LookupAll's request/parse path against a real efetch response rather than a
// captured fixture, for one real run accession and one real study accession.
//
// Note on how the study accession is exercised: verified live (2026-09) that
// NCBI's efetch does NOT resolve a bare SRP id on its own -- "id=SRP393881"
// alone comes back "ID list is empty", the same response eutils gives for a
// wholly unrecognized id (SRP accessions are not first-class efetch ids the
// way SRR/SRX are; esearch resolves "SRP393881" as a free-text term against
// ~19k records, not as one study record). So this test does what a real
// caller batching several known run accessions from one study would do:
// includes both real runs from SRP393881 (SRR40145022, SRR40145023 --
// docset.xml is a trimmed capture of exactly this response) plus the SRP
// accession itself in one request. NCBI silently drops the unresolvable SRP
// id from the id list but still returns the two packages matched by the SRR
// ids; both share STUDY_REF=SRP393881 (confirmed live), which is what lets
// LookupAll's own matching -- run purely against the packages a batch
// already returned, independent of whether NCBI itself resolved every
// requested id -- expand "SRP393881" to both of them. That is the actual
// multi-run-gap bug this PR fixes: Lookup used to return only one.
func TestLookupAllLiveEutils(t *testing.T) {
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}

	c := New()
	all, missing, err := c.LookupAll(context.Background(), []string{"SRR40145022", "SRR40145023", "SRP393881"})
	if err != nil {
		t.Fatalf("LookupAll: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}

	runRecs, ok := all["SRR40145022"]
	if !ok || len(runRecs) == 0 {
		t.Fatal("SRR40145022 not found")
	}
	if runRecs[0].Platform == "" {
		t.Error("Platform is empty for a real run accession")
	}

	studyRecs, ok := all["SRP393881"]
	if !ok {
		t.Fatal("SRP393881 not found")
	}
	if len(studyRecs) < 2 {
		t.Errorf("study SRP393881 returned only %d experiment package(s), expected at least 2 (the multi-run gap this PR fixes)", len(studyRecs))
	}
}
