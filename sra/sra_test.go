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
