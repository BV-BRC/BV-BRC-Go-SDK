package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BV-BRC/BV-BRC-Go-SDK/sra"
)

const docset = `<?xml version="1.0" encoding="UTF-8"?>
<EXPERIMENT_PACKAGE_SET>
<EXPERIMENT_PACKAGE>
  <EXPERIMENT accession="SRX34779904"><TITLE>an experiment</TITLE></EXPERIMENT>
  <STUDY accession="SRP393881">
    <DESCRIPTOR><STUDY_TITLE>A wastewater surveillance study</STUDY_TITLE></DESCRIPTOR>
  </STUDY>
  <RUN_SET><RUN accession="SRR40145022"/></RUN_SET>
</EXPERIMENT_PACKAGE>
</EXPERIMENT_PACKAGE_SET>`

// stub returns a client pointed at a server answering with the given handler,
// plus a count of the requests it received.
func stub(t *testing.T, h http.HandlerFunc) (*sra.Client, *int) {
	t.Helper()
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	return sra.New(sra.WithBaseURL(srv.URL), sra.WithHTTPClient(srv.Client())), &calls
}

func TestLookupSRRTitlesDisabled(t *testing.T) {
	c, calls := stub(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request should be made when validation is off")
	})

	var out bytes.Buffer
	titles, err := lookupSRRTitles(&out, c, false, []string{"SRR40145022"})
	if err != nil || titles != nil || *calls != 0 || out.Len() != 0 {
		t.Errorf("disabled lookup did something: titles=%v err=%v calls=%d out=%q",
			titles, err, *calls, out.String())
	}
}

func TestLookupSRRTitlesReportsTitles(t *testing.T) {
	c, _ := stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(docset))
	})

	var out bytes.Buffer
	titles, err := lookupSRRTitles(&out, c, true, []string{"SRR40145022"})
	if err != nil {
		t.Fatalf("lookupSRRTitles: %v", err)
	}
	if got := titles["SRR40145022"]; got != "A wastewater surveillance study" {
		t.Errorf("title = %q", got)
	}
	if want := "SRR40145022\tA wastewater surveillance study\n"; out.String() != want {
		t.Errorf("stderr = %q, want %q", out.String(), want)
	}
}

// Unknown accessions are named individually and then reported together, so one
// run tells the user about all of them.
func TestLookupSRRTitlesUnknownAccessionFails(t *testing.T) {
	c, _ := stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(docset))
	})

	var out bytes.Buffer
	_, err := lookupSRRTitles(&out, c, true, []string{"SRR40145022", "SRRBOGUS", "SRRNOPE"})
	if err == nil {
		t.Fatal("expected an error for the unknown accessions")
	}
	for _, want := range []string{"SRRBOGUS", "SRRNOPE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
		if !strings.Contains(out.String(), want+"\tnot found at NCBI") {
			t.Errorf("stderr does not report %s: %q", want, out.String())
		}
	}
}

// An unreachable NCBI must not block an otherwise-valid submission.
func TestLookupSRRTitlesOutageWarns(t *testing.T) {
	c, _ := stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c.MaxRetries = 1

	var out bytes.Buffer
	titles, err := lookupSRRTitles(&out, c, true, []string{"SRR40145022"})
	if err != nil {
		t.Fatalf("an outage should not fail the submission: %v", err)
	}
	if len(titles) != 0 {
		t.Errorf("titles = %v, want none", titles)
	}
	if !strings.Contains(out.String(), "warning:") {
		t.Errorf("no warning printed: %q", out.String())
	}
}
