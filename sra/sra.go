// Package sra looks up SRA accession metadata at NCBI.
//
// Submit commands take SRA accessions with --srr-id and pass them through to
// the app unchecked, so a typo is only discovered later, when the job fails
// staging its reads. Lookup both validates an accession and returns its study
// title, which the BV-BRC web UI records alongside the accession in the
// submitted job parameters.
//
// The endpoint is eutils efetch with rettype=docset, the same one
// sra_import/lib/sra_tools.py uses. It accepts bare accessions (no uid
// indirection, unlike esummary), it takes a batch in one request, and it
// silently omits accessions it does not know rather than failing the whole
// request — so "not found" is "absent from the response". ENA's filereport is
// not used: it lags NCBI for recent submissions and returns no rows for them.
package sra

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/BV-BRC/BV-BRC-Go-SDK/version"
)

const (
	// DefaultBaseURL is the eutils efetch endpoint.
	DefaultBaseURL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
	// DefaultMaxRetries bounds retries on throttling and server errors.
	DefaultMaxRetries = 3
	// DefaultBatchSize is the number of accessions sent per request.
	DefaultBatchSize = 20
)

// Record is the metadata kept for one accession.
type Record struct {
	// Accession is the accession as requested, which may name a run, an
	// experiment, or a study.
	Accession string
	// RunAccessions lists the run accessions in the matching experiment package.
	RunAccessions []string
	// ExperimentAccession and StudyAccession identify the enclosing records.
	ExperimentAccession string
	StudyAccession      string
	// ExperimentTitle is the per-experiment title.
	ExperimentTitle string
	// StudyTitle is the study-level title. This is the value the web UI
	// records as "title" on a submitted SRA library.
	StudyTitle string
}

// Client queries NCBI for SRA metadata.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	UserAgent  string
	MaxRetries int
	BatchSize  int
	// APIKey is an NCBI API key, which raises the per-IP rate limit from 3 to
	// 10 requests/second. Defaults to $NCBI_API_KEY.
	APIKey string
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides the efetch endpoint (used by tests).
func WithBaseURL(u string) Option { return func(c *Client) { c.BaseURL = u } }

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.HTTPClient = h } }

// WithUserAgent sets the User-Agent header.
func WithUserAgent(ua string) Option { return func(c *Client) { c.UserAgent = ua } }

// WithAPIKey sets the NCBI API key.
func WithAPIKey(k string) Option { return func(c *Client) { c.APIKey = k } }

// New creates a Client.
func New(opts ...Option) *Client {
	c := &Client{
		BaseURL:    DefaultBaseURL,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		UserAgent:  version.UserAgent(),
		MaxRetries: DefaultMaxRetries,
		BatchSize:  DefaultBatchSize,
		APIKey:     os.Getenv("NCBI_API_KEY"),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Lookup fetches metadata for every accession, batching requests.
//
// found is keyed by the accession as requested; missing lists, in the order
// given, the accessions NCBI did not return. Duplicates and blanks are ignored.
//
// A non-nil error means the service could not be reached — which is a different
// condition from an accession not existing, and callers generally want to treat
// it differently: an unknown accession is the user's mistake, an eutils outage
// is not.
func (c *Client) Lookup(ctx context.Context, accessions []string) (found map[string]Record, missing []string, err error) {
	found = make(map[string]Record)

	var want []string
	seen := make(map[string]bool, len(accessions))
	for _, a := range accessions {
		a = strings.TrimSpace(a)
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		want = append(want, a)
	}
	if len(want) == 0 {
		return found, nil, nil
	}

	size := c.BatchSize
	if size <= 0 {
		size = DefaultBatchSize
	}
	for start := 0; start < len(want); start += size {
		end := start + size
		if end > len(want) {
			end = len(want)
		}
		batch := want[start:end]
		packages, err := c.fetch(ctx, batch)
		if err != nil {
			return nil, nil, err
		}
		for _, a := range batch {
			if rec, ok := matchAccession(a, packages); ok {
				found[a] = rec
			}
		}
	}

	for _, a := range want {
		if _, ok := found[a]; !ok {
			missing = append(missing, a)
		}
	}
	return found, missing, nil
}

// matchAccession finds the experiment package covering the accession. An
// accession may name a run, an experiment, or a study; sra_tools.py accepts all
// three (parse_accession_metadata), so we do too.
func matchAccession(accession string, packages []experimentPackage) (Record, bool) {
	for _, p := range packages {
		hit := strings.EqualFold(accession, p.Experiment.Accession) ||
			strings.EqualFold(accession, p.Study.Accession)
		if !hit {
			for _, r := range p.RunSet.Runs {
				if strings.EqualFold(accession, r.Accession) {
					hit = true
					break
				}
			}
		}
		if !hit {
			continue
		}
		rec := Record{
			Accession:           accession,
			ExperimentAccession: p.Experiment.Accession,
			StudyAccession:      p.Study.Accession,
			ExperimentTitle:     strings.TrimSpace(p.Experiment.Title),
			StudyTitle:          strings.TrimSpace(p.Study.Descriptor.StudyTitle),
		}
		for _, r := range p.RunSet.Runs {
			rec.RunAccessions = append(rec.RunAccessions, r.Accession)
		}
		return rec, true
	}
	return Record{}, false
}

// fetch performs one efetch request and returns the experiment packages it
// contains. An "ID list is empty" error document means none of the accessions
// were recognized, which is not a transport error: it yields no packages.
func (c *Client) fetch(ctx context.Context, batch []string) ([]experimentPackage, error) {
	params := url.Values{}
	params.Set("db", "sra")
	params.Set("rettype", "docset")
	params.Set("retmode", "xml")
	params.Set("id", strings.Join(batch, ","))
	if c.APIKey != "" {
		params.Set("api_key", c.APIKey)
	}
	reqURL := c.BaseURL + "?" + params.Encode()

	retries := c.MaxRetries
	if retries < 0 {
		retries = 0
	}
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			// eutils throttles aggressively; back off before retrying.
			delay := time.Duration(attempt) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		body, retryable, err := c.get(ctx, reqURL)
		if err != nil {
			lastErr = err
			if retryable {
				continue
			}
			return nil, err
		}

		var set experimentPackageSet
		if err := xml.Unmarshal(body, &set); err != nil {
			// An error document is a different root element, so it fails to
			// unmarshal into the package set. Recognize the "no such
			// accession" case and report it as an empty result.
			if isEmptyIDList(body) {
				return nil, nil
			}
			return nil, fmt.Errorf("parsing SRA response: %w", err)
		}
		return set.Packages, nil
	}
	return nil, lastErr
}

// get issues one request, reporting whether a failure is worth retrying.
func (c *Client) get(ctx context.Context, reqURL string) (body []byte, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("User-Agent", c.UserAgent)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("contacting NCBI: %w", err)
	}
	defer resp.Body.Close()

	data, readErr := io.ReadAll(resp.Body)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return nil, true, fmt.Errorf("NCBI returned HTTP %d", resp.StatusCode)
	case resp.StatusCode == http.StatusBadRequest && isEmptyIDList(data):
		// NCBI answers a wholly unrecognized id list with 400 in some cases.
		return data, false, nil
	case resp.StatusCode != http.StatusOK:
		return nil, false, fmt.Errorf("NCBI returned HTTP %d", resp.StatusCode)
	}
	if readErr != nil {
		return nil, true, fmt.Errorf("reading NCBI response: %w", readErr)
	}
	return data, false, nil
}

// isEmptyIDList reports whether the body is eutils' "nothing matched" error.
func isEmptyIDList(body []byte) bool {
	return strings.Contains(string(body), "ID list is empty")
}

// The XML below covers only the fields we need out of the docset response.

type experimentPackageSet struct {
	XMLName  xml.Name            `xml:"EXPERIMENT_PACKAGE_SET"`
	Packages []experimentPackage `xml:"EXPERIMENT_PACKAGE"`
}

type experimentPackage struct {
	Experiment struct {
		Accession string `xml:"accession,attr"`
		Title     string `xml:"TITLE"`
	} `xml:"EXPERIMENT"`
	Study struct {
		Accession  string `xml:"accession,attr"`
		Descriptor struct {
			StudyTitle string `xml:"STUDY_TITLE"`
		} `xml:"DESCRIPTOR"`
	} `xml:"STUDY"`
	RunSet struct {
		Runs []struct {
			Accession string `xml:"accession,attr"`
		} `xml:"RUN"`
	} `xml:"RUN_SET"`
}
