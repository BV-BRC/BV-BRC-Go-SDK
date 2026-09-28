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
	"strconv"
	"strings"
	"time"

	"github.com/BV-BRC/BV-BRC-Go-SDK/v2/version"
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
	// Kept for backward compatibility; Runs (below) carries the same
	// accessions plus full per-run metadata.
	RunAccessions []string
	// ExperimentAccession and StudyAccession identify the enclosing records.
	ExperimentAccession string
	StudyAccession      string
	// ExperimentTitle is the per-experiment title.
	ExperimentTitle string
	// StudyTitle is the study-level title. This is the value the web UI
	// records as "title" on a submitted SRA library.
	StudyTitle string
	// Platform is NCBI's raw PLATFORM tag name (e.g. "ILLUMINA",
	// "PACBIO_SMRT"), taken from the tag itself rather than element content
	// -- see sra_tools.py:87-93 (plat.tag). Empty if the experiment package
	// carries no PLATFORM element.
	Platform string
	// LibraryLayout is "PAIRED" or "SINGLE", derived from the presence of
	// LIBRARY_LAYOUT/PAIRED or LIBRARY_LAYOUT/SINGLE -- see sra_tools.py:108.
	LibraryLayout string
	// Runs carries full per-run metadata for every run in the matching
	// experiment package, in RUN_SET order (same order as RunAccessions).
	Runs []RunRecord
}

// RunRecord is per-run metadata parsed out of one RUN_SET/RUN element.
type RunRecord struct {
	Accession  string
	SizeBytes  int64
	TotalBases int64
	TotalSpots int64
	// NReads is the number of reads eutils' own Statistics block actually
	// evidences (see parseRunRecord) rather than a trusted @nreads
	// attribute. Zero means unknown, not "zero reads".
	NReads int
	// ReadLength is the average read length reported by the first
	// Statistics child that carries one. Zero means unknown.
	ReadLength float64
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
//
// Lookup is a thin first-match wrapper over LookupAll: for a study (SRP) or
// experiment (SRX) accession that spans multiple experiment packages, this
// returns only the first one, matching this method's behavior before
// LookupAll existed (so nothing depending on that — e.g. --validate-srr —
// changes). Callers that need every constituent run of a study should call
// LookupAll directly.
func (c *Client) Lookup(ctx context.Context, accessions []string) (found map[string]Record, missing []string, err error) {
	all, missing, err := c.LookupAll(ctx, accessions)
	if err != nil {
		return nil, nil, err
	}
	found = make(map[string]Record, len(all))
	for a, recs := range all {
		found[a] = recs[0] // first match; recs is never empty (see LookupAll).
	}
	return found, missing, nil
}

// LookupAll fetches metadata for every accession, like Lookup, but returns
// every matching experiment package rather than just the first.
//
// This matters for a study (SRP) accession: every experiment package in the
// study matches it, but Lookup — a linear scan that returns on the first hit
// — only ever reported one. get_metadata-style callers need one row per
// constituent run across the whole study, matching sra_tools.py's
// parse_accession_metadata, which already flattens a study/experiment
// accession into one record per run.
//
// found is keyed by the accession as requested, to a slice of Record (one per
// matching experiment package, in the order NCBI returned them); missing
// lists, in the order given, the accessions NCBI did not return at all.
func (c *Client) LookupAll(ctx context.Context, accessions []string) (found map[string][]Record, missing []string, err error) {
	return c.collectMatches(ctx, accessions, matchAllAccessions)
}

// collectMatches implements the batching/dedup/fetch flow shared by Lookup
// and LookupAll, so both reuse this package's HTTP/retry logic (fetch/get)
// rather than duplicating it. match is called once per requested accession
// against the packages retrieved for its batch.
func (c *Client) collectMatches(ctx context.Context, accessions []string, match func(accession string, packages []experimentPackage) ([]Record, bool)) (found map[string][]Record, missing []string, err error) {
	found = make(map[string][]Record)

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
			if recs, ok := match(a, packages); ok {
				found[a] = recs
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

// packageMatches reports whether accession names p's experiment, its study,
// or one of its runs. An accession may name a run, an experiment, or a study;
// sra_tools.py accepts all three (parse_accession_metadata), so we do too.
func packageMatches(accession string, p experimentPackage) bool {
	if strings.EqualFold(accession, p.Experiment.Accession) ||
		strings.EqualFold(accession, p.Study.Accession) {
		return true
	}
	for _, r := range p.RunSet.Runs {
		if strings.EqualFold(accession, r.Accession) {
			return true
		}
	}
	return false
}

// recordFromPackage builds a Record for accession out of one matching
// experiment package.
func recordFromPackage(accession string, p experimentPackage) Record {
	rec := Record{
		Accession:           accession,
		ExperimentAccession: p.Experiment.Accession,
		StudyAccession:      p.Study.Accession,
		ExperimentTitle:     strings.TrimSpace(p.Experiment.Title),
		StudyTitle:          strings.TrimSpace(p.Study.Descriptor.StudyTitle),
		Platform:            platformOf(p.Experiment.Platform),
		LibraryLayout:       libraryLayoutOf(p.Experiment.LibraryLayout),
	}
	for _, r := range p.RunSet.Runs {
		rec.RunAccessions = append(rec.RunAccessions, r.Accession)
		rec.Runs = append(rec.Runs, parseRunRecord(r))
	}
	return rec
}

// matchAccession finds the first experiment package covering the accession.
// Kept for Lookup's first-match behavior; LookupAll uses matchAllAccessions
// instead.
func matchAccession(accession string, packages []experimentPackage) (Record, bool) {
	for _, p := range packages {
		if packageMatches(accession, p) {
			return recordFromPackage(accession, p), true
		}
	}
	return Record{}, false
}

// matchAllAccessions finds every experiment package covering the accession.
func matchAllAccessions(accession string, packages []experimentPackage) ([]Record, bool) {
	var recs []Record
	for _, p := range packages {
		if packageMatches(accession, p) {
			recs = append(recs, recordFromPackage(accession, p))
		}
	}
	return recs, len(recs) > 0
}

// platformOf returns the PLATFORM element's child tag name (e.g. "ILLUMINA"),
// or "" if there is none.
func platformOf(p platformXML) string {
	if len(p.Any) == 0 {
		return ""
	}
	return p.Any[0].XMLName.Local
}

// libraryLayoutOf classifies a LIBRARY_LAYOUT element by which child is
// present. sra_tools.py:108 only ever tests for PAIRED and defaults to SINGLE
// otherwise ("this might be unreliable. use the existence of paired file");
// this instead tests both children explicitly (matching real SRA documents,
// which always carry exactly one of PAIRED/SINGLE/... per the SRA schema) and
// returns "" only in the pathological case where neither is present —
// slightly stricter than the Python fallback, which would call that case
// SINGLE.
func libraryLayoutOf(l libraryLayoutXML) string {
	switch {
	case l.Paired != nil:
		return "PAIRED"
	case l.Single != nil:
		return "SINGLE"
	default:
		return ""
	}
}

// parseRunRecord builds a RunRecord from one RUN element.
//
// The NReads/ReadLength logic ports sra_tools.py:144-153 exactly: nreads
// might lie (cf SRR6263255), so this counts Statistics children whose own
// "count" attribute is > 0 rather than trusting a naive @nreads summary
// attribute (which this doesn't even read). The same loop also ports that
// block's ReadLength fallback: the first Statistics child carrying an
// "average" attribute sets ReadLength, matching Python's
// "not 'read_length' in rdata" first-wins guard.
func parseRunRecord(r runXML) RunRecord {
	rec := RunRecord{Accession: r.Accession}
	if v, err := strconv.ParseInt(r.TotalBases, 10, 64); err == nil {
		rec.TotalBases = v
	}
	if v, err := strconv.ParseInt(r.TotalSpots, 10, 64); err == nil {
		rec.TotalSpots = v
	}
	if v, err := strconv.ParseInt(r.Size, 10, 64); err == nil {
		rec.SizeBytes = v
	}

	if r.Statistics != nil {
		// nreads might lie. cf SRR6263255
		nreads := 0
		readLengthSet := false
		for _, read := range r.Statistics.Reads {
			if count, err := strconv.Atoi(read.Count); err == nil && count > 0 {
				nreads++
			}
			if !readLengthSet && read.Average != "" {
				if avg, err := strconv.ParseFloat(read.Average, 64); err == nil {
					rec.ReadLength = avg
					readLengthSet = true
				}
			}
		}
		if nreads > 0 {
			rec.NReads = nreads
		}
	}

	return rec
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
		// Platform's value is the tag name of PLATFORM's one child element
		// (e.g. <ILLUMINA>...</ILLUMINA>), not its content -- see
		// sra_tools.py:87-93.
		Platform platformXML `xml:"PLATFORM"`
		// LibraryLayout lives at EXPERIMENT/DESIGN/LIBRARY_DESCRIPTOR/
		// LIBRARY_LAYOUT per the SRA schema; sra_tools.py:108 finds it with a
		// "//" wildcard search instead, but every real document places it
		// here.
		LibraryLayout libraryLayoutXML `xml:"DESIGN>LIBRARY_DESCRIPTOR>LIBRARY_LAYOUT"`
	} `xml:"EXPERIMENT"`
	Study struct {
		Accession  string `xml:"accession,attr"`
		Descriptor struct {
			StudyTitle string `xml:"STUDY_TITLE"`
		} `xml:"DESCRIPTOR"`
	} `xml:"STUDY"`
	RunSet struct {
		Runs []runXML `xml:"RUN"`
	} `xml:"RUN_SET"`
}

// platformXML captures EXPERIMENT/PLATFORM's one child element so its tag
// name (not its content) can be read as the platform. xml:",any" collects
// every child regardless of name; the SRA schema only ever has one.
type platformXML struct {
	Any []struct {
		XMLName xml.Name
	} `xml:",any"`
}

// libraryLayoutXML is a pair of presence probes, not content fields: a
// non-nil pointer means the element was present (its content, if any, is
// ignored), matching sra_tools.py:108's existence-only check.
type libraryLayoutXML struct {
	Paired *struct{} `xml:"PAIRED"`
	Single *struct{} `xml:"SINGLE"`
}

// runXML is one RUN_SET/RUN element. Numeric attributes are kept as strings
// and parsed in parseRunRecord because they are sometimes absent (see
// sra_tools.py's try/except around the same reads) -- an unparseable or
// missing value just leaves the corresponding RunRecord field zero rather
// than failing the whole unmarshal.
type runXML struct {
	Accession  string         `xml:"accession,attr"`
	TotalBases string         `xml:"total_bases,attr"`
	TotalSpots string         `xml:"total_spots,attr"`
	Size       string         `xml:"size,attr"`
	Statistics *statisticsXML `xml:"Statistics"`
}

// statisticsXML mirrors the RUN/Statistics block eutils emits: a summary
// element (whose own "nreads" attribute we deliberately never read -- see
// parseRunRecord) with one child per read, each carrying its own "count" and
// "average" attributes.
type statisticsXML struct {
	Reads []struct {
		Count   string `xml:"count,attr"`
		Average string `xml:"average,attr"`
	} `xml:",any"`
}
