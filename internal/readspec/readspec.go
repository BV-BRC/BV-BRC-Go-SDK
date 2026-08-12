// Package readspec mirrors the read-library parameter construction performed by
// the Perl Bio::KBase::AppService::ReadSpec module (p3_cli/lib).
//
// BV-BRC apps do not agree on the shape of their read-library parameters. Perl
// selects among the variants from a handful of constructor flags; this package
// reproduces that selection so Go submit commands do not each hardcode (and
// re-diverge on) one dialect.
//
// The two axes are:
//
//	SRR parameter name  — "srr_ids" or "srr_libs"        (ReadSpec srr_label)
//	SRR entry shape     — bare string or {srr_accession} (ReadSpec srrAlt)
//
// plus per-library sample_id when Samples is set.
//
// Correspondence to the Perl constructor (ReadSpec.pm:229-236):
//
//	samples || analysis                     => srrAlt = 1
//	samples || analysis || rnaseq           => srr_label = 'srr_libs'
//
// Note that srrAlt alone does not set srr_label. FastqUtils needs object-shaped
// entries under "srr_libs", which those two rules cannot express together, so
// p3-submit-fastqutils.pl:92 pokes the field directly after construction
// ($reader->{srr_label} = 'srr_libs'). SRRKeyOverride is the equivalent here.
// See doc/PERL-READSPEC-ISSUES.md for the Perl-side defects found alongside this.
package readspec

import (
	"path"
	"regexp"
)

// Options mirrors the ReadSpec constructor flags that affect parameter shape.
type Options struct {
	// Simple suppresses the optional per-library tweaks (platform,
	// insert sizes, read orientation).
	Simple bool
	// Samples adds a derived sample_id to every read library.
	Samples bool
	// Analysis adds primers/primer_version/sample_level_date.
	Analysis bool
	// RNASeq adds per-library condition indexes.
	RNASeq bool
	// SRRAlt forces object-shaped SRA entries independently of the above.
	SRRAlt bool
	// SRRKeyOverride names the SRA parameter explicitly, bypassing the
	// derivation in SRRKey. Mirrors assigning ReadSpec's srr_label field
	// directly, which p3-submit-fastqutils.pl does.
	SRRKeyOverride string
}

// srrAlt reports whether SRA entries are object-shaped.
func (o Options) srrAlt() bool { return o.SRRAlt || o.Samples || o.Analysis }

// SRRIsObject reports whether SRA entries are objects rather than bare strings.
func (o Options) SRRIsObject() bool { return o.RNASeq || o.srrAlt() }

// SRRKey returns the parameter name that holds the SRA libraries.
func (o Options) SRRKey() string {
	if o.SRRKeyOverride != "" {
		return o.SRRKeyOverride
	}
	if o.Samples || o.Analysis || o.RNASeq {
		return "srr_libs"
	}
	return "srr_ids"
}

// PairedLib builds a paired_end_libs entry for an already-normalized pair of
// workspace paths. Callers may add app-specific keys to the result.
func (o Options) PairedLib(read1, read2 string) map[string]interface{} {
	lib := map[string]interface{}{"read1": read1, "read2": read2}
	if o.Samples {
		lib["sample_id"] = SampleIDForPair(read1, read2)
	}
	return lib
}

// SingleLib builds a single_end_libs entry.
func (o Options) SingleLib(read string) map[string]interface{} {
	lib := map[string]interface{}{"read": read}
	if o.Samples {
		lib["sample_id"] = SampleIDForFile(read)
	}
	return lib
}

// SRREntry builds one object-shaped SRA entry. Only meaningful when
// SRRIsObject reports true; callers may add app-specific keys.
func (o Options) SRREntry(accession string) map[string]interface{} {
	entry := map[string]interface{}{"srr_accession": accession}
	if o.Samples {
		// Perl uses the accession verbatim as the sample ID.
		entry["sample_id"] = accession
	}
	return entry
}

var (
	// Extension strippers, in Perl's order: ".<ext>.gz" before ".<ext>".
	reGzExt  = regexp.MustCompile(`(?i)^(.+)\.[a-z]+\.gz$`)
	reExt    = regexp.MustCompile(`(?i)^(.+)\.[a-z]+$`)
	reSuffix = regexp.MustCompile(`(?i)^(.+)_[a-z]?$`)
)

// SampleIDForFile mirrors ReadSpec::_compute_sample_id: strip the directory,
// strip one extension (honoring a trailing .gz), then remove a trailing
// "_" or "_<letter>".
func SampleIDForFile(fileName string) string {
	base := path.Base(fileName)
	if m := reGzExt.FindStringSubmatch(base); m != nil {
		base = m[1]
	} else if m := reExt.FindStringSubmatch(base); m != nil {
		base = m[1]
	}
	return removeSuffixes(base)
}

// SampleIDForPair mirrors the sample-ID derivation in ReadSpec::_pairedLib:
// use the common prefix of the two basenames when it is long enough,
// otherwise fall back to single-file derivation on read1.
func SampleIDForPair(read1, read2 string) string {
	r1 := []rune(path.Base(read1))
	r2 := []rune(path.Base(read2))
	// i ends one past the length of the common prefix. Perl's substr-based
	// loop is unbounded and spins forever on identical names; bound it.
	i := 1
	for i <= len(r1) && i <= len(r2) && string(r1[:i]) == string(r2[:i]) {
		i++
	}
	if i >= 5 {
		return removeSuffixes(string(r1[:i-1]))
	}
	return SampleIDForFile(read1)
}

// removeSuffixes mirrors ReadSpec::_remove_suffixes.
func removeSuffixes(name string) string {
	if m := reSuffix.FindStringSubmatch(name); m != nil {
		return m[1]
	}
	return name
}
