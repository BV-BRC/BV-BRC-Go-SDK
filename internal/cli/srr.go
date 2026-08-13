package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/BV-BRC/BV-BRC-Go-SDK/sra"
)

// ValidateSRRUsage is the help text for --validate-srr. Shared so every submit
// command that takes SRA accessions describes the flag the same way.
const ValidateSRRUsage = "look each --srr-id up at NCBI: reject unknown accessions and record the SRA study title with the library"

// srrLookupTimeout bounds the whole batch of NCBI requests. A submission should
// not hang on eutils.
const srrLookupTimeout = 60 * time.Second

// LookupSRRTitles validates SRA accessions at NCBI and returns their study
// titles keyed by accession. It reports each accession and its title on stderr.
//
// When enabled is false it is a no-op, so a command can call it unconditionally
// and stay byte-identical to its pre-validation behaviour (no network call, no
// titles) unless the user asks for validation.
//
// An unknown accession is an error: every accession is checked first, then all
// the bad ones are named in one error, so nothing is uploaded on behalf of a
// submission that cannot work. NCBI being unreachable is not an error — that
// would block valid submissions during an outage — it warns and returns no
// titles.
//
// Not every app can carry the title: apps whose spec takes a bare "srr_ids"
// list have nowhere to put it. Validation is still worth doing there, and such
// commands simply ignore the returned map.
func LookupSRRTitles(enabled bool, accessions []string) (map[string]string, error) {
	return lookupSRRTitles(os.Stderr, sra.New(), enabled, accessions)
}

func lookupSRRTitles(w io.Writer, client *sra.Client, enabled bool, accessions []string) (map[string]string, error) {
	if !enabled || len(accessions) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), srrLookupTimeout)
	defer cancel()

	found, missing, err := client.Lookup(ctx, accessions)
	if err != nil {
		fmt.Fprintf(w, "warning: could not validate SRA accessions: %v\n", err)
		fmt.Fprintf(w, "warning: submitting without SRA study titles\n")
		return nil, nil
	}

	titles := make(map[string]string, len(found))
	for _, acc := range accessions {
		rec, ok := found[acc]
		if !ok {
			continue
		}
		titles[acc] = rec.StudyTitle
		shown := rec.StudyTitle
		if shown == "" {
			shown = "(no study title)"
		}
		fmt.Fprintf(w, "%s\t%s\n", acc, shown)
	}

	if len(missing) > 0 {
		for _, acc := range missing {
			fmt.Fprintf(w, "%s\tnot found at NCBI\n", acc)
		}
		return nil, fmt.Errorf("invalid SRA accession(s): %s", strings.Join(missing, ", "))
	}
	return titles, nil
}
