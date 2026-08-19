package rastcli

import (
	"reflect"
	"strings"
	"testing"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	"github.com/spf13/cobra"
)

// run builds a command, registers an option group on it, parses args, and
// returns whatever the group produced. It is how every test below exercises the
// "only what the user set" rule, because that rule depends on cobra's Changed
// bit and so cannot be tested by poking the struct fields directly.
func run[T any](t *testing.T, add func(*cobra.Command) T, args []string) T {
	t.Helper()
	cmd := &cobra.Command{Use: "test", Run: func(*cobra.Command, []string) {}}
	group := add(cmd)
	cmd.SetArgs(args)
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("parsing %v: %v", args, err)
	}
	return group
}

func TestKmerV1ParamsSendsOnlyWhatWasGiven(t *testing.T) {
	// Nothing but the two options carrying Perl defaults.
	o := run(t, AddKmerV1Flags, nil)
	want := genomeannotation.Params{"kmer_size": 8, "min_size": 48}
	if got := o.Params(); !reflect.DeepEqual(got, want) {
		t.Errorf("no flags: got %v, want %v", got, want)
	}

	// A flag left alone must be absent, not zero: min_hits=0 would mean
	// something quite different to the caller than "unset".
	o = run(t, AddKmerV1Flags, []string{"--min-hits", "5"})
	got := o.Params()
	if got["min_hits"] != 5 {
		t.Errorf("min_hits = %v, want 5", got["min_hits"])
	}
	for _, absent := range []string{"max_gap", "dataset_name", "score_threshold", "annotate_hypothetical_only"} {
		if _, ok := got[absent]; ok {
			t.Errorf("%s present (%v) though the flag was not given", absent, got[absent])
		}
	}
}

func TestKmerV1SendsMaxGap(t *testing.T) {
	// get_params_for_kmer_v1 asks for "max_gaps", which no option defines, so
	// the Perl drops a --max-gap the user typed. This is the divergence.
	o := run(t, AddKmerV1Flags, []string{"--max-gap", "3"})
	if got := o.Params()["max_gap"]; got != 3 {
		t.Errorf("max_gap = %v, want 3", got)
	}
}

func TestKmerV1ThresholdsTakeValues(t *testing.T) {
	// Perl declares these three with no type, making them booleans; the spec
	// types them int.
	o := run(t, AddKmerV1Flags, []string{
		"--score-threshold", "10",
		"--hit-threshold", "20",
		"--sequential-hit-threshold", "30",
	})
	got := o.Params()
	for key, want := range map[string]int{
		"score_threshold": 10, "hit_threshold": 20, "sequential_hit_threshold": 30,
	} {
		if got[key] != want {
			t.Errorf("%s = %v, want %d", key, got[key], want)
		}
	}
}

func TestBoolFlagsGoOnTheWireAsInts(t *testing.T) {
	o := run(t, AddKmerV2Flags, []string{"-H"})
	if got := o.Params()["annotate_hypothetical_only"]; got != 1 {
		t.Errorf("annotate_hypothetical_only = %v (%T), want 1 (int)", got, got)
	}
}

func TestKmerV2ParamsEmptyByDefault(t *testing.T) {
	o := run(t, AddKmerV2Flags, nil)
	if got := o.Params(); len(got) != 0 {
		t.Errorf("got %v, want an empty params object", got)
	}
}

func TestSimilarityParams(t *testing.T) {
	if got := run(t, AddSimilarityFlags, nil).Params(); len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
	if got := run(t, AddSimilarityFlags, []string{"--annotate-hypothetical-only"}).Params(); got["annotate_hypothetical_only"] != 1 {
		t.Errorf("got %v, want annotate_hypothetical_only=1", got)
	}
}

func TestGlimmer3ParamsAlwaysSendsItsDefault(t *testing.T) {
	if got := run(t, AddGlimmer3Flags, nil).Params()["min_training_len"]; got != 2000 {
		t.Errorf("min_training_len = %v, want 2000", got)
	}
	if got := run(t, AddGlimmer3Flags, []string{"--min-training-len", "500"}).Params()["min_training_len"]; got != 500 {
		t.Errorf("min_training_len = %v, want 500", got)
	}
}

func TestRepeatRegionParams(t *testing.T) {
	if got := run(t, AddRepeatRegionFlags, nil).Params(); len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
	got := run(t, AddRepeatRegionFlags, []string{"--min-identity", "95.5", "--min-length", "100"}).Params()
	if got["min_identity"] != 95.5 || got["min_length"] != 100 {
		t.Errorf("got %v", got)
	}
}

func TestGenomeMetadataRenamesGenomeID(t *testing.T) {
	got := run(t, AddGenomeMetadataFlags, []string{
		"--genome-id", "83333.1",
		"--scientific-name", "Escherichia coli",
		"--genetic-code", "11",
	}).Params()

	if got["id"] != "83333.1" {
		t.Errorf("id = %v, want 83333.1 (--genome-id goes on the wire as id)", got["id"])
	}
	if _, ok := got["genome_id"]; ok {
		t.Error("genome_id present; the service expects the key to be named id")
	}
	if got["scientific_name"] != "Escherichia coli" || got["genetic_code"] != 11 {
		t.Errorf("got %v", got)
	}
	if len(got) != 3 {
		t.Errorf("got %d keys (%v), want only the 3 that were given", len(got), got)
	}
}

func TestRRNATypes(t *testing.T) {
	tests := []struct {
		args []string
		want []string
	}{
		// Selecting nothing means everything, not nothing.
		{nil, []string{"ALL"}},
		{[]string{"--call-5S"}, []string{"5S"}},
		{[]string{"--call-SSU"}, []string{"SSU"}},
		{[]string{"--call-5S", "--call-SSU"}, []string{"5S", "SSU"}},
		// Order follows the Perl's qw(5S LSU SSU), not the command line.
		{[]string{"--call-SSU", "--call-LSU", "--call-5S"}, []string{"5S", "LSU", "SSU"}},
	}
	for _, tt := range tests {
		if got := run(t, AddRRNAFlags, tt.args).Types(); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%v: got %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestExportFormats(t *testing.T) {
	if len(ExportFormats) != 14 {
		t.Errorf("got %d formats, want the 14 CmdHelper lists", len(ExportFormats))
	}
	for _, name := range []string{"genbank", "seed_dir", "embl", "patric_genome_metadata"} {
		if !IsExportFormat(name) {
			t.Errorf("%s should be a supported format", name)
		}
	}
	if IsExportFormat("nonesuch") {
		t.Error("nonesuch should not be a supported format")
	}
}

func TestNormalizeExportFormat(t *testing.T) {
	for _, in := range []string{"seed_dir", "seed-dir", "SEED-DIR", "Seed_Dir"} {
		if got := NormalizeExportFormat(in); got != "seed_dir" {
			t.Errorf("NormalizeExportFormat(%q) = %q, want seed_dir", in, got)
		}
	}
}

func TestExportFormatHelpListsEveryFormat(t *testing.T) {
	help := ExportFormatHelp()
	for _, f := range ExportFormats {
		if !strings.Contains(help, f.Name) || !strings.Contains(help, f.Desc) {
			t.Errorf("help text is missing %s", f.Name)
		}
	}
}

func TestClassifierFull(t *testing.T) {
	if run(t, AddClassifierFlags, nil).Full() {
		t.Error("no output files should mean classify_into_bins")
	}
	if !run(t, AddClassifierFlags, []string{"-d", "/tmp/d"}).Full() {
		t.Error("-d should mean classify_full")
	}
	if !run(t, AddClassifierFlags, []string{"-u", "/tmp/u"}).Full() {
		t.Error("-u should mean classify_full")
	}
}
