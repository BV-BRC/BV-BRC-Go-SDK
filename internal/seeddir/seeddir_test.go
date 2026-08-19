package seeddir

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tinyGenome is a hand-built genome exercising every branch write_seed_dir has:
// a CDS with a translation, a CDS without one (so it is translated from the
// contig), a reverse-strand CDS, a non-coding feature (so its DNA is written
// raw), a two-part location, aliases, and an annotation whose comment already
// ends in a newline.
const tinyGenome = `{
  "id": "83333.1",
  "scientific_name": "Escherichia coli K-12",
  "genetic_code": 11,
  "ncbi_taxonomy_id": 83333,
  "taxonomy": ["cellular organisms", "Bacteria", "Proteobacteria"],
  "close_genomes": [
    {"genome_id": "83334.1", "closeness_measure": 0.995, "genome_name": "E. coli O157:H7"}
  ],
  "contigs": [
    {"id": "c1", "dna": "ATGAAACGCTAAGGGTTGCCCTTTAAACAT"}
  ],
  "features": [
    {
      "id": "83333.1.CDS.1",
      "type": "CDS",
      "function": "Translated protein",
      "aliases": ["gene1", "b0001"],
      "location": [["c1", "1", "+", 12]],
      "protein_translation": "MKR*",
      "annotations": [["Called by prodigal", "prodigal", 1600000000, "evt-1"]]
    },
    {
      "id": "83333.1.CDS.2",
      "type": "CDS",
      "location": [["c1", "1", "+", 12]],
      "annotations": [["comment already ends in a newline\n", "someone", 1600000001, "evt-2"]]
    },
    {
      "id": "83333.1.CDS.3",
      "type": "CDS",
      "function": "Reverse strand",
      "location": [["c1", "30", "-", 12]]
    },
    {
      "id": "83333.1.rna.1",
      "type": "rna",
      "function": "tRNA-Ala",
      "location": [["c1", "13", "+", 6], ["c1", "19", "+", 6]]
    }
  ]
}`

func writeTiny(t *testing.T) string {
	t.Helper()

	var g Genome
	if err := json.Unmarshal([]byte(tinyGenome), &g); err != nil {
		t.Fatalf("decoding the test genome: %v", err)
	}

	dir := filepath.Join(t.TempDir(), "seed")
	if err := Write(dir, &g, Options{MapCDSToPeg: true}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return dir
}

func read(t *testing.T, parts ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatalf("reading %s: %v", filepath.Join(parts...), err)
	}
	return string(data)
}

func TestWriteMetadata(t *testing.T) {
	dir := writeTiny(t)

	for _, tc := range []struct{ file, want string }{
		{"GENETIC_CODE", "11\n"},
		{"GENOME", "Escherichia coli K-12\n"},
		// The synthetic "cellular organisms" rank is dropped.
		{"TAXONOMY", "Bacteria; Proteobacteria\n"},
		{"TAXONOMY_ID", "83333\n"},
		{"contigs", ">c1\nATGAAACGCTAAGGGTTGCCCTTTAAACAT\n"},
		{"closest.genomes", "83334.1\t0.995\tE. coli O157:H7\n"},
	} {
		if got := read(t, dir, tc.file); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.file, got, tc.want)
		}
	}
}

func TestTaxonomyFallbacks(t *testing.T) {
	tests := []struct {
		name  string
		json  string
		want  string
		wrote bool
	}{
		{
			name:  "string form has its cellular prefix stripped",
			json:  `{"taxonomy": "cellular organisms; Bacteria; Firmicutes"}`,
			want:  "Bacteria; Firmicutes\n",
			wrote: true,
		},
		{
			name:  "string form without the prefix is passed through",
			json:  `{"taxonomy": "Viruses; Riboviria"}`,
			want:  "Viruses; Riboviria\n",
			wrote: true,
		},
		{
			name:  "ncbi_lineage is the fallback, first column only",
			json:  `{"ncbi_lineage": [["cellular organisms", 131567, "no rank"], ["Bacteria", 2, "superkingdom"]]}`,
			want:  "Bacteria\n",
			wrote: true,
		},
		{
			name:  "no taxonomy at all writes no file",
			json:  `{"scientific_name": "unknown"}`,
			wrote: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var g Genome
			if err := json.Unmarshal([]byte(tc.json), &g); err != nil {
				t.Fatalf("decoding: %v", err)
			}
			dir := filepath.Join(t.TempDir(), "seed")
			if err := Write(dir, &g, Options{MapCDSToPeg: true}); err != nil {
				t.Fatalf("Write: %v", err)
			}

			path := filepath.Join(dir, "TAXONOMY")
			data, err := os.ReadFile(path)
			switch {
			case !tc.wrote:
				if err == nil {
					t.Errorf("TAXONOMY was written as %q, want no file", data)
				}
			case err != nil:
				t.Fatalf("reading TAXONOMY: %v", err)
			case string(data) != tc.want:
				t.Errorf("TAXONOMY = %q, want %q", data, tc.want)
			}
		})
	}
}

func TestFeatureDirectoriesAndIDMapping(t *testing.T) {
	dir := writeTiny(t)

	// CDS became peg, in the directory name and inside the feature ids.
	if _, err := os.Stat(filepath.Join(dir, "Features", "CDS")); err == nil {
		t.Error("Features/CDS exists; map_CDS_to_peg should have named it peg")
	}

	wantTbl := strings.Join([]string{
		"83333.1.peg.1\tc1_1_12\tgene1\tb0001",
		"83333.1.peg.2\tc1_1_12",
		"83333.1.peg.3\tc1_30_19",
		"",
	}, "\n")
	if got := read(t, dir, "Features", "peg", "tbl"); got != wantTbl {
		t.Errorf("Features/peg/tbl =\n%q\nwant\n%q", got, wantTbl)
	}

	// A multi-part location joins with a comma; a non-CDS type keeps its name.
	wantRNA := "83333.1.rna.1\tc1_13_18,c1_19_24\n"
	if got := read(t, dir, "Features", "rna", "tbl"); got != wantRNA {
		t.Errorf("Features/rna/tbl = %q, want %q", got, wantRNA)
	}
}

func TestFeatureSequences(t *testing.T) {
	dir := writeTiny(t)

	// peg.1 has a translation and is written as-is. peg.2 has none, so its DNA
	// (ATGAAACGCTAA) is translated. peg.3 is on the minus strand: the DNA is
	// substr(dna, 30-12, 12) = "CCCTTTAAACAT" reverse-complemented to
	// "ATGTTTAAAGGG", which translates to MFKG.
	want := strings.Join([]string{
		">83333.1.peg.1", "MKR*",
		">83333.1.peg.2", "MKR*",
		">83333.1.peg.3", "MFKG",
		"",
	}, "\n")
	if got := read(t, dir, "Features", "peg", "fasta"); got != want {
		t.Errorf("Features/peg/fasta =\n%q\nwant\n%q", got, want)
	}

	// A non-coding feature gets its DNA, not a translation.
	wantRNA := ">83333.1.rna.1\nGGGTTGCCCTTT\n"
	if got := read(t, dir, "Features", "rna", "fasta"); got != wantRNA {
		t.Errorf("Features/rna/fasta = %q, want %q", got, wantRNA)
	}
}

func TestAssignedFunctions(t *testing.T) {
	dir := writeTiny(t)

	want := strings.Join([]string{
		"83333.1.peg.1\tTranslated protein",
		"83333.1.peg.2\thypothetical protein",
		"83333.1.peg.3\tReverse strand",
		"83333.1.rna.1\ttRNA-Ala",
		"",
	}, "\n")
	if got := read(t, dir, "assigned_functions"); got != want {
		t.Errorf("assigned_functions =\n%q\nwant\n%q", got, want)
	}
}

func TestAnnotations(t *testing.T) {
	dir := writeTiny(t)

	// Four lines per record -- id, time, annotator, comment -- then "//".
	// The second comment already ends in a newline and must not get another.
	want := strings.Join([]string{
		"83333.1.peg.1", "1600000000", "prodigal", "Called by prodigal", "//",
		"83333.1.peg.2", "1600000001", "someone", "comment already ends in a newline", "//",
		"",
	}, "\n")
	if got := read(t, dir, "annotations"); got != want {
		t.Errorf("annotations =\n%q\nwant\n%q", got, want)
	}
}

func TestAssignedFunctionsFileOption(t *testing.T) {
	var g Genome
	if err := json.Unmarshal([]byte(tinyGenome), &g); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "seed")
	if err := Write(dir, &g, Options{MapCDSToPeg: true, AssignedFunctionsFile: "proposed_functions"}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "proposed_functions")); err != nil {
		t.Errorf("proposed_functions: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "assigned_functions")); err == nil {
		t.Error("assigned_functions was written as well as proposed_functions")
	}
}

func TestTypemapDropsAndRenames(t *testing.T) {
	var g Genome
	if err := json.Unmarshal([]byte(tinyGenome), &g); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "seed")
	opts := Options{MapCDSToPeg: true, Typemap: map[string]string{"rna": ""}}
	if err := Write(dir, &g, opts); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "Features", "rna")); err == nil {
		t.Error("Features/rna exists; a type mapped to the empty string is dropped")
	}
	if got := read(t, dir, "assigned_functions"); strings.Contains(got, "rna") {
		t.Errorf("assigned_functions still mentions the dropped type:\n%s", got)
	}
}

func TestWriteWithoutFeaturesStillWritesTheSkeleton(t *testing.T) {
	var g Genome
	if err := json.Unmarshal([]byte(`{"scientific_name": "Nothing here", "genetic_code": 11}`), &g); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "seed")
	if err := Write(dir, &g, Options{MapCDSToPeg: true}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// map_CDS_to_peg adds peg to the type set whether or not the genome has
	// any, exactly as the Perl does.
	for _, name := range []string{"contigs", "GENOME", "assigned_functions", "annotations",
		filepath.Join("Features", "peg", "tbl"), filepath.Join("Features", "peg", "fasta")} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestUntranslatableGeneticCodeOnlyFailsWhenItIsNeeded(t *testing.T) {
	// A genome whose genetic_code is unsupported exports fine as long as every
	// protein-coding feature brought its own translation.
	const withTranslations = `{
      "genetic_code": 25,
      "contigs": [{"id": "c1", "dna": "ATGAAACGCTAA"}],
      "features": [{"id": "g.1.CDS.1", "type": "CDS", "location": [["c1", "1", "+", 12]],
                    "protein_translation": "MKR*"}]
    }`
	const without = `{
      "genetic_code": 25,
      "contigs": [{"id": "c1", "dna": "ATGAAACGCTAA"}],
      "features": [{"id": "g.1.CDS.1", "type": "CDS", "location": [["c1", "1", "+", 12]]}]
    }`

	for _, tc := range []struct {
		name    string
		genome  string
		wantErr bool
	}{
		{"translations supplied", withTranslations, false},
		{"translation needed", without, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g Genome
			if err := json.Unmarshal([]byte(tc.genome), &g); err != nil {
				t.Fatalf("decoding: %v", err)
			}
			err := Write(filepath.Join(t.TempDir(), "seed"), &g, Options{MapCDSToPeg: true})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Write error = %v, want error: %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "genetic code 25") {
				t.Errorf("error %q does not name the offending code", err)
			}
		})
	}
}

func TestFeatureDNAOffContigIsSkipped(t *testing.T) {
	const g = `{
      "genetic_code": 11,
      "contigs": [{"id": "c1", "dna": "ATGAAACGCTAA"}],
      "features": [{"id": "g.1.rna.1", "type": "rna", "location": [["nosuch", "1", "+", 6]]}]
    }`
	var genome Genome
	if err := json.Unmarshal([]byte(g), &genome); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "seed")
	if err := Write(dir, &genome, Options{MapCDSToPeg: true}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The feature is still tabulated; it just has no sequence.
	if got := read(t, dir, "Features", "rna", "tbl"); got != "g.1.rna.1\tnosuch_1_6\n" {
		t.Errorf("tbl = %q", got)
	}
	if got := read(t, dir, "Features", "rna", "fasta"); got != "" {
		t.Errorf("fasta = %q, want empty", got)
	}
}
