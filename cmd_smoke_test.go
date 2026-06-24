// Package p3tests: smoke tests for the newly-ported CLI commands.
//
// These tests run the actual compiled binaries as subprocesses and check that
// they return plausible output. They require:
//   - BVBRC_TEST_INTEGRATION=1 (skipped otherwise)
//   - Compiled binaries in ./bin/ (run `make` or build-linux.sh first)
//   - A valid BV-BRC token (~/.patric_token or P3_AUTH_TOKEN)
//
// Well-known stable test fixtures used throughout:
//   - Genome 511145.12  E. coli K-12 MG1655  (large, well-annotated)
//   - Genome 1313.7001  Streptococcus pneumoniae TIGR4
//   - Feature fig|511145.12.peg.1  first CDS of E. coli K-12
//   - Drug: penicillin
//   - Taxon: 1301 (Streptococcaceae)
package p3tests

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// binDir returns the path to the compiled binaries relative to this file.
func binDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine source file location")
	}
	return filepath.Join(filepath.Dir(file), "bin")
}

// runCmd runs a CLI binary with args and returns stdout.
// Fails the test if the binary exits non-zero.
func runCmd(t *testing.T, name string, args ...string) string {
	t.Helper()
	bin := filepath.Join(binDir(t), name)
	cmd := exec.Command(bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Stdin = strings.NewReader("") // prevent stdin reads blocking
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %v failed: %v\nstderr: %s\nstdout: %s",
			name, args, err, stderr.String(), stdout.String())
	}
	return stdout.String()
}

// runCmdWithStdin runs a CLI binary with args and provided stdin.
func runCmdWithStdin(t *testing.T, stdin, name string, args ...string) string {
	t.Helper()
	bin := filepath.Join(binDir(t), name)
	cmd := exec.Command(bin, args...)
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %v failed: %v\nstderr: %s\nstdout: %s",
			name, args, err, stderr.String(), stdout.String())
	}
	return stdout.String()
}

// checkTabOutput verifies tab-delimited output has at least minRows data rows
// and that the header contains all wantCols.
func checkTabOutput(t *testing.T, cmd, output string, minRows int, wantCols ...string) [][]string {
	t.Helper()
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	if len(lines) < 1 {
		t.Fatalf("%s: no output", cmd)
	}
	header := strings.Split(lines[0], "\t")
	headerSet := make(map[string]bool, len(header))
	for _, h := range header {
		headerSet[h] = true
	}
	for _, col := range wantCols {
		if !headerSet[col] {
			t.Errorf("%s: header missing column %q (got: %v)", cmd, col, header)
		}
	}
	dataRows := lines[1:]
	if len(dataRows) < minRows {
		t.Errorf("%s: got %d data rows, want at least %d", cmd, len(dataRows), minRows)
	}
	var rows [][]string
	for _, l := range dataRows {
		if l != "" {
			rows = append(rows, strings.Split(l, "\t"))
		}
	}
	return rows
}

func skipUnlessIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("BVBRC_TEST_INTEGRATION") == "" {
		t.Skip("Skipping integration test (set BVBRC_TEST_INTEGRATION=1 to run)")
	}
}

// -----------------------------------------------------------------------
// Category 1: p3-all-* enumeration commands
// -----------------------------------------------------------------------

func TestSmokeAllContigs(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-contigs",
		"--eq", "genome_id,511145.12", "--limit", "5",
		"-a", "sequence_id", "-a", "genome_id", "-a", "length")
	checkTabOutput(t, "p3-all-contigs", out, 1, "contig.sequence_id", "contig.genome_id", "contig.length")
}

func TestSmokeAllDrugs(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-drugs",
		"--eq", "antibiotic_name,penicillin",
		"-a", "antibiotic_name", "-a", "cas_id")
	rows := checkTabOutput(t, "p3-all-drugs", out, 1, "drug.antibiotic_name", "drug.cas_id")
	if rows[0][1] != "61-33-6" {
		t.Errorf("penicillin cas_id = %q, want 61-33-6", rows[0][1])
	}
}

func TestSmokeAllGenomeFeatures(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-genome-features",
		"--eq", "genome_id,11053.35", "--limit", "5",
		"-a", "patric_id", "-a", "product")
	checkTabOutput(t, "p3-all-genome-features", out, 1, "feature.patric_id")
}

func TestSmokeAllSfs(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-sfs", "--limit", "5", "-a", "sf_id", "-a", "sf_name")
	checkTabOutput(t, "p3-all-sfs", out, 1, "sf.sf_id")
}

func TestSmokeAllSfvts(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-sfvts", "--limit", "5", "-a", "sf_id", "-a", "sfvt_id")
	checkTabOutput(t, "p3-all-sfvts", out, 1, "sfvt.sf_id")
}

func TestSmokeAllSubsystems(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-subsystems", "--limit", "5",
		"-a", "subsystem_id", "-a", "subsystem_name")
	checkTabOutput(t, "p3-all-subsystems", out, 1, "subsystem.subsystem_id")
}

func TestSmokeAllSubsystemRoles(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-subsystem-roles", "--limit", "5", "-a", "subsystem_id")
	checkTabOutput(t, "p3-all-subsystem-roles", out, 1)
}

func TestSmokeAllTaxonomies(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-all-taxonomies",
		"--eq", "taxon_id,1301",
		"-a", "taxon_id", "-a", "taxon_name", "-a", "taxon_rank")
	rows := checkTabOutput(t, "p3-all-taxonomies", out, 1, "taxonomy.taxon_id", "taxonomy.taxon_name")
	if !strings.Contains(rows[0][1], "Streptococcaceae") {
		t.Errorf("taxon_name = %q, want to contain Streptococcaceae", rows[0][1])
	}
}

// -----------------------------------------------------------------------
// Category 2: p3-get-* keyed lookup commands
// -----------------------------------------------------------------------

const ecoli = "511145.12"
const spn = "1313.7001"
const ecoliPeg1 = "fig|511145.12.peg.1"

func genomeInput(genomeID string) string {
	return fmt.Sprintf("genome_id\n%s\n", genomeID)
}

func featureInput(featureID string) string {
	return fmt.Sprintf("patric_id\n%s\n", featureID)
}

func TestSmokeGetGenomeContigs(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-genome-contigs",
		"-a", "sequence_id", "-a", "length")
	checkTabOutput(t, "p3-get-genome-contigs", out, 1, "contig.sequence_id")
}

func TestSmokeGetGenomeDrugs(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(spn), "p3-get-genome-drugs",
		"--limit", "5", "-a", "antibiotic", "-a", "resistant_phenotype")
	checkTabOutput(t, "p3-get-genome-drugs", out, 1)
}

func TestSmokeGetGenomeSubsystems(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-genome-subsystems",
		"--limit", "5", "-a", "subsystem_name", "-a", "role_name")
	checkTabOutput(t, "p3-get-genome-subsystems", out, 1)
}

func TestSmokeGetGenomeRefseqFeatures(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-genome-refseq-features",
		"--limit", "5", "-a", "feature_id", "-a", "product")
	checkTabOutput(t, "p3-get-genome-refseq-features", out, 1)
}

func TestSmokeGetGenomeSpGenes(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-genome-sp-genes",
		"--limit", "5", "-a", "patric_id", "-a", "property")
	checkTabOutput(t, "p3-get-genome-sp-genes", out, 1)
}

func TestSmokeGetGenomeProteinRegions(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-genome-protein-regions",
		"--limit", "5", "-a", "patric_id", "-a", "source")
	checkTabOutput(t, "p3-get-genome-protein-regions", out, 1)
}

func TestSmokeGetGenomeProteinStructures(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-genome-protein-structures",
		"--limit", "5", "-a", "pdb_id", "-a", "gene")
	checkTabOutput(t, "p3-get-genome-protein-structures", out, 1)
}

func TestSmokeGetFeatureSubsystems(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, featureInput(ecoliPeg1), "p3-get-feature-subsystems",
		"--limit", "5", "-a", "subsystem_name")
	// Not all features have subsystem assignments — just verify it runs
	if out == "" {
		t.Errorf("p3-get-feature-subsystems: no output")
	}
}

func TestSmokeGetFeaturesInRegions(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, genomeInput(ecoli), "p3-get-features-in-regions",
		"--limit", "5", "-a", "patric_id", "-a", "product")
	checkTabOutput(t, "p3-get-features-in-regions", out, 1)
}

func TestSmokeGetTaxonomyData(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, "taxon_id\n1301\n", "p3-get-taxonomy-data",
		"-a", "taxon_name", "-a", "taxon_rank")
	rows := checkTabOutput(t, "p3-get-taxonomy-data", out, 1, "taxonomy.taxon_name")
	if !strings.Contains(rows[0][0], "Streptococcaceae") {
		t.Errorf("taxon_name = %q, want Streptococcaceae", rows[0][0])
	}
}

func TestSmokeGetDrugGenomes(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, "antibiotic_name\npenicillin\n", "p3-get-drug-genomes",
		"--limit", "5", "-a", "genome_id", "-a", "resistant_phenotype")
	checkTabOutput(t, "p3-get-drug-genomes", out, 1)
}

func TestSmokeGetSubsystemFeatures(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, "subsystem_id\nAmino_Acids_and_Derivatives:Histidine_Metabolism:Histidine_Biosynthesis\n",
		"p3-get-subsystem-features", "--limit", "5", "-a", "patric_id", "-a", "role_name")
	checkTabOutput(t, "p3-get-subsystem-features", out, 1)
}

// -----------------------------------------------------------------------
// Category 3: standalone find/query commands
// -----------------------------------------------------------------------

func TestSmokeFindGenomes(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-find-genomes",
		"--eq", "genome_id,511145.12",
		"-a", "genome_id", "-a", "genome_name")
	rows := checkTabOutput(t, "p3-find-genomes", out, 1, "genome.genome_id", "genome.genome_name")
	if !strings.Contains(rows[0][1], "Escherichia") {
		t.Errorf("genome_name = %q, want Escherichia", rows[0][1])
	}
}

func TestSmokeFindFeatures(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-find-features",
		"--eq", "genome_id,11053.35", "--limit", "5",
		"-a", "patric_id", "-a", "product")
	checkTabOutput(t, "p3-find-features", out, 1, "feature.patric_id")
}

func TestSmokeFindSurveillanceData(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-find-surveillance-data",
		"--limit", "5", "-a", "sample_identifier", "-a", "type")
	checkTabOutput(t, "p3-find-surveillance-data", out, 1)
}

func TestSmokeFindSerologyData(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-find-serology-data",
		"--limit", "5", "-a", "sample_identifier", "-a", "host_species")
	checkTabOutput(t, "p3-find-serology-data", out, 1)
}

func TestSmokeGenusSpecies(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmd(t, "p3-genus-species")
	checkTabOutput(t, "p3-genus-species", out, 10, "genus", "species", "count")
}

func TestSmokeRoleFeatures(t *testing.T) {
	skipUnlessIntegration(t)
	out := runCmdWithStdin(t, "product\nDNA gyrase subunit A\n", "p3-role-features",
		"--limit", "5", "-a", "patric_id", "-a", "genome_id")
	checkTabOutput(t, "p3-role-features", out, 1)
}

// -----------------------------------------------------------------------
// Category 4: tab-delimited manipulation (hermetic — no API)
// -----------------------------------------------------------------------

func TestSmokeCollate(t *testing.T) {
	input := "key\tvalue\na\t1\na\t2\na\t3\nb\t4\nb\t5\n"
	out := runCmdWithStdin(t, input, "p3-collate", "2", "--col", "key")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	// header + 2 'a' rows + 2 'b' rows = 5
	if len(lines) != 5 {
		t.Errorf("p3-collate: got %d lines, want 5\n%s", len(lines), out)
	}
}

func TestSmokeShuffle(t *testing.T) {
	input := "id\tname\n1\talpha\n2\tbeta\n3\tgamma\n"
	out := runCmdWithStdin(t, input, "p3-shuffle")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 4 { // header + 3 rows
		t.Errorf("p3-shuffle: got %d lines, want 4", len(lines))
	}
	if lines[0] != "id\tname" {
		t.Errorf("p3-shuffle: header = %q, want id\\tname", lines[0])
	}
}

func TestSmokeStats(t *testing.T) {
	// p3-stats: key-column groupName, value-column as positional param.
	// Usage: p3-stats [options] valueCol
	input := "id\tval\na\t10\nb\t20\nc\t30\n"
	out := runCmdWithStdin(t, input, "p3-stats", "val")
	if out == "" {
		t.Error("p3-stats: no output")
	}
}

func TestSmokePick(t *testing.T) {
	// p3-pick randomly selects N rows; positional arg is count.
	input := "id\tname\n1\talpha\n2\tbeta\n3\tgamma\n4\tdelta\n5\tepsilon\n"
	out := runCmdWithStdin(t, input, "p3-pick", "3")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	// header + 3 randomly selected rows
	if len(lines) != 4 {
		t.Errorf("p3-pick: got %d lines, want 4 (header+3)\n%s", len(lines), out)
	}
}

func TestSmokePickByClass(t *testing.T) {
	input := "id\tclass\tval\n1\tA\t10\n2\tB\t20\n3\tA\t30\n"
	out := runCmdWithStdin(t, input, "p3-pick-by-class", "--col", "class")
	// Just verify it runs and produces output
	if out == "" {
		t.Error("p3-pick-by-class: no output")
	}
}

func TestSmokeFileFilter(t *testing.T) {
	// p3-file-filter keeps input rows whose key column appears in a filter file.
	// Write the filter file listing allowed IDs.
	tmp, err := os.CreateTemp("", "p3filter*.tbl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	tmp.WriteString("id\n1\n3\n")
	tmp.Close()

	// Usage: p3-file-filter [options] filterFile filterCol1 ...
	// filterCol is 1-based index or name; use "1" for first column.
	input := "id\tval\n1\talpha\n2\tbeta\n3\tgamma\n"
	out := runCmdWithStdin(t, input, "p3-file-filter", "--col", "id", tmp.Name(), "id")
	if out == "" {
		t.Error("p3-file-filter: no output")
	}
	// Should have kept rows 1 and 3 (2 data rows + header)
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) < 3 {
		t.Errorf("p3-file-filter: got %d lines, want at least 3 (header+2 matches)\n%s", len(lines), out)
	}
}

func TestSmokeCompareColumns(t *testing.T) {
	// p3-compare-cols compares two columns and reports matches/mismatches.
	input := "a\tb\n1\t1\n2\t3\n4\t4\n"
	out := runCmdWithStdin(t, input, "p3-compare-cols", "a", "b")
	if out == "" {
		t.Error("p3-compare-cols: no output")
	}
}

func TestSmokeMerge(t *testing.T) {
	// p3-merge merges multiple tab-delimited files. Write two files and pass them.
	tmp1, err := os.CreateTemp("", "p3merge1*.tbl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp1.Name())
	tmp1.WriteString("id\tval\n1\talpha\n2\tbeta\n")
	tmp1.Close()

	tmp2, err := os.CreateTemp("", "p3merge2*.tbl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp2.Name())
	tmp2.WriteString("id\tval\n2\tbeta\n3\tgamma\n")
	tmp2.Close()

	// Usage: p3-merge [options] file1 file2 ... fileN  (union by default)
	out := runCmdWithStdin(t, "", "p3-merge", tmp1.Name(), tmp2.Name())
	if out == "" {
		t.Error("p3-merge: no output")
	}
}

func TestSmokeTblToFasta(t *testing.T) {
	input := "id\tseq\nseq1\tACGT\nseq2\tTTTT\n"
	out := runCmdWithStdin(t, input, "p3-tbl-to-fasta", "id", "seq")
	if !strings.Contains(out, ">seq1") || !strings.Contains(out, "ACGT") {
		t.Errorf("p3-tbl-to-fasta: unexpected output:\n%s", out)
	}
}

func TestSmokeFastaMd5(t *testing.T) {
	// p3-fasta-md5 reads a FASTA file and outputs sequence MD5s.
	// Output format is implementation-dependent; just verify it runs and produces output.
	fasta := ">seq1\nACGT\n>seq2\nTTTT\n"
	out := runCmdWithStdin(t, fasta, "p3-fasta-md5")
	if out == "" {
		t.Error("p3-fasta-md5: no output")
	}
	// Should contain the MD5 of ACGT
	if !strings.Contains(out, "8e017184dfbfafb3effceb81f085972d") {
		t.Errorf("p3-fasta-md5: output %q missing expected MD5 for ACGT", out)
	}
}

func TestSmokePivot(t *testing.T) {
	// Usage: p3-pivot [options] keyCol valueCol
	// Pivots a tab-delimited table by grouping on keyCol and spreading valueCol.
	input := "key\tfield\tval\na\tX\t1\na\tY\t2\nb\tX\t3\nb\tY\t4\n"
	out := runCmdWithStdin(t, input, "p3-pivot", "key", "field")
	if out == "" {
		t.Error("p3-pivot: no output")
	}
}

func TestSmokeTblToHtml(t *testing.T) {
	input := "name\tval\nalpha\t1\nbeta\t2\n"
	out := runCmdWithStdin(t, input, "p3-tbl-to-html")
	if !strings.Contains(out, "<table") || !strings.Contains(out, "alpha") {
		t.Errorf("p3-tbl-to-html: unexpected output:\n%s", out)
	}
}
