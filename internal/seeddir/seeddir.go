// Package seeddir writes a genome typed object out as a SEED directory.
//
// It is a port of GenomeTypeObject::write_seed_dir (p3_core/lib/GenomeTypeObject.pm),
// which is what rast-export-SEED is a front end for. Unlike the rest of the
// rast-* family this one talks to no service, so nothing downstream will catch
// a mistake here: the goal is output byte-identical to the Perl's, and the
// tests compare whole directories.
//
// write_seed_dir's correct_fig_id option is not ported. Nothing in this tree
// passes it, and the Perl's guard for it tests the feature id against a regex
// built from the pre-mapping type, which is not a behaviour worth preserving
// unexamined.
package seeddir

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/internal/seq"
)

// Genome is the part of a genome typed object a SEED directory is built from.
// Everything else in the object is ignored, which is why this is a narrow
// struct and not the whole schema.
type Genome struct {
	ScientificName string          `json:"scientific_name"`
	GeneticCode    Scalar          `json:"genetic_code"`
	NCBITaxonomyID Scalar          `json:"ncbi_taxonomy_id"`
	Taxonomy       json.RawMessage `json:"taxonomy"`
	NCBILineage    json.RawMessage `json:"ncbi_lineage"`
	Contigs        []Contig        `json:"contigs"`
	Features       []Feature       `json:"features"`
	CloseGenomes   []CloseGenome   `json:"close_genomes"`
}

// Contig is one input sequence.
type Contig struct {
	ID  string `json:"id"`
	DNA string `json:"dna"`
}

// Feature is one called feature.
type Feature struct {
	ID                 string     `json:"id"`
	Type               string     `json:"type"`
	Function           string     `json:"function"`
	Aliases            []string   `json:"aliases"`
	Location           [][]Scalar `json:"location"`
	ProteinTranslation string     `json:"protein_translation"`
	// Annotations are (comment, annotator, time, analysis event id) tuples.
	Annotations [][]Scalar `json:"annotations"`
}

// CloseGenome is one entry of the close_genomes list.
type CloseGenome struct {
	GenomeID         Scalar `json:"genome_id"`
	ClosenessMeasure Scalar `json:"closeness_measure"`
	GenomeName       Scalar `json:"genome_name"`
}

// Options are write_seed_dir's options hash.
type Options struct {
	// MapCDSToPeg renames the CDS feature type to peg, in the directory name,
	// in the type column and inside the feature ids.
	MapCDSToPeg bool
	// Typemap renames further types. A type mapped to the empty string is
	// dropped from the output entirely.
	Typemap map[string]string
	// AssignedFunctionsFile overrides the name of the assigned_functions file.
	AssignedFunctionsFile string
}

// Write builds the SEED directory for a genome under dir, which is created if
// it does not exist.
func Write(dir string, g *Genome, opts Options) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cannot mkdir %s: %w", dir, err)
	}

	if err := writeContigs(dir, g); err != nil {
		return err
	}
	if err := writeMetadata(dir, g); err != nil {
		return err
	}
	if err := writeCloseGenomes(dir, g); err != nil {
		return err
	}
	return writeFeatures(dir, g, opts)
}

func writeContigs(dir string, g *Genome) error {
	f, err := create(dir, "contigs")
	if err != nil {
		return err
	}
	for _, contig := range g.Contigs {
		if err := seq.WriteFasta(f, seq.Record{ID: contig.ID, Seq: contig.DNA}); err != nil {
			f.Close()
			return fmt.Errorf("writing %s: %w", f.Name(), err)
		}
	}
	return closeFile(f)
}

func writeMetadata(dir string, g *Genome) error {
	if err := writeLine(dir, "GENETIC_CODE", g.GeneticCode.String()); err != nil {
		return err
	}
	if err := writeLine(dir, "GENOME", g.ScientificName); err != nil {
		return err
	}

	if taxonomy, ok := taxonomyString(g); ok {
		if err := writeLine(dir, "TAXONOMY", taxonomy); err != nil {
			return err
		}
	}

	if g.NCBITaxonomyID.Truthy() {
		if err := writeLine(dir, "TAXONOMY_ID", g.NCBITaxonomyID.String()); err != nil {
			return err
		}
	}
	return nil
}

// leadingCellular is write_seed_dir's s/^cellular[^;]+;\s+// -- the synthetic
// "cellular organisms" rank NCBI puts at the head of a lineage.
var leadingCellular = regexp.MustCompile(`^cellular[^;]+;\s+`)

// taxonomyString picks the lineage to record, in write_seed_dir's order of
// preference: the taxonomy field as a list, as a string, and failing both the
// first column of ncbi_lineage.
func taxonomyString(g *Genome) (string, bool) {
	if list, ok := stringList(g.Taxonomy); ok {
		return strings.Join(dropCellular(list), "; "), true
	}

	var text string
	if json.Unmarshal(g.Taxonomy, &text) == nil && text != "" && text != "0" {
		return leadingCellular.ReplaceAllString(text, ""), true
	}

	var lineage [][]Scalar
	if json.Unmarshal(g.NCBILineage, &lineage) == nil && lineage != nil {
		names := make([]string, 0, len(lineage))
		for _, row := range lineage {
			if len(row) > 0 {
				names = append(names, row[0].String())
			}
		}
		return strings.Join(dropCellular(names), "; "), true
	}

	return "", false
}

func dropCellular(names []string) []string {
	if len(names) > 0 && strings.HasPrefix(names[0], "cellular") {
		return names[1:]
	}
	return names
}

func stringList(raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var list []string
	if json.Unmarshal(raw, &list) != nil || list == nil {
		return nil, false
	}
	return list, true
}

func writeCloseGenomes(dir string, g *Genome) error {
	if len(g.CloseGenomes) == 0 {
		return nil
	}

	f, err := create(dir, "closest.genomes")
	if err != nil {
		return err
	}
	for _, c := range g.CloseGenomes {
		line := strings.Join([]string{c.GenomeID.String(), c.ClosenessMeasure.String(), c.GenomeName.String()}, "\t")
		if _, err := fmt.Fprintln(f, line); err != nil {
			f.Close()
			return fmt.Errorf("writing %s: %w", f.Name(), err)
		}
	}
	return closeFile(f)
}

func writeFeatures(dir string, g *Genome, opts Options) error {
	typemap := buildTypemap(g, opts)

	functionsFile := opts.AssignedFunctionsFile
	if functionsFile == "" {
		functionsFile = "assigned_functions"
	}
	functions, err := create(dir, functionsFile)
	if err != nil {
		return err
	}
	defer functions.Close()

	annotations, err := create(dir, "annotations")
	if err != nil {
		return err
	}
	defer annotations.Close()

	files, err := openTypeFiles(dir, typemap)
	if err != nil {
		return err
	}
	defer files.closeAll()

	// The DNA of a feature with no protein_translation is cut out of its
	// contigs, so the contigs are indexed once up front.
	contigs := make(map[string]string, len(g.Contigs))
	for _, c := range g.Contigs {
		contigs[c.ID] = c.DNA
	}

	code, codeErr := geneticCodeFor(g)

	for _, feature := range g.Features {
		fid := feature.ID
		ftype := feature.Type

		if mapped, ok := typemap[ftype]; ok {
			if mapped == "" {
				continue
			}
			fid = strings.Replace(fid, "."+ftype+".", "."+mapped+".", 1)
			ftype = mapped
		}

		function := feature.Function
		if function == "" {
			function = "hypothetical protein"
		}
		if _, err := fmt.Fprintf(functions, "%s\t%s\n", fid, function); err != nil {
			return fmt.Errorf("writing %s: %w", functions.Name(), err)
		}

		location, err := SeedLocation(feature.Location)
		if err != nil {
			return fmt.Errorf("feature %s: %w", feature.ID, err)
		}

		out, ok := files.byType[ftype]
		if !ok {
			// Cannot happen: openTypeFiles is given the same typemap this loop
			// maps through. Report it rather than write nothing.
			return fmt.Errorf("feature %s has type %s, for which no output directory was made", feature.ID, ftype)
		}

		fields := append([]string{fid, location}, feature.Aliases...)
		if _, err := fmt.Fprintln(out.tbl, strings.Join(fields, "\t")); err != nil {
			return fmt.Errorf("writing %s: %w", out.tbl.Name(), err)
		}

		if err := writeFeatureSeq(out.fasta, fid, ftype, feature, contigs, code, codeErr); err != nil {
			return err
		}

		if err := writeAnnotations(annotations, fid, feature); err != nil {
			return err
		}
	}

	if err := files.closeAll(); err != nil {
		return err
	}
	if err := closeFile(annotations); err != nil {
		return err
	}
	return closeFile(functions)
}

// geneticCodeFor resolves the genome's translation table. Its error is
// deliberately not fatal here: the table is only consulted for a protein-coding
// feature that arrived with no translation, so a genome that has all of its
// translations exports fine whatever its genetic_code says.
func geneticCodeFor(g *Genome) (map[string]byte, error) {
	number, ok := g.GeneticCode.Int()
	if !ok {
		return nil, fmt.Errorf("genome has no usable genetic_code (%q)", g.GeneticCode.String())
	}
	return GeneticCode(number)
}

// writeFeatureSeq writes a feature's sequence: its protein translation when it
// has one, a translation of its DNA when it is protein-coding and does not, and
// its DNA otherwise. ftype is the mapped type, which is what the Perl's
// peg/CDS test looks at.
func writeFeatureSeq(out *os.File, fid, ftype string, feature Feature, contigs map[string]string, code map[string]byte, codeErr error) error {
	write := func(s string) error {
		if s == "" {
			return nil
		}
		if err := seq.WriteFasta(out, seq.Record{ID: fid, Seq: s}); err != nil {
			return fmt.Errorf("writing %s: %w", out.Name(), err)
		}
		return nil
	}

	if feature.ProteinTranslation != "" {
		return write(feature.ProteinTranslation)
	}

	dna := featureDNA(feature, contigs)
	if dna == "" {
		return nil
	}

	if ftype == "peg" || ftype == "CDS" {
		if codeErr != nil {
			return fmt.Errorf("feature %s has no protein_translation and cannot be translated: %w", feature.ID, codeErr)
		}
		return write(Translate(dna, code, true))
	}
	return write(dna)
}

// featureDNA cuts a feature's DNA out of its contigs, as
// GenomeTypeObject::get_feature_dna does. A location part on a contig the
// genome does not carry the DNA for contributes nothing rather than failing.
func featureDNA(feature Feature, contigs map[string]string) string {
	var b strings.Builder

	for _, part := range feature.Location {
		if len(part) < 4 {
			continue
		}
		dna, ok := contigs[part[0].String()]
		if !ok || dna == "" {
			continue
		}
		begin, ok := part[1].Int()
		if !ok {
			continue
		}
		length, ok := part[3].Int()
		if !ok {
			continue
		}

		if part[2].String() == "+" || length == 1 {
			b.WriteString(substr(dna, begin-1, length))
		} else {
			b.WriteString(ReverseComplement(substr(dna, begin-length, length)))
		}
	}

	return b.String()
}

// substr is Perl's, in the one respect that matters here: a run that goes off
// either end of the string yields what is actually there instead of panicking.
func substr(s string, offset, length int) string {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(s) || length <= 0 {
		return ""
	}
	end := offset + length
	if end > len(s) {
		end = len(s)
	}
	return s[offset:end]
}

// writeAnnotations appends a feature's annotations to the annotations file.
// Each is a four-line record -- id, time, annotator, comment -- terminated by a
// line holding "//".
func writeAnnotations(out *os.File, fid string, feature Feature) error {
	for _, anno := range feature.Annotations {
		var comment, annotator, when string
		if len(anno) > 0 {
			comment = anno[0].String()
		}
		if len(anno) > 1 {
			annotator = anno[1].String()
		}
		if len(anno) > 2 {
			when = anno[2].String()
		}

		record := strings.Join([]string{fid, when, annotator, comment}, "\n")
		if !strings.HasSuffix(comment, "\n") {
			record += "\n"
		}
		if _, err := fmt.Fprint(out, record+"//\n"); err != nil {
			return fmt.Errorf("writing %s: %w", out.Name(), err)
		}
	}
	return nil
}

// buildTypemap decides what each feature type is called on disk. Every type the
// genome uses maps to itself unless Typemap or MapCDSToPeg says otherwise.
func buildTypemap(g *Genome, opts Options) map[string]string {
	typemap := map[string]string{}
	for _, f := range g.Features {
		typemap[f.Type] = f.Type
	}
	if opts.MapCDSToPeg {
		delete(typemap, "CDS")
		typemap["peg"] = "peg"
	}
	for from, to := range opts.Typemap {
		typemap[from] = to
	}
	if opts.MapCDSToPeg {
		typemap["CDS"] = "peg"
	}
	return typemap
}

// featureFiles is the tbl and fasta file of one Features/<type> directory.
type featureFiles struct {
	tbl   *os.File
	fasta *os.File
}

// typeFiles holds them all. closeAll is idempotent so that it can be both
// deferred and called for its error.
type typeFiles struct {
	byType map[string]*featureFiles
}

func (t *typeFiles) closeAll() error {
	var first error
	for _, f := range t.byType {
		for _, h := range []**os.File{&f.tbl, &f.fasta} {
			if *h == nil {
				continue
			}
			err := closeFile(*h)
			*h = nil
			if err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func openTypeFiles(dir string, typemap map[string]string) (*typeFiles, error) {
	names := make([]string, 0, len(typemap))
	seen := map[string]bool{}
	for _, name := range typemap {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	// Only so that a failure reports the same directory every time.
	sort.Strings(names)

	featuresDir := filepath.Join(dir, "Features")
	if err := os.MkdirAll(featuresDir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot mkdir %s: %w", featuresDir, err)
	}

	files := &typeFiles{byType: map[string]*featureFiles{}}
	for _, name := range names {
		typeDir := filepath.Join(featuresDir, name)
		if err := os.MkdirAll(typeDir, 0o755); err != nil {
			files.closeAll()
			return nil, fmt.Errorf("cannot mkdir %s: %w", typeDir, err)
		}
		tbl, err := create(typeDir, "tbl")
		if err != nil {
			files.closeAll()
			return nil, err
		}
		fasta, err := create(typeDir, "fasta")
		if err != nil {
			tbl.Close()
			files.closeAll()
			return nil, err
		}
		files.byType[name] = &featureFiles{tbl: tbl, fasta: fasta}
	}
	return files, nil
}

func create(dir, name string) (*os.File, error) {
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("cannot create %s: %w", path, err)
	}
	return f, nil
}

// closeFile reports a close error against the file's name: on a full or remote
// filesystem this is where a failed write surfaces.
func closeFile(f *os.File) error {
	if err := f.Close(); err != nil {
		return fmt.Errorf("writing %s: %w", f.Name(), err)
	}
	return nil
}

func writeLine(dir, name, value string) error {
	f, err := create(dir, name)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(f, value); err != nil {
		f.Close()
		return fmt.Errorf("writing %s: %w", f.Name(), err)
	}
	return closeFile(f)
}
