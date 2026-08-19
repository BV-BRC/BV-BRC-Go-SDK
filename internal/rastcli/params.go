package rastcli

import (
	"fmt"
	"strings"

	"github.com/BV-BRC/BV-BRC-Go-SDK/genomeannotation"
	"github.com/spf13/cobra"
)

// The option groups below are CmdHelper.pm's options_* subroutines paired with
// its get_params_for_* subroutines.
//
// The pairing matters in one specific way: get_params_for_* copies a value only
// "if defined", so a flag the user did not give must be absent from the
// parameter object, not present as a zero. Every Params method here therefore
// asks cobra whether the flag was Changed rather than reading its value
// unconditionally -- a --min-hits the user never typed must not arrive at the
// service as 0.

// setIfChanged copies a flag into the parameter object only when the user gave it.
func setIfChanged(p genomeannotation.Params, cmd *cobra.Command, name, key string, value interface{}) {
	if cmd.Flags().Changed(name) {
		p[key] = value
	}
}

// setBoolIfChanged is setIfChanged for the flags the spec declares as int.
// Getopt gives Perl a 1, and JSON::XS puts a bare 1 on the wire; a Go bool would
// send true instead. The service is unlikely to care, but there is no reason to
// be the one client that sends a different type than the spec asks for.
func setBoolIfChanged(p genomeannotation.Params, cmd *cobra.Command, name, key string, value bool) {
	if !cmd.Flags().Changed(name) {
		return
	}
	if value {
		p[key] = 1
	} else {
		p[key] = 0
	}
}

// --- kmer v1 (options_kmer_v1 / get_params_for_kmer_v1) ---------------------

// KmerV1 holds the kmer-v1 annotation options.
type KmerV1 struct {
	cmd *cobra.Command

	KmerSize               int
	DatasetName            string
	ScoreThreshold         int
	HitThreshold           int
	SequentialHitThreshold int
	MinHits                int
	MaxGap                 int
	MinSize                int
	HypotheticalOnly       bool
}

// AddKmerV1Flags registers the kmer-v1 option group.
func AddKmerV1Flags(cmd *cobra.Command) *KmerV1 {
	o := &KmerV1{cmd: cmd}
	f := cmd.Flags()
	f.IntVar(&o.KmerSize, "kmer-size", 8, "kmer size")
	f.StringVar(&o.DatasetName, "dataset-name", "", "kmer dataset name")
	f.IntVar(&o.ScoreThreshold, "score-threshold", 0, "score threshold")
	f.IntVar(&o.HitThreshold, "hit-threshold", 0, "hit threshold")
	f.IntVar(&o.SequentialHitThreshold, "sequential-hit-threshold", 0, "sequential-hit threshold")
	f.IntVar(&o.MinHits, "min-hits", 0, "minimum number of Kmer hits required for a call to be made")
	f.IntVar(&o.MaxGap, "max-gap", 0, "maximum size of a gap allowed for a call to be made")
	f.IntVar(&o.MinSize, "min-size", 48, "minimum size of DNA feature to call")
	f.BoolVarP(&o.HypotheticalOnly, "annotate-hypothetical-only", "H", false, "only annotate features tagged as hypothetical protein")
	return o
}

// Params builds the kmer_v1_parameters object.
//
// kmer-size and min-size carry Perl defaults (8 and 48) and so are always sent;
// the rest go only when given.
//
// Two knowing divergences from get_params_for_kmer_v1, both because it copies
// the wrong key: its list contains "max_gaps", which no option defines, so
// Perl silently drops the --max-gap the user typed. We send it under the name
// the spec gives it, max_gap. And Perl declares --score-threshold,
// --hit-threshold and --sequential-hit-threshold with no type, making them
// booleans that can only ever send 1; the spec types all three as int, so they
// take a value here.
func (o *KmerV1) Params() genomeannotation.Params {
	p := genomeannotation.Params{
		"kmer_size": o.KmerSize,
		"min_size":  o.MinSize,
	}
	setIfChanged(p, o.cmd, "dataset-name", "dataset_name", o.DatasetName)
	setIfChanged(p, o.cmd, "score-threshold", "score_threshold", o.ScoreThreshold)
	setIfChanged(p, o.cmd, "hit-threshold", "hit_threshold", o.HitThreshold)
	setIfChanged(p, o.cmd, "sequential-hit-threshold", "sequential_hit_threshold", o.SequentialHitThreshold)
	setIfChanged(p, o.cmd, "min-hits", "min_hits", o.MinHits)
	setIfChanged(p, o.cmd, "max-gap", "max_gap", o.MaxGap)
	setBoolIfChanged(p, o.cmd, "annotate-hypothetical-only", "annotate_hypothetical_only", o.HypotheticalOnly)
	return p
}

// --- kmer v2 (options_kmer_v2 / get_params_for_kmer_v2) ---------------------

// KmerV2 holds the kmer-v2 annotation options.
type KmerV2 struct {
	cmd *cobra.Command

	MinHits          int
	MaxGap           int
	HypotheticalOnly bool
}

// AddKmerV2Flags registers the kmer-v2 option group.
func AddKmerV2Flags(cmd *cobra.Command) *KmerV2 {
	o := &KmerV2{cmd: cmd}
	f := cmd.Flags()
	f.IntVar(&o.MinHits, "min-hits", 0, "minimum number of Kmer hits required for a call to be made")
	f.IntVar(&o.MaxGap, "max-gap", 0, "maximum size of a gap allowed for a call to be made")
	f.BoolVarP(&o.HypotheticalOnly, "annotate-hypothetical-only", "H", false, "only annotate features tagged as hypothetical protein")
	return o
}

// Params builds the kmer_v2_parameters object.
func (o *KmerV2) Params() genomeannotation.Params {
	p := genomeannotation.Params{}
	setIfChanged(p, o.cmd, "min-hits", "min_hits", o.MinHits)
	setIfChanged(p, o.cmd, "max-gap", "max_gap", o.MaxGap)
	setBoolIfChanged(p, o.cmd, "annotate-hypothetical-only", "annotate_hypothetical_only", o.HypotheticalOnly)
	return p
}

// --- similarity (options_similarity / get_params_for_similarity) ------------

// Similarity holds the similarity-annotation options.
type Similarity struct {
	cmd *cobra.Command

	HypotheticalOnly bool
}

// AddSimilarityFlags registers the similarity option group.
func AddSimilarityFlags(cmd *cobra.Command) *Similarity {
	o := &Similarity{cmd: cmd}
	cmd.Flags().BoolVarP(&o.HypotheticalOnly, "annotate-hypothetical-only", "H", false,
		"only annotate features tagged as hypothetical protein")
	return o
}

// Params builds the similarity_parameters object.
func (o *Similarity) Params() genomeannotation.Params {
	p := genomeannotation.Params{}
	setBoolIfChanged(p, o.cmd, "annotate-hypothetical-only", "annotate_hypothetical_only", o.HypotheticalOnly)
	return p
}

// --- glimmer3 (options_glimmer3 / get_params_for_glimmer3) ------------------

// Glimmer3 holds the glimmer3 gene-calling options.
type Glimmer3 struct {
	MinTrainingLen int
}

// AddGlimmer3Flags registers the glimmer3 option group.
func AddGlimmer3Flags(cmd *cobra.Command) *Glimmer3 {
	o := &Glimmer3{}
	cmd.Flags().IntVar(&o.MinTrainingLen, "min-training-len", 2000,
		"Minimum size of a contig to be used for training glimmer3")
	return o
}

// Params builds the glimmer3_parameters object. min_training_len has a Perl
// default and so is always sent.
func (o *Glimmer3) Params() genomeannotation.Params {
	return genomeannotation.Params{"min_training_len": o.MinTrainingLen}
}

// --- repeat regions (options_repeat_regions_seed) ---------------------------

// RepeatRegion holds the repeat-region-SEED options.
type RepeatRegion struct {
	cmd *cobra.Command

	MinIdentity float64
	MinLength   int
}

// AddRepeatRegionFlags registers the repeat-region option group.
func AddRepeatRegionFlags(cmd *cobra.Command) *RepeatRegion {
	o := &RepeatRegion{cmd: cmd}
	f := cmd.Flags()
	f.Float64Var(&o.MinIdentity, "min-identity", 0, "minimum BLAST identity")
	f.IntVar(&o.MinLength, "min-length", 0, "minimum length")
	return o
}

// Params builds the repeat_region_SEED_parameters object.
func (o *RepeatRegion) Params() genomeannotation.Params {
	p := genomeannotation.Params{}
	setIfChanged(p, o.cmd, "min-identity", "min_identity", o.MinIdentity)
	setIfChanged(p, o.cmd, "min-length", "min_length", o.MinLength)
	return p
}

// --- rRNA (options_rrna_seed) -----------------------------------------------

// RRNA holds the rRNA-SEED type selection.
type RRNA struct {
	Call5S  bool
	CallSSU bool
	CallLSU bool
}

// AddRRNAFlags registers the rRNA option group.
func AddRRNAFlags(cmd *cobra.Command) *RRNA {
	o := &RRNA{}
	f := cmd.Flags()
	f.BoolVar(&o.Call5S, "call-5S", false, "Call 5S RNA features")
	f.BoolVar(&o.CallSSU, "call-SSU", false, "Call SSU RNA features")
	f.BoolVar(&o.CallLSU, "call-LSU", false, "Call LSU RNA features")
	return o
}

// Types returns the rna_type list to send. Asking for nothing in particular
// means ALL, not nothing -- an empty list would call no features at all.
// The order matches the Perl's qw(5S LSU SSU).
func (o *RRNA) Types() []string {
	var types []string
	if o.Call5S {
		types = append(types, "5S")
	}
	if o.CallLSU {
		types = append(types, "LSU")
	}
	if o.CallSSU {
		types = append(types, "SSU")
	}
	if len(types) == 0 {
		return []string{"ALL"}
	}
	return types
}

// --- genome metadata (options_genome_metadata / get_params_for_genome_metadata) ---

// GenomeMetadata holds the genome_metadata option group.
type GenomeMetadata struct {
	cmd *cobra.Command

	GenomeID       string
	ScientificName string
	Domain         string
	GeneticCode    int
	NCBITaxonomyID int
	Source         string
	SourceID       string
}

// AddGenomeMetadataFlags registers the genome-metadata option group.
func AddGenomeMetadataFlags(cmd *cobra.Command) *GenomeMetadata {
	o := &GenomeMetadata{cmd: cmd}
	f := cmd.Flags()
	f.StringVar(&o.GenomeID, "genome-id", "", "Genome identifier")
	f.StringVar(&o.ScientificName, "scientific-name", "", "Scientific name (Genus species strain) for the genome")
	f.StringVar(&o.Domain, "domain", "", "Domain (Bacteria/Archaea/Virus/Eukaryota) for the genome")
	f.IntVar(&o.GeneticCode, "genetic-code", 0, "Genetic code for the genome (probably 11 or 4 for bacterial genomes)")
	f.IntVar(&o.NCBITaxonomyID, "ncbi-taxonomy-id", 0, "NCBI taxonomy identifier for the genome")
	f.StringVar(&o.Source, "source", "", "Source (external database) name for this genome")
	f.StringVar(&o.SourceID, "source-id", "", "Identifier for this genome in the source (external database)")
	return o
}

// Params builds the genome_metadata object. Note the rename: the --genome-id
// flag goes on the wire as "id", exactly as get_params_for_genome_metadata does.
func (o *GenomeMetadata) Params() genomeannotation.Params {
	p := genomeannotation.Params{}
	setIfChanged(p, o.cmd, "genome-id", "id", o.GenomeID)
	setIfChanged(p, o.cmd, "scientific-name", "scientific_name", o.ScientificName)
	setIfChanged(p, o.cmd, "domain", "domain", o.Domain)
	setIfChanged(p, o.cmd, "genetic-code", "genetic_code", o.GeneticCode)
	setIfChanged(p, o.cmd, "ncbi-taxonomy-id", "ncbi_taxonomy_id", o.NCBITaxonomyID)
	setIfChanged(p, o.cmd, "source", "source", o.Source)
	setIfChanged(p, o.cmd, "source-id", "source_id", o.SourceID)
	return p
}

// --- export (options_export, @export_formats) -------------------------------

// ExportFormat is one entry of CmdHelper's @export_formats.
type ExportFormat struct {
	Name string
	Desc string
}

// ExportFormats is the list rast-export-genome accepts.
//
// Like the Perl list it is a local copy of the back end's rast_export.pl, kept
// up to date by hand: the format list changes slowly, and a round trip to the
// service just to validate an argument is not worth it.
var ExportFormats = []ExportFormat{
	{"genbank", "Genbank format"},
	{"genbank_merged", "Genbank format as single merged locus, suitable for Artemis"},
	{"spreadsheet_txt", "RAST-style spreadsheet (tab-separated text format)"},
	{"spreadsheet_xls", "RAST-style spreadsheet (Excel XLS format)"},
	{"feature_data", "Tabular form of feature data"},
	{"protein_fasta", "Protein translations in fasta format"},
	{"contig_fasta", "Contig DNA in fasta format"},
	{"feature_dna", "Feature DNA sequences in fasta format"},
	{"seed_dir", "SEED directory"},
	{"patric_features", "PATRIC features.tab format"},
	{"patric_specialty_genes", "PATRIC spgenes.tab format"},
	{"patric_genome_metadata", "PATRIC genome metadata format"},
	{"gff", "GFF format"},
	{"embl", "EMBL format"},
}

// IsExportFormat reports whether name is a supported export format.
func IsExportFormat(name string) bool {
	for _, f := range ExportFormats {
		if f.Name == name {
			return true
		}
	}
	return false
}

// ExportFormatNames lists the format names, for the message an unrecognized
// format produces.
func ExportFormatNames() string {
	names := make([]string, 0, len(ExportFormats))
	for _, f := range ExportFormats {
		names = append(names, f.Name)
	}
	return strings.Join(names, " ")
}

// ExportFormatHelp renders the format table for a command's help text, the way
// options_export_formats does.
func ExportFormatHelp() string {
	width := 0
	for _, f := range ExportFormats {
		if len(f.Name) > width {
			width = len(f.Name)
		}
	}

	var b strings.Builder
	b.WriteString("Supported formats:\n")
	for _, f := range ExportFormats {
		fmt.Fprintf(&b, "  %-*s %s\n", width+1, f.Name, f.Desc)
	}
	return b.String()
}

// NormalizeExportFormat applies the spelling rast-export-genome accepts:
// case-insensitive, with dashes standing in for underscores.
func NormalizeExportFormat(format string) string {
	return strings.ReplaceAll(strings.ToLower(format), "-", "_")
}

// AddExportFlags registers --feature-type (repeatable).
func AddExportFlags(cmd *cobra.Command) *[]string {
	types := []string{}
	cmd.Flags().StringArrayVar(&types, "feature-type", nil,
		"Include this feature type in output. If no feature-types specified, include all feature types")
	return &types
}

// --- classifier (options_classifier) ----------------------------------------

// Classifier holds the classifier output options.
type Classifier struct {
	DetailedOutputFile     string
	UnclassifiedOutputFile string
}

// AddClassifierFlags registers -d/--detailed-output-file and
// -u/--unclassified-output-file.
func AddClassifierFlags(cmd *cobra.Command) *Classifier {
	o := &Classifier{}
	f := cmd.Flags()
	f.StringVarP(&o.DetailedOutputFile, "detailed-output-file", "d", "",
		"File to write detailed output (reads and hit information)")
	f.StringVarP(&o.UnclassifiedOutputFile, "unclassified-output-file", "u", "",
		"File to write unclassified read IDs to")
	return o
}

// Full reports whether either output file was asked for, which is what makes
// rast-classify use classify_full instead of classify_into_bins.
func (o *Classifier) Full() bool {
	return o.DetailedOutputFile != "" || o.UnclassifiedOutputFile != ""
}

// --- contigs and workflow (options_contigs, options_workflow_specification) ---

// AddContigsFlag registers --contigs.
func AddContigsFlag(cmd *cobra.Command) *string {
	var path string
	cmd.Flags().StringVar(&path, "contigs", "", "Fasta file containing DNA contig data")
	return &path
}

// AddWorkflowFlag registers --workflow.
func AddWorkflowFlag(cmd *cobra.Command) *string {
	var path string
	cmd.Flags().StringVar(&path, "workflow", "", "File containing genome processing workflow specification")
	return &path
}
