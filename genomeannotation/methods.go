package genomeannotation

import (
	"encoding/json"
	"fmt"
)

// GTO is a genome typed object, carried verbatim. See the package comment for
// why it is not a struct.
type GTO = json.RawMessage

// Params is the parameter object an annotation step takes alongside the genome.
// Only the options the user actually gave appear in it: the Perl helpers in
// CmdHelper.pm copy a value only "if defined", and the service distinguishes an
// absent key from a zero one.
type Params map[string]interface{}

// --- steps that take only a genome -----------------------------------------

// The service names these methods in mixed case (call_features_CDS_prodigal,
// call_features_ProtoCDS_kmer_v1); the strings below are the wire names and
// must not be normalized.

func (c *Client) CallFeaturesCDSProdigal(g GTO) (GTO, error) {
	return c.step("call_features_CDS_prodigal", g)
}

func (c *Client) CallFeaturesCDSGenemark(g GTO) (GTO, error) {
	return c.step("call_features_CDS_genemark", g)
}

func (c *Client) CallFeaturesCrispr(g GTO) (GTO, error) {
	return c.step("call_features_crispr", g)
}

func (c *Client) CallFeaturesInsertionSequences(g GTO) (GTO, error) {
	return c.step("call_features_insertion_sequences", g)
}

func (c *Client) CallFeaturesProphagePhispy(g GTO) (GTO, error) {
	return c.step("call_features_prophage_phispy", g)
}

func (c *Client) CallFeaturesPyrrolysoprotein(g GTO) (GTO, error) {
	return c.step("call_features_pyrrolysoprotein", g)
}

func (c *Client) CallFeaturesSelenoprotein(g GTO) (GTO, error) {
	return c.step("call_features_selenoprotein", g)
}

func (c *Client) CallFeaturesStrepPneumoRepeat(g GTO) (GTO, error) {
	return c.step("call_features_strep_pneumo_repeat", g)
}

func (c *Client) CallFeaturesStrepSuisRepeat(g GTO) (GTO, error) {
	return c.step("call_features_strep_suis_repeat", g)
}

func (c *Client) CallFeaturesTRNATrnascan(g GTO) (GTO, error) {
	return c.step("call_features_tRNA_trnascan", g)
}

func (c *Client) AnnotateSpecialProteins(g GTO) (GTO, error) {
	return c.step("annotate_special_proteins", g)
}

func (c *Client) AnnotateFamiliesPatric(g GTO) (GTO, error) {
	return c.step("annotate_families_patric", g)
}

// --- steps that take a genome and a parameter object ------------------------

func (c *Client) AnnotateProteinsKmerV1(g GTO, p Params) (GTO, error) {
	return c.step("annotate_proteins_kmer_v1", g, params(p))
}

func (c *Client) AnnotateProteinsKmerV2(g GTO, p Params) (GTO, error) {
	return c.step("annotate_proteins_kmer_v2", g, params(p))
}

func (c *Client) AnnotateProteinsSimilarity(g GTO, p Params) (GTO, error) {
	return c.step("annotate_proteins_similarity", g, params(p))
}

func (c *Client) CallFeaturesProtoCDSKmerV1(g GTO, p Params) (GTO, error) {
	return c.step("call_features_ProtoCDS_kmer_v1", g, params(p))
}

func (c *Client) CallFeaturesProtoCDSKmerV2(g GTO, p Params) (GTO, error) {
	return c.step("call_features_ProtoCDS_kmer_v2", g, params(p))
}

func (c *Client) CallFeaturesCDSGlimmer3(g GTO, p Params) (GTO, error) {
	return c.step("call_features_CDS_glimmer3", g, params(p))
}

func (c *Client) CallFeaturesRepeatRegionSEED(g GTO, p Params) (GTO, error) {
	return c.step("call_features_repeat_region_SEED", g, params(p))
}

func (c *Client) ResolveOverlappingFeatures(g GTO, p Params) (GTO, error) {
	return c.step("resolve_overlapping_features", g, params(p))
}

func (c *Client) SetMetadata(g GTO, metadata Params) (GTO, error) {
	return c.step("set_metadata", g, params(metadata))
}

// CallFeaturesRRNASEED calls rRNA features. types is a list of 5S/LSU/SSU, or
// the single element "ALL" when the caller asked for none in particular.
func (c *Client) CallFeaturesRRNASEED(g GTO, types []string) (GTO, error) {
	if types == nil {
		types = []string{}
	}
	return c.step("call_features_rRNA_SEED", g, types)
}

// --- genome construction ----------------------------------------------------

// Contig is one entry of the list add_contigs takes.
type Contig struct {
	ID  string `json:"id"`
	DNA string `json:"dna"`
}

func (c *Client) AddContigs(g GTO, contigs []Contig) (GTO, error) {
	if contigs == nil {
		contigs = []Contig{}
	}
	return c.step("add_contigs", g, contigs)
}

// CompactFeature is the tabular form add_features takes: id, location, feature
// type, function, and a comma-separated alias string -- a positional tuple on
// the wire, not an object.
type CompactFeature struct {
	ID       string
	Location string
	Type     string
	Function string
	Aliases  string
}

// MarshalJSON encodes the feature as the 5-element tuple the service expects.
func (f CompactFeature) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{f.ID, f.Location, f.Type, f.Function, f.Aliases})
}

func (c *Client) AddFeatures(g GTO, features []CompactFeature) (GTO, error) {
	if features == nil {
		features = []CompactFeature{}
	}
	return c.step("add_features", g, features)
}

// FunctionAssignment pairs a feature id with its new function.
type FunctionAssignment struct {
	FeatureID string
	Function  string
}

// MarshalJSON encodes the assignment as the 2-element tuple the service expects.
func (a FunctionAssignment) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{a.FeatureID, a.Function})
}

// UpdateFunctions reassigns feature functions. event is the analysis_event
// recorded against the change; the rast-* CLI passes an empty one.
func (c *Client) UpdateFunctions(g GTO, functions []FunctionAssignment, event Params) (GTO, error) {
	if functions == nil {
		functions = []FunctionAssignment{}
	}
	return c.step("update_functions", g, functions, params(event))
}

func (c *Client) CreateGenome(metadata Params) (GTO, error) {
	return c.step("create_genome", params(metadata))
}

func (c *Client) CreateGenomeFromGenbank(data string) (GTO, error) {
	return c.step("create_genome_from_genbank", data)
}

func (c *Client) CreateGenomeFromRAST(genomeOrJobID string) (GTO, error) {
	return c.step("create_genome_from_RAST", genomeOrJobID)
}

// --- export -----------------------------------------------------------------

// ExportGenome renders a genome in one of the formats named by
// rastcli.ExportFormats. featureTypes limits the export to those types; an
// empty list means all of them.
func (c *Client) ExportGenome(g GTO, format string, featureTypes []string) (string, error) {
	if featureTypes == nil {
		featureTypes = []string{}
	}
	raw, err := c.call("export_genome", g, format, featureTypes)
	if err != nil {
		return "", err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("export_genome: decoding exported data: %w", err)
	}
	return s, nil
}

// --- specialty proteins -----------------------------------------------------

func (c *Client) EnumerateSpecialProteinDatabases() ([]string, error) {
	return c.stringList("enumerate_special_protein_databases")
}

// SpecialProteinHit is one row of compute_special_proteins output: the
// positional tuple (protein_id, database_name, database_id, protein_coverage,
// database_coverage, identity, p_value).
//
// The elements are kept as raw JSON so that the two numeric ones reach the
// output the way the service wrote them. Decoding them to float64 and printing
// them back would reformat, and a p-value is exactly where that shows.
type SpecialProteinHit []json.RawMessage

// Fields renders a hit for tab-delimited output: a JSON string loses its
// quotes, anything else is passed through verbatim.
func (h SpecialProteinHit) Fields() []string {
	fields := make([]string, 0, len(h))
	for _, raw := range h {
		text := string(raw)
		var s string
		if json.Unmarshal(raw, &s) == nil {
			text = s
		}
		fields = append(fields, text)
	}
	return fields
}

func (c *Client) ComputeSpecialProteins(g GTO, databases []string) ([]SpecialProteinHit, error) {
	if databases == nil {
		databases = []string{}
	}
	raw, err := c.call("compute_special_proteins", g, databases)
	if err != nil {
		return nil, err
	}
	var hits []SpecialProteinHit
	if err := json.Unmarshal(raw, &hits); err != nil {
		return nil, fmt.Errorf("compute_special_proteins: decoding results: %w", err)
	}
	return hits, nil
}

// --- classifiers ------------------------------------------------------------

func (c *Client) EnumerateClassifiers() ([]string, error) {
	return c.stringList("enumerate_classifiers")
}

// QueryClassifierGroups returns the genome ids in each of a classifier's groups.
func (c *Client) QueryClassifierGroups(classifier string) (map[string][]string, error) {
	raw, err := c.call("query_classifier_groups", classifier)
	if err != nil {
		return nil, err
	}
	var groups map[string][]string
	if err := json.Unmarshal(raw, &groups); err != nil {
		return nil, fmt.Errorf("query_classifier_groups: decoding groups: %w", err)
	}
	return groups, nil
}

// DNAInput is one sequence handed to a classifier: a (id, dna) tuple.
type DNAInput struct {
	ID  string
	DNA string
}

// MarshalJSON encodes the sequence as the 2-element tuple the service expects.
func (d DNAInput) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{d.ID, d.DNA})
}

// ClassifyIntoBins returns the per-group sequence counts.
func (c *Client) ClassifyIntoBins(classifier string, dna []DNAInput) (map[string]int, error) {
	if dna == nil {
		dna = []DNAInput{}
	}
	raw, err := c.call("classify_into_bins", classifier, dna)
	if err != nil {
		return nil, err
	}
	return decodeBins(raw, "classify_into_bins")
}

// ClassifyFull returns the per-group counts, the classifier's raw output, and
// the ids of the sequences it could not assign.
func (c *Client) ClassifyFull(classifier string, dna []DNAInput) (bins map[string]int, raw string, unassigned []string, err error) {
	if dna == nil {
		dna = []DNAInput{}
	}
	res, err := c.callN("classify_full", classifier, dna)
	if err != nil {
		return nil, "", nil, err
	}
	if len(res) < 3 {
		return nil, "", nil, fmt.Errorf("classify_full: expected 3 return values, got %d", len(res))
	}
	if bins, err = decodeBins(res[0], "classify_full"); err != nil {
		return nil, "", nil, err
	}
	if err = json.Unmarshal(res[1], &raw); err != nil {
		return nil, "", nil, fmt.Errorf("classify_full: decoding raw output: %w", err)
	}
	if err = json.Unmarshal(res[2], &unassigned); err != nil {
		return nil, "", nil, fmt.Errorf("classify_full: decoding unassigned list: %w", err)
	}
	return bins, raw, unassigned, nil
}

func decodeBins(raw json.RawMessage, method string) (map[string]int, error) {
	var bins map[string]int
	if err := json.Unmarshal(raw, &bins); err != nil {
		return nil, fmt.Errorf("%s: decoding bins: %w", method, err)
	}
	return bins, nil
}

// --- workflows and batches --------------------------------------------------

// DefaultWorkflow returns the service's default annotation workflow, verbatim,
// so it can be saved, edited, and handed back to RunPipeline.
func (c *Client) DefaultWorkflow() (json.RawMessage, error) {
	return c.call("default_workflow")
}

// RunPipeline runs a whole workflow against a genome in one call.
func (c *Client) RunPipeline(g GTO, workflow json.RawMessage) (GTO, error) {
	return c.step("run_pipeline", g, workflow)
}

// PipelineBatchStatus returns the status document for a submitted batch.
func (c *Client) PipelineBatchStatus(batchID string) (json.RawMessage, error) {
	return c.call("pipeline_batch_status", batchID)
}

// BatchSummary is one row of pipeline_batch_enumerate_batches: a batch id and
// its submission time.
type BatchSummary struct {
	ID         string
	SubmitTime string
}

// PipelineBatchEnumerateBatches lists the calling user's submitted batches.
func (c *Client) PipelineBatchEnumerateBatches() ([]BatchSummary, error) {
	raw, err := c.call("pipeline_batch_enumerate_batches")
	if err != nil {
		return nil, err
	}
	var tuples [][]string
	if err := json.Unmarshal(raw, &tuples); err != nil {
		return nil, fmt.Errorf("pipeline_batch_enumerate_batches: decoding batches: %w", err)
	}
	batches := make([]BatchSummary, 0, len(tuples))
	for _, t := range tuples {
		var b BatchSummary
		if len(t) > 0 {
			b.ID = t[0]
		}
		if len(t) > 1 {
			b.SubmitTime = t[1]
		}
		batches = append(batches, b)
	}
	return batches, nil
}

// --- helpers ----------------------------------------------------------------

// step is the shape almost every annotation method has: hand over a genome,
// get a genome back.
func (c *Client) step(method string, args ...interface{}) (GTO, error) {
	return c.call(method, args...)
}

// params makes sure an absent parameter object goes out as {} rather than null:
// the service's Perl side dereferences it without checking.
func params(p Params) Params {
	if p == nil {
		return Params{}
	}
	return p
}

func (c *Client) stringList(method string) ([]string, error) {
	raw, err := c.call(method)
	if err != nil {
		return nil, err
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("%s: decoding list: %w", method, err)
	}
	return list, nil
}
