"""Reverse-engineer p3-submit-* CLI invocations from app-parameter JSON.

Each inverter takes a params dict (the app payload from a QA fixture) and returns
`(tokens, unsupported)`:

  * tokens      -- a structured arg list rendered per CLI dialect (see render.py).
  * unsupported -- reference param keys the CLI front-ends cannot express. A
                   non-empty list means the fixture is a CLI coverage gap and the
                   runner marks it UNSUPPORTED rather than executing it.

The inverter targets the Go front-end's option set (the more limited of the two),
because the suite feeds the SAME logical invocation to both Go and Perl and checks
they emit identical params. Go flag long-names mirror the Perl ones; the only known
dialect difference is paired-end-lib (Go `f1,f2` vs Perl `f1 f2`), handled by the
("paired", ...) token.

Token forms:
  ("opt", flag, value)   -> [flag, value]            (both dialects)
  ("flag", flag)         -> [flag]                    (boolean switch)
  ("paired", f1, f2)     -> Go: [flag, "f1,f2"]; Perl: [flag, f1, f2]
  ("single", f)          -> ["--single-end-lib", f]
  ("srr", id)            -> ["--srr-id", id]
"""

# Keys that are test-harness metadata, not app params. Stripped before inversion.
METADATA_KEYS = {
    "failure_expected", "why_failure", "debug_level", "comment", "notes",
    "skip", "label_test",
}


def is_empty(v):
    return v is None or v == "" or v == [] or v == {}


def ws(path):
    """Render a workspace input path for the CLI: absolute paths get a ws: prefix
    so the tool treats them as existing workspace objects (no upload)."""
    if isinstance(path, str) and path.startswith("/"):
        return "ws:" + path
    return path


def _csv(value):
    if isinstance(value, list):
        return ",".join(str(x) for x in value)
    return str(value)


# ---------------------------------------------------------------------------
# Standard read-library handling (paired_end_libs / single_end_libs /
# srr_ids / srr_libs). The Go tools support only read1/read2, read, and the SRA
# accession; any other non-empty per-library field (platform, condition,
# read_orientation_outward, sample_id, ...) cannot be expressed -> unsupported.
# ---------------------------------------------------------------------------
def handle_reads(params):
    tokens, unsupported, consumed = [], [], set()

    if params.get("paired_end_libs"):
        consumed.add("paired_end_libs")
        for lib in params["paired_end_libs"]:
            if not isinstance(lib, dict):
                unsupported.append("paired_end_libs:non-object")
                continue
            extra = {k for k in lib if k not in ("read1", "read2") and not is_empty(lib[k])}
            if extra:
                unsupported.append("paired_end_libs:" + ",".join(sorted(extra)))
            if "read1" in lib and "read2" in lib:
                tokens.append(("paired", ws(lib["read1"]), ws(lib["read2"])))
            else:
                unsupported.append("paired_end_libs:incomplete")

    if params.get("single_end_libs"):
        consumed.add("single_end_libs")
        for lib in params["single_end_libs"]:
            if not isinstance(lib, dict):
                unsupported.append("single_end_libs:non-object")
                continue
            extra = {k for k in lib if k != "read" and not is_empty(lib[k])}
            if extra:
                unsupported.append("single_end_libs:" + ",".join(sorted(extra)))
            if "read" in lib:
                tokens.append(("single", ws(lib["read"])))
            else:
                unsupported.append("single_end_libs:incomplete")

    if params.get("srr_ids"):
        consumed.add("srr_ids")
        for sid in params["srr_ids"]:
            tokens.append(("srr", str(sid)))

    if params.get("srr_libs"):
        consumed.add("srr_libs")
        for lib in params["srr_libs"]:
            if isinstance(lib, str):
                tokens.append(("srr", lib))
                continue
            extra = {k for k in lib if k != "srr_accession" and not is_empty(lib[k])}
            if extra:
                unsupported.append("srr_libs:" + ",".join(sorted(extra)))
            if "srr_accession" in lib:
                tokens.append(("srr", str(lib["srr_accession"])))
            else:
                unsupported.append("srr_libs:incomplete")

    return tokens, unsupported, consumed


def _emit_flag(kind, flag, value):
    """Render a single declarative flag rule to tokens."""
    if kind == "bool":
        return [("flag", flag)] if value else []
    if kind == "csv":
        return [("opt", flag, _csv(value))]
    if kind == "wsfile":
        return [("opt", flag, ws(value))]
    # str / int / float
    return [("opt", flag, str(value))]


# Declarative per-command flag maps: param_key -> (kind, flag).
# kind in {str, int, float, bool, csv, wsfile}.
FLAGS = {
    "p3-submit-fastqutils": {
        "reference_genome_id": ("str", "--reference-genome-id"),
    },
    "p3-submit-variation-analysis": {
        "reference_genome_id": ("str", "--reference-genome-id"),
        "mapper": ("str", "--mapper"),
        "caller": ("str", "--caller"),
    },
    "p3-submit-codon-tree": {
        "genome_ids": ("csv", "--genome-ids"),
        "number_of_genes": ("int", "--number-of-genes"),
        "max_genomes_missing": ("int", "--max-genomes-missing"),
        "max_allowed_dups": ("int", "--max-allowed-dups"),
    },
    "p3-submit-gene-tree": {
        "trim_threshold": ("float", "--trim-threshold"),
        "gap_threshold": ("float", "--gap-threshold"),
        "substitution_model": ("str", "--substitution-model"),
        "recipe": ("str", "--recipe"),
    },
    "p3-submit-genome-assembly": {
        "recipe": ("str", "--recipe"),
        "pipeline": ("str", "--pipeline"),
        "racon_iter": ("int", "--racon-iter"),
        "pilon_iter": ("int", "--pilon-iter"),
        "min_contig_len": ("int", "--min-contig-len"),
        "min_contig_cov": ("int", "--min-contig-cov"),
        "genome_size": ("str", "--genome-size"),
        "trim_reads": ("bool", "--trim-reads"),
    },
    "p3-submit-sequence-submission": {
        "affiliation": ("str", "--affiliation"),
        "first_name": ("str", "--first-name"),
        "last_name": ("str", "--last-name"),
        "email": ("str", "--email"),
        "consortium": ("str", "--consortium"),
        "country": ("str", "--country"),
        "phoneNumber": ("str", "--phone"),
        "street": ("str", "--street"),
        "postal_code": ("str", "--postal-code"),
        "city": ("str", "--city"),
        "state": ("str", "--state"),
    },
    "p3-submit-whole-genome-SNP-analysis": {
        "majority-threshold": ("float", "--threshold"),
    },
    "p3-submit-docking": {
        "samples_per_complex": ("int", "--samples-per-complex"),
        "inference_steps": ("int", "--inference-steps"),
    },
    "p3-submit-MSA": {
        "aligner": ("str", "--aligner"),
        "alphabet": ("str", "--alphabet"),
    },
    "p3-submit-taxonomic-classification": {
        "analysis_type": ("str", "--analysis-type"),
        "database": ("str", "--database"),
        "confidence": ("str", "--confidence"),
        "host_genome": ("str", "--host-genome"),
        "save_classified": ("bool", "--save-classified"),
        "save_unclassified": ("bool", "--save-unclassified"),
    },
    "p3-submit-metagenome-binning": {
        "contigs": ("wsfile", "--contigs"),
        "genome_group": ("str", "--genome-group"),
        "skip_indexing": ("bool", "--skip-indexing"),
        "danglen": ("int", "--danglen"),
    },
    "p3-submit-metagenomic-read-mapping": {
        "gene_set_name": ("str", "--gene-set-name"),
    },
    "p3-submit-rnaseq": {
        "reference_genome_id": ("str", "--reference-genome-id"),
    },
    "p3-submit-sars2-assembly": {
        "recipe": ("str", "--recipe"),
        "primers": ("str", "--primers"),
        "primer_version": ("str", "--primer-version"),
        "min_depth": ("int", "--min-depth"),
        "taxonomy_id": ("int", "--taxonomy-id"),
        "label": ("str", "--label"),
    },
    "p3-submit-wastewater-analysis": {
        "strategy": ("str", "--strategy"),
        "primers": ("str", "--primers"),
        "primer_version": ("str", "--primer-version"),
        "date": ("str", "--date"),
    },
    "p3-submit-CGA": {
        "contigs": ("wsfile", "--contigs"),
        "recipe": ("str", "--recipe"),
        "scientific_name": ("str", "--scientific-name"),
        "taxonomy_id": ("int", "--taxonomy-id"),
        "code": ("int", "--code"),
        "domain": ("str", "--domain"),
        "label": ("str", "--label"),
        "min_contig_len": ("int", "--min-contig-length"),
        "min_contig_cov": ("int", "--min-contig-cov"),
        "racon_iter": ("int", "--racon-iter"),
        "pilon_iter": ("int", "--pilon-iter"),
    },
    "p3-submit-genome-annotation": {
        "scientific_name": ("str", "--scientific-name"),
        "taxonomy_id": ("int", "--taxonomy-id"),
        "code": ("int", "--genetic-code"),
        "domain": ("str", "--domain"),
        "recipe": ("str", "--recipe"),
    },
}

# Keys recognized but set implicitly by the CLI (hardcoded constants / defaults);
# present in references, no flag needed, must not count as unsupported.
IMPLICIT = {
    "p3-submit-core-genome-MLST": {"input_genome_type", "analysis_type"},
    "p3-submit-whole-genome-SNP-analysis": {"input_genome_type", "analysis_type"},
    "p3-submit-docking": {"batch_size"},
    "p3-submit-fastqutils": {"recipe"},  # handled by custom
    "p3-submit-ha-subtype-conversion": {"input_source"},
    "p3-submit-sequence-submission": {"input_source"},
    "p3-submit-SubspeciesClassification": {"input_source"},
}

# Commands whose fixtures carry standard read libraries.
USE_READS = {
    "p3-submit-fastqutils", "p3-submit-variation-analysis",
    "p3-submit-genome-assembly", "p3-submit-CGA", "p3-submit-metagenome-binning",
    "p3-submit-metagenomic-read-mapping", "p3-submit-rnaseq",
    "p3-submit-sars2-assembly", "p3-submit-taxonomic-classification",
    "p3-submit-wastewater-analysis",
}


# ---------------------------------------------------------------------------
# Custom inverters for commands whose option->param relationship is not a plain
# scalar mapping. Each returns (tokens, unsupported, consumed_keys).
# ---------------------------------------------------------------------------
def _custom_fastqutils(p):
    tokens, unsupported, consumed = [], [], {"recipe"}
    for r in p.get("recipe", []):
        rl = str(r).lower()
        if rl == "trim":
            tokens.append(("flag", "--trim"))
        elif rl == "fastqc":
            tokens.append(("flag", "--fastqc"))
        elif rl == "paired_filter":
            tokens.append(("flag", "--paired-filter"))
        elif rl == "align":
            pass  # implied by --reference-genome-id (declared in FLAGS)
        else:
            unsupported.append("recipe:" + str(r))
    return tokens, unsupported, consumed


def _custom_gene_tree(p):
    tokens, unsupported, consumed = [], [], {"sequences", "alphabet"}
    for seq in p.get("sequences", []):
        if str(seq.get("type", "")).upper() == "FASTA" and seq.get("filename"):
            tokens.append(("opt", "--sequences", ws(seq["filename"])))
        else:
            unsupported.append("sequences:type=" + str(seq.get("type")))
    if str(p.get("alphabet", "")).upper() == "DNA":
        tokens.append(("flag", "--dna"))
    return tokens, unsupported, consumed


def _custom_ha_subtype(p):
    tokens, unsupported, consumed = [], [], {
        "input_fasta_file", "input_feature_group", "types"}
    src = p.get("input_source")
    if src == "fasta_file" and p.get("input_fasta_file"):
        tokens.append(("opt", "--fasta", ws(p["input_fasta_file"])))
    elif src == "feature_group" and p.get("input_feature_group"):
        tokens.append(("opt", "--group", ws(p["input_feature_group"])))
    else:
        unsupported.append("input_source=" + str(src))
    if p.get("types"):
        tokens.append(("opt", "--types", _csv(p["types"])))
    return tokens, unsupported, consumed


def _custom_sequence_submission(p):
    tokens, unsupported, consumed = [], [], {"input_fasta_file", "metadata"}
    if p.get("input_source") not in (None, "fasta_file"):
        unsupported.append("input_source=" + str(p.get("input_source")))
    if p.get("input_fasta_file"):
        tokens.append(("opt", "--fasta", ws(p["input_fasta_file"])))
    if p.get("metadata"):
        tokens.append(("opt", "--metadata", ws(p["metadata"])))
    return tokens, unsupported, consumed


def _custom_core_mlst(p):
    tokens, unsupported, consumed = [], [], {
        "input_genome_group", "input_schema_selection"}
    if p.get("input_genome_group"):
        tokens.append(("opt", "--group", ws(p["input_genome_group"])))
    if p.get("input_schema_selection"):
        tokens.append(("opt", "--species", str(p["input_schema_selection"])))
    return tokens, unsupported, consumed


def _custom_wgs_snp(p):
    tokens, unsupported, consumed = [], [], {"input_genome_group"}
    if p.get("input_genome_group"):
        tokens.append(("opt", "--group", ws(p["input_genome_group"])))
    return tokens, unsupported, consumed


_LIGAND_LIB = {"small_db": "exemplar", "approved-drugs": "approved",
               "experimental_drugs": "experimental"}


def _custom_docking(p):
    tokens, unsupported = [], []
    consumed = {"protein_input_type", "input_pdb", "user_pdb_file",
                "ligand_library_type", "ligand_named_library", "ligand_ws_file"}
    pit = p.get("protein_input_type")
    if pit == "user_pdb_file" and p.get("user_pdb_file"):
        tokens.append(("opt", "--pdb-file", ws(p["user_pdb_file"][0])))
    elif pit == "input_pdb" and p.get("input_pdb"):
        tokens.append(("opt", "--pdb-id", str(p["input_pdb"][0])))
    else:
        unsupported.append("protein_input_type=" + str(pit))
    llt = p.get("ligand_library_type")
    if llt == "ws_file" and p.get("ligand_ws_file"):
        tokens.append(("opt", "--ligands-file", ws(p["ligand_ws_file"])))
    elif llt == "named_library" and p.get("ligand_named_library"):
        name = _LIGAND_LIB.get(p["ligand_named_library"])
        if name:
            tokens.append(("opt", "--ligands-lib", name))
        else:
            unsupported.append("ligand_named_library=" + str(p["ligand_named_library"]))
    else:
        unsupported.append("ligand_library_type=" + str(llt))
    return tokens, unsupported, consumed


def _custom_comparative_systems(p):
    tokens, unsupported, consumed = [], [], {"genome_ids", "genome_groups"}
    if p.get("genome_ids"):
        tokens.append(("opt", "--genomes", _csv(p["genome_ids"])))
    for g in p.get("genome_groups", []):
        tokens.append(("opt", "--genome-group", ws(g)))
    return tokens, unsupported, consumed


def _custom_viral_assembly(p):
    tokens, unsupported, consumed = [], [], {
        "paired_end_lib", "single_end_lib", "srr_id", "strategy"}
    pe = p.get("paired_end_lib")
    if pe:
        extra = {k for k in pe if k not in ("read1", "read2") and not is_empty(pe[k])}
        if extra:
            unsupported.append("paired_end_lib:" + ",".join(sorted(extra)))
        if "read1" in pe and "read2" in pe:
            tokens.append(("paired", ws(pe["read1"]), ws(pe["read2"])))
    se = p.get("single_end_lib")
    if se and "read" in se:
        tokens.append(("single", ws(se["read"])))
    if p.get("srr_id"):
        tokens.append(("srr", str(p["srr_id"])))
    if p.get("strategy"):
        tokens.append(("opt", "--strategy", str(p["strategy"])))
    return tokens, unsupported, consumed


def _custom_msa(p):
    tokens, unsupported, consumed = [], [], {
        "fasta_files", "feature_groups", "fasta_keyboard_input"}
    for f in p.get("fasta_files", []):
        if f.get("file"):
            tokens.append(("opt", "--fasta-file", ws(f["file"])))
    for g in p.get("feature_groups", []):
        gid = g.get("group") if isinstance(g, dict) else g
        tokens.append(("opt", "--feature-group", ws(gid)))
    if not is_empty(p.get("fasta_keyboard_input")):
        unsupported.append("fasta_keyboard_input")
    return tokens, unsupported, consumed


def _custom_subspecies(p):
    tokens, unsupported, consumed = [], [], {"input_fasta_file", "virus_type"}
    if p.get("input_fasta_file"):
        tokens.append(("opt", "--fasta-file", ws(p["input_fasta_file"])))
    if p.get("virus_type"):
        tokens.append(("opt", "--virus-type", str(p["virus_type"])))
    return tokens, unsupported, consumed


def _custom_genome_annotation(p):
    tokens, unsupported, consumed = [], [], {"contigs", "genbank_file"}
    if p.get("contigs"):
        tokens.append(("opt", "--contigs-file", ws(p["contigs"])))
    if p.get("genbank_file"):
        tokens.append(("opt", "--genbank-file", ws(p["genbank_file"])))
    return tokens, unsupported, consumed


CUSTOM = {
    "p3-submit-fastqutils": _custom_fastqutils,
    "p3-submit-gene-tree": _custom_gene_tree,
    "p3-submit-ha-subtype-conversion": _custom_ha_subtype,
    "p3-submit-sequence-submission": _custom_sequence_submission,
    "p3-submit-core-genome-MLST": _custom_core_mlst,
    "p3-submit-whole-genome-SNP-analysis": _custom_wgs_snp,
    "p3-submit-docking": _custom_docking,
    "p3-submit-comparative-systems": _custom_comparative_systems,
    "p3-submit-viral-assembly": _custom_viral_assembly,
    "p3-submit-MSA": _custom_msa,
    "p3-submit-SubspeciesClassification": _custom_subspecies,
    "p3-submit-genome-annotation": _custom_genome_annotation,
}


def invert(command, params):
    """Return (tokens, unsupported) for a command and its reference params.

    output_path / output_file are NOT included in tokens; the runner appends them
    as the two positional arguments.
    """
    tokens, unsupported = [], []
    consumed = {"output_path", "output_file"}

    for key, (kind, flag) in FLAGS.get(command, {}).items():
        if key in params and not is_empty(params[key]):
            tokens += _emit_flag(kind, flag, params[key])
            consumed.add(key)

    consumed |= IMPLICIT.get(command, set())

    if command in USE_READS:
        t, u, c = handle_reads(params)
        tokens += t
        unsupported += u
        consumed |= c

    if command in CUSTOM:
        t, u, c = CUSTOM[command](params)
        tokens += t
        unsupported += u
        consumed |= c

    for key, val in params.items():
        if key in consumed or key in METADATA_KEYS:
            continue
        if key.startswith("xx"):  # QA convention: xx-prefixed keys are disabled
            continue
        if is_empty(val):
            continue
        unsupported.append(key)

    return tokens, unsupported
