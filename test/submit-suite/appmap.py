"""Mapping from QA App-* test directories to p3-submit-* CLI commands.

The QA fixtures live in /vol/patric3/QA/applications/App-<App>/tests/*.json and
contain the `params` payload a backend app consumes. Each entry below ties an
App directory to the submit command that should be able to reproduce those
params, for both the Perl (p3_cli) and Go (BV-BRC-Go-SDK) front-ends.

`command` is the bare command name; the runner prefixes it with the Perl bin dir
or the Go bin dir. Apps with no submit CLI are simply absent (the runner SKIPs
them).
"""

# App directory name (without the "App-" prefix) -> submit command base name.
APP_TO_COMMAND = {
    "FastqUtils": "p3-submit-fastqutils",
    "GeneTree": "p3-submit-gene-tree",
    "MSA": "p3-submit-MSA",
    "Variation": "p3-submit-variation-analysis",
    "GenomeAssembly2": "p3-submit-genome-assembly",
    "GenomeAnnotation": "p3-submit-genome-annotation",
    "CodonTree": "p3-submit-codon-tree",
    "SequenceSubmission": "p3-submit-sequence-submission",
    "HASubtypeNumberingConversion": "p3-submit-ha-subtype-conversion",
    "TaxonomicClassification": "p3-submit-taxonomic-classification",
    "SubspeciesClassification": "p3-submit-SubspeciesClassification",
    "MetagenomeBinning": "p3-submit-metagenome-binning",
    "MetagenomicReadMapping": "p3-submit-metagenomic-read-mapping",
    "RNASeq": "p3-submit-rnaseq",
    "SARS2Assembly": "p3-submit-sars2-assembly",
    "SARS2Wastewater": "p3-submit-wastewater-analysis",
    "ComparativeSystems": "p3-submit-comparative-systems",
    "ComprehensiveGenomeAnalysis": "p3-submit-CGA",
    "ViralAssembly": "p3-submit-viral-assembly",
    # Ambiguous mappings — verify before enabling:
    # "Homology": "p3-submit-BLAST",
    # "GenomeComparison": "p3-submit-proteome-comparison",
}


def command_for_app(app_name):
    """Return the submit command for an App directory name, or None to SKIP."""
    return APP_TO_COMMAND.get(app_name)
