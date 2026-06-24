# Go SDK ↔ p3_cli Port Status

Tracks which `p3_cli` Perl scripts have been ported to Go commands in this SDK,
and the `p3_cli` commit each Go command was last synced against. The SDK is a
**curated subset** of `p3_cli`, not a full mirror.

- Design/rationale: `p3_cli/GO_PORT_PLAN.md`
- Port pattern & toolchain: see `BV-BRC-Go-SDK` section in `modules/CLAUDE.md`

## How to use this document

**Is a ported command stale?** Compare its "Synced to" commit against the latest
`p3_cli` change to the corresponding script:

```bash
cd modules/p3_cli
# Replace <sha> and <script> from the table below:
git log master <sha>..master -- scripts/<script>.pl
```

Non-empty output = the Perl changed after the port → the Go command needs review.

**Regenerate the stale check for every ported command at once:**

```bash
cd modules
for d in $(ls -d BV-BRC-Go-SDK/cmd/p3-*/ | xargs -n1 basename); do
  pl="p3_cli/scripts/$d.pl"
  [ -f "$pl" ] || continue
  latest=$(cd p3_cli && git log master -1 --format='%h' -- "scripts/$d.pl")
  echo "$d  latest=$latest"   # compare against "Synced to" column below
done
```

**Maintenance rule:** when you port or update a command, update its row's
"Synced to" SHA in the same change. A stale ledger is worse than none.

---

## Ported commands (sourced from `p3_cli/scripts/`)

"Synced to" = the latest `p3_cli` commit touching that script that the Go command
reflects. Status ✅ = verified current as of the date shown.

| Go command | Perl script | Synced to | p3_cli date | Status |
|---|---|---|---|---|
| p3-all-genomes | p3-all-genomes.pl | `0ddd93c` | 2025-06-04 | ✅ |
| p3-count | p3-count.pl | `9fbdbfe` | 2021-09-01 | ✅ |
| p3-echo | p3-echo.pl | `4049829` | 2019-10-25 | ✅ |
| p3-extract | p3-extract.pl | `4049829` | 2019-10-25 | ✅ |
| p3-get-feature-data | p3-get-feature-data.pl | `0ddd93c` | 2025-06-04 | ✅ |
| p3-get-feature-sequence | p3-get-feature-sequence.pl | `9fbdbfe` | 2021-09-01 | ✅ |
| p3-get-genome-data | p3-get-genome-data.pl | `0ddd93c` | 2025-06-04 | ✅ |
| p3-get-genome-features | p3-get-genome-features.pl | `0ddd93c` | 2025-06-04 | ✅ |
| p3-head | p3-head.pl | `4049829` | 2019-10-25 | ✅ |
| p3-job-status | p3-job-status.pl | `7f2a183` | 2021-09-01 | ✅ |
| p3-join | p3-join.pl | `4049829` | 2019-10-25 | ✅ |
| p3-match | p3-match.pl | `4049829` | 2019-10-25 | ✅ |
| p3-sort | p3-sort.pl | `4049829` | 2019-10-25 | ✅ |
| p3-submit-BLAST | p3-submit-BLAST.pl | `7f2a183` | 2021-09-01 | ✅ |
| p3-submit-CGA | p3-submit-CGA.pl | `ae8ced8` | 2025-03-30 | ✅ |
| p3-submit-codon-tree | p3-submit-codon-tree.pl | `723421d` | 2021-09-02 | ✅ |
| p3-submit-comparative-systems | p3-submit-comparative-systems.pl | `d01b24e` | 2025-03-12 | ✅ |
| p3-submit-core-genome-MLST | p3-submit-core-genome-MLST.pl | `a7ad47f` | 2026-04-08 | ✅ ported 2026-06 |
| p3-submit-docking | p3-submit-docking.pl | `b6006ed` | 2026-05-07 | ✅ ported 2026-06 |
| p3-submit-fastqutils | p3-submit-fastqutils.pl | `98ab7f6` | 2026-06-04 | ✅ srr_libs fix 2026-06 |
| p3-submit-gene-tree | p3-submit-gene-tree.pl | `7f2a183` | 2021-09-01 | ✅ |
| p3-submit-genome-annotation | p3-submit-genome-annotation.pl | `0de451f` | 2022-08-26 | ✅ |
| p3-submit-genome-assembly | p3-submit-genome-assembly.pl | `e4f4105` | 2022-03-10 | ✅ |
| p3-submit-ha-subtype-conversion | p3-submit-ha-subtype-conversion.pl | `1422d7b` | 2026-04-06 | ✅ ported 2026-06 |
| p3-submit-influenza-treesort | p3-submit-influenza-treesort.pl | `1a0675c` | 2026-04-06 | ✅ ported 2026-06 |
| p3-submit-metagenome-binning | p3-submit-metagenome-binning.pl | `0095243` | 2022-12-02 | ✅ |
| p3-submit-metagenomic-read-mapping | p3-submit-metagenomic-read-mapping.pl | `723421d` | 2021-09-02 | ✅ |
| p3-submit-MSA | p3-submit-MSA.pl | `c1ade9e` | 2025-07-23 | ✅ |
| p3-submit-proteome-comparison | p3-submit-proteome-comparison.pl | `7f2a183` | 2021-09-01 | ✅ |
| p3-submit-rnaseq | p3-submit-rnaseq.pl | `396eb7a` | 2021-09-05 | ✅ |
| p3-submit-sars2-assembly | p3-submit-sars2-assembly.pl | `84c99b4` | 2023-03-15 | ✅ |
| p3-submit-sequence-submission | p3-submit-sequence-submission.pl | `ebe8f24` | 2026-04-07 | ✅ ported 2026-06 |
| p3-submit-SubspeciesClassification | p3-submit-SubspeciesClassification.pl | `b8f188a` | 2025-03-13 | ✅ |
| p3-submit-taxonomic-classification | p3-submit-taxonomic-classification.pl | `42794ce` | 2024-06-11 | ✅ |
| p3-submit-variation-analysis | p3-submit-variation-analysis.pl | `4ddb85c` | 2025-12-02 | ✅ |
| p3-submit-viral-assembly | p3-submit-viral-assembly.pl | `16ca205` | 2025-02-17 | ✅ |
| p3-submit-wastewater-analysis | p3-submit-wastewater-analysis.pl | `42794ce` | 2024-06-11 | ✅ |
| p3-submit-whole-genome-SNP-analysis | p3-submit-whole-genome-SNP-analysis.pl | `a7ad47f` | 2026-04-08 | ✅ ported 2026-06 |
| p3-tail | p3-tail.pl | `4049829` | 2019-10-25 | ✅ |
| p3-all-contigs | p3-all-contigs.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-all-drugs | p3-all-drugs.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-all-genome-features | p3-all-genome-features.pl | `9d6470a` | 2025-06-06 | ✅ ported 2026-06 |
| p3-all-sfs | p3-all-sfs.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-all-sfvts | p3-all-sfvts.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-all-subsystem-roles | p3-all-subsystem-roles.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-all-subsystems | p3-all-subsystems.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-all-taxonomies | p3-all-taxonomies.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-contigs | p3-get-genome-contigs.pl | `79f2ddc` | 2025-07-16 | ✅ ported 2026-06 |
| p3-get-genome-drugs | p3-get-genome-drugs.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-expression | p3-get-genome-expression.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-protein-regions | p3-get-genome-protein-regions.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-protein-structures | p3-get-genome-protein-structures.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-refseq-features | p3-get-genome-refseq-features.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-sp-genes | p3-get-genome-sp-genes.pl | `a5b52e9` | 2026-03-12 | ✅ ported 2026-06 |
| p3-get-genome-subsystems | p3-get-genome-subsystems.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-feature-protein-regions | p3-get-feature-protein-regions.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-feature-protein-structures | p3-get-feature-protein-structures.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-feature-subsystems | p3-get-feature-subsystems.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-features-in-regions | p3-get-features-in-regions.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-family-data | p3-get-family-data.pl | `d6fd225` | 2025-06-05 | ✅ ported 2026-06 |
| p3-get-family-features | p3-get-family-features.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-sf-data | p3-get-sf-data.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-sf-variants | p3-get-sf-variants.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-subsystem-features | p3-get-subsystem-features.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-subsystem-roles | p3-get-subsystem-roles.pl | `9fbdbfe` | 2021-09-01 | ✅ ported 2026-06 |
| p3-get-drug-genomes | p3-get-drug-genomes.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-taxonomy-data | p3-get-taxonomy-data.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-find-features | p3-find-features.pl | `69ad3c3` | 2025-06-04 | ✅ ported 2026-06 |
| p3-find-genomes | p3-find-genomes.pl | `d6fd225` | 2025-06-05 | ✅ ported 2026-06 |
| p3-find-serology-data | p3-find-serology-data.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-find-surveillance-data | p3-find-surveillance-data.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-role-features | p3-role-features.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-get-genome-group | p3-get-genome-group.pl | `93ea60c` | 2025-08-05 | ✅ ported 2026-06 |
| p3-get-feature-group | p3-get-feature-group.pl | `93ea60c` | 2025-08-05 | ✅ ported 2026-06 |
| p3-put-genome-group | p3-put-genome-group.pl | `93ea60c` | 2025-08-05 | ✅ ported 2026-06 |
| p3-put-feature-group | p3-put-feature-group.pl | `93ea60c` | 2025-08-05 | ✅ ported 2026-06 |
| p3-get-features-by-sequence | p3-get-features-by-sequence.pl | `9fbdbfe` | 2021-09-01 | ✅ ported 2026-06 — uses na/aa_sequence_md5 directly (Perl used protein translation workaround) |
| p3-list-genome-groups | p3-list-genome-groups.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-list-feature-groups | p3-list-feature-groups.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-genus-species | p3-genus-species.pl | `0ddd93c` | 2025-06-04 | ✅ ported 2026-06 |
| p3-collate | p3-collate.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-compare-cols | p3-compare-cols.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-fasta-md5 | p3-fasta-md5.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-file-filter | p3-file-filter.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-merge | p3-merge.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-pick | p3-pick.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-pick-by-class | p3-pick-by-class.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-pivot | p3-pivot.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-shuffle | p3-shuffle.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-stats | p3-stats.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-tbl-to-fasta | p3-tbl-to-fasta.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |
| p3-tbl-to-html | p3-tbl-to-html.pl | `4049829` | 2019-10-25 | ✅ ported 2026-06 |

Note: every pre-2026 port's last Perl change predates the Go SDK's initial commit
(2026-02-04), so they reflect the final state of their scripts. Rows marked "2026-06"
were ported during the 2026-06 sync sessions.

Sync notes (2026-06-24):
- p3-submit-MSA: `progressiveMauve` added to both Perl and Go (was in app spec but missing from both CLIs)
- p3-submit-genome-assembly: `--genome-size` wired in Perl `GetOptions` (was in POD/spec but silently rejected)
- p3-submit-variation-analysis: `Snippy` added to Go mapper/caller enums (was in Perl and app spec)
- p3-submit-SubspeciesClassification: Go virus-type codes corrected (`MASTADENO_A` → `MASTADENOA` etc.)
- All submit cmds: Go now validates output-path existence (mirrors Perl `UploadSpec` check)
- Genome-bearing cmds: Go now validates genome IDs via data API (mirrors Perl `GenomeIdSpec`)

## Commands not sourced from `p3_cli/scripts/`

These Go commands have no `p3_cli/scripts/*.pl` counterpart and are not tracked
against p3_cli here:

- Workspace operations (mirror `Workspace/scripts/`): `p3-cat`, `p3-cp`, `p3-ls`,
  `p3-mkdir`, `p3-rm`
- Auth / SDK built-ins: `p3-login`, `p3-logout`, `p3-whoami`
- `p3-all-features` (verify source before treating as a p3_cli port)

---

## Unported `p3_cli` scripts

As of `p3_cli` master `3e809ac` (2026-06-23) there are **98** `p3_cli/scripts/p3-*.pl`
with no Go command. Most are deliberately out of scope (the SDK is a subset).
Regenerate the current backlog, newest-changed first (best porting candidates):

```bash
cd modules
comm -23 \
  <(ls p3_cli/scripts/p3-*.pl | xargs -n1 basename | sed 's/\.pl$//' | sort) \
  <(ls -d BV-BRC-Go-SDK/cmd/p3-*/ | xargs -n1 basename | sort) \
| while read s; do
    printf '%s\t%s\n' "$(cd p3_cli && git log master -1 --format='%ad' --date=short -- scripts/$s.pl)" "$s"
  done | sort -r
```

This list is intentionally not enumerated inline so it cannot go stale — run the
command above for the authoritative, current backlog.

---

## Known CLI discrepancies and test suite findings (2026-06-24)

Run with: `python3 test/submit-suite/run_suite.py --tool both`
Last result: **PASS=49, MISMATCH=0, UNSUPPORTED=105, ERROR=49, SKIP=132**
MISMATCH=0 means Go and Perl emit identical params on every fixture where both succeed.

### Resolved this session

| Issue | Fix |
|---|---|
| Perl data-API stall (Cloudflare 1010 on `patricbrc.org`) | Switched `P3DataAPI` to `bv-brc.org`; added configurable UA (`BV-BRC P3 Client`) |
| Go rejected `Snippy` mapper/caller (variation) | Added to `validMappers`/`validCallers` |
| Go virus-type codes used underscore (`MASTADENO_A`) | Corrected to match Perl (`MASTADENOA` etc.) |
| `progressiveMauve` rejected by both CLIs | Added to Perl `ALIGNER` constant and Go `validAligners` |
| Go missing output-path existence check | Added `ws.RequireFolder` to all 25 submit commands |
| Go missing genome-ID validation | Added `api.RequireGenomeIDs` to 5 genome-bearing commands |
| Perl genome-assembly rejected `--genome-size` | Added to Perl `GetOptions` |
| Suite stalled on Perl invocations | `user-env.sh` now sourced; `raw_decode` handles trailing output |

### Remaining ERROR class

All 49 remaining ERRORs are **environmental, not CLI bugs**. They fall into three categories:

1. **Fixture output paths belonging to other users (46 errors)** — Perl's `UploadSpec`
   validates that the output folder exists in the workspace before submitting. QA
   fixtures point at workspaces owned by `mkuscuog@bvbrc`, `jsporter@patricbrc.org`,
   `ARWattam@patricbrc.org`, `anwarren@patricbrc.org` etc. Go succeeds (it now also
   validates, but these paths exist for the fixture authors); Perl fails because the
   current user doesn't have access to stat them.
   - Apps affected: `SubspeciesClassification` (45), `Variation` (3), `MSA` (2), `FastqUtils` (1)
   - Resolution: the fixture data is correct; the paths just don't exist for us. No code change needed.

2. **Perl wrappers not yet built for 6 new commands (3 errors)** — `p3-submit-ha-subtype-conversion`
   (and the other 5 newly-ported scripts) aren't in `dev-ubuntu/bin` until `make` is
   run in `p3_cli`. Until then, only the Go side is tested for those apps.
   - Fix: run `make` in `p3_cli` after pulling.

3. **Fixture typo: `JAPENCEPH` instead of `JAPANENCEPH` (3 errors)** — three QA
   fixtures use the misspelled virus-type code; both CLIs correctly reject it.
   - Fix: correct the fixture files.

### Permanent UNSUPPORTED (CLI coverage gaps, 105 fixtures)

These fixtures reference params the CLI front-ends intentionally don't expose.
Notable categories:

| Gap | Affected apps | Notes |
|---|---|---|
| Per-library metadata (`platform`, `sample_id`, `condition`, `date`) in read libs | FastqUtils, Variation, TaxonomicClassification, RNASeq, SARS2Wastewater | Perl ReadSpec supports these; Go does not |
| `fasta_keyboard_input` / `input_type` / `select_genomegroup` | MSA | Newer schema params not yet exposed in either CLI |
| `module` / `reference_type` / `reference_genome_id` in ViralAssembly | ViralAssembly | Extended options not in current Perl or Go CLI |
| `bootstraps` in CodonTree | CodonTree | Present in app spec, absent from Perl `GetOptions` and Go |
| `_preflight` | Variation | Internal scheduler key, not a user-facing option |
| `srr_libs:platform`, `srr_libs:sample_id`, `srr_libs:title` | FastqUtils, TaxonomicClassification | SRA library metadata; only `srr_accession` is expressible |
| `numberOfSequences` | SequenceSubmission | Informational, not a CLI input |

### Paired-end library syntax — RESOLVED (2026-06-24)

Both CLIs now accept the same two-argument form:

```
--paired-end-lib read1.fq read2.fq
```

Go's `NormalizePairedEndLibArgs` (`internal/cli/args.go`) pre-processes `os.Args`
before cobra parses them, joining the two arguments into the internal comma form.
The original comma-joined form (`--paired-end-lib read1.fq,read2.fq`) is still
accepted for backwards compatibility.
