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

Note: every pre-2026 port's last Perl change predates the Go SDK's initial commit
(2026-02-04), so they reflect the final state of their scripts. The seven rows
marked "2026-06" were ported/updated during the 2026-06 sync.

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
