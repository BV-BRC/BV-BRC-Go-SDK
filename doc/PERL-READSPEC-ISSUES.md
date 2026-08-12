# Perl `ReadSpec` issues found while porting read-library handling to Go

Written 2026-08-10 while fixing the Go SDK's read-library parameter
construction (`internal/readspec`). These are defects in the **Perl** side
(`p3_cli/lib/Bio/KBase/AppService/ReadSpec.pm` and the `p3-submit-*.pl`
scripts) or in the app specs themselves. **None of them are fixed here.** Every
one below was reproduced by running the actual Perl tool with `--dry-run`, not
inferred from reading the source.

Why parameter names matter: `AppScript::preprocess_parameters`
(`app_service/lib/Bio/KBase/AppService/AppScript.pm:728`) **silently drops** any
submitted parameter not declared in the app spec — it only warns "provided …
but not specified in the app spec" and keeps the value in `raw_params`, where
nothing reads it. So a wrong parameter *name* does not fail the submission; the
reads just vanish and the job runs on whatever is left.

---

## 1. `srr_label` derivation can't express FastqUtils, which works around it by poking the object

Not a live bug — noted because it is the reason `readspec.Options` needs an
override, and because the workaround is fragile.

The constructor (`ReadSpec.pm:229-236`) derives `srr_label` from a *different*
condition than `srrAlt`:

```perl
if ($retVal->{samples} || $retVal->{analysis})                      { $retVal->{srrAlt}    = 1;          }
if ($retVal->{samples} || $retVal->{analysis} || $retVal->{rnaseq}) { $retVal->{srr_label} = 'srr_libs'; }
```

FastqUtils needs object-shaped entries *under* `srr_libs`, which no combination
of constructor flags produces: `srrAlt => 1` gives the objects but leaves
`srr_label` at `srr_ids`. `p3-submit-fastqutils.pl` therefore reaches into the
object immediately after construction:

```perl
my $reader = Bio::KBase::AppService::ReadSpec->new($uploader, simple => 1, srrAlt => 1);  # :90
$reader->{srr_label} = 'srr_libs';                                                        # :92
```

Confirmed correct in practice:

```
$ p3-submit-fastqutils --srr-id ERR4686055 --srr-id ERR4686056 … --dry-run …
   "srr_libs" : [ { "srr_accession" : "ERR4686055" }, { "srr_accession" : "ERR4686056" } ]
```

This is the only post-construction field poke in any `p3-submit-*.pl`
(`grep -n '\$reader->{' scripts/*.pl`). It works, but it depends on an internal
field name and is invisible to anyone reading the constructor call. Adding
`srrAlt` to the second condition — or a proper `srr_label` constructor option —
would fold it back into the supported interface.

**Go:** `p3-submit-fastqutils` already emitted `srr_libs` and was left alone;
`readspec.Options.SRRKeyOverride` is the equivalent of line 92.

---

## 2. `_processTweaks` has an inverted guard — `--platform`, `--insert-size-*`, `--read-orientation-*` never reach a library

The tweak options are registered **only when `assembling`** (`ReadSpec.pm:258-265`):

```perl
if ($self->{assembling}) {
    push @parms,
        "platform=s"                => sub { $self->_setPlatform($_[1]); },
        "read-orientation-outward"  => sub { $self->{read_orientation_outward} = 1; },
        "read-orientation-inward"   => sub { $self->{read_orientation_outward} = 0; },
        "insert-size-mean=i"        => sub { $self->{insert_size_mean}  = $_[1]; },
        "insert-size-stdev=i"       => sub { $self->{insert_size_stdev} = $_[1]; };
    $self->{platform} = 'infer';
}
```

but they are applied **only when not `assembling`** (`ReadSpec.pm:681-688`):

```perl
sub _processTweaks {
    my ($self, $lib) = @_;
    if (! $self->{assembling}) {
        for my $parm (qw(insert_size_mean insert_size_stdev condition platform read_orientation_outward)) {
            if ( defined $self->{$parm} ) { $lib->{$parm} = $self->{$parm}; }
        }
    }
    if ($self->{analysis}) { $self->_tweakLibs2($lib); }
}
```

`_processTweaks` is the only path by which those five keys reach a library
(call sites: `:320` paired, `:379` interleaved, `:420` single). The two
conditions are mutually exclusive, so for the assembling apps
(`p3-submit-CGA`, `p3-submit-sars2-assembly`) the flags parse and validate
without error and are then discarded. Notably `platform` is defaulted to
`'infer'` in the same block that can never emit it.

`condition` is unaffected — it is set via `_setCondition` under `rnaseq`, where
`assembling` is 0, so the RNA-Seq path works.

Consequences: `ComprehensiveGenomeAnalysis.json`, `GenomeAssembly2.json` and
`SARS2Assembly.json` all declare `platform` / `insert_size_mean` /
`insert_size_stdev` / `read_orientation_outward` on their paired-end library
groups, and no Perl CLI submission can populate them. Reproduced:

```
$ p3-submit-CGA --dry-run --taxonomy-id 1010834 \
    --platform illumina --insert-size-mean 300 --read-orientation-outward \
    --paired-end-lib ws:…_R1.fastq.gz ws:…_R2.fastq.gz /olson@patricbrc.org/home cga-test
   "paired_end_libs" : [ { "read1" : "…_R1.fastq.gz", "read2" : "…_R2.fastq.gz" } ]
```

All three options parse and validate without complaint; none appears in the
output.

**Suggested Perl fix:** drop the `! $self->{assembling}` guard (the per-parameter
`defined` check already makes it a no-op when nothing was set).

---

## 3. `p3-submit-wastewater-analysis.pl` passes an option name the constructor doesn't know

`p3-submit-wastewater-analysis.pl:120`

```perl
my $reader = Bio::KBase::AppService::ReadSpec->new($uploader, assembly => 1, samples => 1, analysis => 1);
```

The constructor reads `assembling`, not `assembly` (`ReadSpec.pm:222`); unknown
keys in `%options` are ignored, so this is a silent no-op. `assembly` appears
nowhere in `ReadSpec.pm` outside two POD paragraphs. `SARS2Wastewater.json`
declares `platform`, `interleaved` and `read_orientation_outward` on its
paired-end group, and with `assembling` unset those options are never even
registered:

```
$ p3-submit-wastewater-analysis --dry-run --platform illumina …
Unknown option: platform
Too many parameters-- only output path and output name should be specified.
  Found : "illumina", "/olson@patricbrc.org/home", "ww-perl2"
```

Note that fixing only this typo would not help while issue #2 stands — setting
`assembling` registers the flags but then disables the block that applies them.
Both need fixing together.

---

## 4. `ReadSpec` initializes primer/date state under names nothing reads

Constructor (`ReadSpec.pm:221-222`):

```perl
sample_date    => undef,
sample_primers => 'ARTIC/V5.3.2',
```

but the setters write `{date}` and `{primers}` (`:544`, `:518`) and
`_tweakLibs2` reads `{primers}` and `{date}` (`:713-717`). `sample_date` and
`sample_primers` are never read anywhere. Two consequences:

- The intended default primer spec is dead. Without an explicit `--primers`,
  `$self->{primers}` is `undef`, so `split /,/, $self->{primers}` yields an
  empty list and `_tweakLibs2` sets `primers` and `primer_version` to `undef`
  on every library, plus an uninitialized-value warning. Reproduced:

  ```
  $ p3-submit-wastewater-analysis --dry-run --paired-end-lib ws:…_R1.fastq.gz ws:…_R2.fastq.gz \
      /olson@patricbrc.org/home ww-perl
  Use of uninitialized value in split at …/ReadSpec.pm line 713.
     "primers" : "ARTIC",                        <- top-level, set by the script
     "paired_end_libs" : [ { …, "primers" : null, "primer_version" : null } ]
  ```

- The dead default is also written with the wrong separator: `'ARTIC/V5.3.2'`
  (slash), while `LEGAL_PRIMERS` (`:145-148`) and the `split /,/` both use a
  comma.

**Suggested Perl fix:** initialize `primers => 'ARTIC,V5.3.2'` and `date => undef`.

**Go behaviour:** `p3-submit-wastewater-analysis` defaults to
`primers=ARTIC`, `primer-version=V5.3.2`, i.e. what Perl was evidently meant to
do. It also accepts Perl's combined `--primers <type>,<version>` spelling and
validates the pair against `LEGAL_PRIMERS`, so an invocation copied from the
Perl tool does not silently land `"midnight,V1"` in `primers` with the default
version alongside it.

---

## 5. Two app specs are not valid JSON

Not a `ReadSpec` issue, but it blocks any strict-parser tooling (including the
survey script used to cross-check these fixes):

| File | Problem |
|------|---------|
| `bvbrc_taxonomic_classification_2/app_specs/TaxonomicClassification.json` | trailing commas before `}` / `]` |
| `bvbrc_SARS2Wastewater/app_specs/SARS2Wastewater.json` | trailing commas |
| `sars2_assembly/app_specs/ComprehensiveSARS2Analysis.json` | `#` comment lines |

Perl's `JSON::XS` in relaxed mode and the JS loader tolerate these; Python's
`json`, Go's `encoding/json`, and `jq` do not. This matters more once
`app_specs/*.json` grows the declarative `outputs` key described in
`modules/PLAN-app-output-specs.md`.

---

## 6. Open question: RNA-Seq `sample_id`

`RNASeq.json` declares `sample_id` on both the paired-end library group and
`srr_libs`, with `required: 1` and a default of `'sample'`. Perl's
`p3-submit-rnaseq` uses `ReadSpec->new($uploader, rnaseq => 1)` — no `samples`
— so it never emits a `sample_id`, and every library falls back to the same
default string.

Go matches Perl here **on purpose**: emitting a derived `sample_id` would be a
new Go/Perl divergence in the submit-suite cross-check, and it is not obvious
whether the app wants per-library sample IDs or is happy with the constant.
This needs a decision from whoever owns the RNA-Seq app; if per-library IDs are
wanted, the fix is `rnaseq => 1, samples => 1` in Perl and
`readspec.Options{RNASeq: true, Samples: true}` in Go, which then agree
automatically.

---

## Reference: what each app actually declares

Surveyed from `*/app_specs/*.json` on 2026-08-10.

| App | paired-end group keys | SRA parameter |
|-----|----------------------|---------------|
| ComprehensiveGenomeAnalysis | read1, read2, platform, interleaved, read_orientation_outward, insert_size_* | `srr_ids` (scalars) |
| FastqUtils | read1, read2, platform | **`srr_libs`** ({srr_accession}) |
| GenomeAssembly / GenomeAssembly2 | read1, read2, platform, interleaved, … | `srr_ids` (scalars) |
| MetagenomeBinning | read1, read2 | `srr_ids` (scalars) |
| MetagenomicReadMapping | read1, read2 | `srr_ids` (scalars) |
| RNASeq | **sample_id**, read1, read2, interleaved, insert_size_*, condition | **`srr_libs`** ({sample_id, srr_accession, condition}) |
| SARS2Assembly | read1, read2, platform, interleaved, read_orientation_outward | `srr_ids` (scalars) |
| SARS2Wastewater | **sample_id**, read1, read2, platform, interleaved, read_orientation_outward, primers, primer_version, **sample_level_date** | **`srr_libs`** |
| TaxonomicClassification | **sample_id**, read1, read2 | **`srr_libs`** ({sample_id, srr_accession}) |
| Variation | read1, read2, interleaved, insert_size_* | `srr_ids` (scalars) |
| ComprehensiveSARS2Analysis | read1, read2, platform, interleaved, read_orientation_outward | `srr_ids` (scalars) |
