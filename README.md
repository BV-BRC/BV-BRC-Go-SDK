# BV-BRC Go SDK

Go SDK and command-line tools for interacting with BV-BRC (Bacterial and Viral
Bioinformatics Resource Center).

## Overview

This module provides:

1. **Go libraries** for programmatic access to BV-BRC services
2. **CLI tools** (138 commands): 101 `p3-*` mirroring the Perl `p3_cli` suite,
   and 37 `rast-*` mirroring `genome_annotation/scripts/`

### Go Libraries

```go
import (
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/api"                // Data API client
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/appservice"         // Job submission
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/auth"               // Authentication
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/genomeannotation"   // Genome annotation service
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/workspace"          // Workspace/file operations
)
```

`genomeannotation` is the client for the GenomeAnnotation JSONRPC service that
the Perl `rast-*` scripts drive. Genome typed objects pass through it as
`json.RawMessage` and are never decoded, so an annotation step returns the
genome it was given with the step's additions and nothing else changed —
decoding into a generic map would renumber integers and reorder keys. Unlike
the other clients its token is optional: `default_workflow`,
`enumerate_classifiers` and `enumerate_special_protein_databases` answer
unauthenticated.

## Installation

Current release: **2.0.16**. Substitute another version below to install it;
the archives are attached to each
[GitHub release](https://github.com/BV-BRC/BV-BRC-Go-SDK/releases).

### conda (all platforms)

```bash
conda install -c bv-brc bvbrc-cli          # latest
conda install -c bv-brc bvbrc-cli=2.0.16   # a specific version
```

Packages are published for linux-64, linux-aarch64, osx-64 and osx-arm64.
There is no Windows conda package — use the installer or zip below.

### Linux

Each archive expands into a versioned directory containing `bin/` — not a bare
`bin/`.

```bash
# x86_64
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-linux-amd64.tar.gz
tar -xzf bvbrc-cli-2.0.16-linux-amd64.tar.gz
sudo cp bvbrc-cli-2.0.16-linux-amd64/bin/* /usr/local/bin/

# ARM64 (e.g. Raspberry Pi, AWS Graviton)
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-linux-arm64.tar.gz
tar -xzf bvbrc-cli-2.0.16-linux-arm64.tar.gz
sudo cp bvbrc-cli-2.0.16-linux-arm64/bin/* /usr/local/bin/
```

A `.deb` is also published for each architecture, and installs into
`/usr/local/bin`:

```bash
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli_2.0.16_amd64.deb
sudo dpkg -i bvbrc-cli_2.0.16_amd64.deb
```

### macOS

#### Installer package (simplest)

A `.pkg` installer is published for each architecture. It installs the tools
into `/usr/local/bin`, which is already on the default `PATH`.

```bash
# Apple Silicon
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-apple-silicon.pkg
sudo installer -pkg bvbrc-cli-2.0.16-apple-silicon.pkg -target /

# Intel
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-intel.pkg
sudo installer -pkg bvbrc-cli-2.0.16-intel.pkg -target /
```

Double-clicking the `.pkg` works too, but see the signing note below — an
unsigned package opened from Finder is refused until you allow it, whereas
`sudo installer` on a file fetched with `curl` is not.

#### Tarball

```bash
# Apple Silicon
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-darwin-arm64.tar.gz
tar -xzf bvbrc-cli-2.0.16-darwin-arm64.tar.gz
sudo cp bvbrc-cli-2.0.16-darwin-arm64/bin/* /usr/local/bin/

# Intel
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-darwin-amd64.tar.gz
tar -xzf bvbrc-cli-2.0.16-darwin-amd64.tar.gz
sudo cp bvbrc-cli-2.0.16-darwin-amd64/bin/* /usr/local/bin/
```

A `darwin-universal` tarball containing fat binaries for both architectures is
also published, if you would rather ship one build to mixed hardware.

> **macOS note: nothing here is code-signed or notarized**, the `.pkg`
> installers included. Two consequences:
>
> - **Opened from Finder**, an unsigned `.pkg` is blocked. Either right-click →
>   **Open** and confirm, or approve it under **System Settings → Privacy &
>   Security** after the first attempt. Installing from the command line with
>   `sudo installer` avoids this.
> - **For the tarballs**, if Gatekeeper blocks the binaries run
>   `xattr -dr com.apple.quarantine /usr/local/bin/p3-* /usr/local/bin/rast-*`
>   after copying — or download with `curl`/`wget` rather than a browser, since
>   only browser downloads set the quarantine attribute.

### Windows

#### Installer (recommended, x64)

Download `bvbrc-cli-2.0.16-windows-amd64-setup.exe` from the releases page and
run it as Administrator. It installs to `C:\Program Files\BVBRC`, **adds that
directory to the system `PATH`**, and registers an entry in Add/Remove Programs
so it can be uninstalled cleanly.

Open a **new** terminal afterwards — a running shell does not pick up a `PATH`
change made by the installer.

```powershell
p3-login
```

> The installer is **not code-signed**, so SmartScreen will warn on first run:
> *More info → Run anyway*.

> There is no ARM64 installer — on Windows ARM64, use the zip below.

#### Zip

```powershell
# x64
Expand-Archive bvbrc-cli-2.0.16-windows-amd64.zip .
# ARM64
Expand-Archive bvbrc-cli-2.0.16-windows-arm64.zip .
```

The `.exe` files sit at the top of the extracted directory (there is no `bin\`
on Windows). Either run the bundled installer from inside it, as Administrator:

```powershell
powershell -ExecutionPolicy Bypass -File install.ps1
```

or copy the executables to a directory of your own that is on `PATH`:

```powershell
$dest = "$env:LOCALAPPDATA\Programs\BVBRC"
New-Item -ItemType Directory -Force $dest
Copy-Item bvbrc-cli-2.0.16-windows-amd64\*.exe $dest
# Add it to your user PATH (takes effect in new terminals)
[Environment]::SetEnvironmentVariable(
    "Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";$dest", "User")
```

> Do **not** copy the tools into `C:\Windows\System32`. Earlier versions of this
> README suggested it; it drops 138 executables into a system directory where
> they are mixed in with Windows' own, cannot be upgraded or removed as a unit,
> and may shadow system commands.

### Verification

```bash
# Linux / macOS: verify checksums
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v2.0.16/bvbrc-cli-2.0.16-checksums.sha256
sha256sum -c bvbrc-cli-2.0.16-checksums.sha256
```

```powershell
# Windows: verify checksum of a single file
Get-FileHash bvbrc-cli-2.0.16-windows-amd64.zip -Algorithm SHA256
```

Every command takes `--version`:

```bash
$ p3-ls --version
p3-ls 2.0.16
bvbrc-cli-go/2.0.16 linux/amd64 go1.25.6
```

The second line is the exact `User-Agent` that binary sends, so it also reports
the product identity (`bvbrc-auth-go` from `p3-login`) and any `P3_USER_AGENT`
override — quote it when reporting a request that was rejected at the edge.

A binary built with a plain `go build` is not stamped and reports `unknown`
for its version; use `make` or a build script. To read the stamp without
running the binary:

```bash
go version -m $(which p3-ls) | grep ldflags   # -X .../version.Version=2.0.16
```

## Getting Started

```bash
# 1. Log in to BV-BRC
p3-login

# 2. List your workspace
p3-ls /your-username@patricbrc.org/home

# 3. Query genomes
p3-all-genomes --eq genus,Yersinia --limit 10

# 4. Find features for a genome
echo "511145.12" | p3-get-genome-features --eq feature_type,CDS --limit 5

# 5. Submit a job
p3-submit-genome-assembly --recipe auto --srr-id SRR12345 \
  /username@patricbrc.org/home/assemblies MyAssembly

# 6. Check job status
p3-job-status
```

## Command Reference

### Authentication
| Command | Description |
|---------|-------------|
| `p3-login` | Log in to BV-BRC |
| `p3-logout` | Log out |
| `p3-whoami` | Display current user |

### Workspace Operations
| Command | Description |
|---------|-------------|
| `p3-ls` | List workspace contents |
| `p3-cat` | Display file contents |
| `p3-cp` | Copy files |
| `p3-mkdir` | Create directories |
| `p3-rm` | Remove files |
| `p3-get-genome-group` | Retrieve genome IDs from a workspace genome group |
| `p3-get-feature-group` | Retrieve feature IDs from a workspace feature group |
| `p3-put-genome-group` | Create or update a genome group from a list of IDs |
| `p3-put-feature-group` | Create or update a feature group from a list of IDs |
| `p3-list-genome-groups` | List genome groups in a workspace folder |
| `p3-list-feature-groups` | List feature groups in a workspace folder |

### Data Enumeration (`p3-all-*`)
| Command | Description |
|---------|-------------|
| `p3-all-genomes` | All genomes |
| `p3-all-features` | All genome features |
| `p3-all-genome-features` | All genome features (alias) |
| `p3-all-contigs` | All contig sequences |
| `p3-all-drugs` | All antibiotic records |
| `p3-all-subsystems` | All subsystem records |
| `p3-all-subsystem-roles` | All subsystem roles |
| `p3-all-taxonomies` | All taxonomy records |
| `p3-all-sfs` | All sequence features |
| `p3-all-sfvts` | All sequence feature variants |

### Keyed Data Lookup (`p3-get-*`)
| Command | Input | Output |
|---------|-------|--------|
| `p3-get-genome-data` | genome IDs | genome metadata |
| `p3-get-genome-features` | genome IDs | features |
| `p3-get-genome-contigs` | genome IDs | contig sequences |
| `p3-get-genome-drugs` | genome IDs | AMR data |
| `p3-get-genome-subsystems` | genome IDs | subsystem assignments |
| `p3-get-genome-expression` | genome IDs | transcriptomics data |
| `p3-get-genome-sp-genes` | genome IDs | specialty genes |
| `p3-get-genome-protein-regions` | genome IDs | protein domain regions |
| `p3-get-genome-protein-structures` | genome IDs | protein structures |
| `p3-get-genome-refseq-features` | genome IDs | RefSeq-annotated features |
| `p3-get-feature-data` | feature IDs | feature metadata |
| `p3-get-feature-sequence` | feature IDs | DNA or protein sequences (FASTA) |
| `p3-get-feature-subsystems` | feature IDs | subsystem memberships |
| `p3-get-feature-protein-regions` | feature IDs | protein domain regions |
| `p3-get-feature-protein-structures` | feature IDs | protein structures |
| `p3-get-features-in-regions` | genome IDs | features in coordinate ranges |
| `p3-get-features-by-sequence` | sequences | features with matching sequence (MD5) |
| `p3-get-family-data` | family IDs | protein family metadata |
| `p3-get-family-features` | family IDs | member features |
| `p3-get-sf-data` | sequence feature IDs | SF metadata |
| `p3-get-sf-variants` | sequence feature IDs | SF variants |
| `p3-get-subsystem-features` | subsystem IDs | member features |
| `p3-get-subsystem-roles` | subsystem IDs | role assignments |
| `p3-get-drug-genomes` | antibiotic names | resistant/susceptible genomes |
| `p3-get-taxonomy-data` | taxon IDs | taxonomy metadata |

### Search / Find
| Command | Description |
|---------|-------------|
| `p3-find-genomes` | Search genomes by arbitrary filters |
| `p3-find-features` | Search features by arbitrary filters |
| `p3-find-serology-data` | Search serology records |
| `p3-find-surveillance-data` | Search surveillance records |
| `p3-genus-species` | List genus/species pairs with genome counts |
| `p3-role-features` | Find features by functional role (product) |

### Data Manipulation (tab-delimited stdin → stdout)
| Command | Description |
|---------|-------------|
| `p3-echo` | Emit literal tab-delimited rows |
| `p3-head` | First N rows |
| `p3-tail` | Last N rows |
| `p3-count` | Count rows |
| `p3-sort` | Sort rows |
| `p3-match` | Filter rows matching a pattern |
| `p3-extract` | Select columns |
| `p3-join` | Join two files on a key column |
| `p3-merge` | Merge multiple files (union/intersection/difference) |
| `p3-collate` | First N rows per value of a key column |
| `p3-compare-cols` | Compare two columns |
| `p3-file-filter` | Keep rows whose key appears in a filter file |
| `p3-pick` | Randomly select N rows |
| `p3-pick-by-class` | Select rows matching a column value |
| `p3-pivot` | Pivot a table by key and value columns |
| `p3-shuffle` | Randomly shuffle rows |
| `p3-stats` | Mean/std/min/max/count for a numeric column |
| `p3-tbl-to-fasta` | Convert tab-delimited id+seq columns to FASTA |
| `p3-tbl-to-html` | Convert tab-delimited to an HTML table |
| `p3-fasta-md5` | Compute MD5 for each FASTA sequence |

### Job Submission (`p3-submit-*`)
| Command | Application |
|---------|-------------|
| `p3-submit-genome-annotation` | Genome annotation |
| `p3-submit-genome-assembly` | Genome assembly |
| `p3-submit-CGA` | Comprehensive Genome Analysis |
| `p3-submit-BLAST` | BLAST searches |
| `p3-submit-MSA` | Multiple sequence alignment (Muscle, Mafft, progressiveMauve) |
| `p3-submit-codon-tree` | Codon-based phylogenetic tree |
| `p3-submit-gene-tree` | Gene phylogeny |
| `p3-submit-comparative-systems` | Comparative systems analysis |
| `p3-submit-fastqutils` | FASTQ quality control, trimming, alignment |
| `p3-submit-rnaseq` | RNA-Seq expression analysis |
| `p3-submit-variation-analysis` | SNP/variant calling (BWA, Bowtie2, Snippy, …) |
| `p3-submit-metagenome-binning` | Metagenome binning |
| `p3-submit-metagenomic-read-mapping` | Metagenomic read mapping |
| `p3-submit-taxonomic-classification` | Taxonomic classification |
| `p3-submit-proteome-comparison` | Proteome comparison |
| `p3-submit-sars2-assembly` | SARS-CoV-2 assembly |
| `p3-submit-viral-assembly` | Viral genome assembly (IRMA) |
| `p3-submit-wastewater-analysis` | Wastewater surveillance |
| `p3-submit-SubspeciesClassification` | Viral subspecies classification |
| `p3-submit-influenza-treesort` | Influenza reassortment inference |
| `p3-submit-ha-subtype-conversion` | Influenza HA subtype numbering conversion |
| `p3-submit-sequence-submission` | Viral sequence validation and submission |
| `p3-submit-core-genome-MLST` | Core genome MLST |
| `p3-submit-whole-genome-SNP-analysis` | Whole genome SNP analysis |
| `p3-submit-docking` | Protein–ligand docking (DiffDock) |

### Job Monitoring
| Command | Description |
|---------|-------------|
| `p3-job-status` | List and check status of submitted jobs |

## Data Query Options

All `p3-all-*`, `p3-get-*`, and `p3-find-*` commands accept standard data query flags:

```
--eq field,value         equal filter
--ne field,value         not-equal filter
--gt field,value         greater-than filter
--lt field,value         less-than filter
--in field,v1,v2,...     in-list filter
--keyword phrase         keyword search
--attr field             select field (repeatable; default fields if omitted)
--limit N                maximum rows to return
--count                  return count only
--cursor                 deprecated no-op (pagination is always cursor-based)
--sort field             sort by field (prefix - for descending)
--max-retries N          retry failed API requests
--verbose                print retry messages
--user-agent UA          override HTTP User-Agent
--col N|name             input key column (for p3-get-* commands)
```

## Building from Source

### Prerequisites

- Go 1.24 or later
- Set PATH: `export PATH=/path/to/go/bin:$PATH`

### Build Commands

```bash
# Build all commands for the current platform
export PATH=/home/olson/P3/go-1.25.6/go/bin:$PATH
go build -buildvcs=false ./...      # verify everything compiles
make                                 # build to bin/

# Release builds (static, stripped, version stamped into the binaries)
VERSION=2.0.16 ./build-linux.sh      # Linux amd64 + arm64 + .deb
VERSION=2.0.16 ./build-macos.sh      # macOS Intel + Apple Silicon + universal
VERSION=2.0.16 ./build-windows.sh    # Windows x64 + ARM64

# Single command
go build -buildvcs=false -o bin/p3-all-genomes ./cmd/p3-all-genomes
```

> **Note:** use `-buildvcs=false` — the git+svn mix in the dev_container tree
> breaks VCS stamping otherwise. A plain `go build` stamps no version at all;
> use `make` or a build script when the reported version matters.

> **Note:** `build-macos.sh` clears all of `dist/`, while the other two clean
> only their own subdirectories — so run them in the order shown above, or
> build each platform into a separate `OUTPUT_DIR`.

Released artifacts are built by CI from a tag rather than by hand; see
[RELEASING.md](RELEASING.md).

### Running Tests

```bash
# Unit tests (no network required)
go test ./...

# Integration tests (requires a valid BV-BRC token and network)
BVBRC_TEST_INTEGRATION=1 go test -v -run TestSmoke ./...
```

## Distribution Packages

Build scripts produce packages in `dist/`:

| Platform | Package | Layout |
|----------|---------|--------|
| Linux x86_64 | `bvbrc-cli-VERSION-linux-amd64.tar.gz` | `<dir>/bin/p3-*`, `<dir>/bin/rast-*` |
| Linux ARM64 | `bvbrc-cli-VERSION-linux-arm64.tar.gz` | `<dir>/bin/p3-*`, `<dir>/bin/rast-*` |
| Linux x86_64 (Debian) | `bvbrc-cli_VERSION_amd64.deb` | `/usr/local/bin` |
| Linux ARM64 (Debian) | `bvbrc-cli_VERSION_arm64.deb` | `/usr/local/bin` |
| macOS Intel | `bvbrc-cli-VERSION-darwin-amd64.tar.gz` | `<dir>/bin/p3-*`, `<dir>/bin/rast-*` |
| macOS Apple Silicon | `bvbrc-cli-VERSION-darwin-arm64.tar.gz` | `<dir>/bin/p3-*`, `<dir>/bin/rast-*` |
| macOS universal | `bvbrc-cli-VERSION-darwin-universal.tar.gz` | `<dir>/bin/p3-*`, `<dir>/bin/rast-*` |
| macOS Intel (installer) | `bvbrc-cli-VERSION-intel.pkg` | `/usr/local/bin` |
| macOS Apple Silicon (installer) | `bvbrc-cli-VERSION-apple-silicon.pkg` | `/usr/local/bin` |
| Windows x64 | `bvbrc-cli-VERSION-windows-amd64.zip` | `<dir>\p3-*.exe`, `<dir>\rast-*.exe` |
| Windows ARM64 | `bvbrc-cli-VERSION-windows-arm64.zip` | `<dir>\p3-*.exe`, `<dir>\rast-*.exe` |
| Windows x64 (installer) | `bvbrc-cli-VERSION-windows-amd64-setup.exe` | `C:\Program Files\BVBRC`, added to `PATH` |

`<dir>` is `bvbrc-cli-VERSION-<platform>`; every *archive* expands into one, and
each also carries a generated `README` and the `LICENSE`. The three installers
(`.pkg`, `-setup.exe`) place the tools directly and need no extraction; none of
them is signed.

The `.deb` and the installers are built by the same CI run as the archives:
the `.pkg` pair on a macOS runner (`pkgbuild`/`productbuild` need macOS) and
the NSIS `-setup.exe` on a Windows runner.

The GitHub release additionally attaches `bvbrc-cli-VERSION-checksums.sha256`
covering all of the above, and Apptainer/Singularity images
(`bvbrc-cli-VERSION-<distro>-amd64.sif` for ubuntu-22, ubuntu-24 and rocky-9)
with the tools preinstalled in `/usr/local/bin`:

```bash
apptainer exec bvbrc-cli-2.0.16-ubuntu-24-amd64.sif p3-all-genomes --limit 1
```

Published packages are built by CI from a `v*` tag pushed to this repository —
see [RELEASING.md](RELEASING.md).

## Project Structure

```
BV-BRC-Go-SDK/
├── api/                    # Data API client (public)
│   ├── client.go           # HTTP client, pagination, cursor support
│   ├── query.go            # Query builder
│   ├── objects.go          # Object type aliases and default fields
│   └── validate.go         # ValidateGenomeIDs / RequireGenomeIDs
├── appservice/             # AppService client (public)
│   └── client.go
├── auth/                   # Authentication (public)
├── genomeannotation/       # GenomeAnnotation service client (public)
│   ├── client.go           # JSONRPC transport, CDMI_TIMEOUT, optional auth
│   └── methods.go          # Annotation steps; GTOs pass through as raw JSON
├── workspace/              # Workspace client (public)
│   └── validate.go         # RequireFolder (output-path existence check)
├── internal/
│   ├── cli/                # Shared CLI utilities (TabReader/Writer, options)
│   │   └── args.go         # NormalizePairedEndLibArgs (Perl dialect compat)
│   ├── rastcli/            # rast-* flags, IO and params (Perl CmdHelper.pm)
│   └── seq/                # FASTA reader/writer (60-column, gjoseqlib rules)
├── cmd/                    # CLI commands (one directory each)
├── test/
│   └── submit-suite/       # CLI integration test suite (reverse-engineers QA fixtures)
├── scripts/
│   └── benchmark-pagination.sh
├── PORT_STATUS.md          # Per-command p3_cli sync ledger
├── to-port                 # Remaining scripts not yet ported
├── dist/                   # Release artifacts (generated)
├── build-*.sh              # Platform build scripts
├── go.mod / go.sum
└── p3_test.go              # Integration tests (BVBRC_TEST_INTEGRATION=1)
```

## Relationship to `p3_cli`

This SDK is a **Go reimplementation of a curated subset** of the Perl `p3_cli`
scripts. It is not a full mirror — 101 of ~137 scripts are ported. The port
prioritises:
- Commands with wide user impact (data query, job submission)
- Commands that benefit from Go's single-binary distribution

**Not yet ported** (require `GenomeTypeObject` Perl library or per-sequence BLAST):
`p3-gto`, `p3-gto-dna`, `p3-gto-fasta`, `p3-gto-fetch`, `p3-gto-scan`,
`p3-feature-upstream`

See `PORT_STATUS.md` for the per-command sync ledger and stale-check commands.

## Using as a Library

```bash
go get github.com/BV-BRC/BV-BRC-Go-SDK/v2@v2.0.17
```

**The import path ends in `/v2`.** Go requires a module at major version 2 or
above to carry the major version in its path, so every import is
`github.com/BV-BRC/BV-BRC-Go-SDK/v2/<package>`.

> **Releases before v2.0.17 cannot be pinned.** The module was declared without
> the `/v2` suffix until then, so the proxy rejects those tags outright:
>
> ```
> $ go get github.com/BV-BRC/BV-BRC-Go-SDK@v2.0.16
> invalid version: module contains a go.mod file, so module path must
> match major version ("github.com/BV-BRC/BV-BRC-Go-SDK/v2")
> ```
>
> There is nothing to be done about those tags — the rejection is based on the
> `go.mod` recorded *in* each one. Use v2.0.17 or later. To build against
> unreleased work, `go get github.com/BV-BRC/BV-BRC-Go-SDK/v2@main` resolves to
> a pseudo-version of the current `main`.

`go install` works the same way, and is a supported way to get a single tool:

```bash
go install github.com/BV-BRC/BV-BRC-Go-SDK/v2/cmd/p3-ls@v2.0.17
```

A binary installed this way is stamped from its module version rather than by
the release build scripts; `p3-ls --version` reports it either way. For the
whole 138-tool suite, a release archive, the conda package or an installer
remains the easier route.

### Example: Query Genomes

```go
package main

import (
    "context"
    "fmt"
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/api"
)

func main() {
    client := api.NewClient()
    q := api.NewQuery().
        Eq("genus", "Yersinia").
        Select("genome_id", "genome_name", "genome_length").
        Limit(5)

    results, err := client.Query(context.Background(), "genome", q)
    if err != nil {
        panic(err)
    }
    for _, r := range results {
        fmt.Printf("%s  %s\n", r["genome_id"], r["genome_name"])
    }
}
```

### Example: Submit a Job

```go
package main

import (
    "fmt"
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/appservice"
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/auth"
)

func main() {
    token, _ := auth.GetToken()
    app := appservice.New(appservice.WithToken(token))

    params := map[string]interface{}{
        "output_path": "/user@patricbrc.org/home/results",
        "output_file": "my-assembly",
        "srr_ids":     []string{"SRR12345"},
        "recipe":      "unicycler",
    }

    task, err := app.StartApp2("GenomeAssembly2", params, appservice.StartParams{})
    if err != nil {
        panic(err)
    }
    fmt.Printf("Submitted job: %s\n", task.GetID())
}
```

### Example: Workspace Operations

```go
package main

import (
    "fmt"
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/auth"
    "github.com/BV-BRC/BV-BRC-Go-SDK/v2/workspace"
)

func main() {
    token, _ := auth.GetToken()
    ws := workspace.New(workspace.WithToken(token))

    entries, _ := ws.Ls("/user@patricbrc.org/home", false)
    for _, e := range entries {
        fmt.Printf("%s  %s\n", e.Type, e.Name)
    }
}
```

## Documentation

- BV-BRC Website: https://www.bv-brc.org
- CLI Tutorial: https://www.bv-brc.org/docs/cli_tutorial/
- Support: help@bv-brc.org

## License

MIT License — see LICENSE file for details.
