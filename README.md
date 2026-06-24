# BV-BRC Go SDK

Go SDK and command-line tools for interacting with BV-BRC (Bacterial and Viral
Bioinformatics Resource Center).

## Overview

This module provides:

1. **Go libraries** for programmatic access to BV-BRC services
2. **CLI tools** (101 commands) mirroring the Perl `p3_cli` suite

### Go Libraries

```go
import (
    "github.com/BV-BRC/BV-BRC-Go-SDK/api"        // Data API client
    "github.com/BV-BRC/BV-BRC-Go-SDK/appservice" // Job submission
    "github.com/BV-BRC/BV-BRC-Go-SDK/auth"       // Authentication
    "github.com/BV-BRC/BV-BRC-Go-SDK/workspace"  // Workspace/file operations
)
```

## Installation

### Linux

```bash
# x86_64
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v1.0.0/bvbrc-cli-1.0.0-linux-amd64.tar.gz
tar -xzf bvbrc-cli-1.0.0-linux-amd64.tar.gz
sudo cp bin/p3-* /usr/local/bin/

# ARM64 (e.g. Raspberry Pi, AWS Graviton)
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v1.0.0/bvbrc-cli-1.0.0-linux-arm64.tar.gz
tar -xzf bvbrc-cli-1.0.0-linux-arm64.tar.gz
sudo cp bin/p3-* /usr/local/bin/
```

### macOS

```bash
# Apple Silicon
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v1.0.0/bvbrc-cli-1.0.0-darwin-arm64.tar.gz
tar -xzf bvbrc-cli-1.0.0-darwin-arm64.tar.gz
sudo cp bin/p3-* /usr/local/bin/

# Intel
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v1.0.0/bvbrc-cli-1.0.0-darwin-amd64.tar.gz
tar -xzf bvbrc-cli-1.0.0-darwin-amd64.tar.gz
sudo cp bin/p3-* /usr/local/bin/
```

> **macOS note:** binaries are not yet code-signed. If Gatekeeper blocks them,
> run `xattr -dr com.apple.quarantine /usr/local/bin/p3-*` after copying, or
> extract with `curl` rather than a browser (browser downloads set the quarantine
> attribute; `curl`/`wget` do not).

### Windows

```powershell
# x64 (most PCs)
# Download bvbrc-cli-1.0.0-windows-amd64.zip from the releases page, then:
Expand-Archive bvbrc-cli-1.0.0-windows-amd64.zip .
# Copy the .exe files to a directory on your PATH, e.g.:
Copy-Item windows-amd64\p3-*.exe C:\Windows\System32\
p3-login
```

Or run the included install script as Administrator:
```powershell
powershell -ExecutionPolicy Bypass -File install.ps1
```

### Verification

```bash
# Linux / macOS: verify checksums
curl -LO https://github.com/BV-BRC/BV-BRC-Go-SDK/releases/download/v1.0.0/bvbrc-cli-1.0.0-checksums.sha256
sha256sum -c bvbrc-cli-1.0.0-checksums.sha256
```

```powershell
# Windows: verify checksum of a single file
Get-FileHash bvbrc-cli-1.0.0-windows-amd64.zip -Algorithm SHA256
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
--cursor                 cursor-based pagination (efficient for large result sets)
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

# Release builds (static, stripped)
VERSION=1.0.0 ./build-linux.sh       # Linux amd64 + arm64 + .deb
VERSION=1.0.0 ./build-macos.sh       # macOS Intel + Apple Silicon tarballs
VERSION=1.0.0 ./build-windows.sh     # Windows x64 + ARM64

# Single command
go build -buildvcs=false -o bin/p3-all-genomes ./cmd/p3-all-genomes
```

> **Note:** use `-buildvcs=false` — the git+svn mix in the dev_container tree
> breaks VCS stamping otherwise.

### Running Tests

```bash
# Unit tests (no network required)
go test ./...

# Integration tests (requires a valid BV-BRC token and network)
BVBRC_TEST_INTEGRATION=1 go test -v -run TestSmoke ./...
```

## Distribution Packages

Build scripts produce packages in `dist/`:

| Platform | Package | v1.0.0 SHA256 (first 16 chars) |
|----------|---------|-------------------------------|
| Linux x86_64 | `bvbrc-cli-VERSION-linux-amd64.tar.gz` | `5fd4d779ef72bb9a` |
| Linux ARM64 | `bvbrc-cli-VERSION-linux-arm64.tar.gz` | `adce7b4deb8b2553` |
| Linux x86_64 (Debian) | `bvbrc-cli_VERSION_amd64.deb` | — |
| Linux ARM64 (Debian) | `bvbrc-cli_VERSION_arm64.deb` | — |
| macOS Intel | `bvbrc-cli-VERSION-darwin-amd64.tar.gz` | `b268bae5ea1320a8` |
| macOS Apple Silicon | `bvbrc-cli-VERSION-darwin-arm64.tar.gz` | `2ea3165d25e0fceb` |
| Windows x64 | `bvbrc-cli-VERSION-windows-amd64.zip` | `d95f5cfd8936493f` |
| Windows ARM64 | `bvbrc-cli-VERSION-windows-arm64.zip` | `a24020ae8b2975c1` |

Full SHA256 checksums: `bvbrc-cli-VERSION-checksums.sha256`

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
├── workspace/              # Workspace client (public)
│   └── validate.go         # RequireFolder (output-path existence check)
├── internal/
│   └── cli/                # Shared CLI utilities (TabReader/Writer, options)
│       └── args.go         # NormalizePairedEndLibArgs (Perl dialect compat)
├── cmd/                    # 101 CLI commands (one directory each)
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
go get github.com/BV-BRC/BV-BRC-Go-SDK
```

### Example: Query Genomes

```go
package main

import (
    "context"
    "fmt"
    "github.com/BV-BRC/BV-BRC-Go-SDK/api"
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
    "github.com/BV-BRC/BV-BRC-Go-SDK/appservice"
    "github.com/BV-BRC/BV-BRC-Go-SDK/auth"
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
    "github.com/BV-BRC/BV-BRC-Go-SDK/auth"
    "github.com/BV-BRC/BV-BRC-Go-SDK/workspace"
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
