# BV-BRC Go SDK

Go SDK and command-line tools for interacting with BV-BRC (Bacterial and Viral Bioinformatics Resource Center).

## Overview

This module provides:

1. **Go libraries** for programmatic access to BV-BRC services
2. **CLI tools** (42 commands) for command-line interaction

### Go Libraries

```go
import (
    "github.com/BV-BRC/BV-BRC-Go-SDK/api"        // Data API client
    "github.com/BV-BRC/BV-BRC-Go-SDK/appservice" // Job submission
    "github.com/BV-BRC/BV-BRC-Go-SDK/auth"       // Authentication
    "github.com/BV-BRC/BV-BRC-Go-SDK/workspace"  // Workspace/file operations
)
```

### CLI Tools
- `p3-login` - Log in to BV-BRC
- `p3-logout` - Log out from BV-BRC
- `p3-whoami` - Display current user information

### Workspace Operations
- `p3-ls` - List workspace contents
- `p3-cat` - Display file contents
- `p3-cp` - Copy files
- `p3-mkdir` - Create directories
- `p3-rm` - Remove files/directories

### Data Queries
- `p3-all-genomes` - Query all genomes
- `p3-all-features` - Query all features
- `p3-get-genome-data` - Get genome metadata
- `p3-get-feature-data` - Get feature metadata
- `p3-get-genome-features` - Get features for genomes
- `p3-get-feature-sequence` - Get feature sequences

### Data Processing
- `p3-echo` - Output tab-delimited data
- `p3-head` - Show first lines
- `p3-tail` - Show last lines
- `p3-count` - Count lines
- `p3-sort` - Sort data
- `p3-match` - Filter matching rows
- `p3-extract` - Extract columns
- `p3-join` - Join data files

### Job Submission (19 commands)
- `p3-submit-genome-annotation` - Genome annotation
- `p3-submit-genome-assembly` - Genome assembly
- `p3-submit-BLAST` - BLAST searches
- `p3-submit-MSA` - Multiple sequence alignment
- `p3-submit-codon-tree` - Codon tree analysis
- `p3-submit-gene-tree` - Gene tree analysis
- `p3-submit-rnaseq` - RNA-Seq analysis
- `p3-submit-variation-analysis` - Variant calling
- `p3-submit-metagenome-binning` - Metagenome binning
- `p3-submit-taxonomic-classification` - Taxonomic classification
- `p3-submit-proteome-comparison` - Proteome comparison
- `p3-submit-CGA` - Comprehensive Genome Analysis
- `p3-submit-comparative-systems` - Comparative systems
- `p3-submit-fastqutils` - FASTQ utilities
- `p3-submit-metagenomic-read-mapping` - Metagenomic read mapping
- `p3-submit-viral-assembly` - Viral genome assembly
- `p3-submit-sars2-assembly` - SARS-CoV-2 assembly
- `p3-submit-SubspeciesClassification` - Subspecies classification
- `p3-submit-wastewater-analysis` - Wastewater analysis

### Job Monitoring
- `p3-job-status` - Check job status

## Building

### Prerequisites

- Go 1.24 or later

### Build Commands

```bash
# Build all commands for current platform
./build-all.sh

# Build for specific platforms
./build-macos.sh      # macOS Intel + Apple Silicon
./build-linux.sh      # Linux x86_64 + ARM64 + .deb packages
./build-windows.sh    # Windows x64 + ARM64

# Build macOS .pkg installer structure
./build-macos-pkg.sh

# Set version (default: 1.0.0)
VERSION=1.2.3 ./build-linux.sh
```

## Distribution Packages

After running the build scripts, packages are created in `dist/`:

| Platform | Architecture | Package |
|----------|--------------|--------|
| macOS | Intel (x86_64) | `bvbrc-cli-VERSION-darwin-amd64.tar.gz` |
| macOS | Apple Silicon | `bvbrc-cli-VERSION-darwin-arm64.tar.gz` |
| Linux | x86_64 | `bvbrc-cli-VERSION-linux-amd64.tar.gz` |
| Linux | ARM64 | `bvbrc-cli-VERSION-linux-arm64.tar.gz` |
| Linux | x86_64 (Debian) | `bvbrc-cli_VERSION_amd64.deb` |
| Linux | ARM64 (Debian) | `bvbrc-cli_VERSION_arm64.deb` |
| Windows | x64 | `bvbrc-cli-VERSION-windows-amd64.tar.gz` |
| Windows | ARM64 | `bvbrc-cli-VERSION-windows-arm64.tar.gz` |

### Creating macOS .pkg Installer

The `.pkg` installer must be built on macOS:

```bash
# On Linux: prepare the package structure
./build-macos-pkg.sh

# Copy dist/ to a Mac, then run:
cd dist && ./build-pkg-on-macos.sh
```

This creates graphical installers:
- `bvbrc-cli-VERSION-intel.pkg`
- `bvbrc-cli-VERSION-apple-silicon.pkg`

## Installation

### macOS

**Option 1: Using tarball**
```bash
# For Intel Macs:
tar -xzf bvbrc-cli-1.0.0-darwin-amd64.tar.gz
sudo cp bin/p3-* /usr/local/bin/

# For Apple Silicon Macs:
tar -xzf bvbrc-cli-1.0.0-darwin-arm64.tar.gz
sudo cp bin/p3-* /usr/local/bin/
```

**Option 2: Using .pkg installer (if available)**
```bash
# Double-click the .pkg file or:
sudo installer -pkg bvbrc-cli-1.0.0-intel.pkg -target /
```

### Linux

**Option 1: Debian/Ubuntu (.deb package)**
```bash
sudo dpkg -i bvbrc-cli_1.0.0_amd64.deb
```

**Option 2: Using tarball**
```bash
tar -xzf bvbrc-cli-1.0.0-linux-amd64.tar.gz
sudo cp bin/p3-* /usr/local/bin/
```

**Option 3: Using install script**
```bash
./install-linux.sh
```

### Windows

**Option 1: Using installer script (recommended)**
1. Extract the archive
2. Right-click `install.bat` and select "Run as administrator"
3. Restart your command prompt

**Option 2: Using PowerShell**
```powershell
# Run as Administrator:
powershell -ExecutionPolicy Bypass -File install.ps1
```

**Option 3: Manual installation**
1. Extract the archive
2. Copy all `.exe` files to `C:\Program Files\BV-BRC\`
3. Add `C:\Program Files\BV-BRC` to your PATH environment variable
4. Restart your command prompt

## Getting Started

After installation:

```bash
# 1. Log in to BV-BRC
p3-login your-username

# 2. List your workspace
p3-ls /your-username@patricbrc.org/home

# 3. Query genomes
p3-all-genomes --eq genome_name,Escherichia | p3-head -n 10

# 4. Get help on any command
p3-login --help
p3-all-genomes --help
```

## Project Structure

```
BV-BRC-Go-SDK/
├── api/                    # Data API client (public)
│   ├── client.go
│   ├── query.go
│   └── objects.go
├── appservice/             # AppService client (public)
│   └── client.go
├── auth/                   # Authentication (public)
│   ├── token.go
│   └── login.go
├── workspace/              # Workspace client (public)
│   └── client.go
├── internal/               # Internal packages (not importable)
│   └── cli/                # CLI utilities
├── cmd/                    # CLI commands
│   ├── p3-login/
│   ├── p3-ls/
│   └── ...                 # 42 commands total
├── dist/                   # Distribution packages (generated)
├── build-*.sh              # Build scripts
├── go.mod
└── go.sum
```

## Development

### Building a single command

```bash
go build -o p3-ls ./cmd/p3-ls
```

### Running tests

```bash
go test ./...
```

### Adding a new command

1. Create a new directory under `cmd/`
2. Implement `main.go` using cobra for CLI parsing
3. Use packages from the SDK for API access
4. Add to build scripts if needed

## Using as a Library

Add the SDK to your Go project:

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
    query := api.NewQuery().
        Eq("genome_name", "Escherichia*").
        Select("genome_id", "genome_name", "contigs").
        Limit(10)

    results, err := client.Query(context.Background(), "genome", query)
    if err != nil {
        panic(err)
    }

    for _, genome := range results {
        fmt.Printf("%s: %s\n", genome["genome_id"], genome["genome_name"])
    }
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
    // Get authentication token
    token, _ := auth.GetToken()

    // Create workspace client
    ws := workspace.New(workspace.WithToken(token))

    // List directory contents
    entries, _ := ws.Ls("/user@patricbrc.org/home", false)
    for _, entry := range entries {
        fmt.Printf("%s  %s\n", entry.Type, entry.Name)
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
        "output_file": "my-annotation",
        "contigs":     "/user@patricbrc.org/home/genome.fasta",
    }

    task, err := app.StartApp2("GenomeAnnotation", params, appservice.StartParams{})
    if err != nil {
        panic(err)
    }

    fmt.Printf("Submitted job: %s\n", task.GetID())
}
```

## Documentation

- BV-BRC Website: https://www.bv-brc.org
- CLI Tutorial: https://www.bv-brc.org/docs/cli_tutorial/
- Support: help@bv-brc.org

## License

MIT License - see LICENSE file for details.
