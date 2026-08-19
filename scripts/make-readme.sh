#!/bin/bash
# Emit the human-readable README that ships inside each BV-BRC CLI distribution
# archive. Shared by build-linux.sh / build-macos.sh / build-windows.sh so the
# text stays consistent across platforms.
#
# Usage:
#   make-readme.sh <version> <platform>   # prints README to stdout
#
#   <platform> is the archive platform tag, e.g. linux-amd64, darwin-arm64,
#   darwin-universal, windows-amd64. Anything starting with "windows" gets the
#   Windows install/getting-started notes; everything else gets the Unix notes.

set -euo pipefail

version="${1:?usage: make-readme.sh <version> <platform>}"
platform="${2:?usage: make-readme.sh <version> <platform>}"

case "$platform" in
    windows*) os="windows" ;;
    *)        os="unix" ;;
esac

# Count the tools rather than hard-coding a number that goes quietly stale
# every time a command is added. nullglob matters under `set -e`: an unmatched
# glob left as a literal would be counted as one directory.
cmd_dir="$(dirname "$0")/../cmd"
shopt -s nullglob
p3_dirs=("$cmd_dir"/p3-*/)
rast_dirs=("$cmd_dir"/rast-*/)
shopt -u nullglob
p3_count=${#p3_dirs[@]}
rast_count=${#rast_dirs[@]}
total_count=$((p3_count + rast_count))

if [ "$rast_count" -gt 0 ]; then
    families="${total_count} command-line tools for working with
the Bacterial and Viral Bioinformatics Resource Center (BV-BRC): ${p3_count}
\`p3-*\` tools and ${rast_count} \`rast-*\` tools"
    rast_para="

The \`rast-*\` tools drive the BV-BRC genome annotation service. Each reads a
genome typed object as JSON, runs one annotation step, and writes the result, so
they are usually piped one into the next to build up an annotation."
else
    families="${p3_count} \`p3-*\` command-line tools for working with
the Bacterial and Viral Bioinformatics Resource Center (BV-BRC)"
    rast_para=""
fi

cat <<EOF
# BV-BRC Command Line Interface (CLI) Tools

**v${version} — ${platform}**

The BV-BRC CLI is a collection of ${families}.
The \`p3-*\` tools cover authentication, workspace management, data queries
against the BV-BRC data API, and submission and monitoring of analysis jobs.${rast_para}

The tools are statically linked and have no runtime dependencies.

## Package contents

EOF

if [ "$os" = "windows" ]; then
    cat <<'EOF'
    *.exe          the command-line tools
    install.bat    installer (run as Administrator)
    install.ps1    PowerShell installer (run as Administrator)
    README.txt     this file
    LICENSE        MIT license

## Installation (Windows)

Option 1 — installer (recommended):
    Right-click install.bat and "Run as administrator", or in an elevated
    PowerShell:
        .\install.ps1

Option 2 — manual:
    Copy the .exe files to a folder (e.g. C:\Program Files\BVBRC) and add that
    folder to your PATH.
EOF
else
    cat <<'EOF'
    bin/           the command-line tools
    README.md      this file
    LICENSE        MIT license

## Installation (Linux / macOS)

Option 1 — add the bin/ directory to your PATH:
    export PATH="$PWD/bin:$PATH"

Option 2 — install the tools system-wide:
    sudo cp bin/* /usr/local/bin/
EOF
fi

cat <<'EOF'

## Getting started

    p3-login YOUR_USERNAME    # authenticate; creates a login token
    p3-whoami                 # confirm you are logged in

    # Example query: list Brucella genomes
    p3-all-genomes --eq genus,Brucella --attr genome_id,genome_name | head

## Documentation

  CLI tutorial:  https://www.bv-brc.org/docs/cli_tutorial/index.html
  BV-BRC home:   https://www.bv-brc.org

## Support

  Help & documentation:  https://www.bv-brc.org/docs/
  Email:                 help@bv-brc.org
EOF
