#!/bin/bash
# Install pre-built BV-BRC CLI binaries into the conda prefix.
#
# The release tarball is rooted at bin/p3-*, but conda-build flattens a source
# tarball whose contents are a single top-level directory: it strips the leading
# bin/ and the p3-* binaries land directly in the work dir. So depending on the
# layout the binaries are either in ./bin/ or in . — handle both.

set -euo pipefail

install -d "$PREFIX/bin"

if compgen -G "bin/p3-*" > /dev/null; then
    src="bin"
elif compgen -G "p3-*" > /dev/null; then
    src="."
else
    echo "ERROR: no p3-* binaries found in source tarball (looked in ./bin and .)" >&2
    ls -la >&2
    exit 1
fi

install -m 755 "$src"/p3-* "$PREFIX/bin/"

echo "Installed $(ls "$src"/p3-* | wc -l) BV-BRC CLI tools to $PREFIX/bin/"
