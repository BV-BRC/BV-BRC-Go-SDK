#!/bin/bash
# Install pre-built BV-BRC CLI binaries into the conda prefix.
#
# The release tarball is rooted at bin/, but conda-build flattens a source
# tarball whose contents are a single top-level directory: it strips the leading
# bin/ and the binaries land directly in the work dir. So depending on the
# layout the binaries are either in ./bin/ or in . — handle both.
#
# The toolkit is two command families, p3-* and rast-*. We install by those
# prefixes rather than everything in the directory because the flattened layout
# puts README.md and LICENSE alongside the binaries, and those do not belong in
# $PREFIX/bin.

set -euo pipefail

# PREFIXES must list every cmd/ name prefix; TestPackageFileListsCoverEveryFamily
# fails if a command is added that none of them matches.
PREFIXES=(p3 rast)

install -d "$PREFIX/bin"

find_src() {
    local dir
    for dir in bin .; do
        local prefix
        for prefix in "${PREFIXES[@]}"; do
            if compgen -G "$dir/$prefix-*" > /dev/null; then
                echo "$dir"
                return 0
            fi
        done
    done
    return 1
}

if ! src=$(find_src); then
    echo "ERROR: no BV-BRC CLI binaries found in source tarball (looked in ./bin and .)" >&2
    ls -la >&2
    exit 1
fi

count=0
for prefix in "${PREFIXES[@]}"; do
    if compgen -G "$src/$prefix-*" > /dev/null; then
        install -m 755 "$src/$prefix-"* "$PREFIX/bin/"
        count=$((count + $(ls -d "$src/$prefix-"* | wc -l)))
    fi
done

echo "Installed $count BV-BRC CLI tools to $PREFIX/bin/"
