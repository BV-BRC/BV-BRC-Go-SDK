#!/bin/sh
#
# Print the version stamped into the binaries (version.Version, which is also
# what the User-Agent reports) and used to name release artifacts:
#
#   $VERSION   wins if set  — the release workflow sets it from the pushed tag
#   2.0.12     when HEAD is exactly at a tag (a leading "v" is dropped)
#   68cffe6    otherwise: the short commit hash, "-dirty" if the tree is modified
#   unknown    outside a git checkout (e.g. an extracted source tarball)
#
set -eu

if [ -n "${VERSION:-}" ]; then
    printf '%s\n' "$VERSION"
    exit 0
fi

cd "$(dirname "$0")/.."

if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo unknown
    exit 0
fi

if tag=$(git describe --tags --exact-match 2>/dev/null); then
    printf '%s\n' "${tag#v}"
    exit 0
fi

if ! hash=$(git rev-parse --short HEAD 2>/dev/null); then
    echo unknown
    exit 0
fi

# Untracked files do not count: the release tarballs are built from a tree that
# always has some.
if ! git diff --quiet HEAD 2>/dev/null; then
    hash="$hash-dirty"
fi

printf '%s\n' "$hash"
