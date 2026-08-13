#!/bin/bash
# Update conda-recipe/meta.yaml with the sha256 checksums for a given release.
#
# Usage:
#   ./scripts/update-conda-recipe.sh <version>
#   ./scripts/update-conda-recipe.sh 2.1.0
#
# The script downloads the four platform tarballs from the GitHub release,
# computes their sha256 checksums, and patches meta.yaml in-place.
# Run this before tagging a release (if building locally) or let the CI
# conda-publish job call it automatically.

set -euo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>  (e.g. 2.1.0)" >&2
    exit 1
fi

# Download assets from whichever repo the release was published to. In CI,
# GITHUB_REPOSITORY is set automatically to the repo running the workflow (the
# same value the release job passes to `gh release upload --repo`), so the
# upload and download sides stay consistent whether the tag is pushed to
# BV-BRC/... or a fork (e.g. olsonanl/...). Falls back to the canonical repo
# for local/manual runs.
REPO="${GITHUB_REPOSITORY:-BV-BRC/BV-BRC-Go-SDK}"
BASE_URL="https://github.com/${REPO}/releases/download/v${VERSION}"
RECIPE="$(cd "$(dirname "$0")/.." && pwd)/conda-recipe/meta.yaml"

[ -f "$RECIPE" ] || { echo "ERROR: $RECIPE not found" >&2; exit 1; }

echo "Fetching checksums for v${VERSION} from GitHub..."

declare -A SHAS
PLATFORMS=(
    "linux-amd64:linux and x86_64"
    "linux-arm64:linux and aarch64"
    "darwin-amd64:osx and x86_64"
    "darwin-arm64:osx and arm64"
)

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

for entry in "${PLATFORMS[@]}"; do
    tarball_suffix="${entry%%:*}"
    selector="${entry##*:}"
    filename="bvbrc-cli-${VERSION}-${tarball_suffix}.tar.gz"
    url="${BASE_URL}/${filename}"

    echo -n "  ${filename} ... "
    if curl -fsSL --retry 3 -o "${TMPDIR}/${filename}" "${url}"; then
        sha=$(sha256sum "${TMPDIR}/${filename}" | cut -d' ' -f1)
        SHAS["${selector}"]="${sha}"
        echo "${sha}"
    else
        echo "FAILED (release not yet uploaded?)" >&2
        exit 1
    fi
done

echo
echo "Patching ${RECIPE}..."

# Update version
sed -i "s|environ.get(\"VERSION\", \"[^\"]*\")|environ.get(\"VERSION\", \"${VERSION}\")|" "$RECIPE"

# Update sha256 for each platform using the selector comment as the anchor
for entry in "${PLATFORMS[@]}"; do
    selector="${entry##*:}"
    sha="${SHAS["${selector}"]}"
    # Match the sha256 line that has the matching selector comment
    sed -i "s|^\(    sha256: \)[0-9a-f]*\(  # \[${selector}\]\)|\1${sha}\2|" "$RECIPE"
done

echo "Done. Updated conda-recipe/meta.yaml:"
grep -E "version|sha256|url" "$RECIPE" | head -20
