#!/bin/bash
# Build script for BV-BRC CLI tools - macOS installer

set -e

# Use GO env var if set, then the local dev path if present, then PATH fallback
GO="${GO:-/home/olson/P3/go-1.25.6/go/bin/go}"
command -v "$GO" &>/dev/null || GO=go
VERSION="${VERSION:-$(sh "$(dirname "$0")/scripts/version.sh")}"
# Stamp the version into the binaries; it is what the User-Agent reports.
LDFLAGS_VERSION="-X github.com/BV-BRC/BV-BRC-Go-SDK/version.Version=${VERSION}"
OUTPUT_DIR="dist"
PKG_ID="org.bvbrc.cli"

cd "$(dirname "$0")"
SDK_DIR="$(pwd)"

# Get list of all commands
COMMANDS=$(ls -d cmd/p3-*/ | xargs -n1 basename)

echo "Building BV-BRC CLI tools v${VERSION}"
echo "Commands to build: $(echo $COMMANDS | wc -w)"

# Clean and create output directories
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/darwin-amd64/bin"
mkdir -p "$OUTPUT_DIR/darwin-arm64/bin"
mkdir -p "$OUTPUT_DIR/darwin-universal/bin"

# Build for macOS Intel (amd64)
echo ""
echo "Building for macOS Intel (amd64)..."
for cmd in $COMMANDS; do
    echo "  $cmd"
    GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $GO build -buildvcs=false -ldflags="-s -w $LDFLAGS_VERSION" -o "$OUTPUT_DIR/darwin-amd64/bin/$cmd" "./cmd/$cmd"
done

# Build for macOS Apple Silicon (arm64)
echo ""
echo "Building for macOS Apple Silicon (arm64)..."
for cmd in $COMMANDS; do
    echo "  $cmd"
    GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $GO build -buildvcs=false -ldflags="-s -w $LDFLAGS_VERSION" -o "$OUTPUT_DIR/darwin-arm64/bin/$cmd" "./cmd/$cmd"
done

# Create universal binaries using lipo (if available)
if command -v lipo &> /dev/null; then
    echo ""
    echo "Creating universal binaries..."
    for cmd in $COMMANDS; do
        echo "  $cmd"
        lipo -create \
            "$OUTPUT_DIR/darwin-amd64/bin/$cmd" \
            "$OUTPUT_DIR/darwin-arm64/bin/$cmd" \
            -output "$OUTPUT_DIR/darwin-universal/bin/$cmd"
    done
    INSTALL_SRC="$OUTPUT_DIR/darwin-universal"
else
    echo ""
    echo "lipo not available - skipping universal binary creation"
    echo "Will create separate installers for each architecture"
fi

# Create tarball distributions
echo ""
echo "Creating distribution archives..."

cd "$OUTPUT_DIR"

# Each archive expands into a versioned directory containing bin/, README, and
# LICENSE (rather than a bare bin/).
stage_darwin() {
    local plat="$1"   # darwin-amd64 | darwin-arm64 | darwin-universal
    local stage="bvbrc-cli-${VERSION}-${plat}"
    rm -rf "$stage"
    mkdir -p "$stage"
    cp -R "${plat}/bin" "$stage/bin"
    bash "$SDK_DIR/scripts/make-readme.sh" "$VERSION" "$plat" > "$stage/README.md"
    cp "$SDK_DIR/LICENSE" "$stage/LICENSE"
    tar -czf "${stage}.tar.gz" "$stage"
    echo "  Created ${stage}.tar.gz"
}

stage_darwin darwin-amd64
stage_darwin darwin-arm64
if [ -d "darwin-universal/bin" ] && [ "$(ls -A darwin-universal/bin)" ]; then
    stage_darwin darwin-universal
fi

cd ..

echo ""
echo "Build complete!"
echo ""
echo "Distribution files in $OUTPUT_DIR/:"
ls -lh "$OUTPUT_DIR"/*.tar.gz
echo ""
echo "Installation instructions:"
echo "  tar -xzf bvbrc-cli-${VERSION}-darwin-<arch>.tar.gz"
echo "  sudo cp bin/p3-* /usr/local/bin/"
echo ""
echo "Or add the bin directory to your PATH:"
echo "  export PATH=\$PATH:\$(pwd)/bin"
