#!/bin/bash
# Build an Apptainer/Singularity .sif image with the BV-BRC CLI toolkit
# (the p3-* Go tools) preinstalled.
#
# Usage:
#   ./build-apptainer.sh <distro>
#
#   <distro> is one of: ubuntu-22 | ubuntu-24 | rocky-9
#
# The CLI binaries are fully static (build-linux.sh uses CGO_ENABLED=0), so the
# container only needs to drop the release tarball's bin/ into /usr/local/bin and
# ensure a CA bundle is present (the static Go binary reads the system CA store to
# do TLS against the data API; Ubuntu base images ship without ca-certificates).
#
# Env vars:
#   VERSION              CLI version (default 1.0.0) — selects the dist tarball.
#   OUTPUT_DIR           where tarball lives and .def/.sif are written (default dist).
#   APPTAINER            apptainer/singularity executable (default apptainer).
#   APPTAINER_BUILD_ARGS args passed to `<APPTAINER> build` (default --fakeroot).
#                        In CI on ubuntu-latest fakeroot is configured by
#                        setup-apptainer. For a local build where fakeroot is not
#                        set up, run under sudo with APPTAINER_BUILD_ARGS= e.g.:
#                          APPTAINER_BUILD_ARGS= sudo -E ./build-apptainer.sh rocky-9

set -e

VERSION="${VERSION:-$(sh "$(dirname "$0")/scripts/version.sh")}"
OUTPUT_DIR="${OUTPUT_DIR:-dist}"
APPTAINER="${APPTAINER:-apptainer}"
APPTAINER_BUILD_ARGS="${APPTAINER_BUILD_ARGS---fakeroot}"

cd "$(dirname "$0")"
script_dir="$(pwd)"

usage() {
    echo "Usage: $0 <distro>" 1>&2
    echo "  distro: ubuntu-22 | ubuntu-24 | rocky-9" 1>&2
    exit 1
}

[[ $# -eq 1 ]] || usage
distro="$1"

case "$distro" in
    ubuntu-22) base_image="ubuntu:22.04"; family="debian" ;;
    ubuntu-24) base_image="ubuntu:24.04"; family="debian" ;;
    rocky-9)   base_image="rockylinux:9"; family="rhel" ;;
    *) echo "Unknown distro: $distro" 1>&2; usage ;;
esac

arch="amd64"
tarball="$script_dir/$OUTPUT_DIR/bvbrc-cli-${VERSION}-linux-${arch}.tar.gz"
def_file="$script_dir/$OUTPUT_DIR/bvbrc-cli-${VERSION}-${distro}-${arch}.def"
sif_file="$script_dir/$OUTPUT_DIR/bvbrc-cli-${VERSION}-${distro}-${arch}.sif"

# Ensure the Linux tarball exists. In CI it is downloaded as the linux-dist
# artifact before this runs; locally we build it on demand.
if [[ ! -f "$tarball" ]]; then
    echo "Tarball $tarball not found; running build-linux.sh ..."
    VERSION="$VERSION" bash "$script_dir/build-linux.sh"
fi
[[ -f "$tarball" ]] || { echo "Error: $tarball still missing after build" 1>&2; exit 1; }

mkdir -p "$script_dir/$OUTPUT_DIR"

echo "Generating $def_file ..."

# Distro-specific %post: install a CA bundle, then extract bin/ to /usr/local
# (-> /usr/local/bin/p3-*, already on PATH in all supported bases).
if [[ "$family" == "debian" ]]; then
    post_block=$(cat <<'EOF'
    export DEBIAN_FRONTEND=noninteractive
    apt-get -y update
    apt-get -y install --no-install-recommends ca-certificates
    apt-get clean && rm -rf /var/lib/apt/lists/*
    tar -xzf /opt/bvbrc-cli.tar.gz -C /usr/local --strip-components=1   # strip bvbrc-cli-<ver>-<plat>/ wrapper -> /usr/local/bin
    rm /opt/bvbrc-cli.tar.gz
EOF
)
else
    post_block=$(cat <<'EOF'
    dnf -y install ca-certificates || true
    dnf clean all
    tar -xzf /opt/bvbrc-cli.tar.gz -C /usr/local --strip-components=1   # strip bvbrc-cli-<ver>-<plat>/ wrapper -> /usr/local/bin
    rm /opt/bvbrc-cli.tar.gz
EOF
)
fi

cat > "$def_file" <<EOF
Bootstrap: docker
From: ${base_image}

%files
    ${tarball} /opt/bvbrc-cli.tar.gz

%post
${post_block}

%environment
    export LC_ALL=C

%runscript
    exec "\$@"

%labels
    Author BV-BRC
    Version ${VERSION}
    Distribution ${distro}
    Toolkit bvbrc-cli (p3-* tools)

%help
    BV-BRC CLI toolkit (p3-* commands) preinstalled in /usr/local/bin.
    Run a tool directly, e.g.:
        apptainer exec ${sif_file##*/} p3-login --help
EOF

echo "Building $sif_file ..."
"$APPTAINER" build $APPTAINER_BUILD_ARGS "$sif_file" "$def_file"

echo ""
echo "Done. Image: $sif_file"
echo "Test with: $APPTAINER exec $sif_file p3-login --help"
