# Releasing

**Pushing a `v*` tag to `BV-BRC/BV-BRC-Go-SDK` is the entire release
procedure.** `.github/workflows/release.yml` triggers on the tag push and
derives everything else from the tag name (`VERSION=${GITHUB_REF_NAME#v}`).
There is no manual artifact upload and no separate "publish" step.

## Where releases come from

Releases are cut from **`BV-BRC/BV-BRC-Go-SDK`** — this repository — as of
2026-08-18. Earlier releases (v2.0.2 through v2.0.13) were cut from the
`olsonanl/BV-BRC-Go-SDK` fork, which at the time was the only place the
`ANACONDA_API_TOKEN` secret existed; that is why this repository's tags jump
from v2.0.1 to v2.0.14.

The fork is now a development fork only, and its copy of this workflow is
**disabled** so that a stray tag there cannot republish. This matters because
both repositories publish to the same conda channel and the upload is
`--force`: two release runs for one version would silently overwrite each
other's packages.

**Never restart the version sequence and never reuse a version number.** The
sequence is a property of the product, not of the repository it was built in.

## Prerequisites (already in place; check if a run fails)

| requirement | where |
|---|---|
| `ANACONDA_API_TOKEN` with **both** `api:read` and `api:write`, held by an identity with upload rights to the `bv-brc` org | repo secret |
| Workflow permissions = **write** (`gh release create`/`upload` need it) | Settings → Actions → General |
| Actions enabled, `Release` workflow active | Settings → Actions |

A write-only anaconda token fails at the very last step, ~20 minutes in, after
everything else has succeeded.

## 1. Verify the commit you are about to tag

CI builds but does **not** run tests, so a broken commit ships. Tag a commit on
`main` — never a feature branch, which may be rebased or deleted out from under
the tag.

```bash
git checkout main && git pull
go build -buildvcs=false ./...
go test ./...
```

Optionally rehearse one platform locally; a broken `build-*.sh` is much cheaper
to find before a tag exists than after:

```bash
VERSION=2.0.14 ./build-linux.sh
go version -m dist/bvbrc-cli-2.0.14-linux-amd64/bin/p3-ls | grep ldflags
#   -X github.com/BV-BRC/BV-BRC-Go-SDK/version.Version=2.0.14
```

## 2. Tag and push

```bash
git tag v2.0.14 <commit>          # lightweight, matching the existing tags
git push origin v2.0.14
```

The tag name *is* the version: `v` + semver, or the glob does not match and
nothing runs. Do not create the GitHub Release by hand first — the workflow
creates it, and a **draft** release does not create the tag at all, so nothing
would trigger. Equally, wrapping a release around a tag that already exists
triggers nothing: the trigger is the tag push, not the release object.

The workflow file is read from the tagged commit, so tagging a commit older
than `release.yml` runs nothing.

## 3. Watch

```bash
gh run list --workflow release.yml --limit 3
gh run watch <id>
```

Six jobs: `build-linux` ∥ `build-macos` ∥ `build-windows` → `release` →
`build-apptainer` (ubuntu-22 / ubuntu-24 / rocky-9) ∥ `conda-publish`.

`conda-publish` runs last and independently of the GitHub Release, so if the
anaconda token is wrong the archives, `.deb`s and `.sif` images are still
published; fix the token and `gh run rerun --job <conda-job-id>`.

## 4. Verify the result

```bash
gh release view v2.0.14 --json isDraft,isPrerelease,assets \
  --jq '{isDraft, isPrerelease, assets: [.assets[].name]}'
```

Expect ten base assets (7 archives + 2 `.deb` + checksums) plus three `.sif`
images. The `release` job creates the release as a prerelease when absent, but
check the flag rather than assuming it — v2.0.13 came out as a full release
despite the `--prerelease` flag on the create path. Flip it with
`gh release edit v2.0.14 --prerelease=false` if needed.

Confirm the version actually got stamped, which is the failure most likely to
go unnoticed:

```bash
tar xzf bvbrc-cli-2.0.14-linux-amd64.tar.gz
./bvbrc-cli-2.0.14-linux-amd64/bin/p3-ls --version   # must print 2.0.14, not "unknown"
#   p3-ls 2.0.14
#   bvbrc-cli-go/2.0.14 linux/amd64 go1.25.6
go version -m ./bvbrc-cli-2.0.14-linux-amd64/bin/p3-ls | grep ldflags   # same, without running it
```

An unstamped binary reports `p3-ls unknown` and a User-Agent of
`bvbrc-cli-go/unknown` — right product, no version. And the conda side:

```bash
curl -s https://api.anaconda.org/package/bv-brc/bvbrc-cli | jq .latest_version
```

## If a release goes wrong

Deleting and re-pushing the same tag does re-trigger the build, but the conda
package for that version has already been published and would be
`--force`-overwritten, and anyone who fetched the old artifacts now has
different bytes under the same version. Burn the number and release the next
patch instead.

## Note on `go install`

`go install github.com/BV-BRC/BV-BRC-Go-SDK/cmd/p3-ls@v2.0.14` does **not**
work, from any repository: the module path has no `/v2` suffix, so the module
proxy rejects every `v2.x` tag ("module path must match major version"). The
release archives and the conda package are the distribution channels. Fixing
this would mean renaming the module to `…/v2` — which rewrites every import
path in the tree — or moving to a v1 line.
