# p3-submit-* CLI Test Suite

Reverse-engineers `p3-submit-*` invocations from the BV-BRC QA application
parameter fixtures and verifies that the Perl (`p3_cli`) and Go
(`BV-BRC-Go-SDK`) CLI front-ends reproduce them.

The QA fixtures under `/vol/patric3/QA/applications/App-*/tests/*.json` are the
`params` payloads the backend apps consume. The existing `test-common.sh` harness
runs the *backend* apps with those params; this suite instead tests the *CLI
front-end* by inverting each params file into the command-line flags a user would
type, then running the CLI in `--dry-run` and comparing what it emits.

## What it checks

For each fixture:

1. **Invert** the params into a CLI invocation (see `inverters.py`).
2. If a param cannot be expressed by the CLI → **UNSUPPORTED** (a coverage gap,
   not a failure — reported separately).
3. Otherwise run `p3-submit-<app> --dry-run <argv> <out-path> <out-name>` for each
   selected front-end and parse the emitted params JSON.
4. **PASS** iff the front-ends ran and — when both are selected — emit **identical**
   params (the Go-vs-Perl cross-check, which also validates the Go port). The
   emitted-vs-reference diff is reported as INFO (`ref-diff` / `ref defaults
   added`), since references legitimately carry app-spec defaults the CLI omits.

Result categories: `PASS`, `MISMATCH` (Go≠Perl, or `--strict-ref` ref diff),
`UNSUPPORTED`, `ERROR` (a tool failed/timed out/unparseable), `SKIP` (no submit
tool, or not a param file), `SUBMITTED` (under `--live`).

## Prerequisites

- **Auth token:** run `p3-login`. Dry-run needs it; the Go tools `Stat` `ws:`
  inputs over the network, so the referenced QA workspace files must exist. **This
  is an integration suite, not hermetic.**
- **Go binaries:** build them first —
  ```bash
  cd BV-BRC-Go-SDK
  export PATH=/home/olson/P3/go-1.25.6/go/bin:$PATH
  for d in cmd/p3-submit-*/; do go build -buildvcs=false -o "bin/$(basename $d)" "./$d"; done
  ```
  (`-buildvcs=false` avoids the git/svn VCS-stamp conflict in this tree.)
  Point `--go-bin` elsewhere if needed (default: `../../bin`).
- **Perl wrappers:** default `--perl-bin /home/olson/P3/dev-ubuntu/bin`. Newly
  added submit scripts only appear there after `make` in `p3_cli` (and
  re-sourcing `user-env.sh`); until then the Perl side reports `ERROR: executable
  not found` for those apps and only the Go side is checked.

## Usage

```bash
python3 run_suite.py                       # all mapped apps, both front-ends, dry-run
python3 run_suite.py --apps FastqUtils -v  # one app, verbose
python3 run_suite.py --tool go             # Go only (fast; skips Perl)
python3 run_suite.py --strict-ref          # treat ref diffs as failures too
python3 run_suite.py --timeout 20          # shorter per-call timeout
python3 run_suite.py --apps FastqUtils --live   # actually submit; prints task ids
```

Exit code is non-zero if any `MISMATCH` or `ERROR` occurred.

## Files

| File | Role |
|------|------|
| `run_suite.py` | Runner: discover fixtures, invert, execute, compare, report. |
| `inverters.py` | params → CLI tokens, per app. The core mapping logic. |
| `render.py` | Render tokens to argv per dialect (Go `f1,f2` vs Perl `f1 f2`). |
| `compare.py` | Semantic JSON normalize + strict / subset comparison. |
| `appmap.py` | App directory → submit command. Edit to add/disable apps. |

## Adding or fixing an app

1. Map the App dir in `appmap.py`.
2. Add a flag table in `inverters.FLAGS` (`param_key -> (kind, flag)`), and/or a
   custom inverter in `inverters.CUSTOM` for input-source selectors, read
   libraries, recipe→flag expansion, etc. Mark hardcoded/implicit keys in
   `inverters.IMPLICIT`. The authoritative flag names are the Go
   `cmd/p3-submit-*/main.go` definitions; the Perl `GetOptions` + `$params` hash
   is the source of truth for the option→param mapping.
3. Re-run `--apps <App> -v`. Leftover reference keys surface as UNSUPPORTED until
   mapped.

## Known caveats (environment / findings, not suite bugs)

- **Perl genome-ID validation can hang.** `p3-submit-*` Perl scripts validate
  `--reference-genome-id` (and similar) via the BV-BRC data API. Where that API
  is slow/unreachable, the Perl call stalls until `--timeout`. The Go tools do not
  validate, so they are fast — itself a Go/Perl behavioral difference worth noting.
- **Perl validates output-path existence; Go does not.** Some fixtures point at
  output paths that no longer exist → Perl `ERROR`, Go `PASS`.
- **QA fixtures predate the current CLI/param schema.** Many use richer or older
  keys (e.g. `fasta_keyboard_input`, `select_genomegroup`, per-library
  `sample_id`/`platform`, `progressiveMauve`) the current CLIs don't expose →
  `UNSUPPORTED`/`ERROR`. That output is a deliverable: it maps CLI coverage gaps
  and CLI/fixture drift.
- **`--live` does not poll job completion** (v1). It reports the returned task id.
