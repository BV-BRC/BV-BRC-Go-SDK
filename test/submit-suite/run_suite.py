#!/usr/bin/env python3
"""Reverse-engineer p3-submit-* invocations from QA app-parameter fixtures and
verify the Go and Perl CLI front-ends reproduce them.

For each fixture under /vol/patric3/QA/applications/App-*/tests/*.json:
  1. Strip test-harness metadata; invert the params into a CLI invocation.
  2. If any param can't be expressed by the CLI  -> UNSUPPORTED (coverage gap).
  3. Otherwise run `p3-submit-<app> --dry-run <argv> <out-path> <out-name>` for
     each selected front-end and parse the emitted params JSON.
  4. PASS iff the front-ends ran and (when both selected) emit IDENTICAL params
     (the cross-check). The emitted-vs-reference diff is reported as INFO, since
     references legitimately carry app-spec defaults the CLI doesn't set.

Requires a valid BV-BRC token (run p3-login). Go stats ws: inputs over the
network, so the referenced QA workspace files must exist. This is an integration
suite, not hermetic.

  --live submits for real (no --dry-run) and reports the returned task id.
"""
import argparse
import json
import os
import re
import subprocess
import sys

import appmap
import compare
import inverters
import render

DEFAULT_QA_ROOT = "/vol/patric3/QA/applications"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_GO_BIN = os.path.normpath(os.path.join(HERE, "..", "..", "bin"))
DEFAULT_PERL_BIN = "/home/olson/P3/dev-ubuntu/bin"

YEAR_RE = re.compile(r"^\d{4}$")


def discover(qa_root, only_apps):
    """Yield (app_name, command, param_path) for each candidate fixture."""
    for entry in sorted(os.listdir(qa_root)):
        if not entry.startswith("App-"):
            continue
        app = entry[len("App-"):]
        if only_apps and app not in only_apps:
            continue
        command = appmap.command_for_app(app)
        tests = os.path.join(qa_root, entry, "tests")
        if not os.path.isdir(tests):
            continue
        for fn in sorted(os.listdir(tests)):
            if not fn.endswith(".json") or fn.endswith(".bak"):
                continue
            yield app, command, os.path.join(tests, fn)


def split_metadata(params):
    """Return (real_params, metadata) separating test-harness keys."""
    meta = {k: params[k] for k in params if k in inverters.METADATA_KEYS}
    real = {k: v for k, v in params.items() if k not in inverters.METADATA_KEYS}
    return real, meta


def extract_json(stdout):
    """Parse the first JSON object from a tool's stdout.

    Uses raw_decode so trailing text (e.g. Perl dry-run footer lines) after the
    closing brace is silently ignored — fixing false 'Extra data' parse errors.
    """
    idx = stdout.find("{")
    if idx < 0:
        raise ValueError("no JSON object in output")
    obj, _ = json.JSONDecoder().raw_decode(stdout, idx)
    return obj


USER_ENV = "/home/olson/P3/dev-ubuntu/user-env.sh"


def _env_with_user_env():
    """Return an environment with user-env.sh sourced (for Perl wrappers)."""
    import shlex
    try:
        result = subprocess.run(
            ["bash", "-c", f"source {USER_ENV} 2>/dev/null && env"],
            capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
        env = {}
        for line in result.stdout.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                env[k] = v
        return env or None
    except Exception:
        return None


_sourced_env = None  # cached


def run_tool(binpath, command, argv, out_path, out_name, dry_run, timeout):
    global _sourced_env
    exe = os.path.join(binpath, command)
    cmd = [exe]
    if dry_run:
        cmd.append("--dry-run")
    cmd += argv + [out_path, out_name]
    # For Perl wrappers, source user-env.sh so the correct data-API URL and
    # PERL5LIB are in effect. Go binaries are self-contained and don't need it.
    env = None
    if binpath == DEFAULT_PERL_BIN:
        if _sourced_env is None:
            _sourced_env = _env_with_user_env()
        env = _sourced_env
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                              stdin=subprocess.DEVNULL, env=env)
    except FileNotFoundError:
        return {"ok": False, "error": f"executable not found: {exe}", "cmd": cmd}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "timeout", "cmd": cmd}
    if proc.returncode != 0:
        return {"ok": False, "error": (proc.stderr or proc.stdout).strip(),
                "cmd": cmd, "rc": proc.returncode}
    return {"ok": True, "stdout": proc.stdout, "cmd": cmd}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tool", choices=["go", "perl", "both"], default="both")
    ap.add_argument("--apps", help="comma-separated App names to limit to")
    ap.add_argument("--qa-root", default=DEFAULT_QA_ROOT)
    ap.add_argument("--go-bin", default=DEFAULT_GO_BIN)
    ap.add_argument("--perl-bin", default=DEFAULT_PERL_BIN)
    ap.add_argument("--live", action="store_true",
                    help="actually submit (no --dry-run); reports task ids")
    ap.add_argument("--strict-ref", action="store_true",
                    help="treat emitted-vs-reference diffs as failures")
    ap.add_argument("--timeout", type=int, default=45,
                    help="per-invocation timeout in seconds (default 45). The "
                         "Perl tools validate genome IDs over the network and may "
                         "stall where the data API is unreachable.")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    only = set(args.apps.split(",")) if args.apps else None
    want_go = args.tool in ("go", "both")
    want_perl = args.tool in ("perl", "both")
    dry = not args.live

    counts = {}
    rows = []

    for app, command, path in discover(args.qa_root, only):
        rel = os.path.join(os.path.basename(os.path.dirname(os.path.dirname(path))),
                           "tests", os.path.basename(path))
        if command is None:
            rows.append((app, os.path.basename(path), "SKIP", "no submit tool"))
            counts["SKIP"] = counts.get("SKIP", 0) + 1
            continue
        try:
            with open(path) as fh:
                raw = json.load(fh)
        except (ValueError, OSError) as e:
            rows.append((app, os.path.basename(path), "SKIP", f"unreadable: {e}"))
            counts["SKIP"] = counts.get("SKIP", 0) + 1
            continue

        params, meta = split_metadata(raw)
        if "output_path" not in params or "output_file" not in params:
            rows.append((app, os.path.basename(path), "SKIP", "not a param file"))
            counts["SKIP"] = counts.get("SKIP", 0) + 1
            continue

        try:
            tokens, unsupported = inverters.invert(command, params)
        except Exception as e:  # malformed fixture shape -> report, don't crash
            rows.append((app, os.path.basename(path), "ERROR", f"invert: {e}"))
            counts["ERROR"] = counts.get("ERROR", 0) + 1
            continue
        xfail = bool(meta.get("failure_expected"))

        if unsupported:
            note = "cannot express: " + ", ".join(sorted(set(unsupported)))
            rows.append((app, os.path.basename(path), "UNSUPPORTED", note))
            counts["UNSUPPORTED"] = counts.get("UNSUPPORTED", 0) + 1
            if args.verbose:
                print(f"  UNSUPPORTED {rel}: {note}")
            continue

        out_path = params["output_path"]
        out_name = str(params["output_file"])
        results = {}
        if want_go:
            results["go"] = run_tool(args.go_bin, command,
                                     render.render(tokens, "go"),
                                     out_path, out_name, dry, args.timeout)
        if want_perl:
            results["perl"] = run_tool(args.perl_bin, command,
                                       render.render(tokens, "perl"),
                                       out_path, out_name, dry, args.timeout)

        # Tool execution errors.
        errored = [t for t, r in results.items() if not r["ok"]]
        if errored:
            detail = "; ".join(f"{t}: {results[t]['error'][:160]}" for t in errored)
            status = "XFAIL" if (xfail and args.live) else "ERROR"
            rows.append((app, os.path.basename(path), status, detail))
            counts[status] = counts.get(status, 0) + 1
            if args.verbose:
                for t in errored:
                    print(f"  {status} {rel} [{t}] cmd: {' '.join(results[t]['cmd'])}")
                    print(f"        {results[t]['error'][:400]}")
            continue

        if args.live:
            ids = []
            for t, r in results.items():
                m = re.search(r"id\s+(\S+)", r["stdout"])
                ids.append(f"{t}={m.group(1) if m else '?'}")
            rows.append((app, os.path.basename(path), "SUBMITTED", ", ".join(ids)))
            counts["SUBMITTED"] = counts.get("SUBMITTED", 0) + 1
            continue

        # Parse emitted params.
        emitted = {}
        parse_err = None
        for t, r in results.items():
            try:
                emitted[t] = extract_json(r["stdout"])
            except ValueError as e:
                parse_err = f"{t}: {e}"
        if parse_err:
            rows.append((app, os.path.basename(path), "ERROR", parse_err))
            counts["ERROR"] = counts.get("ERROR", 0) + 1
            continue

        notes = []
        status = "PASS"

        # Cross-check Go vs Perl.
        if want_go and want_perl:
            ok, diffs = compare.compare_strict(emitted["go"], emitted["perl"])
            if not ok:
                status = "MISMATCH"
                notes.append("go!=perl: " + "; ".join(diffs[:4]))

        # Reference comparison (informational unless --strict-ref).
        ref_side = emitted.get("go", emitted.get("perl"))
        ok, mism, info_only = compare.compare_subset(ref_side, params)
        if mism:
            tag = "ref-diff: " + "; ".join(mism[:4])
            notes.append(tag)
            if args.strict_ref and status == "PASS":
                status = "MISMATCH"
        elif info_only:
            notes.append("ref defaults added: " + ",".join(sorted(info_only)[:6]))

        rows.append((app, os.path.basename(path), status, " | ".join(notes)))
        counts[status] = counts.get(status, 0) + 1
        if args.verbose and (status != "PASS" or notes):
            print(f"  {status} {rel}: {' | '.join(notes)}")

    # Summary.
    print("\n=== Summary ===")
    width = max((len(a) for a, *_ in rows), default=10)
    for app, fn, status, note in rows:
        if status != "PASS" or args.verbose:
            print(f"  {status:12} {app:<{width}} {fn}  {note}")
    print("\n  counts: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    bad = counts.get("MISMATCH", 0) + counts.get("ERROR", 0)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
