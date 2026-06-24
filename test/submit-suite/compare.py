"""Semantic comparison of app-parameter JSON structures.

Two comparisons are needed by the suite:

  * Go-emitted vs Perl-emitted params  -- strict (they should be identical).
  * CLI-emitted vs reference params     -- tolerant (the CLI legitimately omits
    keys the backend fills from app-spec defaults, and may add a default the
    reference omitted).

Both run on a normalized form so key order, null/empty values, and
number-vs-numeric-string differences don't cause spurious mismatches.
"""


def _coerce_scalar(v):
    """Coerce numeric-looking strings to numbers so '100' == 100, '0.5' == 0.5."""
    if isinstance(v, str):
        s = v.strip()
        try:
            return int(s)
        except ValueError:
            pass
        try:
            return float(s)
        except ValueError:
            pass
        return v
    if isinstance(v, bool):
        # JSON true/false; keep as-is (bool is an int subclass, handle first).
        return v
    return v


def _is_empty(v):
    return v is None or v == "" or v == [] or v == {}


def normalize(value):
    """Recursively normalize a JSON value into a canonical, comparable form.

    - dicts: drop empty/null values, normalize remaining values, sort keys.
    - lists: normalize each element; if all elements are dicts, sort them by a
      stable JSON key so element order doesn't matter.
    - scalars: coerce numeric strings to numbers.
    """
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            nv = normalize(v)
            if _is_empty(nv):
                continue
            out[k] = nv
        return out
    if isinstance(value, list):
        items = [normalize(v) for v in value]
        items = [v for v in items if not _is_empty(v)]
        if items and all(isinstance(v, dict) for v in items):
            items = sorted(items, key=lambda d: _stable_key(d))
        return items
    return _coerce_scalar(value)


def _stable_key(obj):
    import json
    return json.dumps(obj, sort_keys=True, default=str)


def diff(expected, actual, path="$"):
    """Return a list of human-readable difference strings between two normalized
    structures. Empty list means equal."""
    diffs = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        for k in sorted(set(expected) | set(actual)):
            if k not in expected:
                diffs.append(f"{path}.{k}: only in actual = {actual[k]!r}")
            elif k not in actual:
                diffs.append(f"{path}.{k}: only in expected = {expected[k]!r}")
            else:
                diffs += diff(expected[k], actual[k], f"{path}.{k}")
    elif isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            diffs.append(f"{path}: list length {len(expected)} != {len(actual)}")
        else:
            for i, (e, a) in enumerate(zip(expected, actual)):
                diffs += diff(e, a, f"{path}[{i}]")
    else:
        if expected != actual:
            diffs.append(f"{path}: expected {expected!r} != actual {actual!r}")
    return diffs


def compare_strict(a, b):
    """Strict semantic equality. Returns (ok, diffs)."""
    na, nb = normalize(a), normalize(b)
    d = diff(na, nb)
    return (not d, d)


def compare_subset(emitted, reference):
    """Tolerant: every key the CLI emitted must match the reference. Reference
    keys absent from the emitted set are reported as INFO, not failure.

    Returns (ok, mismatches, info_only_keys).
    """
    ne, nr = normalize(emitted), normalize(reference)
    mismatches = []
    info_only = []
    if not isinstance(ne, dict) or not isinstance(nr, dict):
        return compare_strict(emitted, reference) + ([],)
    for k, ev in ne.items():
        if k not in nr:
            mismatches.append(f"$.{k}: emitted but not in reference = {ev!r}")
        else:
            mismatches += diff(nr[k], ev, f"$.{k}")
    for k in nr:
        if k not in ne:
            info_only.append(k)
    return (not mismatches, mismatches, info_only)
