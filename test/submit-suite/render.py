"""Render inverter tokens into a concrete argv for a given CLI dialect.

The only dialect difference today is the paired-end library:
  * Go expects one comma-joined value:  --paired-end-lib f1,f2
  * Perl expects two separate values:   --paired-end-lib f1 f2   (=s{2})
"""


def render(tokens, dialect):
    argv = []
    for tok in tokens:
        kind = tok[0]
        if kind == "opt":
            argv += [tok[1], tok[2]]
        elif kind == "flag":
            argv += [tok[1]]
        elif kind == "paired":
            if dialect == "perl":
                argv += ["--paired-end-lib", tok[1], tok[2]]
            else:
                argv += ["--paired-end-lib", f"{tok[1]},{tok[2]}"]
        elif kind == "single":
            argv += ["--single-end-lib", tok[1]]
        elif kind == "srr":
            argv += ["--srr-id", tok[1]]
        else:
            raise ValueError(f"unknown token kind: {kind!r}")
    return argv
