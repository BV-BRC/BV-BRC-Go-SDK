"""Render inverter tokens into a concrete argv for a given CLI dialect.

Both Go and Perl now accept the same two-argument form for paired-end libraries:
  --paired-end-lib read1.fq read2.fq

Go also accepts the original comma-joined form (--paired-end-lib f1,f2) via a
pre-processor in NormalizePairedEndLibArgs, but the two-argument form is
preferred as it matches Perl's ReadSpec =s{2} syntax.
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
            # Both dialects use the same two-argument form.
            argv += ["--paired-end-lib", tok[1], tok[2]]
        elif kind == "single":
            argv += ["--single-end-lib", tok[1]]
        elif kind == "srr":
            argv += ["--srr-id", tok[1]]
        else:
            raise ValueError(f"unknown token kind: {kind!r}")
    return argv
