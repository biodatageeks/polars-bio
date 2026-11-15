"""The `seq` expression namespace: sequence operations on String columns.

Importing `polars_bio` registers it, so every Polars expression gains
`.seq.revcomp()`, `.seq.reverse()`, `.seq.complement()`, `.seq.gc_content()`,
`.seq.translate()` and `.seq.mean_quality()`. They work on any String
column, not only on frames produced by polars-bio readers.

Each function is a native Polars expression plugin compiled into the
polars-bio extension module. It runs element-wise, in parallel and without
the GIL, and it streams with ``collect(engine="streaming")``. The ``seq_*``
SQL functions in :func:`polars_bio.sql` use the same kernels.

The semantics follow seqkit, seqfu, seqtk and Biopython:

- A null input gives null. A ratio with a zero denominator gives null, never
  NaN, so a filter such as ``gc > 0.3`` drops empty reads.
- ``revcomp`` and ``complement`` use the IUPAC nucleotide codes, preserve
  case, and copy any other byte (including the gaps ``-`` and ``.``)
  unchanged unless ``strict=True``.
"""

from pathlib import Path

import polars as pl
from polars.plugins import register_plugin_function

_LIB = Path(__file__).parent

_ALPHABETS = ("dna", "rna")
_DENOMINATORS = ("length", "acgt")
_QUALITY_METHODS = ("error_prob", "arithmetic")
_FRAMES = (1, 2, 3, -1, -2, -3)
# NCBI translation table ids; 7, 8 and 17-20 are unassigned.
_CODON_TABLES = (
    1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33,
)  # fmt: skip


def _choice(name, value, allowed):
    if not isinstance(value, str) or value.lower() not in allowed:
        raise ValueError(
            f"{name} must be one of {', '.join(map(repr, allowed))}, got {value!r}"
        )
    return value.lower()


def _flag(name, value):
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be True or False, got {value!r}")
    return value


def _int(name, value, allowed=None, lo=None, hi=None):
    ok = isinstance(value, int) and not isinstance(value, bool)
    if ok and allowed is not None:
        ok = value in allowed
    if ok and lo is not None and hi is not None:
        ok = lo <= value <= hi
    if not ok:
        if allowed is not None:
            expected = f"one of {', '.join(map(str, allowed))}"
        elif lo is not None and hi is not None:
            expected = f"an integer between {lo} and {hi}"
        else:
            expected = "an integer"
        raise ValueError(f"{name} must be {expected}, got {value!r}")
    return value


def _plugin(name, expr, **kwargs):
    return register_plugin_function(
        plugin_path=_LIB,
        function_name=f"seq_{name}",
        args=expr,
        kwargs=kwargs,
        is_elementwise=True,
    )


@pl.api.register_expr_namespace("seq")
class SequenceExpr:
    """Sequence operations, available as ``pl.col(...).seq``."""

    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def revcomp(self, alphabet: str = "dna", strict: bool = False) -> pl.Expr:
        """Reverse complement.

        Parameters:
            alphabet: ``"dna"`` pairs A with T (and U with A); ``"rna"`` pairs
                A with U.
            strict: Raise ``ComputeError`` on the first byte outside
                ``ACGTURYSWKMBDHVN`` (either case) instead of copying it.

        Example:
            ```python
            import polars as pl
            import polars_bio  # registers .seq

            pl.DataFrame({"s": ["ACGTN", None, "aacc"]}).select(
                pl.col("s").seq.revcomp()
            )  # ["NACGT", None, "ggtt"]
            ```
        """
        return _plugin(
            "revcomp",
            self._expr,
            alphabet=_choice("alphabet", alphabet, _ALPHABETS),
            strict=_flag("strict", strict),
        )

    def reverse(self) -> pl.Expr:
        """Reverse a string, for example a FASTQ quality alongside :meth:`revcomp`.

        Equivalent to ``str.reverse()`` for sequence and quality data, but
        much faster: it reverses ASCII bytes directly instead of Unicode
        grapheme clusters. A multi-byte character is kept whole.

        Example:
            ```python
            import polars as pl
            import polars_bio as pb  # registers .seq

            pb.scan_fastq("reads.fastq.gz").with_columns(
                sequence=pl.col("sequence").seq.revcomp(),
                quality_scores=pl.col("quality_scores").seq.reverse(),
            )
            ```
        """
        return _plugin("reverse", self._expr)

    def complement(self, alphabet: str = "dna", strict: bool = False) -> pl.Expr:
        """Complement, without reversing. Same rules as :meth:`revcomp`."""
        return _plugin(
            "complement",
            self._expr,
            alphabet=_choice("alphabet", alphabet, _ALPHABETS),
            strict=_flag("strict", strict),
        )

    def gc_content(self, denominator: str = "length") -> pl.Expr:
        """GC fraction between 0 and 1, as Float64.

        Counts G and C in either case (not S, like every reference tool).

        Parameters:
            denominator: ``"length"`` divides by the full sequence length in
                bytes (seqkit ``fx2tab -B GC``, seqkit ``stats``, seqfu);
                ``"acgt"`` divides by the count of A, C, G and T only
                (seqkit ``fx2tab -g``). A zero denominator gives null.
        """
        return _plugin(
            "gc_content",
            self._expr,
            denominator=_choice("denominator", denominator, _DENOMINATORS),
        )

    def translate(
        self, frame: int = 1, table: int = 1, to_stop: bool = False
    ) -> pl.Expr:
        """Translate nucleotides to amino acids.

        A trailing partial codon is dropped and stops are ``*``. An ambiguous
        codon gives an amino acid only when every base it can stand for
        agrees, otherwise ``X``. Three gap characters give ``-``; any other
        unreadable codon gives ``X``.

        Parameters:
            frame: 1, 2 or 3, or -1, -2, -3 to read the reverse complement.
            table: NCBI translation table id.
            to_stop: End the protein before the first stop codon.
        """
        return _plugin(
            "translate",
            self._expr,
            frame=_int("frame", frame, allowed=_FRAMES),
            table=_int("table", table, allowed=_CODON_TABLES),
            to_stop=_flag("to_stop", to_stop),
        )

    def mean_quality(self, offset: int = 33, method: str = "error_prob") -> pl.Expr:
        """Mean Phred quality of a quality string, as Float64.

        Parameters:
            offset: Phred offset, 33 (Sanger/Illumina 1.8+) or 64. A byte
                below it raises ``ComputeError``.
            method: ``"error_prob"`` averages the error probabilities and
                converts back, ``-10·log10(mean(10^(-Q/10)))``, as seqkit
                does; ``"arithmetic"`` averages the Phred scores, as FastQC
                does. An empty string gives null.
        """
        return _plugin(
            "mean_quality",
            self._expr,
            offset=_int("offset", offset, lo=0, hi=255),
            method=_choice("method", method, _QUALITY_METHODS),
        )
