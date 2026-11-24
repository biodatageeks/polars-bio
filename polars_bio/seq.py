"""The `seq` expression namespace: sequence operations on String columns.

Importing `polars_bio` registers it, so every Polars expression gains the
`.seq` functions: reverse complement, translation (with Biopython's CDS
validation), transcription, GC content and skew, ORF finding, IUPAC motif
search, molecular weight, hashing, read quality summaries and quality
trimming. They work on any String column, not only on frames produced by
polars-bio readers. :func:`orfs` is the frame-level form of
:meth:`SequenceExpr.orfs`, ready for the range operations.

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

import os
from pathlib import Path
from typing import Optional, Union

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


_AMBIGUITY = ("x", "biopython")
_SEQ_TYPES = ("dna", "rna", "protein")
_STRANDS = ("both", "+", "-")
_START_CODONS = ("table", "atg")
_IUPAC = frozenset("ACGTURYSWKMBDHVNacgturyswkmbdhvn")


def _positive(name, value, allow_none=False):
    if value is None and allow_none:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return value


def _optional_int(name, value):
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer or None, got {value!r}")
    return value


def _zero_based(use_zero_based):
    """Resolve ``use_zero_based``; ``None`` reads the session option now."""
    if use_zero_based is None:
        from ._metadata import _get_global_zero_based

        return _get_global_zero_based()
    if not isinstance(use_zero_based, bool):
        raise ValueError(
            f"use_zero_based must be True, False or None, got {use_zero_based!r}"
        )
    return use_zero_based


def _orf_kwargs(
    table,
    min_len,
    strands,
    start_codons,
    allow_partial,
    include_protein,
    use_zero_based,
):
    if not isinstance(min_len, int) or isinstance(min_len, bool) or min_len < 0:
        raise ValueError(f"min_len must be a non-negative integer, got {min_len!r}")
    return dict(
        table=_int("table", table, allowed=_CODON_TABLES),
        min_len=min_len,
        strands=_choice("strands", strands, _STRANDS),
        start_codons=_choice("start_codons", start_codons, _START_CODONS),
        allow_partial=_flag("allow_partial", allow_partial),
        include_protein=_flag("include_protein", include_protein),
        zero_based=_zero_based(use_zero_based),
    )


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
        self,
        frame: int = 1,
        table: int = 1,
        to_stop: bool = False,
        *,
        stop_symbol: str = "*",
        cds: bool = False,
        ambiguous: str = "x",
    ) -> pl.Expr:
        """Translate nucleotides to amino acids.

        A trailing partial codon is dropped and stops are ``stop_symbol``. An
        ambiguous codon gives an amino acid only when every base it can stand
        for agrees, otherwise ``X``. Three gap characters give ``-``; any
        other unreadable codon gives ``X``.

        Parameters:
            frame: 1, 2 or 3, or -1, -2, -3 to read the reverse complement.
            table: NCBI translation table id.
            to_stop: End the protein before the first stop codon.
            stop_symbol: Text emitted for a stop codon; may be empty or longer
                than one character, as in Biopython.
            cds: Require a complete coding sequence, as Biopython's
                ``Seq.translate(cds=True)``: a start codon of the table first
                (emitted as ``M``), a length that is a multiple of three, one
                final stop (not emitted) and no other stop. An invalid row
                raises ``ComputeError``; use :meth:`cds_status` to find or
                filter those rows first.
            ambiguous: ``"x"`` (seqkit) or ``"biopython"``, which returns
                ``B`` for {D, N}, ``Z`` for {E, Q} and ``J`` for {I, L}, as
                Biopython does.

        Example:
            ```python
            cds = pb.scan_fasta("cds.fa").with_columns(
                status=pl.col("sequence").seq.cds_status(table=11)
            )
            proteins = cds.filter(pl.col("status").is_null()).with_columns(
                protein=pl.col("sequence").seq.translate(table=11, cds=True)
            )
            ```
        """
        if not isinstance(stop_symbol, str):
            raise ValueError(f"stop_symbol must be a string, got {stop_symbol!r}")
        return _plugin(
            "translate",
            self._expr,
            frame=_int("frame", frame, allowed=_FRAMES),
            table=_int("table", table, allowed=_CODON_TABLES),
            to_stop=_flag("to_stop", to_stop),
            stop_symbol=stop_symbol,
            cds=_flag("cds", cds),
            ambiguous=_choice("ambiguous", ambiguous, _AMBIGUITY),
        )

    def cds_status(self, frame: int = 1, table: int = 1) -> pl.Expr:
        """Why a sequence is not a complete CDS, or null if it is one.

        Null exactly when ``translate(frame, table, cds=True)`` would succeed
        (and for a null input). Otherwise the first failing check, in
        Biopython's order: ``"no_start_codon"``, ``"length_not_multiple_of_3"``,
        ``"no_stop_codon"`` or ``"internal_stop_codon"``.
        """
        return _plugin(
            "cds_status",
            self._expr,
            frame=_int("frame", frame, allowed=_FRAMES),
            table=_int("table", table, allowed=_CODON_TABLES),
        )

    def transcribe(self) -> pl.Expr:
        """DNA to RNA: ``T`` to ``U`` and ``t`` to ``u``, as Biopython's ``Seq.transcribe``."""
        return _plugin("transcribe", self._expr)

    def back_transcribe(self) -> pl.Expr:
        """RNA to DNA: ``U`` to ``T`` and ``u`` to ``t``, as Biopython's ``Seq.back_transcribe``."""
        return _plugin("back_transcribe", self._expr)

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

    def expected_errors(self, offset: int = 33) -> pl.Expr:
        """Expected number of errors of a read: the sum of ``10^(-Q/10)``.

        The quantity behind vsearch/USEARCH ``--fastq_maxee``; filter with
        ``expected_errors() <= 1.0``. An empty string gives 0. A byte below
        ``offset`` raises ``ComputeError``.
        """
        return _plugin(
            "expected_errors", self._expr, offset=_int("offset", offset, lo=0, hi=255)
        )

    def min_quality(self, offset: int = 33) -> pl.Expr:
        """Lowest Phred score of a quality string, as Int32 (null when empty)."""
        return _plugin(
            "min_quality", self._expr, offset=_int("offset", offset, lo=0, hi=255)
        )

    def max_quality(self, offset: int = 33) -> pl.Expr:
        """Highest Phred score of a quality string, as Int32 (null when empty)."""
        return _plugin(
            "max_quality", self._expr, offset=_int("offset", offset, lo=0, hi=255)
        )

    def orfs(
        self,
        table: int = 1,
        min_len: int = 30,
        strands: str = "both",
        start_codons: str = "table",
        allow_partial: bool = False,
        include_protein: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.Expr:
        """Open reading frames, as a list of structs per sequence.

        In each frame, an ORF runs from the first start codon after a stop to
        the next stop, so every stop yields at most one ORF, the longest.
        Every codon of the table's stop set ends an ORF (including the
        dual-meaning codons of tables 27, 28 and 31). A codon is a start or a
        stop only if every expansion of its IUPAC codes is one.

        Each struct has ``start``, ``end`` (byte offsets on the sequence as
        given, for either strand, including the stop codon), ``strand`` (``"+"`` or
        ``"-"``), ``frame`` (as in :meth:`translate`), ``aa_len`` (without the
        stop), ``has_start``, ``has_stop`` and, with ``include_protein``,
        ``protein`` (first codon as ``M``).

        Parameters:
            table: NCBI translation table id.
            min_len: Minimum length in amino acids (at least 1 is required).
            strands: ``"both"``, ``"+"`` or ``"-"``.
            start_codons: ``"table"`` (the table's start codons) or ``"ATG"``.
            allow_partial: Also report ORFs cut by a sequence end
                (``has_start`` or ``has_stop`` false). Never changes the
                complete ORFs.
            include_protein: Add the translated ``protein`` field.
            use_zero_based: 0-based half-open (True) or 1-based closed
                (False) coordinates; None uses the session setting at the
                time the expression is built.

        Use :func:`polars_bio.orfs` to get one row per ORF, tagged with its
        coordinate system for the range operations.
        """
        return _plugin(
            "orfs",
            self._expr,
            **_orf_kwargs(
                table,
                min_len,
                strands,
                start_codons,
                allow_partial,
                include_protein,
                use_zero_based,
            ),
        )

    def find_motif(
        self,
        pattern: str,
        both_strands: bool = False,
        overlapping: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.Expr:
        """Start positions of an IUPAC motif, as ``List[Int64]``.

        Like Biopython's ``nt_search``: ambiguity codes are expanded in the
        pattern only, so ``N`` in the sequence matches nothing. Unlike
        ``nt_search``, the sequence's case is ignored and ``U`` matches
        ``T``. With the default 1-based setting positions are
        ``nt_search``'s plus one. Positions are byte offsets, which equal
        character offsets for ASCII sequences.

        Parameters:
            pattern: IUPAC nucleotide codes (``ACGTURYSWKMBDHVN``).
            both_strands: Also search the reverse complement of the pattern;
                hits are reported once, at their leftmost position.
            overlapping: Report overlapping hits; otherwise resume after
                each hit (each orientation is scanned independently).
            use_zero_based: As in :meth:`orfs`.
        """
        if not isinstance(pattern, str) or not pattern or not set(pattern) <= _IUPAC:
            raise ValueError(
                "pattern must be a non-empty string of IUPAC nucleotide codes "
                f"(ACGTURYSWKMBDHVN), got {pattern!r}"
            )
        return _plugin(
            "find_motif",
            self._expr,
            pattern=pattern,
            both_strands=_flag("both_strands", both_strands),
            overlapping=_flag("overlapping", overlapping),
            zero_based=_zero_based(use_zero_based),
        )

    def gc_content_windows(
        self, window: int, step: Optional[int] = None, denominator: str = "length"
    ) -> pl.Expr:
        """GC content per window, as ``List[Float64]``.

        A window starts at every multiple of ``step`` (default ``window``)
        and is cut at the sequence end. Each value is :meth:`gc_content` of
        the window; a zero denominator gives a null element.
        """
        return _plugin(
            "gc_content_windows",
            self._expr,
            window=_positive("window", window),
            step=_positive("step", step, allow_none=True),
            denominator=_choice("denominator", denominator, _DENOMINATORS),
        )

    def gc_skew(self, window: int = 100, step: Optional[int] = None) -> pl.Expr:
        """GC skew ``(G - C) / (G + C)`` per window, as ``List[Float64]``.

        Matches Biopython's ``GC_skew(seq, window)`` (the default
        ``step = window``): the last window may be shorter, and a window with
        no G or C gives 0.
        """
        return _plugin(
            "gc_skew",
            self._expr,
            window=_positive("window", window),
            step=_positive("step", step, allow_none=True),
        )

    def molecular_weight(
        self,
        seq_type: str = "DNA",
        double_stranded: bool = False,
        circular: bool = False,
        monoisotopic: bool = False,
    ) -> pl.Expr:
        """Molecular weight, as Biopython's ``Bio.SeqUtils.molecular_weight``.

        Null for a sequence Biopython rejects (an ambiguous or unknown
        letter, such as ``N``). An empty sequence gives one water.
        """
        seq_type = _choice("seq_type", seq_type, _SEQ_TYPES)
        if _flag("double_stranded", double_stranded) and seq_type == "protein":
            raise ValueError("double_stranded must be False for seq_type='protein'")
        return _plugin(
            "molecular_weight",
            self._expr,
            seq_type=seq_type,
            double_stranded=double_stranded,
            circular=_flag("circular", circular),
            monoisotopic=_flag("monoisotopic", monoisotopic),
        )

    def hash(self, canonical: bool = False) -> pl.Expr:
        """Stable 64-bit hash (XXH3-64, seed 0) of the sequence bytes, as UInt64.

        With ``canonical``, the smaller of the hashes of the sequence and of
        its reverse complement, so both strands hash equally (``seqkit rmdup
        -s``). Case is not folded. The values never change between releases.
        With 64 bits, ``n`` distinct sequences collide with probability
        about ``n² / 2^65``.
        """
        return _plugin("hash", self._expr, canonical=_flag("canonical", canonical))

    def trim_quality(
        self,
        quality: Union[str, pl.Expr],
        *,
        leading: Optional[int] = None,
        trailing: Optional[int] = None,
        sliding_window: Optional[tuple] = None,
        offset: int = 33,
    ) -> pl.Expr:
        """Quality-trim reads as Trimmomatic's ``LEADING``, ``TRAILING`` and ``SLIDINGWINDOW``.

        Called on the sequence; ``quality`` is the quality column name or
        expression. Returns ``Struct{sequence, quality_scores}`` with both
        trimmed to the same span, so ``unnest`` restores the ``scan_fastq``
        columns. Steps run in the order leading, trailing, sliding window. A
        read trimmed away (dropped by Trimmomatic) gives two empty strings.
        As in Trimmomatic, an ``N`` base counts as quality 0.

        Parameters:
            leading: Remove bases from the start below this quality.
            trailing: Remove bases from the end below this quality.
            sliding_window: ``(size, quality)``; cut where the mean quality of
                a window falls below ``quality``.
            offset: Phred offset.
        """
        leading = _optional_int("leading", leading)
        trailing = _optional_int("trailing", trailing)
        window_size = window_quality = None
        if sliding_window is not None:
            if (
                not isinstance(sliding_window, (tuple, list))
                or len(sliding_window) != 2
                or not isinstance(sliding_window[1], (int, float))
                or isinstance(sliding_window[1], bool)
            ):
                raise ValueError(
                    f"sliding_window must be (size, quality), got {sliding_window!r}"
                )
            window_size = _positive("sliding_window size", sliding_window[0])
            window_quality = float(sliding_window[1])
        if leading is None and trailing is None and window_size is None:
            raise ValueError(
                "trim_quality needs at least one of leading, trailing and sliding_window"
            )
        quality = pl.col(quality) if isinstance(quality, str) else quality
        return register_plugin_function(
            plugin_path=_LIB,
            function_name="seq_trim_quality",
            args=[self._expr, quality],
            kwargs=dict(
                leading=leading,
                trailing=trailing,
                window_size=window_size,
                window_quality=window_quality,
                offset=_int("offset", offset, lo=0, hi=255),
            ),
            is_elementwise=True,
        )


_ORF_COLUMN = "__polars_bio_orf"
_ORF_FIELDS = ("start", "end", "strand", "frame", "aa_len", "has_start", "has_stop")


def orfs(
    source: Union[str, os.PathLike, pl.DataFrame, pl.LazyFrame],
    *,
    sequence_col: str = "sequence",
    table: int = 1,
    min_len: int = 30,
    strands: str = "both",
    start_codons: str = "table",
    allow_partial: bool = False,
    include_protein: bool = False,
    use_zero_based: Optional[bool] = None,
) -> pl.LazyFrame:
    """One row per open reading frame, ready for the range operations.

    The frame-level form of :meth:`SequenceExpr.orfs`. The result keeps every
    input column except the sequence column (for a FASTA path: ``name`` and
    ``description``), followed by the ORF fields as columns, and carries
    coordinate-system metadata, so :func:`polars_bio.overlap` and the other
    range operations accept it next to a tagged frame.

    Parameters:
        source: A FASTA path (read with ``scan_fasta``) or a Polars
            DataFrame or LazyFrame with a sequence column.
        sequence_col: The sequence column of a frame.
        use_zero_based: Coordinate system of the result; None uses the
            session setting at the time of the call.

    The other parameters are those of :meth:`SequenceExpr.orfs`.

    Example:
        ```python
        import polars_bio as pb

        found = pb.orfs("genome.fa", min_len=100)
        hits = pb.overlap(found, genes, cols1=["name", "start", "end"])
        ```
    """
    if isinstance(source, (str, os.PathLike)):
        from .io import IOOperations

        lf = IOOperations.scan_fasta(os.fspath(source))
    elif isinstance(source, pl.DataFrame):
        lf = source.lazy()
    elif isinstance(source, pl.LazyFrame):
        lf = source
    else:
        raise TypeError(
            "source must be a FASTA path or a Polars DataFrame or LazyFrame, "
            f"got {type(source).__name__}"
        )
    names = lf.collect_schema().names()
    if sequence_col not in names:
        raise ValueError(f"sequence_col {sequence_col!r} is not a column of source")
    fields = list(_ORF_FIELDS) + (["protein"] if include_protein else [])
    clashes = [c for c in names if c != sequence_col and c in fields]
    if clashes:
        raise ValueError(
            f"source columns {clashes} clash with the ORF fields {fields}; "
            "rename or drop them first"
        )
    zero_based = _zero_based(use_zero_based)
    found = pl.col(sequence_col).seq.orfs(
        table=table,
        min_len=min_len,
        strands=strands,
        start_codons=start_codons,
        allow_partial=allow_partial,
        include_protein=include_protein,
        use_zero_based=zero_based,
    )
    out = (
        lf.with_columns(found.alias(_ORF_COLUMN))
        .drop(sequence_col)
        .explode(_ORF_COLUMN)
        .drop_nulls(_ORF_COLUMN)
        .unnest(_ORF_COLUMN)
    )
    from ._metadata import set_coordinate_system

    set_coordinate_system(out, zero_based)
    return out
