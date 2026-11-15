"""The `seq` expression namespace and the `seq_*` SQL functions."""

import io
import random

import polars as pl
import pytest
from polars.exceptions import ComputeError

import polars_bio as pb

FASTQ = "tests/data/io/fastq/example.fastq"

CODON_TABLES = [1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16]
CODON_TABLES += [21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]


def col(values, expr):
    return pl.DataFrame({"s": values}, schema={"s": pl.String}).select(expr)["s"]


def s():
    return pl.col("s")


# --- revcomp / complement -------------------------------------------------


def test_revcomp_on_an_arbitrary_frame():
    assert col(["ACGTN", None, "aacc"], s().seq.revcomp()).to_list() == [
        "NACGT",
        None,
        "ggtt",
    ]


@pytest.mark.parametrize(
    "seq, expected",
    [
        ("ACGTRYSWKMBDHVN", "NBDHVKMWSRYACGT"),  # every oracle tool agrees
        ("acgtrysw", "wsryacgt"),
        ("AC-G.T", "A.C-GT"),
        ("ACGZ", "ZCGT"),  # unknown bytes pass through, like seqfu/seqtk/Biopython
        ("ACGU", "ACGT"),
        ("AéC", "GéT"),
        ("", ""),
    ],
)
def test_revcomp_semantics(seq, expected):
    assert col([seq], s().seq.revcomp()).item() == expected


def test_rna_alphabet():
    assert col(["ACGU"], s().seq.revcomp(alphabet="rna")).item() == "ACGU"
    assert col(["acgu"], s().seq.complement(alphabet="RNA")).item() == "ugca"


def test_reverse_matches_str_reverse():
    values = ["II5#", "", None, "A\u00e9C\u20ac", "!" * 151]
    got = col(values, s().seq.reverse()).to_list()
    assert got == col(values, s().str.reverse()).to_list()
    assert got[0] == "#5II"


def test_complement():
    assert col(["ACGTZ-", None], s().seq.complement()).to_list() == ["TGCAZ-", None]


def test_strict_raises_and_later_expressions_still_work():
    with pytest.raises(ComputeError, match="'Z'"):
        col(["ACGZ"], s().seq.revcomp(strict=True))
    with pytest.raises(ComputeError, match="'-'"):
        col(["AC-G"], s().seq.complement(strict=True))
    assert col(["ACGT"], s().seq.revcomp(strict=True)).item() == "ACGT"


def test_revcomp_is_an_involution_without_uracil():
    rng = random.Random(1)
    seqs = [
        "".join(
            rng.choice("ACGTRYSWKMBDHVNacgtn-.XZ") for _ in range(rng.randint(0, 80))
        )
        for _ in range(500)
    ]
    assert col(seqs, s().seq.revcomp().seq.revcomp()).to_list() == seqs


# --- gc_content ---------------------------------------------------------------


@pytest.mark.parametrize(
    "seq, acgt, length",
    [  # seqkit fx2tab -g and -B GC, in percent
        ("GGNN", 100.0, 50.0),
        ("GG--", 100.0, 50.0),
        ("GGRR", 100.0, 50.0),
        ("SSAA", 0.0, 0.0),
        ("ggaa", 50.0, 50.0),
    ],
)
def test_gc_content_seqkit_probes(seq, acgt, length):
    assert col([seq], s().seq.gc_content(denominator="acgt")).item() * 100 == acgt
    assert col([seq], s().seq.gc_content()).item() * 100 == length


def test_gc_content_zero_denominator_is_null_and_filtered_out():
    out = col(["", "NNN", None, "GG"], s().seq.gc_content(denominator="acgt"))
    assert out.to_list() == [None, None, None, 1.0]
    assert out.dtype == pl.Float64
    df = pl.DataFrame({"s": ["GGG", "", "AAA"]})
    kept = df.filter(pl.col("s").seq.gc_content() > 0.3)
    assert kept["s"].to_list() == ["GGG"]


# --- translate ----------------------------------------------------------------


@pytest.mark.parametrize(
    "seq, kwargs, expected",
    [  # seqkit translate and Biopython agree on all of these
        ("ATGGCCTAAGC", {}, "MA*"),
        ("GCNAARTARNNNAAN", {}, "AK*XX"),
        ("atggcc", {}, "MA"),
        ("AGATGA", {"table": 2}, "*W"),
        ("ATG---GCC", {}, "M-A"),
        ("ATGGCCTAAGC", {"frame": -1}, "A*A"),
        ("ATGGCCTAAGC", {"to_stop": True}, "MA"),
        ("TAATAGTGA", {"table": 27}, "QQW"),
        # seqkit, not Biopython's B/Z/J
        ("RATSARMTT", {}, "XXX"),
        ("ATGA-GZZZ", {}, "MXX"),
    ],
)
def test_translate_probes(seq, kwargs, expected):
    assert col([seq], s().seq.translate(**kwargs)).item() == expected


def test_translate_matches_biopython():
    Seq = pytest.importorskip("Bio.Seq").Seq
    rng = random.Random(7)
    seqs = [
        "".join(
            rng.choice("ACGT" * 8 + "acgtNRYKM") for _ in range(rng.randint(0, 100))
        )
        for _ in range(60)
    ]

    def bio(seq, frame, table, to_stop):
        x = Seq(seq)
        if frame < 0:
            x = x.reverse_complement()
        x = x[abs(frame) - 1 :]
        x = x[: len(x) // 3 * 3]
        aa = str(x.translate(table=table, to_stop=to_stop))
        return aa.translate(str.maketrans("BZJ", "XXX"))

    for table in CODON_TABLES:
        for frame in (1, 2, 3, -1, -2, -3):
            # Biopython refuses to_stop for tables with dual stop/amino-acid codons.
            for to_stop in (False,) if table in (27, 28, 31) else (False, True):
                got = col(seqs, s().seq.translate(frame, table, to_stop)).to_list()
                want = [bio(x, frame, table, to_stop) for x in seqs]
                assert got == want, (frame, table, to_stop)


# --- mean_quality -------------------------------------------------------------


def test_mean_quality():
    out = col(["I#", "5I", "????", "", None], s().seq.mean_quality())
    assert [None if v is None else round(v, 2) for v in out] == [
        5.01,  # seqkit fx2tab -q
        22.97,
        30.0,
        None,
        None,
    ]
    assert col(["I#"], s().seq.mean_quality(method="arithmetic")).item() == 21.0
    assert (
        col(["hh"], s().seq.mean_quality(offset=64, method="arithmetic")).item() == 40.0
    )


def test_mean_quality_below_offset():
    with pytest.raises(ComputeError, match="below the Phred offset 33"):
        col(["II "], s().seq.mean_quality())


# --- argument and type errors ---------------------------------------------------


@pytest.mark.parametrize(
    "make, match",
    [
        (lambda: s().seq.revcomp(alphabet="protein"), "alphabet"),
        (lambda: s().seq.revcomp(strict=1), "strict"),
        (lambda: s().seq.gc_content(denominator="ignore_n"), "denominator"),
        (lambda: s().seq.translate(frame=0), "frame"),
        (lambda: s().seq.translate(table=7), "table"),
        (lambda: s().seq.translate(frame=True), "frame"),
        (lambda: s().seq.mean_quality(offset=300), "offset"),
        (lambda: s().seq.mean_quality(method="mean"), "method"),
    ],
)
def test_invalid_arguments(make, match):
    with pytest.raises(ValueError, match=match):
        make()


def test_non_string_input():
    with pytest.raises(pl.exceptions.PolarsError, match="(?i)str"):
        pl.DataFrame({"s": [1, 2]}).select(pl.col("s").seq.revcomp())


# --- composition --------------------------------------------------------------


def _gc_reference(seq):
    return sum(c in "GCgc" for c in seq) / len(seq) if seq else None


def test_scan_fastq_streaming():
    lf = pb.scan_fastq(FASTQ).with_columns(
        gc=pl.col("sequence").seq.gc_content(),
        rc=pl.col("sequence").seq.revcomp(),
        q=pl.col("quality_scores").seq.mean_quality(),
    )
    streamed = lf.collect(engine="streaming")
    in_memory = lf.collect()
    assert streamed.equals(in_memory)
    assert streamed["gc"].to_list() == [_gc_reference(x) for x in streamed["sequence"]]
    assert streamed.height > 0


def test_filter_on_seq_expression_through_pushdown():
    # The predicate translator cannot push a plugin call down, so the filter
    # must fall back to the client side and still return exactly the right rows.
    full = pb.scan_fastq(FASTQ).collect()
    expected = [
        n
        for n, x in zip(full["name"], full["sequence"])
        if (_gc_reference(x) or 0) > 0.5
    ]
    got = (
        pb.scan_fastq(FASTQ)
        .filter(pl.col("sequence").seq.gc_content() > 0.5)
        .select("name")
        .collect()
    )
    assert got["name"].to_list() == expected


def test_group_by_agg():
    df = pl.DataFrame({"k": ["a", "a", "b"], "s": ["GG", "AA", "GC"]})
    out = df.group_by("k").agg(pl.col("s").seq.gc_content().mean()).sort("k")
    assert out.to_dicts() == [{"k": "a", "s": 0.5}, {"k": "b", "s": 1.0}]


def test_serialize_round_trip():
    lf = pl.LazyFrame({"s": ["ACGTN", "GGCC"]}).select(
        rc=pl.col("s").seq.revcomp(), aa=pl.col("s").seq.translate(frame=-1)
    )
    data = lf.serialize()
    assert b"seq_revcomp" in data
    restored = pl.LazyFrame.deserialize(io.BytesIO(data))
    assert restored.collect().equals(lf.collect())


def test_streaming_large_frame_matches_in_memory():
    rng = random.Random(3)
    seqs = ["".join(rng.choice("ACGTN") for _ in range(150)) for _ in range(2000)]
    lf = pl.LazyFrame({"s": seqs * 200}).select(
        rc=pl.col("s").seq.revcomp(), gc=pl.col("s").seq.gc_content()
    )
    assert lf.collect(engine="streaming").equals(lf.collect())


# --- SQL ---------------------------------------------------------------------


def test_sql_matches_expressions():
    df = pl.DataFrame(
        {
            "s": ["ACGTN", "aacc", None, "ATGGCCTAAGC", "GGNN"],
            "q": ["IIIII", "I#I#", None, "???????????", "5555"],
        }
    )
    pb.from_polars("seq_sql_parity", df)
    got = pb.sql(
        "SELECT seq_revcomp(s) rc, seq_reverse(q) rq, seq_complement(s) c, "
        "seq_gc_content(s, 'acgt') gc, "
        "seq_translate(s, -1, 1, false) aa, seq_mean_quality(q) mq FROM seq_sql_parity"
    ).collect()
    want = df.select(
        rc=pl.col("s").seq.revcomp(),
        rq=pl.col("q").seq.reverse(),
        c=pl.col("s").seq.complement(),
        gc=pl.col("s").seq.gc_content(denominator="acgt"),
        aa=pl.col("s").seq.translate(frame=-1),
        mq=pl.col("q").seq.mean_quality(),
    )
    assert got.equals(want)


def test_builtin_translate_still_available():
    out = pb.sql("SELECT translate('ACGT', 'AT', 'TA') AS t").collect()
    assert out["t"].item() == "TCGA"
