"""Biopython-parity sequence operations (issue #512).

Translation options, CDS validation, transcription, read quality summaries,
ORFs, motif search, windowed GC, molecular weight, hashing and quality
trimming. Biopython is the oracle wherever it has the operation; the upstream
crate additionally checks the kernels against golden files from Biopython 1.88
and Trimmomatic 0.39.
"""

import io
import math
import random

import polars as pl
import pytest
from polars.exceptions import ComputeError

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system

ZERO_BASED = pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED
CODON_TABLES = [1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16]
CODON_TABLES += [21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]
BASES = "TCAG"
CODONS = [a + b + c for a in BASES for b in BASES for c in BASES]
IUPAC = "ACGTRYSWKMBDHVN"


def col(values, expr):
    return pl.DataFrame({"s": values}, schema={"s": pl.String}).select(expr)["s"]


def s():
    return pl.col("s")


def revcomp(seq):
    return seq[::-1].translate(str.maketrans("ACGTacgt", "TGCAtgca"))


@pytest.fixture
def zero_based_session():
    """Run a test with the session coordinate system set to 0-based."""
    before = pb.get_option(ZERO_BASED)
    pb.set_option(ZERO_BASED, True)
    yield
    pb.set_option(ZERO_BASED, before)


# --- translate: stop_symbol, ambiguous ---------------------------------------


def test_translate_defaults_are_unchanged():
    rng = random.Random(11)
    seqs = ["".join(rng.choice("ACGTNRYacgt-") for _ in range(60)) for _ in range(50)]
    for frame in (1, -2):
        plain = col(seqs, s().seq.translate(frame, 11, True))
        explicit = col(
            seqs,
            s().seq.translate(
                frame, 11, True, stop_symbol="*", cds=False, ambiguous="x"
            ),
        )
        assert plain.to_list() == explicit.to_list()


def test_stop_symbol():
    seq = ["ATGTAAGCCTGA", None]
    assert col(seq, s().seq.translate(stop_symbol="#")).to_list() == ["M#A#", None]
    assert col(seq, s().seq.translate(stop_symbol="")).to_list() == ["MA", None]
    assert col(seq, s().seq.translate(stop_symbol="**")).to_list() == ["M**A**", None]
    # to_stop still ends at the stop, whatever its symbol.
    assert col(seq, s().seq.translate(to_stop=True, stop_symbol="#"))[0] == "M"


def test_biopython_ambiguity_letters():
    seq = ["RATSARMTTWTT"]
    assert col(seq, s().seq.translate(ambiguous="biopython")).item() == "BZJX"
    assert col(seq, s().seq.translate()).item() == "XXXX"


def test_translate_matches_biopython_letter_for_letter():
    Seq = pytest.importorskip("Bio.Seq").Seq
    rng = random.Random(7)
    seqs = [
        "".join(
            rng.choice("ACGT" * 6 + "acgt" + IUPAC) for _ in range(rng.randint(0, 100))
        )
        for _ in range(60)
    ]

    def bio(seq, frame, table):
        x = Seq(seq)
        if frame < 0:
            x = x.reverse_complement()
        x = x[abs(frame) - 1 :]
        x = x[: len(x) // 3 * 3]
        return str(x.translate(table=table))

    letters = 0
    for table in CODON_TABLES:
        for frame in (1, 2, 3, -1, -2, -3):
            got = col(seqs, s().seq.translate(frame, table, ambiguous="biopython"))
            want = [bio(x, frame, table) for x in seqs]
            assert got.to_list() == want, (frame, table)
            letters += sum(c in "BZJ" for aa in want for c in aa)
    assert letters > 100  # the corpus does exercise B, Z and J


# --- translate(cds=True) and cds_status --------------------------------------

CDS_PROBES = [  # Biopython 1.88 Seq.translate(table=..., cds=True)
    ("ATGGCCTAA", 1, "MA"),
    ("GTGGCCTAA", 11, "MA"),
    ("TTGGCCTAA", 11, "MA"),
    ("CTGGCCTAA", 1, "MA"),
    ("ATTGCCTAA", 2, "MA"),
    ("ATGTAA", 1, "M"),
    ("atggcctaa", 1, "MA"),
    ("AUGGCCUAA", 1, "MA"),
    ("ATGGCCTAR", 1, "MA"),
    ("RTGGCCTAA", 11, "MA"),
    ("NTGGCCTAA", 11, "MA"),
    ("ATGNNNTAA", 1, "MX"),
    ("ATGTANTAA", 1, "MX"),
    ("ATG---TAA", 1, "M-"),
    ("ATGGCCTGA", 27, "MA"),
    ("ATGTGAGCCTGA", 27, "MWA"),
]


@pytest.mark.parametrize("seq, table, expected", CDS_PROBES)
def test_cds_probes(seq, table, expected):
    assert col([seq], s().seq.translate(table=table, cds=True)).item() == expected
    assert col([seq], s().seq.cds_status(table=table)).item() is None


def test_cds_with_biopython_letters_and_ignored_options():
    seq = ["ATGRATTAA"]
    assert col(seq, s().seq.translate(cds=True, ambiguous="biopython")).item() == "MB"
    # to_stop and stop_symbol change nothing for a valid CDS.
    out = col(["ATGGCCTAA"], s().seq.translate(to_stop=True, stop_symbol="#", cds=True))
    assert out.item() == "MA"


def test_cds_status_reports_the_first_failing_check():
    seqs = ["ATGGCCTAA", "GTGGCCTAA", "ATGGCCTA", "ATGGCCGCC", "ATGTAAGCCTAA", None]
    out = col(seqs, s().seq.cds_status())
    assert out.dtype == pl.String
    assert out.to_list() == [
        None,
        "no_start_codon",
        "length_not_multiple_of_3",
        "no_stop_codon",
        "internal_stop_codon",
        None,
    ]
    # The start-codon check comes before the length check, as in Biopython.
    assert col(["GTGGCCTA", "", "ATG"], s().seq.cds_status()).to_list() == [
        "no_start_codon",
        "no_start_codon",
        "no_stop_codon",
    ]


def test_cds_status_arguments_follow_translate():
    # TTAGGCCAC is the reverse complement of GTGGCCTAA: a CDS in table 11 only.
    seq = ["TTAGGCCAC"]
    assert col(seq, s().seq.cds_status(-1, 11)).item() is None
    assert col(seq, s().seq.cds_status(frame=-1, table=1)).item() == "no_start_codon"
    assert col(seq, s().seq.translate(-1, 11, cds=True)).item() == "MA"


def test_invalid_cds_raises_and_later_expressions_still_work():
    with pytest.raises(ComputeError, match="in-frame stop codon 'TAA'") as err:
        col(["ATGGCCTAA", "ATGTAAGCCTAA"], s().seq.translate(cds=True))
    assert "cds_status" in str(err.value)
    with pytest.raises(ComputeError, match="first codon 'GTG' is not a start codon"):
        col(["GTGGCCTAA"], s().seq.translate(cds=True))
    assert col(["ATGGCCTAA"], s().seq.translate(cds=True)).item() == "MA"


def _cds_corpus(rng, table):
    """Valid CDSs and one mutation of each kind, for one NCBI table."""
    CodonTable = pytest.importorskip("Bio.Data.CodonTable")
    t = CodonTable.unambiguous_dna_by_id[table]
    sense = [c for c in CODONS if c not in t.stop_codons]
    out = ["", "A", "ATG", "ATGTAA"]
    for _ in range(12):
        body = [rng.choice(sense) for _ in range(rng.randint(0, 15))]
        cds = rng.choice(t.start_codons) + "".join(body) + rng.choice(t.stop_codons)
        n = len(cds)
        out += [
            cds,
            rng.choice([c for c in sense if c not in t.start_codons]) + cds[3:],
            cds[: n - rng.choice([1, 2])],
            cds[: n - 3] + rng.choice(sense),
            cds.lower(),
            cds.replace("T", "U"),
        ]
        if body:
            k = 3 * rng.randint(1, len(body))
            out.append(cds[:k] + rng.choice(t.stop_codons) + cds[k:])
        for pos in (rng.randrange(n), rng.randrange(3), n - 1 - rng.randrange(3)):
            out.append(cds[:pos] + rng.choice("RYSWKMN") + cds[pos + 1 :])
    return out


@pytest.mark.parametrize("table", [1, 2, 4, 11, 25, 27, 28, 31])
def test_cds_matches_biopython(table):
    Seq = pytest.importorskip("Bio.Seq").Seq
    checks = {
        "First codon": "no_start_codon",
        "Sequence length": "length_not_multiple_of_3",
        "Final codon": "no_stop_codon",
        "Extra in frame stop codon": "internal_stop_codon",
    }
    seqs = _cds_corpus(random.Random(table), table)
    want_status, want_protein = [], []
    for seq in seqs:
        try:
            want_protein.append(str(Seq(seq).translate(table=table, cds=True)))
            want_status.append(None)
        except Exception as e:  # Bio.Data.CodonTable.TranslationError
            reason = [v for k, v in checks.items() if str(e).startswith(k)]
            assert reason, e
            want_status.append(reason[0])
    assert len(want_protein) >= 10
    # Tables 27, 28 and 31 have no codon that is only a stop, so no CDS of
    # theirs can fail the in-frame stop check.
    expected_failures = {"no_start_codon", "length_not_multiple_of_3", "no_stop_codon"}
    if table not in (27, 28, 31):
        expected_failures.add("internal_stop_codon")
    assert expected_failures <= set(want_status)

    df = pl.DataFrame({"s": seqs}).with_columns(status=s().seq.cds_status(table=table))
    assert df["status"].to_list() == want_status
    # Filter, then translate: never raises, and equals Biopython.
    proteins = df.filter(pl.col("status").is_null()).select(
        s().seq.translate(table=table, cds=True, ambiguous="biopython")
    )
    assert proteins["s"].to_list() == want_protein


# --- transcribe ------------------------------------------------------------------


def test_transcribe_and_back():
    assert col(["ACGTacgtN-U", None], s().seq.transcribe()).to_list() == [
        "ACGUacguN-U",
        None,
    ]
    assert col(["ACGUacgu"], s().seq.back_transcribe()).item() == "ACGTacgt"


def test_transcribe_matches_biopython():
    Seq = pytest.importorskip("Bio.Seq").Seq
    rng = random.Random(5)
    seqs = [
        "".join(rng.choice("ACGTUacgtuNRY-.") for _ in range(40)) for _ in range(80)
    ]
    assert col(seqs, s().seq.transcribe()).to_list() == [
        str(Seq(x).transcribe()) for x in seqs
    ]
    assert col(seqs, s().seq.back_transcribe()).to_list() == [
        str(Seq(x).back_transcribe()) for x in seqs
    ]


# --- expected_errors, min_quality, max_quality ------------------------------------


def test_expected_errors():
    out = col(["I#", "", None, "!"], s().seq.expected_errors())
    assert out.dtype == pl.Float64
    assert out.to_list() == [pytest.approx(1e-4 + 10**-0.2, rel=1e-15), 0.0, None, 1.0]
    assert col(["hh"], s().seq.expected_errors(offset=64)).item() == 2e-4


def test_expected_errors_matches_the_definition():
    rng = random.Random(9)
    quals = [
        "".join(chr(33 + rng.randint(0, 41)) for _ in range(rng.randint(0, 150)))
        for _ in range(200)
    ]
    got = col(quals, s().seq.expected_errors()).to_list()
    want = [math.fsum(10 ** (-(ord(c) - 33) / 10) for c in q) for q in quals]
    assert got == pytest.approx(want, rel=1e-12)
    # The vsearch/USEARCH --fastq_maxee rule keeps reads with EE <= threshold.
    kept = pl.DataFrame({"s": quals}).filter(s().seq.expected_errors() <= 1.0)
    assert kept["s"].to_list() == [q for q, ee in zip(quals, want) if ee <= 1.0]


def test_min_and_max_quality():
    quals = ["I5#", "", None, "5"]
    lowest = col(quals, s().seq.min_quality())
    highest = col(quals, s().seq.max_quality())
    assert lowest.dtype == pl.Int32 and highest.dtype == pl.Int32
    assert lowest.to_list() == [2, None, None, 20]
    assert highest.to_list() == [40, None, None, 20]
    assert col(["hB"], s().seq.max_quality(offset=64)).item() == 40


@pytest.mark.parametrize("method", ["expected_errors", "min_quality", "max_quality"])
def test_quality_byte_below_offset(method):
    with pytest.raises(ComputeError, match="below the Phred offset 33"):
        col(["II "], getattr(s().seq, method)())


# --- orfs ------------------------------------------------------------------------

ORF_FIELDS = {
    "start": pl.Int64,
    "end": pl.Int64,
    "strand": pl.String,
    "frame": pl.Int32,
    "aa_len": pl.Int64,
    "has_start": pl.Boolean,
    "has_stop": pl.Boolean,
}


def orfs(seq, **kwargs):
    """ORFs of one sequence as (start, end, strand, frame, aa_len, ...) tuples."""
    kwargs.setdefault("min_len", 1)
    kwargs.setdefault("use_zero_based", True)
    out = col([seq], s().seq.orfs(**kwargs)).item()
    return [tuple(o.values()) for o in out]


def test_orfs_type_and_shape():
    out = col(["ATGGCCTAA", "GGGGGG", None], s().seq.orfs(min_len=1))
    assert out.dtype == pl.List(pl.Struct(ORF_FIELDS))
    assert out.list.len().to_list() == [1, 0, None]
    with_protein = col(["ATGGCCTAA"], s().seq.orfs(min_len=1, include_protein=True))
    assert with_protein.dtype == pl.List(
        pl.Struct({**ORF_FIELDS, "protein": pl.String})
    )


def test_orfs_default_min_len_is_30_amino_acids():
    short = "ATG" + "GCC" * 28 + "TAA"  # 29 amino acids
    long = "ATG" + "GCC" * 29 + "TAA"  # 30 amino acids
    lengths = col([short, long], s().seq.orfs(strands="+")).list.len().to_list()
    assert lengths == [0, 1]


def test_one_orf_per_stop_from_the_first_start():
    assert orfs("ATGAAAATGCCCTAA", start_codons="ATG", include_protein=True) == [
        (0, 15, "+", 1, 4, True, True, "MKMP")
    ]


def test_orf_on_the_reverse_strand():
    forward = orfs("ATGGCCTAA", start_codons="ATG", include_protein=True)
    assert forward == [(0, 9, "+", 1, 2, True, True, "MA")]
    assert orfs(revcomp("ATGGCCTAA"), start_codons="ATG", include_protein=True) == [
        (0, 9, "-", -1, 2, True, True, "MA")
    ]
    # Flanked: the interval is on the sequence as given.
    assert orfs("CTTAGGCCATGG", start_codons="ATG") == [(1, 10, "-", -3, 2, True, True)]
    assert orfs("ATGGCCTAA", start_codons="ATG", strands="-") == []


def test_orf_coordinates_follow_use_zero_based():
    zero = orfs("CCATGGCCTAAC", start_codons="ATG", use_zero_based=True)
    one = orfs("CCATGGCCTAAC", start_codons="ATG", use_zero_based=False)
    assert zero == [(2, 11, "+", 3, 2, True, True)]
    assert one == [(3, 11, "+", 3, 2, True, True)]  # same end, start + 1


def test_orf_start_codons():
    assert orfs("CTGGCCTAA", include_protein=True) == [
        (0, 9, "+", 1, 2, True, True, "MA")  # CTG starts table 1 and reads as M
    ]
    assert orfs("CTGGCCTAA", start_codons="ATG") == []


def test_dual_meaning_stop_codons_end_orfs():
    assert orfs("ATGGCCTGAGCC", table=27) == [(0, 9, "+", 1, 2, True, True)]


def test_partial_orfs_are_only_added():
    kwargs = dict(start_codons="ATG", strands="+", include_protein=True)
    # The sequence begins inside an ORF: no start codon before the first stop.
    assert orfs("GCCGCCTAA", **kwargs) == []
    partial = orfs("GCCGCCTAA", allow_partial=True, **kwargs)
    assert (0, 9, "+", 1, 2, False, True, "AA") in partial
    # A start codon without a stop after it.
    assert (0, 9, "+", 1, 3, True, False, "MAA") in orfs(
        "ATGGCCGCCGC", allow_partial=True, **kwargs
    )
    # A complete ORF is reported unchanged, not extended to the sequence start.
    frame1 = [
        o for o in orfs("GCCATGGCCTAA", allow_partial=True, **kwargs) if o[3] == 1
    ]
    assert frame1 == [(3, 12, "+", 1, 2, True, True, "MA")]


def test_partial_output_contains_the_default_output():
    rng = random.Random(21)
    for _ in range(40):
        seq = "".join(
            rng.choice(["ATG", "TAA", "TGA", "A", "C", "G", "T"]) for _ in range(40)
        )
        complete = orfs(seq, include_protein=True)
        with_partial = orfs(seq, include_protein=True, allow_partial=True)
        assert all(o in with_partial for o in complete)


def _biopython_orfs(seq, table, min_len):
    """Complete ATG-initiated ORFs, after the ORF recipe in the Biopython tutorial."""
    Seq = pytest.importorskip("Bio.Seq").Seq
    n, found = len(seq), []
    for strand, nuc in (("+", Seq(seq)), ("-", Seq(seq).reverse_complement())):
        for offset in range(3):
            length = 3 * ((n - offset) // 3)
            protein = str(nuc[offset : offset + length].translate(table))
            aa_start = 0
            for segment in protein.split("*")[:-1]:  # the last one has no stop
                first_met = segment.find("M")
                if first_met != -1 and len(segment) - first_met >= min_len:
                    start = offset + 3 * (aa_start + first_met)
                    end = offset + 3 * (aa_start + len(segment) + 1)
                    frame = offset + 1
                    if strand == "-":
                        start, end, frame = n - end, n - start, -frame
                    found.append((start, end, strand, frame, segment[first_met:]))
                aa_start += len(segment) + 1
    return sorted(found, key=lambda o: (o[0], o[1], o[2] == "-"))


@pytest.mark.parametrize("table", [1, 4, 11])  # tables where only ATG encodes M
def test_orfs_match_a_biopython_reference(table):
    rng = random.Random(table)
    tokens = ["ATG", "TAA", "TAG", "TGA", "CAT", "TTA", "CTA", "TCA"] + list("ACGT") * 6
    seqs = [
        "".join(rng.choice(tokens) for _ in range(rng.randint(0, 150)))
        for _ in range(60)
    ]
    total = 0
    for min_len in (1, 5):
        got = col(
            seqs,
            s().seq.orfs(
                table=table,
                min_len=min_len,
                start_codons="ATG",
                include_protein=True,
                use_zero_based=True,
            ),
        ).to_list()
        for seq, found in zip(seqs, got):
            want = _biopython_orfs(seq, table, min_len)
            assert [
                (o["start"], o["end"], o["strand"], o["frame"], o["protein"])
                for o in found
            ] == want, seq
            assert all(o["aa_len"] == len(o["protein"]) for o in found)
            total += len(want)
    assert total > 300


def test_a_complete_orf_is_a_valid_cds():
    rng = random.Random(3)
    tokens = ["ATG", "CTG", "TTG", "TAA", "TAG", "TGA", "CAT", "CAG", "TTA"] + list(
        "ACGT"
    ) * 5
    seqs = ["".join(rng.choice(tokens) for _ in range(120)) for _ in range(40)]
    df = (
        pl.DataFrame({"id": range(len(seqs)), "s": seqs})
        .with_columns(
            o=s().seq.orfs(
                table=11, min_len=1, include_protein=True, use_zero_based=True
            )
        )
        .explode("o")
        .drop_nulls("o")
        .unnest("o")
        .with_columns(
            piece=s().str.slice(pl.col("start"), pl.col("end") - pl.col("start"))
        )
        .with_columns(
            cds=pl.when(pl.col("strand") == "-")
            .then(pl.col("piece").seq.revcomp())
            .otherwise(pl.col("piece"))
        )
        .with_columns(
            status=pl.col("cds").seq.cds_status(table=11),
            translated=pl.col("cds").seq.translate(table=11, cds=True),
        )
    )
    assert df.height > 100
    assert df["status"].null_count() == df.height
    assert df["translated"].to_list() == df["protein"].to_list()


def test_orfs_argument_validation():
    for kwargs, match in [
        (dict(table=7), "table"),
        (dict(min_len=-1), "min_len"),
        (dict(strands="plus"), "strands"),
        (dict(start_codons="ATA"), "start_codons"),
        (dict(allow_partial=1), "allow_partial"),
        (dict(use_zero_based="yes"), "use_zero_based"),
    ]:
        with pytest.raises(ValueError, match=match):
            s().seq.orfs(**kwargs)


# --- pb.orfs ---------------------------------------------------------------------

GENOME = {
    "chr1": "CC" + "ATGGCCTAA" + "ACC" + revcomp("ATGGCCAAATAA") + "GG",
    "chr2": "GGGGGGGGGG",  # no ORF
    "chr3": "ATGAAATGA",
}


@pytest.fixture
def genome_fasta(tmp_path):
    path = tmp_path / "genome.fa"
    path.write_text("".join(f">{name} test\n{seq}\n" for name, seq in GENOME.items()))
    return str(path)


def test_pb_orfs_from_a_fasta_path(genome_fasta):
    lf = pb.orfs(genome_fasta, min_len=1, start_codons="ATG", use_zero_based=True)
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()
    assert "sequence" not in df.columns
    assert df.columns[:2] == ["name", "description"]
    assert df.columns[2:] == list(ORF_FIELDS)
    assert df.select("name", "start", "end", "strand").rows() == [
        ("chr1", 2, 11, "+"),
        ("chr1", 14, 26, "-"),
        ("chr3", 0, 9, "+"),
    ]
    assert get_coordinate_system(lf) is True


def test_pb_orfs_from_a_frame_equals_the_expression():
    df = pl.DataFrame({"id": ["a", "b"], "dna": ["ATGGCCTAATTAGGCCAT", "GGGG"]})
    out = pb.orfs(
        df, sequence_col="dna", min_len=1, include_protein=True, use_zero_based=False
    ).collect()
    assert out.columns == ["id", *ORF_FIELDS, "protein"]
    expected = col(
        [df["dna"][0]],
        s().seq.orfs(min_len=1, include_protein=True, use_zero_based=False),
    ).item()
    assert out.drop("id").to_dicts() == expected.to_list()
    assert out["id"].unique().to_list() == ["a"]
    assert get_coordinate_system(out) is False
    # A LazyFrame works too, and the session option is the default.
    lazy = pb.orfs(df.lazy(), sequence_col="dna", min_len=1)
    assert get_coordinate_system(lazy) is False
    assert lazy.collect().height == out.height


def test_pb_orfs_into_a_range_operation(genome_fasta):
    genes = pl.DataFrame(
        {
            "name": ["chr1", "chr3"],
            "start": [1, 100],
            "end": [12, 200],
            "gene": ["g1", "g2"],
        }
    )
    genes.config_meta.set(coordinate_system_zero_based=True)
    found = pb.orfs(genome_fasta, min_len=1, start_codons="ATG", use_zero_based=True)
    cols = ["name", "start", "end"]
    hits = pb.overlap(
        found, genes, cols1=cols, cols2=cols, output_type="polars.DataFrame"
    )
    assert hits.select("name_1", "start_1", "end_1", "gene_2").rows() == [
        ("chr1", 2, 11, "g1")
    ]

    one_based = pb.orfs(genome_fasta, min_len=1, use_zero_based=False)
    with pytest.raises(pb.CoordinateSystemMismatchError):
        pb.overlap(
            one_based, genes, cols1=cols, cols2=cols, output_type="polars.DataFrame"
        )


def test_pb_orfs_argument_validation():
    df = pl.DataFrame({"sequence": ["ATG"]})
    with pytest.raises(ValueError, match="sequence_col"):
        pb.orfs(df, sequence_col="dna")
    with pytest.raises(TypeError, match="source"):
        pb.orfs(42)


# --- find_motif ------------------------------------------------------------------


def motif(seq, pattern, **kwargs):
    kwargs.setdefault("use_zero_based", True)
    return col([seq], s().seq.find_motif(pattern, **kwargs)).item().to_list()


def test_find_motif_type_and_nulls():
    out = col(
        ["ACGTACGT", "TTTT", None], s().seq.find_motif("ACG", use_zero_based=True)
    )
    assert out.dtype == pl.List(pl.Int64)
    assert out.to_list() == [[0, 4], [], None]


def test_find_motif_rules():
    # Ambiguity applies to the pattern only: N in the sequence matches nothing.
    assert motif("ACGTNACGT", "N") == [0, 1, 2, 3, 5, 6, 7, 8]
    assert motif("ACGTNACGT", "ACGTN") == []
    assert motif("ACGTRYACGT", "R") == [0, 2, 6, 8]
    assert motif("AAAAA", "AA") == [0, 1, 2, 3]
    assert motif("AAAAA", "AA", overlapping=False) == [0, 2]
    # Deliberate differences from nt_search: case and U are folded.
    assert motif("acgtacgt", "ACG") == [0, 4]
    assert motif("ACGUACGU", "ACGT") == [0, 4]
    assert motif("ACGTACGT", "acgu") == [0, 4]


def test_find_motif_both_strands():
    assert motif("TTGAATTCTT", "GAATTC", both_strands=True) == [2]  # a palindrome, once
    assert motif("AACGTT", "AAC", both_strands=True) == [0, 3]
    assert motif("AACGTT", "AAC") == [0]
    # The two orientations are scanned independently.
    assert motif("ACGT", "ACG", both_strands=True, overlapping=False) == [0, 1]


def test_find_motif_coordinates(zero_based_session):
    seq = ["ACGTACGT"]
    assert col(seq, s().seq.find_motif("ACG")).item().to_list() == [0, 4]
    assert col(
        seq, s().seq.find_motif("ACG", use_zero_based=False)
    ).item().to_list() == [1, 5]


def test_coordinate_setting_is_captured_when_the_expression_is_built():
    assert pb.get_option(ZERO_BASED) == "false"  # the session default is 1-based
    one_based = s().seq.find_motif("ACG")
    before = pb.get_option(ZERO_BASED)
    pb.set_option(ZERO_BASED, True)
    try:
        zero_based = s().seq.find_motif("ACG")
        assert col(["ACGTACGT"], one_based).item().to_list() == [1, 5]
    finally:
        pb.set_option(ZERO_BASED, before)
    assert col(["ACGTACGT"], zero_based).item().to_list() == [0, 4]


def test_find_motif_matches_nt_search():
    nt_search = pytest.importorskip("Bio.SeqUtils").nt_search
    rng = random.Random(13)
    hits = 0
    for _ in range(150):
        seq = "".join(
            rng.choice("ACGT" * 6 + "N") for _ in range(rng.choice([0, 3, 40, 300]))
        )
        pattern = "".join(
            rng.choice("ACGT" * 3 + IUPAC) for _ in range(rng.randint(1, 5))
        )
        want = nt_search(seq, pattern)[1:]
        assert motif(seq, pattern) == want, (seq, pattern)
        hits += len(want)
    assert hits > 1000


@pytest.mark.parametrize("pattern", ["", "AC!", "A-C", "AXC", 5, None])
def test_find_motif_invalid_pattern(pattern):
    with pytest.raises(ValueError, match="pattern"):
        s().seq.find_motif(pattern)


# --- gc_content_windows, gc_skew ---------------------------------------------------


def test_gc_skew_probes():
    assert col(["GGGGCCCCGGGCAAAA"], s().seq.gc_skew(4)).item().to_list() == [
        1.0,
        -1.0,
        0.5,
        0.0,
    ]
    out = col(["AAAAGG", "", None], s().seq.gc_skew(window=4))
    assert out.dtype == pl.List(pl.Float64)
    assert out.to_list() == [[0.0, 1.0], [], None]
    assert col(["GGCC"], s().seq.gc_skew()).item().to_list() == [
        0.0
    ]  # default window 100
    # A window starts at every multiple of the step and is cut at the end.
    assert col(["GGCCGG"], s().seq.gc_skew(4, 2)).item().to_list() == [0.0, 0.0, 1.0]


def test_gc_skew_matches_biopython():
    GC_skew = pytest.importorskip("Bio.SeqUtils").GC_skew
    rng = random.Random(17)
    seqs = [
        "".join(rng.choice("ACGTacgtNS-") for _ in range(rng.randint(0, 400)))
        for _ in range(60)
    ]
    for window in (1, 7, 100):
        got = col(seqs, s().seq.gc_skew(window)).to_list()
        assert got == [GC_skew(x, window) for x in seqs]


def test_gc_content_windows():
    out = col(["GGCCAAAT", "GGCCAA", "", None], s().seq.gc_content_windows(4))
    assert out.dtype == pl.List(pl.Float64)
    assert out.to_list() == [[1.0, 0.0], [1.0, 0.0], [], None]
    # A zero denominator is a null element.
    acgt = col(["GGNNNNNN"], s().seq.gc_content_windows(4, denominator="acgt"))
    assert acgt.item().to_list() == [1.0, None]
    # One window over the whole sequence equals gc_content.
    df = pl.DataFrame({"s": ["GATTACA", "ggcc", "NNNN"]})
    one = df.select(
        w=s().seq.gc_content_windows(1000).list.first(), g=s().seq.gc_content()
    )
    assert one["w"].to_list() == one["g"].to_list()


@pytest.mark.parametrize(
    "call",
    [
        lambda: s().seq.gc_skew(0),
        lambda: s().seq.gc_skew(4, 0),
        lambda: s().seq.gc_content_windows(-1),
        lambda: s().seq.gc_content_windows(4, denominator="gc"),
        lambda: s().seq.gc_content_windows(2.5),
    ],
)
def test_window_argument_validation(call):
    with pytest.raises(ValueError):
        call()


# --- molecular_weight ------------------------------------------------------------


def test_molecular_weight_probes():
    out = col(["ACGT", "ACGN", None, "acgt"], s().seq.molecular_weight())
    assert out.dtype == pl.Float64
    assert out.to_list() == [
        pytest.approx(1253.8027, rel=1e-12),
        None,
        None,
        pytest.approx(1253.8027, rel=1e-12),
    ]
    assert col(["MKV"], s().seq.molecular_weight("protein")).item() == pytest.approx(
        376.5146, rel=1e-12
    )


def test_molecular_weight_matches_biopython():
    molecular_weight = pytest.importorskip("Bio.SeqUtils").molecular_weight
    rng = random.Random(19)
    alphabets = {"DNA": "ACGT", "RNA": "ACGU", "protein": "ACDEFGHIKLMNPQRSTVWY"}
    rejected = 0
    for seq_type, letters in alphabets.items():
        seqs = [
            "".join(rng.choice(letters) for _ in range(rng.choice([0, 1, 9, 200])))
            for _ in range(30)
        ]
        seqs += [seqs[5].lower(), seqs[6] + "N", seqs[7] + "X", "B", "MOUK", "AC GT"]
        for double_stranded in (False, True) if seq_type != "protein" else (False,):
            for circular in (False, True):
                for monoisotopic in (False, True):
                    want = []
                    for seq in seqs:
                        try:
                            want.append(
                                molecular_weight(
                                    seq,
                                    seq_type,
                                    double_stranded,
                                    circular,
                                    monoisotopic,
                                )
                            )
                        except ValueError:
                            want.append(None)
                    got = col(
                        seqs,
                        s().seq.molecular_weight(
                            seq_type, double_stranded, circular, monoisotopic
                        ),
                    ).to_list()
                    for seq, g, w in zip(seqs, got, want):
                        assert (g is None) == (w is None), (seq_type, seq)
                        if w is not None:
                            assert g == pytest.approx(w, rel=1e-12), (seq_type, seq)
                    rejected += want.count(None)
    assert rejected > 20


def test_molecular_weight_argument_validation():
    with pytest.raises(ValueError, match="seq_type"):
        s().seq.molecular_weight("xna")
    with pytest.raises(ValueError, match="double_stranded"):
        s().seq.molecular_weight("protein", double_stranded=True)


# --- hash ------------------------------------------------------------------------


def test_hash_values_are_stable():
    # XXH3-64, seed 0 (python-xxhash 4.1.0). These values must never change.
    out = col(["", "A", "ACGT", "acgt", "TTAGGCCAT", "ATGGCCTAA", None], s().seq.hash())
    assert out.dtype == pl.UInt64
    assert out.to_list() == [
        3244421341483603138,
        15047818145317598341,
        17060466519357990319,
        251730389294227419,
        17056848131934351943,
        2721565146873327973,
        None,
    ]


def test_canonical_hash_ignores_strand():
    rng = random.Random(23)
    seqs = ["".join(rng.choice("ACGTN") for _ in range(50)) for _ in range(50)]
    df = pl.DataFrame({"s": seqs, "rc": [revcomp(x) for x in seqs]})
    out = df.select(
        a=s().seq.hash(canonical=True),
        b=pl.col("rc").seq.hash(canonical=True),
        plain_a=s().seq.hash(),
        plain_b=pl.col("rc").seq.hash(),
    )
    assert out["a"].to_list() == out["b"].to_list()
    assert (
        out["a"].to_list()
        == out.select(pl.min_horizontal("plain_a", "plain_b")).to_series().to_list()
    )


def test_hash_deduplicates_like_rmdup():
    reads = pl.DataFrame(
        {"name": ["r1", "r2", "r3", "r4"], "s": ["ACGTTT", "AAACGT", "ACGTTT", "GGGG"]}
    )
    # r2 is the reverse complement of r1; r3 is an exact copy.
    hashed = reads.with_columns(
        h=s().seq.hash(), canonical=s().seq.hash(canonical=True)
    )
    same_strand = hashed.unique(subset="h", keep="first", maintain_order=True)
    both_strands = hashed.unique(subset="canonical", keep="first", maintain_order=True)
    assert same_strand["name"].to_list() == ["r1", "r2", "r4"]
    assert both_strands["name"].to_list() == ["r1", "r4"]


# --- trim_quality ------------------------------------------------------------------

TRIM_READS = pl.DataFrame(  # probed with Trimmomatic 0.39 SE -phred33
    {
        "sequence": [
            "ACGTA",
            "ACGTAC",
            "ACGTACGTACGT",
            "ACG",
            "ANNNA",
            "NNACG",
            "ACGTACGT",
            "nnACG",
            None,
        ],
        "quality_scores": [
            "I!!!!",
            "!!II!!",
            "IIII!!!!IIII",
            "III",
            "IIIII",
            "IIIII",
            "!!!!IIII",
            "IIIII",
            "II",
        ],
    }
)


def trim(**kwargs):
    out = TRIM_READS.select(
        t=pl.col("sequence").seq.trim_quality("quality_scores", **kwargs)
    )
    return [
        None if r is None else (r["sequence"], r["quality_scores"]) for r in out["t"]
    ]


def test_trim_quality_type():
    out = TRIM_READS.select(
        t=pl.col("sequence").seq.trim_quality(pl.col("quality_scores"), leading=10)
    )
    assert out["t"].dtype == pl.Struct(
        {"sequence": pl.String, "quality_scores": pl.String}
    )


def test_trim_quality_matches_trimmomatic():
    dropped = ("", "")
    assert trim(leading=10) == [
        ("ACGTA", "I!!!!"),
        ("GTAC", "II!!"),
        ("ACGTACGTACGT", "IIII!!!!IIII"),
        ("ACG", "III"),
        ("ANNNA", "IIIII"),
        ("ACG", "III"),  # an N base counts as quality 0
        ("ACGT", "IIII"),
        ("nnACG", "IIIII"),  # lowercase n keeps its quality
        None,
    ]
    # TRAILING never examines the first base, so the first read is dropped.
    assert trim(trailing=10)[:2] == [dropped, ("ACGT", "!!II")]
    assert trim(sliding_window=(4, 15)) == [
        dropped,
        ("ACGT", "!!II"),
        ("ACGT", "IIII"),
        dropped,  # shorter than the window
        dropped,
        ("NNACG", "IIIII"),
        dropped,
        ("nnACG", "IIIII"),
        None,
    ]
    assert trim(leading=10, trailing=10, sliding_window=(4, 15)) == [
        dropped,
        dropped,
        ("ACGT", "IIII"),
        dropped,
        dropped,
        dropped,
        ("ACGT", "IIII"),
        ("nnACG", "IIIII"),
        None,
    ]


def test_trim_quality_keeps_sequence_and_quality_in_sync():
    rng = random.Random(29)
    n = 300
    reads = pl.DataFrame(
        {
            "name": [f"r{i}" for i in range(n)],
            "sequence": [
                "".join(rng.choice("ACGTN") for _ in range(80)) for _ in range(n)
            ],
            "quality_scores": [
                "".join(chr(33 + rng.randint(0, 41)) for _ in range(80))
                for _ in range(n)
            ],
        }
    )
    trimmed = (
        reads.with_columns(
            t=pl.col("sequence").seq.trim_quality(
                "quality_scores", leading=5, trailing=5, sliding_window=(4, 15)
            )
        )
        .drop("sequence", "quality_scores")
        .unnest("t")
    )
    assert trimmed.columns == ["name", "sequence", "quality_scores"]
    lengths = trimmed.select(
        a=pl.col("sequence").str.len_bytes(), b=pl.col("quality_scores").str.len_bytes()
    )
    assert lengths["a"].to_list() == lengths["b"].to_list()
    assert 0 < lengths["a"].sum() < 80 * n
    # Each result is a contiguous piece of its read.
    for read, piece in zip(reads["sequence"], trimmed["sequence"]):
        assert piece in read


def test_trim_quality_errors():
    df = pl.DataFrame({"s": ["ACG"], "q": ["II"]})
    with pytest.raises(ComputeError, match="differs from quality length"):
        df.select(s().seq.trim_quality("q", leading=10))
    with pytest.raises(ComputeError, match="below the Phred offset"):
        pl.DataFrame({"s": ["ACG"], "q": ["I I"]}).select(
            s().seq.trim_quality("q", leading=10)
        )
    with pytest.raises(ValueError, match="at least one"):
        s().seq.trim_quality("q")
    with pytest.raises(ValueError, match="sliding_window"):
        s().seq.trim_quality("q", sliding_window=(0, 15))
    with pytest.raises(ValueError, match="sliding_window"):
        s().seq.trim_quality("q", sliding_window=4)
    with pytest.raises(ValueError, match="leading"):
        s().seq.trim_quality("q", leading="3")


# --- engines, SQL, serialization ---------------------------------------------------


def _all_new_expressions():
    q = pl.col("q")
    return dict(
        cds=s().seq.translate(cds=False, stop_symbol="#", ambiguous="biopython"),
        status=s().seq.cds_status(),
        rna=s().seq.transcribe(),
        dna=s().seq.back_transcribe(),
        ee=q.seq.expected_errors(),
        qmin=q.seq.min_quality(),
        qmax=q.seq.max_quality(),
        orfs=s().seq.orfs(min_len=1, include_protein=True, use_zero_based=False),
        hits=s().seq.find_motif("AC", both_strands=True, use_zero_based=False),
        gcw=s().seq.gc_content_windows(4, 2, denominator="acgt"),
        skew=s().seq.gc_skew(4),
        mw=s().seq.molecular_weight(),
        h=s().seq.hash(canonical=True),
        trimmed=s().seq.trim_quality(q, leading=10, sliding_window=(4, 15)),
    )


def _mixed_frame():
    return pl.DataFrame(
        {
            "s": [
                "ACGTN",
                "aacc",
                None,
                "ATGGCCTAAGC",
                "GGNN",
                "ATGGCCTAATTAGGCCAT",
                "",
            ],
            "q": [
                "IIIII",
                "I#I#",
                None,
                "???????????",
                "5555",
                "IIIIIIIIIIIIII!!!!",
                "",
            ],
        }
    )


def test_sql_matches_expressions():
    df = _mixed_frame()
    pb.from_polars("seq_biopython_parity", df)
    got = pb.sql(
        "SELECT seq_translate(s, 1, 1, false, '#', false, 'biopython') cds, "
        "seq_cds_status(s) status, seq_transcribe(s) rna, seq_back_transcribe(s) dna, "
        "seq_expected_errors(q) ee, seq_min_quality(q) qmin, seq_max_quality(q) qmax, "
        "seq_orfs(s, 1, 1, 'both', 'table', false, true) orfs, "
        "seq_find_motif(s, 'AC', true) hits, "
        "seq_gc_content_windows(s, 4, 2, 'acgt') gcw, seq_gc_skew(s, 4) skew, "
        "seq_molecular_weight(s) mw, seq_hash(s, true) h, "
        "seq_trim_quality(s, q, 10, NULL, 4, 15) trimmed FROM seq_biopython_parity"
    ).collect()
    want = df.select(**_all_new_expressions())
    assert got.columns == want.columns
    for name in want.columns:
        assert got[name].to_list() == want[name].to_list(), name
    # SQL takes the coordinate system as an argument and defaults to 1-based.
    zero = pb.sql(
        "SELECT seq_find_motif(s, 'AC', true, true, true) hits, "
        "seq_orfs(s, 1, 1, 'both', 'table', false, false, true) orfs FROM seq_biopython_parity"
    ).collect()
    expected = df.select(
        hits=s().seq.find_motif("AC", both_strands=True, use_zero_based=True),
        orfs=s().seq.orfs(min_len=1, use_zero_based=True),
    )
    assert zero["hits"].to_list() == expected["hits"].to_list()
    assert zero["orfs"].to_list() == expected["orfs"].to_list()


def test_streaming_and_chunked_input_match_in_memory():
    df = _mixed_frame()
    chunked = pl.concat(
        [df.slice(0, 3), df.slice(3), df, df.slice(1, 4)], rechunk=False
    )
    assert chunked["s"].n_chunks() > 1
    exprs = _all_new_expressions()
    eager = chunked.select(**exprs)
    assert eager.height == chunked.height
    assert chunked.rechunk().select(**exprs).equals(eager)
    big = pl.concat([chunked] * 400, rechunk=False).lazy().select(**exprs)
    assert big.collect(engine="streaming").equals(big.collect())


def test_serialize_round_trip():
    lf = _mixed_frame().lazy().select(**_all_new_expressions())
    data = lf.serialize()
    for name in (
        b"seq_orfs",
        b"seq_find_motif",
        b"seq_trim_quality",
        b"seq_cds_status",
    ):
        assert name in data
    restored = pl.LazyFrame.deserialize(io.BytesIO(data))
    assert restored.collect().equals(lf.collect())


def test_scan_fastq_quality_control_pipeline():
    reads = "tests/data/io/fastq/example.fastq"
    full = pb.scan_fastq(reads).collect()
    out = (
        pb.scan_fastq(reads)
        .filter(pl.col("quality_scores").seq.expected_errors() <= 50.0)
        .with_columns(
            t=pl.col("sequence").seq.trim_quality(
                "quality_scores", leading=3, trailing=3, sliding_window=(4, 15)
            ),
            h=pl.col("sequence").seq.hash(canonical=True),
        )
        .collect(engine="streaming")
    )
    assert 0 < out.height <= full.height
    assert out["h"].dtype == pl.UInt64
    kept = out.drop("sequence", "quality_scores").unnest("t")
    assert kept.height == out.height
    same_length = (
        pl.col("sequence").str.len_bytes() == pl.col("quality_scores").str.len_bytes()
    )
    assert kept.select(same_length.all()).item()


@pytest.mark.parametrize(
    "make, match",
    [
        (lambda: s().seq.translate(stop_symbol=None), "stop_symbol"),
        (lambda: s().seq.translate(cds="yes"), "cds"),
        (lambda: s().seq.translate(ambiguous="bzj"), "ambiguous"),
        (lambda: s().seq.cds_status(frame=0), "frame"),
        (lambda: s().seq.cds_status(table=7), "table"),
        (lambda: s().seq.expected_errors(offset=300), "offset"),
        (lambda: s().seq.hash(canonical="yes"), "canonical"),
        (lambda: s().seq.find_motif("ACG", both_strands=1), "both_strands"),
    ],
)
def test_invalid_arguments(make, match):
    with pytest.raises(ValueError, match=match):
        make()


def test_pb_orfs_rejects_columns_that_clash_with_orf_fields():
    df = pl.DataFrame({"sequence": ["ATGAAATAA"], "start": [5]})
    with pytest.raises(ValueError, match="start"):
        pb.orfs(df, min_len=1)
    # protein clashes only when it is requested.
    clash = pl.DataFrame({"sequence": ["ATGAAATAA"], "protein": ["x"]})
    assert pb.orfs(clash, min_len=1).collect().height == 1
    with pytest.raises(ValueError, match="protein"):
        pb.orfs(clash, min_len=1, include_protein=True)


def test_trim_quality_broadcasts_a_literal_quality():
    df = pl.DataFrame({"s": ["ACGT", "ACGT"]})
    out = df.select(t=s().seq.trim_quality(pl.lit("##II"), leading=20))["t"]
    assert out.to_list() == [{"sequence": "GT", "quality_scores": "II"}] * 2
