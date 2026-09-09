"""A2M / A3M / Stockholm readers.

Parity oracles (see tests/data/io/msa/README.md):

* Biopython ``SeqIO`` — byte-level record parsing of A2M/A3M.
* pyhmmer (Easel, the reference implementation) — Stockholm structure and
  A2M/A3M alignment semantics.
* Checked-in expected outputs generated with ``esl-alistat``, ``esl-reformat``
  and hh-suite ``reformat.pl`` (``expected.json``, ``*_pfam.sto``,
  ``query_dotted.a2m``) — so no HMMER binary or Perl is needed here.
"""

import contextlib
import gzip
import json
import re
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

pyhmmer = pytest.importorskip("pyhmmer")
Bio = pytest.importorskip("Bio")

from Bio import SeqIO  # noqa: E402
from pyhmmer.easel import MSAFile  # noqa: E402

DATA = Path(__file__).parent / "data" / "io" / "msa"
EXPECTED = json.loads((DATA / "expected.json").read_text())
TARGET_PARTITIONS = "datafusion.execution.target_partitions"
PSEUDO_PATTERN = r"^(ss_|sa_|aa_)"
EASEL_NORMALISED_GF = {"GA", "TC", "NC"}


def p(name: str) -> str:
    return str(DATA / name)


@contextlib.contextmanager
def _target_partitions(partitions: int):
    previous = pb.get_option(TARGET_PARTITIONS)
    pb.set_option(TARGET_PARTITIONS, str(partitions))
    try:
        yield
    finally:
        if previous is not None:
            pb.set_option(TARGET_PARTITIONS, previous)


def _open_text(name: str):
    path = DATA / name
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return open(path)


def _biopython_records(name: str, fmt: str = "fasta"):
    with _open_text(name) as fh:
        return [(r.id, str(r.seq)) for r in SeqIO.parse(fh, fmt)]


def _match_columns(seq: str) -> int:
    return sum(1 for c in seq if c.isupper() or c == "-")


def _naive_stockholm_lines(name: str, prefix: str):
    """The deliberately dumb reference parser for annotation lines."""
    out = []
    for line in (DATA / name).read_text().splitlines():
        if line.startswith(prefix):
            parts = re.split(r"\s+", line.strip(), maxsplit=2)
            out.append((parts[1], parts[2] if len(parts) > 2 else ""))
    return out


# ---------------------------------------------------------------------------
# A2M / A3M
# ---------------------------------------------------------------------------


def test_a3m_schema_matches_fasta():
    df = pb.read_a3m(p("query.a3m"))
    assert df.height == 59
    assert df.schema == {
        "name": pl.String,
        "description": pl.String,
        "sequence": pl.String,
    }
    assert df.equals(pb.scan_a3m(p("query.a3m")).collect())


@pytest.mark.parametrize(
    "name, ref_name, reader",
    [
        ("query.a3m", "query.a3m", pb.read_a3m),
        ("query.a3m.gz", "query.a3m", pb.read_a3m),
        ("test_head.a3m", "test_head.a3m", pb.read_a3m),
        ("single.a3m", "single.a3m", pb.read_a3m),
        ("no_desc.a3m", "no_desc.a3m", pb.read_a3m),
        # Biopython's plain "fasta" parser refuses the leading "#A3M#" lines and
        # its "fasta-blast" mode mis-assigns every record the first id, so the
        # header-bearing file is compared against the records of the file it
        # was built from.
        ("hdr.a3m", "query.a3m", pb.read_a3m),
        ("query_dotted.a2m", "query_dotted.a2m", pb.read_a2m),
    ],
)
def test_records_match_biopython_bytes(name, ref_name, reader):
    ref = _biopython_records(ref_name)
    got = reader(p(name)).select("name", "sequence").rows()
    assert got == ref


def test_a3m_rows_are_ragged_and_verbatim():
    seqs = pb.read_a3m(p("query.a3m"))["sequence"].to_list()
    assert len({len(s) for s in seqs}) > 1
    assert seqs[1].startswith("------------------Yvqrkesgiegplgsratdgragg")


def test_a3m_header_split_on_whitespace_not_comma():
    df = pb.read_a3m(p("query.a3m"))
    assert df["name"][1] == "tr|Q4S137|Q4S137_TETNG"
    assert "SCAF14770, whole genome" in df["description"][1]
    nd = pb.read_a3m(p("no_desc.a3m"))
    assert nd["description"].to_list() == [None, "desc"]


def test_a3m_pseudo_sequences_are_rows_in_file_order():
    names = pb.read_a3m(p("test_head.a3m"))["name"].to_list()
    exp = EXPECTED["a3m"]["test_head.a3m"]
    assert len(names) == exp["with_pseudo_records"]
    assert names[:3] == exp["pseudo_names"]
    assert names[3] == "1a7j_A"


@pytest.mark.parametrize("name", ["query.a3m", "test_head.a3m", "query_dotted.a2m"])
def test_a3m_match_columns_agree_with_easel(name):
    reader = pb.read_a2m if name.endswith(".a2m") else pb.read_a3m
    df = reader(p(name)).filter(~pl.col("name").str.contains(PSEUDO_PATTERN))
    ours = {_match_columns(s) for s in df["sequence"].to_list()}
    assert len(ours) == 1, "match-column count must be constant across rows"

    if name == "test_head.a3m":
        # Easel rejects ss_conf digits, so the reference is the stripped file.
        stripped = DATA / "_tmp_nopseudo.a3m"
        stripped.write_text(
            "".join(f">{n}\n{s}\n" for n, s in df.select("name", "sequence").rows())
        )
        src, exp = stripped, EXPECTED["a3m"][name]["without_pseudo"][0]
    else:
        src, exp = DATA / name, EXPECTED["a3m"][name][0]
    try:
        with MSAFile(str(src), format="a2m") as mf:
            msa = next(mf)
    finally:
        if name == "test_head.a3m":
            src.unlink()
    assert df.height == len(msa.sequences) == exp["n_sequences"]
    assert len(msa.alignment[0]) == exp["alignment_length"]
    assert ours == {_match_columns(msa.alignment[0])}


def test_a2m_dotted_expansion_matches_easel_reconstruction():
    """The checked-in dotted A2M came from reformat.pl and was asserted byte-identical
    to Easel; reading it back verbatim must reproduce Easel's rows exactly."""
    with MSAFile(p("query.a3m"), format="a2m") as mf:
        msa = next(mf)
    got = pb.read_a2m(p("query_dotted.a2m"))
    assert got["name"].to_list() == list(msa.names)
    assert got["sequence"].to_list() == list(msa.alignment)


def test_a3m_empty_projection_count_limit_and_sql():
    lf = pb.scan_a3m(p("query.a3m"))
    assert lf.select("name").collect().columns == ["name"]
    assert lf.select(pl.len()).collect().item() == 59
    assert lf.limit(5).collect().height == 5
    pb.register_a3m(p("query.a3m"), "msa_t")
    assert pb.sql("SELECT count(*) AS n FROM msa_t").collect().item() == 59
    assert (
        pb.sql("SELECT name FROM msa_t WHERE name NOT LIKE 'ss_%' LIMIT 3")
        .collect()
        .height
        == 3
    )


def test_a3m_empty_file():
    assert pb.read_a3m(p("empty.a3m")).height == 0


def test_a3m_predicate_pushdown_matches_client_filter():
    pred = pl.col("name") == "sp|Q5VUD6|FA69B_HUMAN"
    off = pb.scan_a3m(p("query.a3m"), predicate_pushdown=False).filter(pred).collect()
    on = pb.scan_a3m(p("query.a3m"), predicate_pushdown=True).filter(pred).collect()
    assert off.height == 1
    assert off.equals(on)


# ---------------------------------------------------------------------------
# Stockholm
# ---------------------------------------------------------------------------

BAG = pl.List(pl.Struct({"tag": pl.String, "value": pl.String}))


def test_sto_schema():
    df = pb.read_sto(p("PF00001.sto"))
    assert df.schema == {
        "alignment_id": pl.String,
        "name": pl.String,
        "sequence": pl.String,
        "gs": BAG,
        "gr": BAG,
    }
    assert df.height == 63
    assert df.equals(pb.scan_sto(p("PF00001.sto")).collect())


@pytest.mark.parametrize(
    "name", ["PF00001.sto", "RF00001.sto", "PF00001_hmmalign.sto", "query_hh.sto"]
)
def test_sto_names_and_sequences_match_easel(name):
    with MSAFile(p(name), format="stockholm") as mf:
        msa = next(mf)
    df = pb.read_sto(p(name))
    exp = EXPECTED["stockholm"][name][0]
    assert df.height == len(msa.sequences) == exp["n_sequences"]
    assert df["name"].to_list() == list(msa.names)
    assert df["sequence"].to_list() == list(msa.alignment)
    assert {len(s) for s in df["sequence"].to_list()} == {exp["alignment_length"]}


def test_sto_alignment_id_and_gs():
    df = pb.read_sto(p("PF00001.sto"))
    assert df["alignment_id"].unique().to_list() == ["7tm_1"]
    assert df["name"][0] == "NPY1R_HUMAN/57-320"
    assert df["gs"][0].to_list() == [{"tag": "AC", "value": "P25929.1"}]
    assert df["gr"].null_count() == df.height
    with MSAFile(p("PF00001.sto"), format="stockholm") as mf:
        msa = next(mf)
    assert [g[0]["value"] for g in df["gs"].to_list()] == [
        s.accession.decode() if isinstance(s.accession, bytes) else s.accession
        for s in msa.sequences
    ]


def test_sto_interleaved_rfam_is_concatenated():
    df = pb.read_sto(p("RF00001.sto"))
    assert df.height == 712
    assert df["name"].n_unique() == 712
    assert df["alignment_id"].unique().to_list() == ["5S_rRNA"]
    seq0 = df["sequence"][0]
    assert seq0.startswith("--CUUGAC-GA-U-C-AU-AGA----GC-G-U-U-G---GA")
    assert seq0[200:].startswith("--AGUA----GG-U-CA-UC--G-UCAAGC")


def test_sto_deinterleaved_canonical_form_is_identical():
    """esl-reformat pfam rewrote RF00001 into single-block form; our reader must
    produce identical rows from both."""
    a = pb.read_sto(p("RF00001.sto"))
    b = pb.read_sto(p("RF00001_pfam.sto"))
    assert a.select("name", "sequence").equals(b.select("name", "sequence"))
    assert a["gs"].to_list() == b["gs"].to_list()
    assert a["gr"].to_list() == b["gr"].to_list()


def test_sto_hmmalign_gr_pp_and_gc_pp_cons():
    """hmmalign writes #=GR PP per sequence and #=GC PP_cons, both wrapped in
    200-column blocks. pyhmmer exposes only the consensus (`posterior_probabilities`
    is the PP_cons string), so per-sequence PP is checked against the naive
    line-splitter concatenating the blocks."""
    name = "PF00001_hmmalign.sto"
    df = pb.read_sto(p(name))
    pp = {
        n: next(d["value"] for d in row if d["tag"] == "PP")
        for n, row in zip(df["name"].to_list(), df["gr"].to_list())
    }
    ref: dict[str, str] = {}
    for line in (DATA / name).read_text().splitlines():
        if line.startswith("#=GR"):
            _, seq_name, feature, value = line.split()
            if feature == "PP":
                ref[seq_name] = ref.get(seq_name, "") + value
    assert pp == ref
    assert all(len(v) == len(s) for v, s in zip(pp.values(), df["sequence"].to_list()))

    with MSAFile(p(name), format="stockholm") as mf:
        msa = next(mf)
    pp_cons = (
        pb.describe_sto(p(name))
        .filter((pl.col("kind") == "GC") & (pl.col("feature") == "PP_cons"))["value"]
        .item()
    )
    assert pp_cons == msa.posterior_probabilities


def test_sto_multi_alignment_counts():
    df = pb.scan_sto(p("multi.sto")).group_by("alignment_id").len().collect()
    counts = dict(df.rows())
    exp = EXPECTED["stockholm"]["multi.sto"]
    assert counts == {"7tm_1": exp[0]["n_sequences"], "5S_rRNA": exp[1]["n_sequences"]}


def test_sto_partitioned_scan_matches_single_partition():
    key = ["alignment_id", "name"]
    with _target_partitions(1):
        one = pb.read_sto(p("multi.sto")).sort(key)
    with _target_partitions(3):
        many = pb.read_sto(p("multi.sto")).sort(key)
    assert one.height == 63 + 712
    assert one.equals(many)


def test_sto_ordinal_fallback(tmp_path):
    f = tmp_path / "anon.sto"
    f.write_text(
        "# STOCKHOLM 1.0\nseqA ACGT\n//\n"
        "# STOCKHOLM 1.0\n#=GF AC ACC2\nseqB ACGT\n//\n"
        "# STOCKHOLM 1.0\nseqC ACGT\n//\n"
    )
    assert pb.read_sto(str(f))["alignment_id"].to_list() == ["0", "ACC2", "2"]


def test_sto_gs_fields_promotion_and_sentinel():
    df = pb.read_sto(p("PF00001.sto"), gs_fields=["AC", "DE"])
    assert df.columns == ["alignment_id", "name", "sequence", "AC", "DE", "gr"]
    assert df["AC"][0] == "P25929.1"
    assert df["DE"].null_count() == df.height
    both = pb.read_sto(p("PF00001.sto"), gs_fields=["AC", "gs"])
    assert both.columns == ["alignment_id", "name", "sequence", "AC", "gs", "gr"]
    assert both["gs"][0].to_list() == [{"tag": "AC", "value": "P25929.1"}]
    pb.register_sto(p("PF00001.sto"), "pfam_t", gs_fields=["AC"])
    out = pb.sql('SELECT name, "AC" FROM pfam_t LIMIT 1').collect()
    assert out.rows() == [("NPY1R_HUMAN/57-320", "P25929.1")]


def test_sto_edge_cases():
    assert pb.read_sto(p("missing_terminator.sto")).height == 63
    assert pb.read_sto(p("empty.sto")).height == 0
    single = pb.read_sto(p("single.sto"))
    assert single.rows() == [("single", "only", "ACDE.F-G", None, None)]
    with pytest.raises(Exception) as exc:
        pb.read_sto(p("wrong_header.sto"))
    assert "wrong_header.sto" in str(exc.value)
    assert "# STOCKHOLM 1.0" in str(exc.value)


@pytest.mark.parametrize(
    "plain, compressed, reader",
    [
        ("PF00001.sto", "PF00001.sto.gz", pb.read_sto),
        ("PF00001.sto", "PF00001.sto.bgz", pb.read_sto),
        ("RF00001.sto", "RF00001.sto.gz", pb.read_sto),
        ("query.a3m", "query.a3m.bgz", pb.read_a3m),
    ],
)
def test_compressed_inputs_match_plain(plain, compressed, reader):
    assert reader(p(plain)).equals(reader(p(compressed)))


def test_sto_projection_count_limit_and_pushdown():
    lf = pb.scan_sto(p("RF00001.sto"))
    assert lf.select("name").collect().columns == ["name"]
    assert lf.select(pl.len()).collect().item() == 712
    assert lf.limit(10).collect().height == 10
    pred = pl.col("name") == "X01556.1/3-118"
    off = pb.scan_sto(p("RF00001.sto"), predicate_pushdown=False).filter(pred).collect()
    on = pb.scan_sto(p("RF00001.sto"), predicate_pushdown=True).filter(pred).collect()
    assert off.height == 1
    assert off.equals(on)
    pb.register_sto(p("RF00001.sto"), "rfam_t")
    assert pb.sql("SELECT count(*) FROM rfam_t").collect().item() == 712


# ---------------------------------------------------------------------------
# describe_sto
# ---------------------------------------------------------------------------


def test_describe_sto_schema_and_counts():
    d = pb.describe_sto(p("PF00001.sto"))
    assert d.schema == {
        "alignment_id": pl.String,
        "kind": pl.String,
        "feature": pl.String,
        "value": pl.String,
        "n_sequences": pl.UInt32,
        "alignment_length": pl.UInt32,
    }
    exp = EXPECTED["stockholm"]["PF00001.sto"][0]
    assert d["n_sequences"].unique().to_list() == [exp["n_sequences"]]
    assert d["alignment_length"].unique().to_list() == [exp["alignment_length"]]
    assert d["alignment_id"].unique().to_list() == ["7tm_1"]


def test_describe_sto_preserves_repeated_gf_in_order():
    d = pb.describe_sto(p("PF00001.sto"))
    gf = d.filter(pl.col("kind") == "GF")
    assert gf.select("feature", "value").rows() == _naive_stockholm_lines(
        "PF00001.sto", "#=GF"
    )
    feats = gf["feature"].to_list()
    assert len(feats) == 49
    assert feats[:3] == ["ID", "AC", "DE"]
    assert feats.count("DR") == 11
    assert feats.count("CC") == 10
    gc = d.filter(pl.col("kind") == "GC")
    assert gc["feature"].to_list() == ["seq_cons", "RF"]
    assert {len(v) for v in gc["value"].to_list()} == {722}


def test_describe_sto_keeps_file_order_across_kinds(tmp_path):
    f = tmp_path / "mixed.sto"
    f.write_text(
        "# STOCKHOLM 1.0\n"
        "#=GF ID mixed\n"
        "#=GC RF xxxx\n"
        "#=GF DE after a GC line\n"
        "#=GC SS_cons ....\n"
        "#=GF CC trailing\n"
        "seqA ACGT\n"
        "#=GC RF yyyy\n"
        "//\n"
    )
    d = pb.describe_sto(str(f))
    assert d.select("kind", "feature", "value").rows() == [
        ("GF", "ID", "mixed"),
        # Repeated across two blocks, reported at its first position.
        ("GC", "RF", "xxxxyyyy"),
        ("GF", "DE", "after a GC line"),
        ("GC", "SS_cons", "...."),
        ("GF", "CC", "trailing"),
    ]


@pytest.mark.parametrize(
    "header",
    ["# STOCKHOLM 2.0", "# STOCKHOLM1.0", "# STOCKHOLM  1.0", "#STOCKHOLM 1.0"],
)
def test_sto_rejects_unsupported_headers(tmp_path, header):
    f = tmp_path / "h.sto"
    f.write_text(f"{header}\nseqA ACGT\n//\n")
    with pytest.raises(Exception) as exc:
        pb.read_sto(str(f))
    assert "# STOCKHOLM 1.0" in str(exc.value)


def test_describe_sto_interleaved_matches_easel_canonical_form():
    """#=GF lines must equal esl-reformat's canonical output except the three
    features Easel parses and normalises; #=GC per feature, order-insensitive."""
    ours = pb.describe_sto(p("RF00001.sto"))
    gf_ours = [
        (f, v)
        for f, v in ours.filter(pl.col("kind") == "GF")
        .select("feature", "value")
        .rows()
        if f not in EASEL_NORMALISED_GF
    ]
    gf_easel = [
        (f, v)
        for f, v in _naive_stockholm_lines("RF00001_pfam.sto", "#=GF")
        if f not in EASEL_NORMALISED_GF
    ]
    assert gf_ours == gf_easel
    gc_ours = dict(
        ours.filter(pl.col("kind") == "GC").select("feature", "value").rows()
    )
    gc_easel = dict(_naive_stockholm_lines("RF00001_pfam.sto", "#=GC"))
    assert gc_ours == gc_easel
    assert {len(v) for v in gc_ours.values()} == {230}


def test_describe_sto_multi_alignment():
    d = pb.describe_sto(p("multi.sto"))
    per = dict(d.group_by("alignment_id").agg(pl.col("n_sequences").first()).rows())
    assert per == {"7tm_1": 63, "5S_rRNA": 712}


# ---------------------------------------------------------------------------
# Live Easel CLI (only where HMMER is installed)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("esl-alistat") is None, reason="HMMER not on PATH")
@pytest.mark.parametrize("name", ["PF00001.sto", "RF00001.sto", "multi.sto"])
def test_live_esl_alistat_counts(name):
    out = subprocess.run(
        ["esl-alistat", p(name)], capture_output=True, text=True, check=True
    )
    nseq = [
        int(line.split()[-1])
        for line in out.stdout.splitlines()
        if line.startswith("Number of sequences")
    ]
    d = pb.describe_sto(p(name))
    ours = [
        n
        for _, n in d.group_by("alignment_id", maintain_order=True)
        .agg(pl.col("n_sequences").first())
        .rows()
    ]
    assert sorted(ours) == sorted(nseq)
