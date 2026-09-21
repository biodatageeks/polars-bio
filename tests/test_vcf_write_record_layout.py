"""Reproducing a record's own INFO and FORMAT key order with sink_vcf."""

import polars as pl

import polars_bio as pb

HEADER = (
    "##fileformat=VCFv4.2\n"
    "##contig=<ID=chr1,length=248956422>\n"
    '##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">\n'
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
    '##FORMAT=<ID=PS,Number=1,Type=Integer,Description="Phase set">\n'
    '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n"
)
# INFO in an order the header does not use, and PS present but missing: neither
# is recoverable from the typed columns.
RECORDS = [
    "chr1\t100\t.\tA\tG\t50\tPASS\tAF=0.5;DP=10\tGT:PS:DP\t0/1:.:25",
    "chr1\t200\t.\tC\tT\t50\tPASS\tDP=20\tGT:DP\t1/1:30",
    "chr1\t300\t.\tG\tA\t50\tPASS\tDP=30;AF=0.25\tDP:GT\t12:0/1",
]


def _source(tmp_path):
    src = tmp_path / "in.vcf"
    src.write_text(HEADER + "\n".join(RECORDS) + "\n")
    return src


def _records(path):
    return [l for l in path.read_text().splitlines() if not l.startswith("#")]


def test_the_layout_carry_reproduces_every_record_byte_for_byte(tmp_path):
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(_source(tmp_path)), preserve_record_layout=True)
    pb.sink_vcf(lf, str(out))
    assert _records(out) == RECORDS


def test_the_layout_carry_survives_a_row_filter(tmp_path):
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(_source(tmp_path)), preserve_record_layout=True)
    pb.sink_vcf(lf.filter(pl.col("start") != 200), str(out))
    assert _records(out) == [RECORDS[0], RECORDS[2]]


def test_the_layout_columns_are_not_written_as_info(tmp_path):
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(_source(tmp_path)), preserve_record_layout=True)
    pb.sink_vcf(lf, str(out))
    text = out.read_text()
    assert "_vcf_info_keys" not in text and "_vcf_format_keys" not in text


def test_read_vcf_carries_the_layout_too(tmp_path):
    out = tmp_path / "out.vcf"
    df = pb.read_vcf(str(_source(tmp_path)), preserve_record_layout=True)
    assert {"_vcf_info_keys", "_vcf_format_keys"} <= set(df.columns)
    pb.write_vcf(df, str(out))
    assert _records(out) == RECORDS


def test_without_the_carry_the_frame_and_the_output_are_as_before(tmp_path):
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(_source(tmp_path)))
    assert not [c for c in lf.collect_schema().names() if c.startswith("_vcf_")]
    pb.sink_vcf(lf, str(out))
    first = _records(out)[0].split("\t")
    # Header order, and a key missing in every sample is not written.
    assert first[7] == "DP=10;AF=0.5"
    assert first[8:10] == ["GT:DP", "0/1:25"]
