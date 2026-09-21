"""Header lines and FORMAT key order that sink_vcf has to keep."""

import polars_bio as pb

VCF = """##fileformat=VCFv4.2
##FILTER=<ID=LowQual,Description="Low quality">
##ALT=<ID=DEL,Description="Deletion">
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t50\tLowQual\tDP=10
"""


def test_filter_alt_and_fileformat_survive_a_round_trip(tmp_path):
    src = tmp_path / "in.vcf"
    src.write_text(VCF)
    out = tmp_path / "out.vcf"
    pb.sink_vcf(pb.scan_vcf(str(src)), str(out))
    header = [line for line in out.read_text().splitlines() if line.startswith("##")]
    assert "##fileformat=VCFv4.2" in header
    assert '##FILTER=<ID=LowQual,Description="Low quality">' in header
    assert '##ALT=<ID=DEL,Description="Deletion">' in header


def test_gt_is_the_first_format_key(tmp_path):
    src = tmp_path / "in.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1,length=248956422>\n"
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n"
        "chr1\t100\t.\tA\tG\t50\tPASS\t.\tGT:DP\t0/1:25\n"
    )
    out = tmp_path / "out.vcf"
    pb.sink_vcf(pb.scan_vcf(str(src)), str(out))
    record = [
        line for line in out.read_text().splitlines() if not line.startswith("#")
    ][0].split("\t")
    assert record[8:10] == ["GT:DP", "0/1:25"]


def test_gt_is_the_first_format_key_for_several_samples(tmp_path):
    # Several samples are read into a nested `genotypes` struct, which the
    # serializer writes through a different path from the single-sample one.
    src = tmp_path / "in.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1,length=248956422>\n"
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n"
        "chr1\t100\t.\tA\tG\t50\tPASS\t.\tGT:DP\t0/1:25\t1/1:30\n"
    )
    out = tmp_path / "out.vcf"
    pb.sink_vcf(pb.scan_vcf(str(src)), str(out))
    record = [
        line for line in out.read_text().splitlines() if not line.startswith("#")
    ][0].split("\t")
    assert record[8:11] == ["GT:DP", "0/1:25", "1/1:30"]


# Everything below the typed model: a date, the caller, a tool's command line,
# the PASS filter, and contig attributes other than ID and length.
FULL_HEADER = [
    "##fileformat=VCFv4.2",
    "##fileDate=20160824",
    "##source=myCaller-1.2",
    '##FILTER=<ID=PASS,Description="All filters passed">',
    '##FILTER=<ID=LowQual,Description="Low quality">',
    "##contig=<ID=chr1,length=248956422,assembly=GRCh38,md5=abc123>",
    '##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
    '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">',
    "##bcftools_normCommand=norm -m -both in.vcf",
]
FULL_BODY = (
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n"
    "chr1\t100\t.\tA\tG\t50\tPASS\tDP=10;AF=0.5\tGT:DP\t0/1:25\n"
)


def _write_full(tmp_path):
    src = tmp_path / "in.vcf"
    src.write_text("\n".join(FULL_HEADER) + "\n" + FULL_BODY)
    return src


def _meta_lines(path):
    return [line for line in path.read_text().splitlines() if line.startswith("##")]


def test_the_source_header_is_written_back_line_for_line(tmp_path):
    src = _write_full(tmp_path)
    out = tmp_path / "out.vcf"
    pb.sink_vcf(pb.scan_vcf(str(src)), str(out))
    assert _meta_lines(out) == FULL_HEADER


def test_the_source_header_survives_a_row_filter_and_a_collect(tmp_path):
    import polars as pl

    src = _write_full(tmp_path)
    out = tmp_path / "out.vcf"
    pb.write_vcf(pb.scan_vcf(str(src)).filter(pl.col("DP") == 10).collect(), str(out))
    assert _meta_lines(out) == FULL_HEADER


def test_a_caller_can_add_header_lines_of_its_own(tmp_path):
    # An annotator records what produced the fields it adds.
    src = _write_full(tmp_path)
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(src))
    header = dict(pb.get_metadata(lf)["header"])
    header["raw_lines"] = header["raw_lines"] + ['##myAnnotator="1.0" cache="x"']
    pb.set_source_metadata(lf, format="vcf", path=str(src), header=header)
    pb.sink_vcf(lf, str(out))
    assert _meta_lines(out) == FULL_HEADER + ['##myAnnotator="1.0" cache="x"']


def test_a_field_added_downstream_is_declared_after_the_source_header(tmp_path):
    import polars as pl

    src = _write_full(tmp_path)
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(src))
    header = dict(pb.get_metadata(lf)["header"])
    header["info_fields"] = {
        **header["info_fields"],
        "NEW": {"number": "1", "type": "Integer", "description": "Added downstream"},
    }
    lf = lf.with_columns(pl.lit(7, dtype=pl.Int32).alias("NEW"))
    pb.set_source_metadata(lf, format="vcf", path=str(src), header=header)
    pb.sink_vcf(lf, str(out))
    lines = _meta_lines(out)
    assert lines[: len(FULL_HEADER)] == FULL_HEADER
    assert (
        '##INFO=<ID=NEW,Number=1,Type=Integer,Description="Added downstream">'
        in lines[len(FULL_HEADER) :]
    )
    record = [l for l in out.read_text().splitlines() if not l.startswith("#")][0]
    assert "NEW=7" in record.split("\t")[7]


def test_clearing_a_declaration_list_removes_it_from_the_written_header(tmp_path):
    # The typed lists say which FILTER / contig / ALT declarations the header
    # should carry. An empty list is a statement too: all of them were removed.
    src = _write_full(tmp_path)
    out = tmp_path / "out.vcf"
    lf = pb.scan_vcf(str(src))
    header = dict(pb.get_metadata(lf)["header"])
    header["filters"] = []
    pb.set_source_metadata(lf, format="vcf", path=str(src), header=header)
    pb.sink_vcf(lf, str(out))
    lines = _meta_lines(out)
    assert not [line for line in lines if line.startswith("##FILTER=")]
    # Everything else is still the source's.
    assert [l for l in lines if not l.startswith("##FILTER=")] == [
        l for l in FULL_HEADER if not l.startswith("##FILTER=")
    ]
