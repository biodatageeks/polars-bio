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
