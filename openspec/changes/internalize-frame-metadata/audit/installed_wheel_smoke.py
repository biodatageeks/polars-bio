"""Run with a clean wheel environment's python -I; writes a verification report."""

import hashlib
import importlib.metadata
import json
import platform
import sys
import tempfile
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

import polars_bio as pb
from polars_bio import _frame_metadata


def main():
    project = Path(__file__).resolve().parents[4]
    try:
        importlib.metadata.distribution("polars-config-meta")
    except importlib.metadata.PackageNotFoundError:
        pass
    else:
        raise AssertionError("The smoke environment contains the old distribution")
    assert "polars_config_meta" not in sys.modules
    assert Path(pb.__file__).parent.resolve() != project / "polars_bio"
    source = (project / "polars_bio/_frame_metadata.py").read_bytes()
    assert Path(_frame_metadata.__file__).read_bytes() == source
    assert (Path(pb.__file__).parent / "licenses/polars-config-meta/LICENSE").is_file()

    with tempfile.TemporaryDirectory(prefix="pb-metadata-wheel-") as directory:
        root = Path(directory)
        data = pl.DataFrame({"chrom": ["1", "1"], "start": [1, 10], "end": [5, 15]})
        data.write_csv(root / "input.csv")
        lf = pb.scan_csv(root / "input.csv", zero_based=True)
        spans = lf.group_by("chrom").agg(pl.col("start").min(), pl.col("end").max())
        spans = spans.filter(pl.col("start") >= 0)
        result = spans.collect()
        assert type(spans) is pl.LazyFrame and type(result) is pl.DataFrame
        assert result.config_meta.get_metadata()["coordinate_system_zero_based"]
        assert pb.overlap(spans, lf).collect().height == 2
        result.config_meta.write_parquet(root / "tagged.parquet")
        assert (
            pb.scan_parquet(root / "tagged.parquet", zero_based=True).collect().height
            == 1
        )

        nested = pl.LazyFrame({"calls": [[1, None], [2, 3]]})
        nested.config_meta.set(source_header='{"samples":["S1"]}')
        nested.config_meta.write_parquet(root / "nested.parquet")
        stored = json.loads(
            pq.read_schema(root / "nested.parquet").metadata[b"polars_plugin_meta"]
        )
        assert stored == nested.config_meta.get_metadata()

        vcf_path = project / "tests/data/io/vcf/vep.vcf"
        vcf = (
            pb.scan_vcf(str(vcf_path), use_zero_based=True)
            .filter(pl.col("start") >= 0)
            .head(2)
        )
        header = pb.get_metadata(vcf)["header"]
        pb.write_vcf(vcf, str(root / "output.vcf"))
        restored = pb.read_vcf(str(root / "output.vcf"), use_zero_based=True)
        assert restored.height == 2
        assert (
            pb.get_metadata(restored)["header"]["sample_names"]
            == header["sample_names"]
        )

    report = dict(
        python=platform.python_version(),
        polars=pl.__version__,
        polars_bio=pb.__version__,
        provider_sha256=hashlib.sha256(source).hexdigest(),
        legacy_distribution_absent=True,
        installed_source_matches_workspace=True,
        attribution_present=True,
        csv_group_range_parquet="passed",
        nested_schema_metadata="passed",
        vcf_roundtrip="passed",
    )
    Path(__file__).with_name("clean-install.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print(json.dumps(report))


if __name__ == "__main__":
    main()
