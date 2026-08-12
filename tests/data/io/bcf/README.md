# BCF parity fixtures

These files are binary conversions of the existing fixtures in
`tests/data/io/vcf`. They let the same test expectations exercise the VCF and
BCF decoders without maintaining a second hand-written dataset.

The fixtures were generated with bcftools 1.21 using `bcftools view
--no-version -Ob`. For VCF headers without a `##contig` declaration, a temporary
header used only during conversion was populated from the observed contigs;
the original VCF fixtures were not modified. Coordinate-sorted BCF files were
indexed with `bcftools index --csi`.

| VCF source | BCF fixture |
| --- | --- |
| `antku_small.vcf.gz` | `antku_small.bcf` |
| `ensembl.vcf` | `ensembl.bcf` |
| `ensembl-2.vcf` | `ensembl-2.bcf` |
| `info_bare_key.vcf` | `info_bare_key.bcf` |
| `info_bare_key_realdata.vcf` | `info_bare_key_realdata.bcf` |
| `info_invalid_flag_value.vcf` | `info_invalid_flag_value.bcf` |
| `info_missing_array.vcf` | `info_missing_array.bcf` |
| `multi_chrom.vcf.gz` | `multi_chrom.bcf` |
| `multisample.vcf` | `multisample.bcf` |
| `multisample.vcf.gz` | `multisample_large.bcf` |
| `single_sample_collision.vcf` | `single_sample_collision.bcf` |
| `vep.vcf` | `vep.bcf` |
| `vep_annotate_test.vcf` | `vep_annotate_test.bcf` |

`ensembl.bcf` intentionally has no CSI because its source records are not in
coordinate order. The invalid Flag fixture is expected to fail in both
encodings: BCF preserves the incompatible typed value rather than making the
record valid.
