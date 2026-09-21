# Cloud storage ☁️

**On this page:** [Example](#example) · [Supported features](#supported-features) · [Tuning concurrent requests](#tuning-concurrent-requests) · [AWS S3](#aws-s3-configuration) · [Google Cloud Storage](#google-cloud-storage-configuration) · [Azure Blob Storage](#azure-blob-storage-configuration)

polars-bio supports direct streamed reading from cloud storages (e.g. S3, GCS) enabling processing large-scale genomics data without materializing in memory.
It is built upon the [OpenDAL](https://opendal.apache.org/) project, a unified data access layer for cloud storage, which allows to read  bioinformatic file formats from various cloud storage providers. For Apache DataFusion **native** file formats, such as Parquet or CSV please
refer to [DataFusion user guide](https://datafusion.apache.org/user-guide/cli/datasources.html#locations).


## Example
```python
import polars_bio as pb
## Register VCF files from Google Cloud Storage that will be streamed - no need to download them to the local disk, size ~0.8TB
pb.register_vcf("gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/vcf/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz", "gnomad_big", allow_anonymous=True)
pb.register_vcf("gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz", "gnomad_sv", allow_anonymous=True)
pb.overlap("gnomad_sv", "gnomad_big", streaming=True).sink_parquet("/tmp/overlap.parquet")
```
It is  especially useful when combined with [SQL](sql.md#sql-processing) support for preprocessing and [streaming](https://docs.pola.rs/user-guide/concepts/streaming/) processing capabilities.

!!! tip
    If you access cloud storage with authentication provided, please make sure the `allow_anonymous` parameter is set to `False` in the read/describe/register_table functions.

## Supported features

| Feature                         | AWS S3             | Google Cloud Storage | Azure Blob Storage |
|---------------------------------|--------------------|----------------------|--------------------|
| Anonymous access                | :white_check_mark: | :white_check_mark:   |                    |
| Authenticated access            | :white_check_mark: | :white_check_mark:   | :white_check_mark: |
| Requester Pays                  | :white_check_mark: |                      |                    |
| Concurrent requests<sup>1</sup> | :white_check_mark:<sup>2</sup> | :white_check_mark:   |                    |
| Streaming reads                 | :white_check_mark: | :white_check_mark:   | :white_check_mark: |

!!! note
    <sup>1</sup>For more information on concurrent requests and block size tuning please refer to [issue](https://github.com/biodatageeks/polars-bio/issues/132#issuecomment-2967687947).
    <sup>2</sup>The pinned `datafusion-bio-formats` revision includes [#253](https://github.com/biodatageeks/datafusion-bio-formats/pull/253). Older builds without that fix stream S3 objects over one connection regardless of `concurrent_fetches`.

## Tuning concurrent requests

The `read_*`, `scan_*`, `describe_*` and `register_*` functions that expose
`concurrent_fetches` and `chunk_size` accept the following object-store options.
Backend support varies by format and reader path. Check the function signature:
for example, `describe_vcf`, `describe_bcf`,
`describe_bgen`, `describe_pgen` and `describe_cool` do not expose them.

| Option | Default | Meaning |
|--------|---------|---------|
| `concurrent_fetches` | `8` | Maximum number of ranged requests in flight for a whole-object read (S3, GCS, HTTP). |
| `chunk_size` | `8` MiB for `read_*`, `scan_*`, `describe_*` and `register_fasta`; `64` MiB for other `register_*` functions | Size of each ranged request. |

Parallel ranged requests can improve whole-file throughput on high-latency
links. Actual throughput depends on the backend, network and file size. Up to
`concurrent_fetches × chunk_size` MiB can be in flight per stream: 64 MiB for
an 8 MiB chunk, or 512 MiB for a 64 MiB chunk.

Set `concurrent_fetches=1` to disable parallel fetching. S3 whole-object reads
then use one sequential request without a size preflight. HTTP and GCS may
still split the read into chunks and issue a HEAD request; setting concurrency
to 1 does not guarantee compatibility with GET-only pre-signed HTTP URLs.
Readers using the core full-object streaming helper retry with one sequential
GET if the server refuses the HEAD preflight. Indexed and other reader paths
may still require HEAD support.

```python
import polars as pl
import polars_bio as pb

# default: 8 concurrent 8 MiB ranged requests
pb.scan_vcf("s3://bucket/cohort.vcf.bgz").select(pl.len()).collect()

# one sequential S3 request
pb.scan_vcf("s3://bucket/cohort.vcf.bgz", concurrent_fetches=1)
```

## AWS S3 configuration
Supported environment variables:

| Variable                          | Description                                                 |
|-----------------------------------|-------------------------------------------------------------|
| AWS_ACCESS_KEY_ID                 | AWS access key ID for authenticated access to S3.           |
| AWS_SECRET_ACCESS_KEY             | AWS secret access key for authenticated access to S3.       |
| AWS_ENDPOINT_URL                  | Custom S3 endpoint URL for accessing S3-compatible storage. |
| AWS_REGION  or AWS_DEFAULT_REGION | AWS region for accessing S3.                                |

## Google Cloud Storage configuration

Supported environment variables:

| Variable                       | Description                                                                        |
|--------------------------------|------------------------------------------------------------------------------------|
| GOOGLE_APPLICATION_CREDENTIALS | Path to the Google Cloud service account key file for authenticated access to GCS. |

## Azure Blob Storage configuration
Supported environment variables:

| Variable              | Description                                                                |
|-----------------------|----------------------------------------------------------------------------|
| AZURE_STORAGE_ACCOUNT | Azure Storage account name for authenticated access to Azure Blob Storage. |
| AZURE_STORAGE_KEY     | Azure Storage account key for authenticated access to Azure Blob Storage.  |
| AZURE_ENDPOINT_URL    | Azure Blob Storage endpoint URL for accessing Azure Blob Storage.          |
