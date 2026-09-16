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
    <sup>2</sup>Parallel ranged reads from S3 need `datafusion-bio-formats` with [#253](https://github.com/biodatageeks/datafusion-bio-formats/pull/253); older builds stream S3 objects over one connection regardless of `concurrent_fetches`.

## Tuning concurrent requests

Every `read_*`, `scan_*`, `describe_*` and `register_*` function takes two
object-store options:

| Option               | Default | Meaning                                                                                   |
|----------------------|---------|-------------------------------------------------------------------------------------------|
| `concurrent_fetches` | `8`     | Number of ranged requests in flight for a whole-object read (S3, GCS, HTTP).              |
| `chunk_size`         | `8`     | Size in MiB of each ranged request (`register_*` functions default to `64`).             |

The default reads a whole file at the speed of a parallel download such as
`aws s3 cp`; on a high-latency link one sequential connection can take about
twice as long. Set `concurrent_fetches=1` to stream the object over a single
request. That is the right choice for pre-signed URLs that allow `GET` but
refuse `HEAD` (a chunked read needs the object size first), and it also keeps
memory lowest, since up to `concurrent_fetches × chunk_size` MiB can be in
flight per stream.

```python
import polars as pl
import polars_bio as pb

# default: 8 concurrent 8 MiB ranged requests
pb.scan_vcf("s3://bucket/cohort.vcf.bgz").select(pl.len()).collect()

# one sequential request, e.g. for a pre-signed GET-only URL
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
