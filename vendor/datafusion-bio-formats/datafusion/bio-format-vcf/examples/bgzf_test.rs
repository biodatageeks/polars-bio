use futures::StreamExt;
use noodles::{bgzf, vcf};
use opendal::layers::LoggingLayer;
use opendal::{Operator, services::S3};
use std::time::Instant;
use tokio_util::io::StreamReader;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let remote_files = vec![
        "release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz",
        "release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr21.vcf.bgz",
    ];

    for file_path in remote_files {
        for chunk_size in [16 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024 * 1024] {
            for concurrent in [8, 4, 2, 1] {
                let start_time = Instant::now();

                let builder = S3::default()
                    .region("us-east-1")
                    .bucket("gnomad-public-us-east-1")
                    .disable_ec2_metadata()
                    .allow_anonymous();

                let operator = Operator::new(builder)?
                    .layer(LoggingLayer::default())
                    .finish();

                println!(
                    "Testing file: {}, chunk size: {}MB, concurrent: {}",
                    file_path,
                    chunk_size / (1024 * 1024),
                    concurrent
                );

                let stream_start = Instant::now();
                let stream = operator
                    .reader_with(file_path)
                    .chunk(chunk_size)
                    .concurrent(concurrent)
                    .await?
                    .into_bytes_stream(..)
                    .await?;

                let stream_time = stream_start.elapsed();
                println!("Stream setup time: {stream_time:?}");

                let inner = bgzf::r#async::io::Reader::new(StreamReader::new(stream));
                let mut reader = vcf::r#async::io::Reader::new(inner);

                println!("Reading header...");
                let header = reader.read_header().await?;
                println!(
                    "Header read successfully. Contains {} contigs, {} filters, {} info fields, and {} format fields",
                    header.contigs().len(),
                    header.filters().len(),
                    header.infos().len(),
                    header.formats().len()
                );

                let fileformat = header.file_format();
                println!("VCF file format: {fileformat:?}");

                let essential_fields = ["AC", "AF", "AN"];
                for field in essential_fields {
                    if header.infos().get(field).is_some() {
                        println!("Found essential INFO field: {field}");
                    } else {
                        println!("Warning: Missing essential INFO field: {field}");
                    }
                }

                if header.contigs().is_empty() {
                    println!("Warning: No contig information in header");
                } else {
                    println!(
                        "File contains information for {} chromosomes/contigs",
                        header.contigs().len()
                    );
                }

                let mut count = 0;
                let mut batch_start = Instant::now();
                let mut batch_count = 0;
                const BATCH_SIZE: usize = 100000;

                let mut records = reader.records();
                while let Some(record_result) = records.next().await {
                    let _ = record_result?;
                    count += 1;
                    batch_count += 1;

                    if batch_count == BATCH_SIZE {
                        let batch_time = batch_start.elapsed();
                        let records_per_sec = BATCH_SIZE as f64 / batch_time.as_secs_f64();
                        println!(
                            "Processed batch of {BATCH_SIZE} records in {batch_time:?} ({records_per_sec:.2} records/sec). Total records: {count}"
                        );
                        batch_count = 0;
                        batch_start = Instant::now();
                    }
                }

                let total_time = start_time.elapsed();
                let avg_speed = count as f64 / total_time.as_secs_f64();
                println!("Records count: {count}");
                println!("Total time: {total_time:?}");
                println!("Average speed: {avg_speed:.2} records/sec");
                println!("---------------------------------------------");
            }
        }
    }

    Ok(())
}
