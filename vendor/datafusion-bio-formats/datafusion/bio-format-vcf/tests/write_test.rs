//! Integration tests for VCF write functionality

use datafusion::arrow::array::{
    Float64Array, Int32Builder, LargeListBuilder, LargeStringBuilder, ListArray, ListBuilder,
    RecordBatch, StringArray, StringBuilder, StringViewBuilder, StructArray, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion_bio_format_core::metadata::{
    AltAlleleMetadata, ContigMetadata, FilterMetadata, VCF_ALTERNATIVE_ALLELES_KEY,
    VCF_CONTIGS_KEY, VCF_FIELD_DESCRIPTION_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY,
    VCF_FIELD_TYPE_KEY, VCF_FILE_FORMAT_KEY, VCF_FILTERS_KEY, VCF_FORMAT_FIELDS_KEY,
    VCF_GENOTYPES_SAMPLE_NAMES_KEY, VCF_SAMPLE_NAMES_KEY, VcfFieldMetadata, from_json_string,
    to_json_string,
};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use datafusion_bio_format_vcf::writer::VcfCompressionType;
use std::sync::Arc;
use tokio::fs;

/// Sample VCF with INFO and FORMAT fields for round-trip testing
const SAMPLE_VCF_ROUNDTRIP: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Sample read depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs1	A	T	60	PASS	DP=50;AF=0.25;DB	GT:DP:GQ	0/1:20:99	1/1:30:95
chr1	200	rs2	G	C	80	PASS	DP=60;AF=0.10	GT:DP:GQ	0/0:25:99	0/1:35:90
"#;

/// Simple VCF for basic write tests
const SIMPLE_VCF: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	30	PASS	DP=50
chr1	200	rs2	G	C	40	PASS	DP=60
"#;

async fn create_test_vcf(name: &str, content: &str) -> std::io::Result<String> {
    let path = format!("/tmp/test_write_{name}.vcf");
    fs::write(&path, content).await?;
    Ok(path)
}

async fn cleanup_files(paths: &[&str]) {
    for path in paths {
        let _ = fs::remove_file(path).await;
    }
}

fn create_read_provider(
    path: &str,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
) -> VcfTableProvider {
    VcfTableProvider::new(
        path.to_string(),
        info_fields,
        format_fields,
        None, // object_storage_options
        true, // coordinate_system_zero_based
    )
    .unwrap()
}

#[tokio::test]
async fn test_write_vcf_basic() {
    let input_path = create_test_vcf("basic_input", SIMPLE_VCF).await.unwrap();
    let output_path = "/tmp/test_write_basic_output.vcf";

    let ctx = SessionContext::new();

    // Register source table (reads from existing file)
    let source = create_read_provider(&input_path, Some(vec!["DP".to_string()]), None);
    let source_schema = source.schema();
    let sample_names: Vec<String> = vec![];
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Register destination table using write-only constructor
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        source_schema,
        vec!["DP".to_string()],
        vec![],
        sample_names,
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    // Write data
    let result = ctx
        .sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify count
    assert_eq!(result.len(), 1);
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2);

    // Read back and verify
    let content = fs::read_to_string(output_path).await.unwrap();
    assert!(content.contains("##fileformat=VCF"));
    assert!(content.contains("#CHROM\tPOS"));
    assert!(content.contains("chr1\t100"));
    assert!(content.contains("chr1\t200"));

    cleanup_files(&[&input_path, output_path]).await;
}

#[tokio::test]
async fn test_write_vcf_preserves_info_description() {
    let input_path = create_test_vcf("desc_input", SIMPLE_VCF).await.unwrap();
    let output_path = "/tmp/test_write_desc_output.vcf";

    let ctx = SessionContext::new();

    // Register source table
    let source = create_read_provider(&input_path, Some(vec!["DP".to_string()]), None);
    let source_schema = source.schema();
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Register dest with write-only constructor
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        source_schema,
        vec!["DP".to_string()],
        vec![],
        vec![],
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    // Write data
    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read output and verify header contains original description
    let content = fs::read_to_string(output_path).await.unwrap();
    assert!(
        content.contains("Description=\"Read Depth\""),
        "Output should preserve original INFO description. Got: {content}"
    );

    cleanup_files(&[&input_path, output_path]).await;
}

#[tokio::test]
async fn test_write_vcf_multi_sample() {
    let input_path = create_test_vcf("multi_input", SAMPLE_VCF_ROUNDTRIP)
        .await
        .unwrap();
    let output_path = "/tmp/test_write_multi_output.vcf";

    let ctx = SessionContext::new();

    // Register source table with INFO and FORMAT fields
    let source = create_read_provider(
        &input_path,
        Some(vec!["DP".to_string(), "AF".to_string(), "DB".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()]),
    );
    let source_schema = source.schema();
    let sample_names = vec!["Sample1".to_string(), "Sample2".to_string()];
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Register destination table with write-only constructor
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        source_schema,
        vec!["DP".to_string(), "AF".to_string(), "DB".to_string()],
        vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()],
        sample_names,
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    // Write data
    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read output and verify
    let content = fs::read_to_string(output_path).await.unwrap();

    // Check header metadata is preserved
    assert!(content.contains("##INFO=<ID=DP"));
    assert!(content.contains("##INFO=<ID=AF"));
    assert!(content.contains("##FORMAT=<ID=GT"));
    assert!(content.contains("##FORMAT=<ID=DP"));

    // Check sample names in header
    assert!(content.contains("Sample1"));
    assert!(content.contains("Sample2"));

    // Check data rows
    assert!(content.contains("chr1\t100"));
    assert!(content.contains("0/1")); // Sample1 GT
    assert!(content.contains("1/1")); // Sample2 GT

    cleanup_files(&[&input_path, output_path]).await;
}

#[tokio::test]
async fn test_insert_overwrite_with_inferred_fields_preserves_info_and_format() {
    let source_path = create_test_vcf("infer_fields_source", SAMPLE_VCF_ROUNDTRIP)
        .await
        .unwrap();
    // Destination must exist so VcfTableProvider::new keeps info_fields/format_fields as None.
    let output_path = create_test_vcf("infer_fields_dest", SAMPLE_VCF_ROUNDTRIP)
        .await
        .unwrap();

    let ctx = SessionContext::new();

    let source = create_read_provider(
        &source_path,
        Some(vec!["DP".to_string(), "AF".to_string(), "DB".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()]),
    );
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Keep None/None to validate inference from schema during INSERT OVERWRITE.
    let dest = create_read_provider(&output_path, None, None);
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(&output_path).await.unwrap();

    // INFO/FORMAT headers and per-record values should be preserved.
    assert!(content.contains("##INFO=<ID=DP"));
    assert!(content.contains("##INFO=<ID=AF"));
    assert!(content.contains("##FORMAT=<ID=GT"));
    assert!(content.contains("##FORMAT=<ID=DP"));
    assert!(content.contains("GT:DP:GQ"));
    assert!(content.contains("0/1:20:99"));
    assert!(content.contains("1/1:30:95"));

    cleanup_files(&[&source_path, &output_path]).await;
}

#[tokio::test]
async fn test_write_vcf_multisample_large_list_roundtrip_preserves_gt_values() {
    let output_path = "/tmp/test_write_multisample_large_list_roundtrip.vcf";

    let mut gt_metadata = std::collections::HashMap::new();
    gt_metadata.insert(
        VCF_FIELD_DESCRIPTION_KEY.to_string(),
        "Genotype".to_string(),
    );
    gt_metadata.insert(VCF_FIELD_TYPE_KEY.to_string(), "String".to_string());
    gt_metadata.insert(VCF_FIELD_NUMBER_KEY.to_string(), "1".to_string());
    gt_metadata.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT".to_string());

    // Columnar schema: genotypes: Struct<GT: List<Utf8>>
    let gt_list_field = Field::new(
        "GT",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )
    .with_metadata(gt_metadata);

    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![gt_list_field.clone()].into()),
            true,
        ),
    ]));

    // Build columnar genotypes: GT = ["0/1", "1/1"]
    let mut gt_builder = ListBuilder::new(StringBuilder::new());
    gt_builder.values().append_value("0/1");
    gt_builder.values().append_value("1/1");
    gt_builder.append(true);
    let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let genotypes =
        Arc::new(StructArray::try_new(vec![gt_list_field].into(), vec![gt_array], None).unwrap());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![Some("rs1")])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["T"])),
            Arc::new(Float64Array::from(vec![Some(60.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            genotypes,
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let source = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("source", Arc::new(source)).unwrap();

    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        schema,
        vec![],
        vec!["GT".to_string()],
        vec!["Sample1".to_string(), "Sample2".to_string()],
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();
    assert!(content.contains("##FORMAT=<ID=GT"));
    assert!(content.contains("GT\t0/1\t1/1"));

    let roundtrip = create_read_provider(output_path, Some(vec![]), Some(vec!["GT".to_string()]));
    let ctx_read = SessionContext::new();
    ctx_read.register_table("rt", Arc::new(roundtrip)).unwrap();

    let batches = ctx_read
        .sql("SELECT genotypes FROM rt")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Columnar layout: genotypes is a StructArray with List<Utf8> children
    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let gt_list = genotypes
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let row0_gt = gt_list.value(0);
    let gts = row0_gt.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(gts.value(0), "0/1");
    assert_eq!(gts.value(1), "1/1");

    cleanup_files(&[output_path]).await;
}

#[tokio::test]
async fn test_write_vcf_coordinate_conversion() {
    let input_path = create_test_vcf("coord_input", SIMPLE_VCF).await.unwrap();
    let output_path = "/tmp/test_write_coord_output.vcf";

    let ctx = SessionContext::new();

    // Read with 0-based coordinates
    let source = create_read_provider(&input_path, Some(vec!["DP".to_string()]), None);
    let source_schema = source.schema();
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Write with 0-based coordinates (should convert back to 1-based in VCF)
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        source_schema,
        vec!["DP".to_string()],
        vec![],
        vec![],
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify positions are 1-based in output (100, 200)
    let content = fs::read_to_string(output_path).await.unwrap();
    assert!(
        content.contains("chr1\t100\t"),
        "Position should be 1-based (100)"
    );
    assert!(
        content.contains("chr1\t200\t"),
        "Position should be 1-based (200)"
    );

    cleanup_files(&[&input_path, output_path]).await;
}

#[tokio::test]
async fn test_write_vcf_with_filter() {
    let input_path = create_test_vcf("filter_input", SIMPLE_VCF).await.unwrap();
    let output_path = "/tmp/test_write_filter_output.vcf";

    let ctx = SessionContext::new();

    let source = create_read_provider(&input_path, Some(vec!["DP".to_string()]), None);
    let source_schema = source.schema();
    ctx.register_table("source", Arc::new(source)).unwrap();

    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        source_schema,
        vec!["DP".to_string()],
        vec![],
        vec![],
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    // Filter to only include first record (0-based start = 99)
    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source WHERE start = 99")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();
    assert!(content.contains("chr1\t100\t"));
    assert!(!content.contains("chr1\t200\t"));

    cleanup_files(&[&input_path, output_path]).await;
}

#[tokio::test]
async fn test_compression_type_detection() {
    assert_eq!(
        VcfCompressionType::from_path("test.vcf"),
        VcfCompressionType::Plain
    );
    assert_eq!(
        VcfCompressionType::from_path("test.vcf.gz"),
        VcfCompressionType::Gzip
    );
    assert_eq!(
        VcfCompressionType::from_path("test.vcf.bgz"),
        VcfCompressionType::Bgzf
    );
    assert_eq!(
        VcfCompressionType::from_path("test.vcf.bgzf"),
        VcfCompressionType::Bgzf
    );
}

#[tokio::test]
async fn test_schema_field_metadata_preserved() {
    let input_path = create_test_vcf("meta_input", SAMPLE_VCF_ROUNDTRIP)
        .await
        .unwrap();

    // Create table provider and check schema metadata
    let provider = VcfTableProvider::new(
        input_path.clone(),
        Some(vec!["DP".to_string(), "AF".to_string()]),
        Some(vec!["GT".to_string()]),
        None,
        true,
    )
    .unwrap();

    let schema = provider.schema();

    // Check INFO field metadata using new bio.vcf.field.* keys
    let dp_field = schema.field_with_name("DP").unwrap();
    let dp_metadata = dp_field.metadata();
    assert_eq!(
        dp_metadata.get(VCF_FIELD_DESCRIPTION_KEY),
        Some(&"Total read depth".to_string())
    );
    assert_eq!(
        dp_metadata.get(VCF_FIELD_TYPE_KEY),
        Some(&"Integer".to_string())
    );
    assert_eq!(
        dp_metadata.get(VCF_FIELD_NUMBER_KEY),
        Some(&"1".to_string())
    );

    let af_field = schema.field_with_name("AF").unwrap();
    let af_metadata = af_field.metadata();
    assert_eq!(
        af_metadata.get(VCF_FIELD_DESCRIPTION_KEY),
        Some(&"Allele frequency".to_string())
    );
    assert_eq!(
        af_metadata.get(VCF_FIELD_TYPE_KEY),
        Some(&"Float".to_string())
    );
    assert_eq!(
        af_metadata.get(VCF_FIELD_NUMBER_KEY),
        Some(&"A".to_string())
    );

    // Check FORMAT field metadata in columnar multisample schema
    let genotypes = schema.field_with_name("genotypes").unwrap();
    let struct_fields = match genotypes.data_type() {
        DataType::Struct(fields) => fields,
        other => panic!("expected Struct for genotypes, got {other:?}"),
    };
    let gt_field = struct_fields.iter().find(|f| f.name() == "GT").unwrap();
    let gt_metadata = gt_field.metadata();
    assert_eq!(
        gt_metadata.get(VCF_FIELD_DESCRIPTION_KEY),
        Some(&"Genotype".to_string())
    );
    assert_eq!(
        gt_metadata.get(VCF_FIELD_FORMAT_ID_KEY),
        Some(&"GT".to_string())
    );

    // Check schema-level FORMAT metadata map for nested multisample schemas
    let format_fields_json = schema
        .metadata()
        .get(VCF_FORMAT_FIELDS_KEY)
        .expect("schema should include bio.vcf.format_fields metadata");
    let format_fields =
        from_json_string::<std::collections::HashMap<String, VcfFieldMetadata>>(format_fields_json)
            .expect("bio.vcf.format_fields should be valid JSON");
    let gt_def = format_fields.get("GT").expect("GT should be present");
    assert_eq!(gt_def.number, "1");
    assert_eq!(gt_def.field_type, "String");
    assert_eq!(gt_def.description, "Genotype");

    cleanup_files(&[&input_path]).await;
}

#[tokio::test]
async fn test_multisample_schema_metadata_exposes_format_descriptions() {
    let input_path = create_test_vcf("multisample_format_meta", SAMPLE_VCF_ROUNDTRIP)
        .await
        .unwrap();

    let provider = VcfTableProvider::new(
        input_path.clone(),
        Some(vec!["DP".to_string(), "AF".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()]),
        None,
        true,
    )
    .unwrap();
    let schema = provider.schema();

    let format_fields_json = schema
        .metadata()
        .get(VCF_FORMAT_FIELDS_KEY)
        .expect("schema should include bio.vcf.format_fields metadata");
    let format_fields =
        from_json_string::<std::collections::HashMap<String, VcfFieldMetadata>>(format_fields_json)
            .expect("bio.vcf.format_fields should be valid JSON");

    let gt = format_fields.get("GT").expect("GT should be present");
    assert_eq!(gt.number, "1");
    assert_eq!(gt.field_type, "String");
    assert_eq!(gt.description, "Genotype");

    let dp = format_fields.get("DP").expect("DP should be present");
    assert_eq!(dp.number, "1");
    assert_eq!(dp.field_type, "Integer");
    assert_eq!(dp.description, "Sample read depth");

    cleanup_files(&[&input_path]).await;
}

#[tokio::test]
async fn test_header_metadata_roundtrip() {
    const FULL_HEADER_VCF: &str = r#"##fileformat=VCFv4.3
##FILTER=<ID=LowQual,Description="Low quality">
##contig=<ID=chr1,length=249250621>
##ALT=<ID=DEL,Description="Deletion">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	.	A	T	30	PASS	DP=50
"#;

    let input_path = create_test_vcf("header_meta", FULL_HEADER_VCF)
        .await
        .unwrap();

    let provider = create_read_provider(&input_path, Some(vec!["DP".to_string()]), None);

    let schema = provider.schema();
    let metadata = schema.metadata();

    // Verify file format
    assert_eq!(metadata.get(VCF_FILE_FORMAT_KEY).unwrap(), "VCFv4.3");

    // Verify filters using shared utilities
    let filters_json = metadata.get(VCF_FILTERS_KEY).unwrap();
    let filters = from_json_string::<Vec<FilterMetadata>>(filters_json).unwrap();
    assert!(
        filters
            .iter()
            .any(|f| f.id == "LowQual" && f.description == "Low quality")
    );

    // Verify contigs using shared utilities
    let contigs_json = metadata.get(VCF_CONTIGS_KEY).unwrap();
    let contigs = from_json_string::<Vec<ContigMetadata>>(contigs_json).unwrap();
    assert!(
        contigs
            .iter()
            .any(|c| c.id == "chr1" && c.length == Some(249250621))
    );

    // Verify ALT using shared utilities
    let alts_json = metadata.get(VCF_ALTERNATIVE_ALLELES_KEY).unwrap();
    let alts = from_json_string::<Vec<AltAlleleMetadata>>(alts_json).unwrap();
    assert!(
        alts.iter()
            .any(|a| a.id == "DEL" && a.description == "Deletion")
    );

    // Verify samples
    let samples_json = metadata.get(VCF_SAMPLE_NAMES_KEY).unwrap();
    let samples = from_json_string::<Vec<String>>(samples_json).unwrap();
    assert_eq!(samples.len(), 0); // No samples in this VCF

    cleanup_files(&[&input_path]).await;
}

/// Tests writing from a `named_struct()` transformed query where the genotypes
/// struct has no sample name metadata. The writer should infer sample count from
/// list lengths in the first batch and generate SAMPLE_0, SAMPLE_1, ... names.
#[tokio::test]
async fn test_write_vcf_from_named_struct_genotypes() {
    let output_path = "/tmp/test_write_named_struct_genotypes.vcf";

    // Build a genotypes struct column with NO sample name metadata (simulates named_struct)
    let gt_list_field = Field::new(
        "GT",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![gt_list_field.clone()].into()),
            true,
        ),
    ]));

    // Build GT = ["0/1", "1/1"] for 2 samples
    let mut gt_builder = ListBuilder::new(StringBuilder::new());
    gt_builder.values().append_value("0/1");
    gt_builder.values().append_value("1/1");
    gt_builder.append(true);
    // Second row
    gt_builder.values().append_value("0/0");
    gt_builder.values().append_value("0/1");
    gt_builder.append(true);
    let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let genotypes =
        Arc::new(StructArray::try_new(vec![gt_list_field].into(), vec![gt_array], None).unwrap());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["chr1", "chr1"])),
            Arc::new(UInt32Array::from(vec![99u32, 199])),
            Arc::new(UInt32Array::from(vec![100u32, 200])),
            Arc::new(StringArray::from(vec![Some("rs1"), Some("rs2")])),
            Arc::new(StringArray::from(vec!["A", "G"])),
            Arc::new(StringArray::from(vec!["T", "C"])),
            Arc::new(Float64Array::from(vec![Some(60.0), Some(80.0)])),
            Arc::new(StringArray::from(vec![Some("PASS"), Some("PASS")])),
            genotypes,
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let source = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Destination has empty sample_names and format_fields — will fall back to input schema
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        schema,
        vec![],
        vec![],
        vec![], // empty sample_names triggers inference
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();

    // Header should have FORMAT and generated sample names
    assert!(
        content.contains("FORMAT\tSAMPLE_0\tSAMPLE_1"),
        "Expected FORMAT\\tSAMPLE_0\\tSAMPLE_1 in header. Got:\n{content}"
    );
    assert!(
        content.contains("##FORMAT=<ID=GT"),
        "Expected GT FORMAT header line. Got:\n{content}"
    );
    // Data rows should have per-sample GT values
    assert!(
        content.contains("GT\t0/1\t1/1"),
        "Expected GT values for row 1. Got:\n{content}"
    );
    assert!(
        content.contains("GT\t0/0\t0/1"),
        "Expected GT values for row 2. Got:\n{content}"
    );

    cleanup_files(&[output_path]).await;
}

/// Tests that when the input schema has VCF_GENOTYPES_SAMPLE_NAMES_KEY metadata
/// on the genotypes field, the writer uses those sample names instead of generating them.
#[tokio::test]
async fn test_write_vcf_input_schema_metadata_fallback() {
    let output_path = "/tmp/test_write_input_schema_metadata_fallback.vcf";

    let gt_list_field = Field::new(
        "GT",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );

    // Genotypes field WITH sample name metadata
    let mut genotypes_metadata = std::collections::HashMap::new();
    genotypes_metadata.insert(
        VCF_GENOTYPES_SAMPLE_NAMES_KEY.to_string(),
        to_json_string(&vec!["NA12878".to_string(), "NA12891".to_string()]),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![gt_list_field.clone()].into()),
            true,
        )
        .with_metadata(genotypes_metadata),
    ]));

    // Build GT = ["0/1", "1/1"]
    let mut gt_builder = ListBuilder::new(StringBuilder::new());
    gt_builder.values().append_value("0/1");
    gt_builder.values().append_value("1/1");
    gt_builder.append(true);
    let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let genotypes =
        Arc::new(StructArray::try_new(vec![gt_list_field].into(), vec![gt_array], None).unwrap());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![Some("rs1")])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["T"])),
            Arc::new(Float64Array::from(vec![Some(60.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            genotypes,
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let source = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("source", Arc::new(source)).unwrap();

    // Destination has same column structure but NO sample name metadata on genotypes,
    // and empty sample_names/format_fields — will fall back to input schema metadata
    let dest_gt_field = Field::new(
        "GT",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    let dest_schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![dest_gt_field].into()),
            true,
        ),
    ]));
    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        dest_schema,
        vec![],
        vec![],
        vec![], // empty — will fall back to input schema metadata
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();

    // Should use sample names from input schema metadata
    assert!(
        content.contains("FORMAT\tNA12878\tNA12891"),
        "Expected FORMAT\\tNA12878\\tNA12891 in header. Got:\n{content}"
    );
    assert!(
        content.contains("GT\t0/1\t1/1"),
        "Expected GT values. Got:\n{content}"
    );

    cleanup_files(&[output_path]).await;
}

/// Tests that LargeListArray children in genotypes struct are handled correctly
/// for sample name inference. DataFusion and Polars default to LargeUtf8/LargeList types
/// when building structs via named_struct(), so the writer must handle both variants.
#[tokio::test]
async fn test_write_vcf_from_large_list_genotypes() {
    let output_path = "/tmp/test_write_large_list_genotypes.vcf";

    // Build a genotypes struct with LargeList<LargeUtf8> children (DataFusion default)
    let gt_large_list_field = Field::new(
        "GT",
        DataType::LargeList(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
        true,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![gt_large_list_field.clone()].into()),
            true,
        ),
    ]));

    // Build GT = ["0/1", "1/1"] using LargeListBuilder<LargeStringBuilder>
    let mut gt_builder = LargeListBuilder::new(LargeStringBuilder::new());
    gt_builder.values().append_value("0/1");
    gt_builder.values().append_value("1/1");
    gt_builder.append(true);
    let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let genotypes = Arc::new(
        StructArray::try_new(vec![gt_large_list_field].into(), vec![gt_array], None).unwrap(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![Some("rs1")])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["T"])),
            Arc::new(Float64Array::from(vec![Some(60.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            genotypes,
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let source = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("source", Arc::new(source)).unwrap();

    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        schema,
        vec![],
        vec![],
        vec![], // empty — triggers LargeList inference path
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();

    assert!(
        content.contains("FORMAT\tSAMPLE_0\tSAMPLE_1"),
        "Expected FORMAT\\tSAMPLE_0\\tSAMPLE_1 in header. Got:\n{content}"
    );
    assert!(
        content.contains("GT\t0/1\t1/1"),
        "Expected GT values. Got:\n{content}"
    );

    cleanup_files(&[output_path]).await;
}

/// Tests that Utf8View (StringViewArray) values in genotypes struct are correctly serialized.
/// DataFusion uses Utf8View for string values in certain operations like named_struct().
#[tokio::test]
async fn test_write_vcf_utf8view_genotypes() {
    let output_path = "/tmp/test_write_utf8view_genotypes.vcf";

    // Build genotypes with LargeList<Utf8View> — the type DataFusion produces via named_struct()
    let gt_field = Field::new(
        "GT",
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8View, true))),
        true,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![gt_field.clone()].into()),
            true,
        ),
    ]));

    // Build GT = ["0/1", "1/1"] using LargeListBuilder<StringViewBuilder>
    let mut gt_builder = LargeListBuilder::new(StringViewBuilder::new());
    gt_builder.values().append_value("0/1");
    gt_builder.values().append_value("1/1");
    gt_builder.append(true);
    let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let genotypes =
        Arc::new(StructArray::try_new(vec![gt_field].into(), vec![gt_array], None).unwrap());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![Some("rs1")])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["T"])),
            Arc::new(Float64Array::from(vec![Some(60.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            genotypes,
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let source = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("source", Arc::new(source)).unwrap();

    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        schema,
        vec![],
        vec![],
        vec![],
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();

    // GT values must be actual genotypes, not "."
    assert!(
        content.contains("GT\t0/1\t1/1"),
        "GT values should be '0/1' and '1/1', not '.'. Got:\n{content}"
    );

    cleanup_files(&[output_path]).await;
}

/// Tests that FORMAT field types are correctly inferred from Arrow types in the deferred
/// header path. LargeList<Int32> should produce Type=Integer, not Type=String.
#[tokio::test]
async fn test_write_vcf_format_type_inference_from_arrow_types() {
    let output_path = "/tmp/test_write_format_type_inference.vcf";

    // Build genotypes with mixed types: GT=LargeList<Utf8View>, DP=LargeList<Int32>
    let gt_field = Field::new(
        "GT",
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8View, true))),
        true,
    );
    let dp_field = Field::new(
        "DP",
        DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new(
            "genotypes",
            DataType::Struct(vec![gt_field.clone(), dp_field.clone()].into()),
            true,
        ),
    ]));

    // Build GT = ["0/1", "1/1"], DP = [25, 30]
    let mut gt_builder = LargeListBuilder::new(StringViewBuilder::new());
    gt_builder.values().append_value("0/1");
    gt_builder.values().append_value("1/1");
    gt_builder.append(true);
    let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let mut dp_builder = LargeListBuilder::new(Int32Builder::new());
    dp_builder.values().append_value(25);
    dp_builder.values().append_value(30);
    dp_builder.append(true);
    let dp_array = Arc::new(dp_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

    let genotypes = Arc::new(
        StructArray::try_new(
            vec![gt_field, dp_field].into(),
            vec![gt_array, dp_array],
            None,
        )
        .unwrap(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![Some("rs1")])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["T"])),
            Arc::new(Float64Array::from(vec![Some(60.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            genotypes,
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let source = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("source", Arc::new(source)).unwrap();

    let dest = VcfTableProvider::new_for_write(
        output_path.to_string(),
        schema,
        vec![],
        vec![],
        vec![],
        true,
    );
    ctx.register_table("dest", Arc::new(dest)).unwrap();

    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let content = fs::read_to_string(output_path).await.unwrap();

    // GT should be Type=String
    assert!(
        content.contains("##FORMAT=<ID=GT,Number=1,Type=String"),
        "GT should be Type=String. Got:\n{content}"
    );
    // DP should be Type=Integer (not Type=String)
    assert!(
        content.contains("##FORMAT=<ID=DP,Number=1,Type=Integer"),
        "DP should be Type=Integer. Got:\n{content}"
    );
    // Data should have correct values
    assert!(
        content.contains("GT:DP\t0/1:25\t1/1:30"),
        "Expected GT:DP values. Got:\n{content}"
    );

    cleanup_files(&[output_path]).await;
}
