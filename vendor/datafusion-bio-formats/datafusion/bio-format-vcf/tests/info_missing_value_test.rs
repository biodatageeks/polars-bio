use datafusion::arrow::array::{Array, Float32Array, Int32Array, ListArray, StringArray};
use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;
use tokio::fs;

/// VCF with multi-valued INFO fields containing `.` (missing value).
/// AD has Number=R (one per allele including ref), AF has Number=A, ALLELE_ID has Number=.
const VCF_WITH_MISSING_INFO_ARRAY: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=AD,Number=R,Type=Integer,Description="Allelic depths for ref and alt alleles">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=ALLELE_ID,Number=.,Type=String,Description="Allele identifiers">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs1	A	T	60	PASS	AD=.,15;AF=0.5;ALLELE_ID=.,alt1
chr1	200	rs2	G	C,T	80	PASS	AD=10,.,5;AF=.,0.3;ALLELE_ID=ref2,.,alt2
chr1	300	rs3	C	T,A	70	PASS	AD=5,.,10;AF=0.3,.;ALLELE_ID=ref3,alt3a,.
chr1	400	rs4	T	G	90	PASS	AD=20,30;AF=0.6;ALLELE_ID=ref4,alt4
"#;

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::NONE),
    }
}

#[tokio::test]
async fn test_info_array_missing_values_no_row_loss() -> Result<(), Box<dyn std::error::Error>> {
    let temp_file = "/tmp/test_info_missing_value.vcf";
    fs::write(temp_file, VCF_WITH_MISSING_INFO_ARRAY).await?;

    let table = VcfTableProvider::new(
        temp_file.to_string(),
        Some(vec![
            "AD".to_string(),
            "AF".to_string(),
            "ALLELE_ID".to_string(),
        ]),
        None,
        Some(create_object_storage_options()),
        true,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("test_vcf", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, `AD`, `AF`, `ALLELE_ID` FROM test_vcf")
        .await?;
    let results = df.collect().await?;

    // All 4 rows must be present - no silent data loss
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "Expected 4 rows, got {total_rows} - missing value caused row loss"
    );

    let batch = &results[0];

    let chrom = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ad = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let af = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let allele_id = batch
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Row 0: AD=.,15 -> [null, 15]
    assert_eq!(chrom.value(0), "chr1");
    let ad_row0 = ad.value(0);
    let ad_row0_ints = ad_row0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ad_row0_ints.len(), 2);
    assert!(ad_row0_ints.is_null(0), "AD[0] should be null for '.'");
    assert_eq!(ad_row0_ints.value(1), 15);

    // Row 0: AF=0.5 -> [0.5]
    let af_row0 = af.value(0);
    let af_row0_floats = af_row0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row0_floats.len(), 1);
    assert_eq!(af_row0_floats.value(0), 0.5);

    // Row 0: ALLELE_ID=.,alt1 -> [null, "alt1"]
    let aid_row0 = allele_id.value(0);
    let aid_row0_strs = aid_row0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(aid_row0_strs.len(), 2);
    assert!(
        aid_row0_strs.is_null(0),
        "ALLELE_ID[0] should be null for '.'"
    );
    assert_eq!(aid_row0_strs.value(1), "alt1");

    // Row 1: AD=10,.,5 -> [10, null, 5]
    let ad_row1 = ad.value(1);
    let ad_row1_ints = ad_row1.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ad_row1_ints.len(), 3);
    assert_eq!(ad_row1_ints.value(0), 10);
    assert!(ad_row1_ints.is_null(1), "AD[1] should be null for '.'");
    assert_eq!(ad_row1_ints.value(2), 5);

    // Row 1: AF=.,0.3 -> [null, 0.3]
    let af_row1 = af.value(1);
    let af_row1_floats = af_row1.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(af_row1_floats.len(), 2);
    assert!(af_row1_floats.is_null(0), "AF[0] should be null for '.'");
    assert_eq!(af_row1_floats.value(1), 0.3);

    // Row 1: ALLELE_ID=ref2,.,alt2 -> ["ref2", null, "alt2"]
    let aid_row1 = allele_id.value(1);
    let aid_row1_strs = aid_row1.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(aid_row1_strs.len(), 3);
    assert_eq!(aid_row1_strs.value(0), "ref2");
    assert!(
        aid_row1_strs.is_null(1),
        "ALLELE_ID[1] should be null for '.'"
    );
    assert_eq!(aid_row1_strs.value(2), "alt2");

    // Row 2: AD=5,.,10 -> [5, null, 10]  (tri-allelic)
    let ad_row2 = ad.value(2);
    let ad_row2_ints = ad_row2.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ad_row2_ints.len(), 3);
    assert_eq!(ad_row2_ints.value(0), 5);
    assert!(ad_row2_ints.is_null(1), "AD[1] should be null for '.'");
    assert_eq!(ad_row2_ints.value(2), 10);

    // Row 3: AD=20,30 -> [20, 30] (no missing values, still works)
    let ad_row3 = ad.value(3);
    let ad_row3_ints = ad_row3.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ad_row3_ints.len(), 2);
    assert_eq!(ad_row3_ints.value(0), 20);
    assert_eq!(ad_row3_ints.value(1), 30);

    Ok(())
}
