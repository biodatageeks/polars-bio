use exon::ExonSession;
use crate::option::FilterOp;


async fn do_count_overlaps_coverage_naive(
    ctx: &ExonSession,
    left_table: String,
    right_table: String,
    overlap_filter: FilterOp,
    columns_1: Vec<String>,
    columns_2: Vec<String>,
    coverage: bool,
) -> datafusion::dataframe::DataFrame {
    let session = &ctx.session;
    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = session
        .table(right_table_ref.clone())
        .await
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    let count_overlaps_provider = CountOverlapsProvider::new(
        Arc::new(session.clone()),
        left_table,
        right_table,
        right_schema,
        columns_1,
        columns_2,
        overlap_filter,
        coverage,
    );
    let table_name = "count_overlaps_coverage".to_string();
    session.deregister_table(table_name.clone()).unwrap();
    session
        .register_table(table_name.clone(), Arc::new(count_overlaps_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}
