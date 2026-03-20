//! Shared test utilities for projection pushdown and execution plan analysis

use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Recursively check that no ProjectionExec wrapper exists in the plan tree
pub fn assert_no_projection_exec(plan: &dyn ExecutionPlan) {
    assert_ne!(
        plan.name(),
        "ProjectionExec",
        "ProjectionExec found — projection not pushed down"
    );
    for child in plan.children() {
        assert_no_projection_exec(child.as_ref());
    }
}

/// Find the leaf exec node (custom format exec) by walking children
pub fn find_leaf_exec(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.children().is_empty() {
        return Arc::clone(plan);
    }
    find_leaf_exec(plan.children().first().unwrap())
}

/// Assert plan's leaf node has expected name and projected columns
pub fn assert_plan_projection(
    plan: &Arc<dyn ExecutionPlan>,
    expected_exec_name: &str,
    expected_columns: &[&str],
) {
    assert_no_projection_exec(plan.as_ref());
    let leaf = find_leaf_exec(plan);
    assert_eq!(leaf.name(), expected_exec_name);
    let schema = leaf.schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(col_names, expected_columns);
}
