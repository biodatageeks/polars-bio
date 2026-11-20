//! Bridge from Rust `log` records to Python's `logging` module.

use std::borrow::Cow;
use std::sync::OnceLock;

use log::{LevelFilter, Log, Metadata, Record};
use pyo3::prelude::*;
use pyo3_log::{Caching, Logger, ResetHandle};

/// Root of the Python logger hierarchy that receives every Rust record.
const PYTHON_ROOT: &str = "polars_bio";

/// Most verbose level forwarded to Python; matches `pyo3_log`'s default.
const MAX_LEVEL: LevelFilter = LevelFilter::Debug;

/// Handle to the installed bridge's cache of Python loggers and their levels.
static LEVEL_CACHE: OnceLock<ResetHandle> = OnceLock::new();

/// Forwards Rust log records to Python loggers under `polars_bio`.
///
/// `pyo3_log` names the Python logger after the record's Rust target. On its
/// own that puts records from dependency crates on top-level loggers such as
/// `datafusion_bio_function_ranges.*`, which `pb.set_loglevel` does not govern
/// (#520). This wrapper rewrites the target first, so those records reach
/// `polars_bio.datafusion_bio_function_ranges.*` instead.
///
/// `pyo3_log::Logger::set_prefix` is not used because it would also move this
/// crate's own records from `polars_bio` to `polars_bio.polars_bio`.
struct PolarsBioLogger {
    inner: Logger,
}

/// Maps a Rust log target to one inside the `polars_bio` hierarchy.
///
/// Targets that are already `polars_bio` or below it are returned unchanged.
fn python_target(target: &str) -> Cow<'_, str> {
    match target.strip_prefix(PYTHON_ROOT) {
        Some(rest) if rest.is_empty() || rest.starts_with("::") => Cow::Borrowed(target),
        _ => Cow::Owned(format!("{PYTHON_ROOT}::{target}")),
    }
}

impl Log for PolarsBioLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let target = python_target(metadata.target());
        self.inner.enabled(
            &Metadata::builder()
                .level(metadata.level())
                .target(&target)
                .build(),
        )
    }

    fn log(&self, record: &Record) {
        let target = python_target(record.target());
        self.inner.log(
            &Record::builder()
                .args(*record.args())
                .level(record.level())
                .target(&target)
                .module_path(record.module_path())
                .file(record.file())
                .line(record.line())
                .build(),
        );
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

/// Installs the bridge as this extension module's Rust logger.
///
/// The `log` crate's global logger is a static inside this module's binary, so
/// a logger installed by another extension module, such as the `datafusion`
/// Python package with its own `pyo3_log`, does not collide with this one.
/// Installing fails only if a logger is already set inside this binary, that
/// is if this module is initialised twice, and then `LEVEL_CACHE` already
/// refers to the bridge that stays installed.
pub(crate) fn init(py: Python<'_>) -> PyResult<()> {
    let inner = Logger::new(py, Caching::LoggersAndLevels)?;
    let level_cache = inner.reset_handle();
    if log::set_boxed_logger(Box::new(PolarsBioLogger { inner })).is_ok() {
        log::set_max_level(MAX_LEVEL);
        let _ = LEVEL_CACHE.set(level_cache);
    }
    Ok(())
}

/// Drops the cached Python logger levels so the next record re-reads them.
///
/// The bridge caches each Python logger's level the first time a target logs,
/// to filter later records without taking the GIL. Python code that changes a
/// level has to call this, or records stay filtered by the old level.
#[pyfunction]
pub(crate) fn py_reset_log_level_cache() {
    if let Some(level_cache) = LEVEL_CACHE.get() {
        level_cache.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::python_target;

    #[test]
    fn dependency_targets_move_under_polars_bio() {
        assert_eq!(
            python_target("datafusion_bio_function_ranges::physical_planner"),
            "polars_bio::datafusion_bio_function_ranges::physical_planner"
        );
        assert_eq!(python_target("datafusion"), "polars_bio::datafusion");
    }

    #[test]
    fn own_targets_are_unchanged() {
        assert_eq!(python_target("polars_bio"), "polars_bio");
        assert_eq!(python_target("polars_bio::scan"), "polars_bio::scan");
    }

    #[test]
    fn crates_that_only_share_the_name_prefix_are_moved() {
        assert_eq!(
            python_target("polars_bio_extra::reader"),
            "polars_bio::polars_bio_extra::reader"
        );
    }
}
