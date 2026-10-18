use std::collections::HashMap;

use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryLimit;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_function_ranges::{register_ranges_functions, BioConfig, BioSessionExt};
use datafusion_python::dataframe::PyDataFrame;
use log::debug;
use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, PyResult, Python};
use tokio::runtime::Runtime;

#[pyclass(name = "BioSessionContext")]
// #[derive(Clone)]
pub struct PyBioSessionContext {
    pub ctx: SessionContext,
    pub session_config: HashMap<String, String>,
    #[pyo3(get, set)]
    pub seed: String,
}

#[pymethods]
impl PyBioSessionContext {
    #[pyo3(signature = (seed))]
    #[new]
    pub fn new(seed: String) -> PyResult<Self> {
        let ctx = create_context();
        let session_config: HashMap<String, String> = HashMap::new();

        Ok(PyBioSessionContext {
            ctx,
            session_config,
            seed,
        })
    }
    /// Sets a configuration option.
    ///
    /// Keys in the standard DataFusion namespaces (including
    /// `datafusion.runtime.*`) are validated and raise `ValueError` when the
    /// key or value is rejected. Other namespaces (`bio.*`,
    /// `datafusion.bio.*`) are stored leniently because some of them are only
    /// read back by the Python layer.
    #[pyo3(signature = (key, value, temporary=Some(false)))]
    pub fn set_option(&mut self, key: &str, value: &str, temporary: Option<bool>) -> PyResult<()> {
        if is_strict_option(key) {
            apply_option(&self.ctx, key, value).map_err(|e| {
                PyValueError::new_err(format!("Failed to set option {key} = {value}: {e}"))
            })?;
        } else {
            set_option_internal(&self.ctx, key, value);
        }
        // Runtime settings are read back from the RuntimeEnv itself.
        if !temporary.unwrap_or(false) && !is_runtime_option(key) {
            self.session_config
                .insert(key.to_string(), value.to_string());
        }
        Ok(())
    }

    #[pyo3(signature = (key))]
    pub fn get_option(&self, key: &str) -> Option<String> {
        if is_runtime_option(key) {
            // Reflect the live RuntimeEnv, so values applied via SQL
            // `SET`/`RESET` are reported too (same values as df_settings).
            return self
                .ctx
                .runtime_env()
                .config_entries()
                .into_iter()
                .find(|entry| entry.key == key)
                .and_then(|entry| entry.value);
        }
        self.session_config.get(key).cloned()
    }

    #[pyo3(signature = ())]
    pub fn sync_options(&mut self) {
        for (key, value) in self.session_config.iter() {
            debug!("Setting option {} to {}", key, value);
            set_option_internal(&self.ctx, key, value);
        }
    }

    /// Returns the memory limit (in bytes) of the session's memory pool, or
    /// `None` when the pool is unbounded.
    ///
    /// Used to give secondary datafusion-python contexts the same budget.
    #[pyo3(signature = ())]
    pub fn runtime_memory_limit(&self) -> Option<usize> {
        match self.ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => Some(limit),
            MemoryLimit::Infinite | MemoryLimit::Unknown => None,
        }
    }

    /// Returns the temp directories the session's disk manager currently
    /// uses for spill files (empty until any exist in OS temp-dir mode).
    ///
    /// Reflects `SET`/`RESET datafusion.runtime.temp_directory` too.
    #[pyo3(signature = ())]
    pub fn runtime_temp_directories(&self) -> Vec<String> {
        self.ctx
            .runtime_env()
            .disk_manager
            .temp_dir_paths()
            .iter()
            .map(|p| p.display().to_string())
            .collect()
    }

    /// Removes a previously registered table by name.
    #[pyo3(signature = (name))]
    pub fn deregister_table(&self, name: &str) -> PyResult<()> {
        self.ctx.deregister_table(name).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to deregister table '{}': {}",
                name, e
            ))
        })?;
        Ok(())
    }

    /// Returns a DataFrame for a registered table by name.
    #[pyo3(signature = (name))]
    pub fn table(&self, name: &str, py: Python) -> PyResult<PyDataFrame> {
        let table_name = name.to_string();
        py.detach(|| {
            let rt = Runtime::new().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to create Tokio runtime: {}",
                    e
                ))
            })?;
            let ctx = &self.ctx;
            match rt.block_on(ctx.table(&table_name)) {
                Ok(df) => Ok(PyDataFrame::new(df)),
                Err(e) => Err(pyo3::exceptions::PyKeyError::new_err(format!(
                    "Table '{}' not found: {}",
                    table_name, e
                ))),
            }
        })
    }
}

const RUNTIME_PREFIX: &str = "datafusion.runtime.";

fn is_runtime_option(key: &str) -> bool {
    key.starts_with(RUNTIME_PREFIX)
}

/// Standard DataFusion keys must be valid; extension namespaces are lenient.
fn is_strict_option(key: &str) -> bool {
    key.starts_with("datafusion.") && !key.starts_with("datafusion.bio.")
}

/// Applies an option to the session, returning an error if it is rejected.
///
/// `datafusion.runtime.*` settings live on the `RuntimeEnv`, not on
/// `ConfigOptions`, so they are routed through DataFusion's own `SET`
/// statement handling, which rebuilds the runtime environment.
fn apply_option(ctx: &SessionContext, key: &str, value: &str) -> Result<()> {
    if !is_runtime_option(key) {
        let state = ctx.state_ref();
        return state.write().config_mut().options_mut().set(key, value);
    }
    if !key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(DataFusionError::Configuration(format!(
            "Invalid runtime configuration key: {key}"
        )));
    }
    let stmt = format!("SET {key} = '{}'", value.replace('\'', "''"));
    // SET is executed eagerly while planning and needs no Tokio reactor, so a
    // plain executor is enough. Unlike `Runtime::block_on`, it does not panic
    // when the caller is already inside a Tokio runtime (see tests below).
    futures::executor::block_on(ctx.sql(&stmt)).map(|_| ())
}

pub fn set_option_internal(ctx: &SessionContext, key: &str, value: &str) {
    let res = apply_option(ctx, key, value);
    if let Err(e) = res {
        // Avoid panicking on unknown namespaces/keys; log for debugging.
        debug!("Failed to set option {} = {}: {:?}", key, value, e);
    }
}

fn create_context() -> SessionContext {
    let mut options = ConfigOptions::new();
    let tuning_options = vec![
        ("datafusion.optimizer.repartition_joins", "false"),
        ("datafusion.execution.coalesce_batches", "false"),
    ];

    for o in tuning_options {
        options.set(o.0, o.1).expect("TODO: panic message");
    }

    let mut bio_config = BioConfig::default();
    bio_config.prefer_interval_join = true;

    let config = SessionConfig::from(options)
        .with_option_extension(bio_config)
        .with_information_schema(true);

    let ctx = SessionContext::new_with_bio(config);

    // Register coverage() and count_overlaps() SQL UDTFs
    register_ranges_functions(&ctx);

    // Register depth UDTF for SQL: SELECT * FROM depth('file.bam')
    ctx.register_udtf("depth", std::sync::Arc::new(crate::pileup::DepthFunction));

    // Register fastqc UDTF for SQL: SELECT * FROM fastqc('file.fastq')
    ctx.register_udtf("fastqc", std::sync::Arc::new(crate::fastqc::FastqcFunction));

    ctx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_limit(ctx: &SessionContext) -> Option<usize> {
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => Some(limit),
            _ => None,
        }
    }

    #[test]
    fn runtime_option_reconfigures_runtime_env() {
        let ctx = create_context();
        assert_eq!(memory_limit(&ctx), None);
        apply_option(&ctx, "datafusion.runtime.memory_limit", "64M").unwrap();
        assert_eq!(memory_limit(&ctx), Some(64 * 1024 * 1024));
    }

    #[test]
    fn runtime_option_can_be_set_from_inside_a_tokio_runtime() {
        let ctx = create_context();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            apply_option(&ctx, "datafusion.runtime.memory_limit", "2M").unwrap();
        });
        assert_eq!(memory_limit(&ctx), Some(2 * 1024 * 1024));
    }

    #[test]
    fn invalid_runtime_options_are_rejected() {
        let ctx = create_context();
        assert!(apply_option(&ctx, "datafusion.runtime.memory_limit", "64X").is_err());
        assert!(apply_option(&ctx, "datafusion.runtime.no_such_option", "1").is_err());
        assert!(apply_option(&ctx, "datafusion.runtime.memory_limit; DROP", "1").is_err());
        assert_eq!(memory_limit(&ctx), None);
    }
}
