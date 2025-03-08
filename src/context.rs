use std::collections::HashMap;

use datafusion::config::ConfigOptions;
use datafusion::prelude::SessionConfig;
use exon::config::ExonConfigExtension;
use exon::ExonSession;
use log::debug;
use pyo3::{pyclass, pymethods, PyResult};
use sequila_core::session_context::SequilaConfig;

#[pyclass(name = "BioSessionContext")]
// #[derive(Clone)]
pub struct PyBioSessionContext {
    pub ctx: ExonSession,
    pub session_config: HashMap<String, String>,
    #[pyo3(get, set)]
    pub seed: String,
    pub catalog_dir: String,
}

#[pymethods]
impl PyBioSessionContext {
    #[pyo3(signature = (seed, catalog_dir))]
    #[new]
    pub fn new(seed: String, catalog_dir: String) -> PyResult<Self> {
        let ctx = create_context().unwrap();
        let session_config: HashMap<String, String> = HashMap::new();

        Ok(PyBioSessionContext {
            ctx,
            session_config,
            seed,
            catalog_dir,
        })
    }
    #[pyo3(signature = (key, value, temporary=Some(false)))]
    pub fn set_option(&mut self, key: &str, value: &str, temporary: Option<bool>) {
        if !temporary.unwrap_or(false) {
            self.session_config
                .insert(key.to_string(), value.to_string());
        }
        set_option_internal(&self.ctx, key, value);
    }

    #[pyo3(signature = (key))]
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.session_config.get(key).map(|v| v.as_str())
    }

    #[pyo3(signature = ())]
    pub fn sync_options(&mut self) {
        for (key, value) in self.session_config.iter() {
            debug!("Setting option {} to {}", key, value);
            set_option_internal(&self.ctx, key, value);
        }
    }
}

pub fn set_option_internal(ctx: &ExonSession, key: &str, value: &str) {
    let state = ctx.session.state_ref();
    state
        .write()
        .config_mut()
        .options_mut()
        .set(key, value)
        .unwrap();
}

fn create_context() -> exon::Result<ExonSession> {
    let mut options = ConfigOptions::new();
    options.extensions.insert(ExonConfigExtension::default());
    let tuning_options = vec![
        ("datafusion.optimizer.repartition_joins", "false"),
        ("datafusion.execution.coalesce_batches", "false"),
    ];

    for o in tuning_options {
        options.set(o.0, o.1).expect("TODO: panic message");
    }

    let mut sequila_config = SequilaConfig::default();
    sequila_config.prefer_interval_join = true;

    let config = SessionConfig::from(options)
        .with_option_extension(sequila_config)
        .with_information_schema(true);

    ExonSession::with_config_exon(config)
}
