//! Retained native providers: every LazyFrame collection executes a fresh scan.
use crate::{
    context::PyBioSessionContext,
    option::{pyobject_storage_options_to_object_storage_options, PyObjectStorageOptions},
};
use datafusion::catalog::TableProvider;
use datafusion_bio_format_foldcomp::{FoldcompOptions, FoldcompTableProvider};
use datafusion_bio_format_structure::{
    manifest::TextFormat, AltlocSelection, ModelSelection, StructureLevel, StructureOptions,
    StructureTableProvider,
};
use datafusion_python::dataframe::PyDataFrame;
use pyo3::{exceptions::PyValueError, prelude::*};
use std::sync::Arc;

#[pyclass(name = "StructureReadOptions", from_py_object)]
#[derive(Clone)]
pub struct PyStructureReadOptions {
    options: StructureOptions,
}
#[pymethods]
impl PyStructureReadOptions {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(level="atom", model="all", altloc=None, include_non_peptide=false, max_peptide_bond=1.8, max_input_bytes=268435456, max_decoded_bytes=536870912, max_atoms=5000000))]
    fn new(
        level: &str,
        model: &str,
        altloc: Option<&str>,
        include_non_peptide: bool,
        max_peptide_bond: f64,
        max_input_bytes: usize,
        max_decoded_bytes: usize,
        max_atoms: usize,
    ) -> PyResult<Self> {
        let level = match level {
            "atom" => StructureLevel::Atom,
            "residue" => StructureLevel::Residue,
            _ => return Err(PyValueError::new_err("level must be 'atom' or 'residue'")),
        };
        let model = match model {
            "all" => ModelSelection::All,
            "first" => ModelSelection::First,
            id => ModelSelection::Id(id.parse().map_err(|_| {
                PyValueError::new_err("model must be 'all', 'first', or an integer")
            })?),
        };
        let altloc = match altloc {
            None if level == StructureLevel::Residue => AltlocSelection::BestBackbone,
            None | Some("all") => AltlocSelection::All,
            Some("best_backbone") => AltlocSelection::BestBackbone,
            Some(id) => AltlocSelection::Id(id.to_owned()),
        };
        if level == StructureLevel::Residue && altloc == AltlocSelection::All {
            return Err(PyValueError::new_err(
                "residue level requires 'best_backbone' or an explicit alternate ID",
            ));
        }
        let options = StructureOptions {
            level,
            model,
            altloc,
            include_non_peptide,
            max_peptide_bond,
            max_input_bytes,
            max_decoded_bytes,
            max_atoms,
        };
        options
            .validate()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { options })
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature=(py_ctx,paths,format,options,ids=None,entry_keys=None,table_name=None,object_storage_options=None))]
pub fn py_scan_structure(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    paths: Vec<String>,
    format: &str,
    options: PyStructureReadOptions,
    ids: Option<Vec<String>>,
    entry_keys: Option<Vec<u64>>,
    table_name: Option<String>,
    object_storage_options: Option<PyObjectStorageOptions>,
) -> PyResult<PyDataFrame> {
    py.detach(|| {
        let make = || -> datafusion::common::Result<Arc<dyn TableProvider>> {
            if format == "foldcomp" {
                if paths.len() != 1 {
                    return Err(datafusion_bio_format_structure::error(
                        "Foldcomp requires exactly one source",
                    ));
                }
                if object_storage_options.is_some() {
                    return Err(datafusion_bio_format_structure::error(
                        "Foldcomp does not support object storage options",
                    ));
                }
                Ok(Arc::new(FoldcompTableProvider::new(
                    paths[0].clone(),
                    FoldcompOptions {
                        structure: options.options,
                        ids,
                        entry_keys,
                    },
                )?))
            } else {
                let format = match format {
                    "auto" => None,
                    "pdb" => Some(TextFormat::Pdb),
                    "mmcif" => Some(TextFormat::Mmcif),
                    _ => {
                        return Err(datafusion_bio_format_structure::error(
                            "unsupported structure format",
                        ))
                    },
                };
                Ok(Arc::new(StructureTableProvider::new(
                    paths,
                    format,
                    options.options,
                    pyobject_storage_options_to_object_storage_options(object_storage_options),
                )?))
            }
        };
        let provider = make().map_err(|e| PyValueError::new_err(e.to_string()))?;
        if let Some(name) = table_name {
            py_ctx
                .ctx
                .register_table(name, provider.clone())
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        }
        let df = py_ctx
            .ctx
            .read_table(provider)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyDataFrame::new(df))
    })
}
