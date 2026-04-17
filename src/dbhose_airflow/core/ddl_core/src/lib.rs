mod common;
mod clickhouse;
mod postgres;

use pyo3::prelude::*;

use clickhouse::clickhouse_ddl;
use postgres::{
    postgres_ddl,
    postgres_sequence_ddl,
};


#[pymodule]
fn ddl_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(clickhouse_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(postgres_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(postgres_sequence_ddl, m)?)?;
    Ok(())
}
