use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::IntoPyObjectExt;
use pyo3::types::{
    PyAny,
    PyDict,
    PyList,
};
use serde::Serialize;
use serde_json::{
    Value,
    to_value,
};


pub fn struct_to_py_dict<T: Serialize>(
    py: Python<'_>,
    value: &T,
) -> PyResult<Py<PyAny>> {
    let json_value = to_value(value)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    serde_value_to_py(py, &json_value)
}


fn serde_value_to_py(
    py: Python<'_>,
    value: &Value,
) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py_any(py)?),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py_any(py)?)
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py_any(py)?)
            } else {
                Ok(n.to_string().into_py_any(py)?)
            }
        }
        Value::String(s) => Ok(s.clone().into_py_any(py)?),
        Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                let converted = serde_value_to_py(py, item)?;
                py_list.append(converted)?;
            }
            Ok(py_list.unbind().into())
        }
        Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (k, v) in obj {
                let converted = serde_value_to_py(py, v)?;
                py_dict.set_item(k.as_str(), converted)?;
            }
            Ok(py_dict.unbind().into())
        }
    }
}
