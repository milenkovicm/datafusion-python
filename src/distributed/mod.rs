use pyo3::{types::PyAnyMethods, Python};

pub mod codec;
mod pickle;

pub fn setup_python_path() {
    Python::with_gil(|py| {
        let version = py.version_info();
        let sys = py.import_bound("sys").expect("sys");
        let path = sys.getattr("path").expect("path");
        path.call_method1(
            "append",
            (format!(
                ".venv/lib/python{}.{}/site-packages",
                version.major, version.minor
            ),),
        )
        .expect("set");
    });
}
