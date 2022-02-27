use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::PyResult;

use data_pods_store::protocol::{CallResult, GetResult};
use data_pods_store::values::Value;

use std::sync::{Arc, Condvar, Mutex};

use std::time::Duration;

pub struct RequestResult<ResultType: Clone> {
    cond: Condvar,
    inner: Mutex<Option<ResultType>>,
    latency: Mutex<Option<Duration>>,
}

pub type GetRequestResult = RequestResult<GetResult>;
pub type CallRequestResult = RequestResult<CallResult>;

#[pyclass]
pub struct GetRequestHandle {
    result: Arc<GetRequestResult>,
}

#[pyclass]
pub struct CallRequestHandle {
    result: Arc<CallRequestResult>,
}

impl<T: Clone> RequestResult<T> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cond: Condvar::new(),
            inner: Mutex::new(None),
            latency: Mutex::new(None),
        }
    }

    pub fn set_inner(&self, value: T, lat_value: Duration) {
        let mut inner = self.inner.lock().unwrap();
        let mut latency = self.latency.lock().unwrap();

        *inner = Some(value);
        *latency = Some(lat_value);

        self.cond.notify_all();
    }
}

impl GetRequestHandle {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let result = Arc::new(GetRequestResult::new());
        Self { result }
    }

    pub fn clone_result(&self) -> Arc<GetRequestResult> {
        self.result.clone()
    }
}

impl CallRequestHandle {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let result = Arc::new(CallRequestResult::new());
        Self { result }
    }

    pub fn clone_result(&self) -> Arc<CallRequestResult> {
        self.result.clone()
    }
}

#[pymethods]
impl CallRequestHandle {
    /// Block until request is done
    pub fn wait(&self) {
        let mut lock = self.result.inner.lock().unwrap();

        while lock.is_none() {
            lock = self.result.cond.wait(lock).unwrap();
        }
    }

    /// Non-blocking check if request is done
    pub fn is_done(&self) -> bool {
        self.result.inner.lock().unwrap().is_some()
    }

    pub fn get_result(&self) -> PyResult<Value> {
        let inner = self.result.inner.lock().unwrap();

        match &*inner {
            Some(res) => {
                match &*res {
                    //TODO verify proof
                    Ok(witness) => {
                        //FIXME move, don't copy here
                        Ok(witness.result.clone())
                    }
                    Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
                }
            }
            None => Err(PyRuntimeError::new_err("Result not ready yet!")),
        }
    }

    /// Return latency (in milliseconds)
    pub fn get_latency(&self) -> u128 {
        let latency = self.result.latency.lock().unwrap();

        match &*latency {
            Some(val) => val.as_millis(),
            None => {
                panic!("Result not ready yet!");
            }
        }
    }
}

#[pymethods]
impl GetRequestHandle {
    /// Block until request is done
    pub fn wait(&self) {
        let mut lock = self.result.inner.lock().unwrap();

        while lock.is_none() {
            lock = self.result.cond.wait(lock).unwrap();
        }
    }

    /// Non-blocking check if request is done
    pub fn is_done(&self) -> bool {
        self.result.inner.lock().unwrap().is_some()
    }

    pub fn get_result(&self) -> PyResult<Value> {
        let inner = self.result.inner.lock().unwrap();

        match &*inner {
            Some(res) => match &*res {
                Ok(val) => Ok(val.clone_as_value()),
                Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
            },
            None => Err(PyRuntimeError::new_err("Result not ready yet!")),
        }
    }
}
