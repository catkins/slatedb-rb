use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use magnus::prelude::*;
use magnus::{method, Error, Ruby};
/// Ruby wrapper for SlateDB metrics registry.
///
/// This struct is exposed to Ruby as `SlateDb::Metrics`.
#[magnus::wrap(class = "SlateDb::Metrics", free_immediately, size)]
pub struct Metrics {
    inner: Arc<Mutex<HashMap<String, i64>>>,
}

impl Metrics {
    pub fn new(inner: Arc<Mutex<HashMap<String, i64>>>) -> Self {
        Self { inner }
    }

    /// Return a list of metric names.
    pub fn names(&self) -> Result<magnus::RArray, Error> {
        let metrics = self.inner.lock().expect("metrics mutex poisoned");
        let ruby = Ruby::get().expect("Ruby runtime not available");
        let result = ruby.ary_new_capa(metrics.len());

        for name in metrics.keys() {
            result.push(ruby.str_new(name))?;
        }

        Ok(result)
    }

    /// Get the current value of a metric by name.
    pub fn get(&self, name: String) -> Result<Option<i64>, Error> {
        let metrics = self.inner.lock().expect("metrics mutex poisoned");
        Ok(metrics.get(&name).copied())
    }
}

/// Define the Metrics class on the SlateDb module.
pub fn define_metrics_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Metrics", ruby.class_object())?;

    class.define_method("names", method!(Metrics::names, 0))?;
    class.define_method("get", method!(Metrics::get, 1))?;

    Ok(())
}
