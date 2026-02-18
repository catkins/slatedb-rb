use std::sync::Arc;

use magnus::prelude::*;
use magnus::{method, Error, Ruby};
use slatedb::stats::StatRegistry;

/// Ruby wrapper for SlateDB metrics registry.
///
/// This struct is exposed to Ruby as `SlateDb::Metrics`.
#[magnus::wrap(class = "SlateDb::Metrics", free_immediately, size)]
pub struct Metrics {
    inner: Arc<StatRegistry>,
}

impl Metrics {
    pub fn new(inner: Arc<StatRegistry>) -> Self {
        Self { inner }
    }

    /// Return a list of metric names.
    pub fn names(&self) -> Result<magnus::RArray, Error> {
        let names = self.inner.names();
        let ruby = Ruby::get().expect("Ruby runtime not available");
        let result = ruby.ary_new_capa(names.len());

        for name in names {
            result.push(ruby.str_new(name))?;
        }

        Ok(result)
    }

    /// Get the current value of a metric by name.
    pub fn get(&self, name: String) -> Result<Option<i64>, Error> {
        let stat_name = self
            .inner
            .names()
            .into_iter()
            .find(|n| *n == name);

        let Some(stat_name) = stat_name else {
            return Ok(None);
        };

        let stat = self.inner.lookup(stat_name);
        Ok(stat.map(|s| s.get()))
    }
}

/// Define the Metrics class on the SlateDb module.
pub fn define_metrics_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Metrics", ruby.class_object())?;

    class.define_method("names", method!(Metrics::names, 0))?;
    class.define_method("get", method!(Metrics::get, 1))?;

    Ok(())
}
