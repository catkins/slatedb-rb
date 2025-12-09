use std::cell::RefCell;
use std::sync::Arc;

use magnus::prelude::*;
use magnus::{method, Error, RHash, Ruby};
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions};
use slatedb::DbSnapshot;

use crate::errors::{closed_error, invalid_argument_error, map_error};
use crate::iterator::Iterator;
use crate::runtime::block_on;
use crate::utils::get_optional;

/// Ruby wrapper for SlateDB Snapshot.
///
/// This struct is exposed to Ruby as `SlateDb::Snapshot`.
/// Provides a consistent, read-only view of the database at a point in time.
#[magnus::wrap(class = "SlateDb::Snapshot", free_immediately, size)]
pub struct Snapshot {
    inner: RefCell<Option<Arc<DbSnapshot>>>,
}

impl Snapshot {
    /// Create a new Snapshot from a DbSnapshot.
    pub fn new(snapshot: Arc<DbSnapshot>) -> Self {
        Self {
            inner: RefCell::new(Some(snapshot)),
        }
    }

    /// Get a value by key from the snapshot.
    pub fn get(&self, key: String) -> Result<Option<String>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let guard = self.inner.borrow();
        let snapshot = guard
            .as_ref()
            .ok_or_else(|| closed_error("snapshot is closed"))?;

        let result = block_on(async { snapshot.get(key.as_bytes()).await }).map_err(map_error)?;

        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a value by key with options from the snapshot.
    pub fn get_with_options(&self, key: String, kwargs: RHash) -> Result<Option<String>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let mut opts = ReadOptions::default();

        if let Some(df) = get_optional::<String>(&kwargs, "durability_filter")? {
            opts.durability_filter = match df.as_str() {
                "remote" => DurabilityLevel::Remote,
                "memory" => DurabilityLevel::Memory,
                other => {
                    return Err(invalid_argument_error(&format!(
                        "invalid durability_filter: {} (expected 'remote' or 'memory')",
                        other
                    )))
                }
            };
        }

        if let Some(dirty) = get_optional::<bool>(&kwargs, "dirty")? {
            opts.dirty = dirty;
        }

        let guard = self.inner.borrow();
        let snapshot = guard
            .as_ref()
            .ok_or_else(|| closed_error("snapshot is closed"))?;

        let result = block_on(async { snapshot.get_with_options(key.as_bytes(), &opts).await })
            .map_err(map_error)?;

        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Scan a range of keys from the snapshot.
    pub fn scan(&self, start: String, end_key: Option<String>) -> Result<Iterator, Error> {
        if start.is_empty() {
            return Err(invalid_argument_error("start key cannot be empty"));
        }

        let guard = self.inner.borrow();
        let snapshot = guard
            .as_ref()
            .ok_or_else(|| closed_error("snapshot is closed"))?;

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on(async {
            let range = match end_bytes {
                Some(end) => snapshot.scan(start_bytes..end).await,
                None => snapshot.scan(start_bytes..).await,
            };
            range.map_err(map_error)
        })?;

        Ok(Iterator::new(iter))
    }

    /// Scan a range of keys with options from the snapshot.
    pub fn scan_with_options(
        &self,
        start: String,
        end_key: Option<String>,
        kwargs: RHash,
    ) -> Result<Iterator, Error> {
        if start.is_empty() {
            return Err(invalid_argument_error("start key cannot be empty"));
        }

        let mut opts = ScanOptions::default();

        if let Some(df) = get_optional::<String>(&kwargs, "durability_filter")? {
            opts.durability_filter = match df.as_str() {
                "remote" => DurabilityLevel::Remote,
                "memory" => DurabilityLevel::Memory,
                other => {
                    return Err(invalid_argument_error(&format!(
                        "invalid durability_filter: {} (expected 'remote' or 'memory')",
                        other
                    )))
                }
            };
        }

        if let Some(dirty) = get_optional::<bool>(&kwargs, "dirty")? {
            opts.dirty = dirty;
        }

        if let Some(rab) = get_optional::<usize>(&kwargs, "read_ahead_bytes")? {
            opts.read_ahead_bytes = rab;
        }

        if let Some(cb) = get_optional::<bool>(&kwargs, "cache_blocks")? {
            opts.cache_blocks = cb;
        }

        if let Some(mft) = get_optional::<usize>(&kwargs, "max_fetch_tasks")? {
            opts.max_fetch_tasks = mft;
        }

        let guard = self.inner.borrow();
        let snapshot = guard
            .as_ref()
            .ok_or_else(|| closed_error("snapshot is closed"))?;

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on(async {
            let range = match end_bytes {
                Some(end) => snapshot.scan_with_options(start_bytes..end, &opts).await,
                None => snapshot.scan_with_options(start_bytes.., &opts).await,
            };
            range.map_err(map_error)
        })?;

        Ok(Iterator::new(iter))
    }

    /// Close the snapshot and release resources.
    pub fn close(&self) -> Result<(), Error> {
        let _ = self.inner.borrow_mut().take();
        Ok(())
    }

    /// Check if the snapshot is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.borrow().is_none()
    }
}

/// Define the Snapshot class on the SlateDb module.
pub fn define_snapshot_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Snapshot", ruby.class_object())?;

    // Instance methods
    class.define_method("_get", method!(Snapshot::get, 1))?;
    class.define_method("_get_with_options", method!(Snapshot::get_with_options, 2))?;
    class.define_method("_scan", method!(Snapshot::scan, 2))?;
    class.define_method(
        "_scan_with_options",
        method!(Snapshot::scan_with_options, 3),
    )?;
    class.define_method("close", method!(Snapshot::close, 0))?;
    class.define_method("closed?", method!(Snapshot::is_closed, 0))?;

    Ok(())
}
