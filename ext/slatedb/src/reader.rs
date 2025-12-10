use std::sync::Arc;

use magnus::prelude::*;
use magnus::{function, method, Error, RHash, Ruby};
use slatedb::config::{DbReaderOptions, DurabilityLevel, ReadOptions, ScanOptions};
use slatedb::DbReader;

use crate::errors::invalid_argument_error;
use crate::iterator::Iterator;
use crate::runtime::block_on_result;
use crate::utils::{get_optional, resolve_object_store};

/// Ruby wrapper for SlateDB Reader.
///
/// This struct is exposed to Ruby as `SlateDb::Reader`.
/// Provides read-only access to a database, optionally pinned to a checkpoint.
#[magnus::wrap(class = "SlateDb::Reader", free_immediately, size)]
pub struct Reader {
    inner: Arc<DbReader>,
}

impl Reader {
    /// Open a reader at the given path.
    ///
    /// # Arguments
    /// * `path` - The path identifier for the database
    /// * `url` - Optional object store URL
    /// * `checkpoint_id` - Optional checkpoint UUID to read at
    /// * `kwargs` - Additional options (manifest_poll_interval, checkpoint_lifetime, max_memtable_bytes)
    pub fn open(
        path: String,
        url: Option<String>,
        checkpoint_id: Option<String>,
        kwargs: RHash,
    ) -> Result<Self, Error> {
        // Parse options
        let manifest_poll_interval = get_optional::<u64>(&kwargs, "manifest_poll_interval")?
            .map(std::time::Duration::from_millis);
        let checkpoint_lifetime = get_optional::<u64>(&kwargs, "checkpoint_lifetime")?
            .map(std::time::Duration::from_millis);
        let max_memtable_bytes = get_optional::<u64>(&kwargs, "max_memtable_bytes")?;

        // Parse checkpoint_id as UUID
        let checkpoint_uuid =
            if let Some(id_str) = checkpoint_id {
                Some(uuid::Uuid::parse_str(&id_str).map_err(|e| {
                    invalid_argument_error(&format!("invalid checkpoint_id: {}", e))
                })?)
            } else {
                None
            };

        let reader = block_on_result(async {
            let object_store: Arc<dyn object_store::ObjectStore> = if let Some(ref url) = url {
                resolve_object_store(url)?
            } else {
                Arc::new(object_store::memory::InMemory::new())
            };

            let mut options = DbReaderOptions::default();
            if let Some(interval) = manifest_poll_interval {
                options.manifest_poll_interval = interval;
            }
            if let Some(lifetime) = checkpoint_lifetime {
                options.checkpoint_lifetime = lifetime;
            }
            if let Some(max_bytes) = max_memtable_bytes {
                options.max_memtable_bytes = max_bytes;
            }

            DbReader::open(path, object_store, checkpoint_uuid, options).await
        })?;

        Ok(Self {
            inner: Arc::new(reader),
        })
    }

    /// Get a value by key.
    pub fn get(&self, key: String) -> Result<Option<String>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let result = block_on_result(async { self.inner.get(key.as_bytes()).await })?;
        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a value by key with options.
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

        let result =
            block_on_result(async { self.inner.get_with_options(key.as_bytes(), &opts).await })?;
        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a value by key as raw bytes.
    pub fn get_bytes(&self, key: String) -> Result<Option<Vec<u8>>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let result = block_on_result(async { self.inner.get(key.as_bytes()).await })?;
        Ok(result.map(|b| b.to_vec()))
    }

    /// Scan a range of keys.
    pub fn scan(&self, start: String, end_key: Option<String>) -> Result<Iterator, Error> {
        if start.is_empty() {
            return Err(invalid_argument_error("start key cannot be empty"));
        }

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on_result(async {
            match end_bytes {
                Some(end) => self.inner.scan(start_bytes..end).await,
                None => self.inner.scan(start_bytes..).await,
            }
        })?;

        Ok(Iterator::new(iter))
    }

    /// Scan a range of keys with options.
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

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on_result(async {
            match end_bytes {
                Some(end) => self.inner.scan_with_options(start_bytes..end, &opts).await,
                None => self.inner.scan_with_options(start_bytes.., &opts).await,
            }
        })?;

        Ok(Iterator::new(iter))
    }

    /// Close the reader.
    pub fn close(&self) -> Result<(), Error> {
        block_on_result(async { self.inner.close().await })?;
        Ok(())
    }
}

/// Define the Reader class on the SlateDb module.
pub fn define_reader_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Reader", ruby.class_object())?;

    // Class methods
    class.define_singleton_method("_open", function!(Reader::open, 4))?;

    // Instance methods
    class.define_method("_get", method!(Reader::get, 1))?;
    class.define_method("_get_with_options", method!(Reader::get_with_options, 2))?;
    class.define_method("get_bytes", method!(Reader::get_bytes, 1))?;
    class.define_method("_scan", method!(Reader::scan, 2))?;
    class.define_method("_scan_with_options", method!(Reader::scan_with_options, 3))?;
    class.define_method("close", method!(Reader::close, 0))?;

    Ok(())
}
