use std::sync::Arc;

use magnus::prelude::*;
use magnus::{function, method, Error, RHash, Ruby};
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions, Ttl, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use crate::errors::{invalid_argument_error, map_error};
use crate::iterator::Iterator;
use crate::runtime::block_on;
use crate::snapshot::Snapshot;
use crate::transaction::Transaction;
use crate::utils::get_optional;
use crate::write_batch::WriteBatch;

/// Ruby wrapper for SlateDB database.
///
/// This struct is exposed to Ruby as `SlateDb::Database`.
#[magnus::wrap(class = "SlateDb::Database", free_immediately, size)]
pub struct Database {
    inner: Arc<Db>,
}

impl Database {
    /// Open a database at the given path.
    ///
    /// # Arguments
    /// * `path` - The path identifier for the database
    /// * `url` - Optional object store URL (e.g., "s3://bucket/path")
    ///
    /// # Returns
    /// A new Database instance
    pub fn open(path: String, url: Option<String>) -> Result<Self, Error> {
        let db = block_on(async {
            let object_store: Arc<dyn object_store::ObjectStore> = if let Some(ref url) = url {
                Db::resolve_object_store(url).map_err(map_error)?
            } else {
                // Use in-memory store for local testing
                Arc::new(InMemory::new())
            };

            Db::builder(path, object_store)
                .build()
                .await
                .map_err(map_error)
        })?;

        Ok(Self {
            inner: Arc::new(db),
        })
    }

    /// Get a value by key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The value as a String, or nil if not found
    pub fn get(&self, key: String) -> Result<Option<String>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let opts = ReadOptions::default();

        let result = block_on(async { self.inner.get_with_options(key.as_bytes(), &opts).await })
            .map_err(map_error)?;

        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a value by key with options.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    /// * `kwargs` - Keyword arguments (durability_filter, dirty, cache_blocks)
    ///
    /// # Returns
    /// The value as a String, or nil if not found
    pub fn get_with_options(&self, key: String, kwargs: RHash) -> Result<Option<String>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let mut opts = ReadOptions::default();

        // Parse durability_filter
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

        // Parse dirty
        if let Some(dirty) = get_optional::<bool>(&kwargs, "dirty")? {
            opts.dirty = dirty;
        }

        let result = block_on(async { self.inner.get_with_options(key.as_bytes(), &opts).await })
            .map_err(map_error)?;

        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a value by key as raw bytes.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The value as bytes, or nil if not found
    pub fn get_bytes(&self, key: String) -> Result<Option<Vec<u8>>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let opts = ReadOptions::default();

        let result = block_on(async { self.inner.get_with_options(key.as_bytes(), &opts).await })
            .map_err(map_error)?;

        Ok(result.map(|b| b.to_vec()))
    }

    /// Store a key-value pair.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    pub fn put(&self, key: String, value: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let put_opts = PutOptions { ttl: Ttl::Default };

        let write_opts = WriteOptions {
            await_durable: true,
        };

        block_on(async {
            self.inner
                .put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
                .await
        })
        .map_err(map_error)?;

        Ok(())
    }

    /// Store a key-value pair with options.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    /// * `kwargs` - Keyword arguments (ttl, await_durable)
    pub fn put_with_options(&self, key: String, value: String, kwargs: RHash) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        // Parse ttl
        let ttl = get_optional::<u64>(&kwargs, "ttl")?;
        let put_opts = PutOptions {
            ttl: match ttl {
                Some(ms) => Ttl::ExpireAfter(ms),
                None => Ttl::Default,
            },
        };

        // Parse await_durable
        let await_durable = get_optional::<bool>(&kwargs, "await_durable")?.unwrap_or(true);
        let write_opts = WriteOptions { await_durable };

        block_on(async {
            self.inner
                .put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
                .await
        })
        .map_err(map_error)?;

        Ok(())
    }

    /// Delete a key.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    pub fn delete(&self, key: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let write_opts = WriteOptions {
            await_durable: true,
        };

        block_on(async {
            self.inner
                .delete_with_options(key.as_bytes(), &write_opts)
                .await
        })
        .map_err(map_error)?;

        Ok(())
    }

    /// Delete a key with options.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    /// * `kwargs` - Keyword arguments (await_durable)
    pub fn delete_with_options(&self, key: String, kwargs: RHash) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let await_durable = get_optional::<bool>(&kwargs, "await_durable")?.unwrap_or(true);
        let write_opts = WriteOptions { await_durable };

        block_on(async {
            self.inner
                .delete_with_options(key.as_bytes(), &write_opts)
                .await
        })
        .map_err(map_error)?;

        Ok(())
    }

    /// Scan a range of keys.
    ///
    /// # Arguments
    /// * `start` - The start key (inclusive)
    /// * `end_key` - Optional end key (exclusive). If not provided, scans to end.
    ///
    /// # Returns
    /// An Iterator over key-value pairs
    pub fn scan(&self, start: String, end_key: Option<String>) -> Result<Iterator, Error> {
        if start.is_empty() {
            return Err(invalid_argument_error("start key cannot be empty"));
        }

        let opts = ScanOptions::default();

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on(async {
            let range = match end_bytes {
                Some(end) => self.inner.scan_with_options(start_bytes..end, &opts).await,
                None => self.inner.scan_with_options(start_bytes.., &opts).await,
            };
            range.map_err(map_error)
        })?;

        Ok(Iterator::new(iter))
    }

    /// Scan a range of keys with options.
    ///
    /// # Arguments
    /// * `start` - The start key (inclusive)
    /// * `end_key` - Optional end key (exclusive)
    /// * `kwargs` - Keyword arguments (durability_filter, dirty, read_ahead_bytes, cache_blocks, max_fetch_tasks)
    ///
    /// # Returns
    /// An Iterator over key-value pairs
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

        // Parse durability_filter
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

        // Parse dirty
        if let Some(dirty) = get_optional::<bool>(&kwargs, "dirty")? {
            opts.dirty = dirty;
        }

        // Parse read_ahead_bytes
        if let Some(rab) = get_optional::<usize>(&kwargs, "read_ahead_bytes")? {
            opts.read_ahead_bytes = rab;
        }

        // Parse cache_blocks
        if let Some(cb) = get_optional::<bool>(&kwargs, "cache_blocks")? {
            opts.cache_blocks = cb;
        }

        // Parse max_fetch_tasks
        if let Some(mft) = get_optional::<usize>(&kwargs, "max_fetch_tasks")? {
            opts.max_fetch_tasks = mft;
        }

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on(async {
            let range = match end_bytes {
                Some(end) => self.inner.scan_with_options(start_bytes..end, &opts).await,
                None => self.inner.scan_with_options(start_bytes.., &opts).await,
            };
            range.map_err(map_error)
        })?;

        Ok(Iterator::new(iter))
    }

    /// Write a batch of operations atomically.
    ///
    /// # Arguments
    /// * `batch` - The WriteBatch to write
    pub fn write(&self, batch: &WriteBatch) -> Result<(), Error> {
        let batch_inner = batch.take()?;

        block_on(async { self.inner.write(batch_inner).await }).map_err(map_error)?;

        Ok(())
    }

    /// Write a batch of operations atomically with options.
    ///
    /// # Arguments
    /// * `batch` - The WriteBatch to write
    /// * `kwargs` - Keyword arguments (await_durable)
    pub fn write_with_options(&self, batch: &WriteBatch, kwargs: RHash) -> Result<(), Error> {
        let await_durable = get_optional::<bool>(&kwargs, "await_durable")?.unwrap_or(true);
        let write_opts = WriteOptions { await_durable };

        let batch_inner = batch.take()?;

        block_on(async {
            self.inner
                .write_with_options(batch_inner, &write_opts)
                .await
        })
        .map_err(map_error)?;

        Ok(())
    }

    /// Begin a new transaction.
    ///
    /// # Arguments
    /// * `isolation` - Optional isolation level ("snapshot" or "serializable")
    ///
    /// # Returns
    /// A new Transaction instance
    pub fn begin_transaction(&self, isolation: Option<String>) -> Result<Transaction, Error> {
        let isolation_level = match isolation.as_deref().unwrap_or("snapshot") {
            "snapshot" | "si" => IsolationLevel::Snapshot,
            "serializable" | "ssi" | "serializable_snapshot" => {
                IsolationLevel::SerializableSnapshot
            }
            other => {
                return Err(invalid_argument_error(&format!(
                    "invalid isolation level: {} (expected 'snapshot' or 'serializable')",
                    other
                )))
            }
        };

        let txn = block_on(async { self.inner.begin(isolation_level).await }).map_err(map_error)?;

        Ok(Transaction::new(txn))
    }

    /// Create a snapshot for consistent reads.
    ///
    /// # Returns
    /// A new Snapshot instance
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let snap = block_on(async { self.inner.snapshot().await }).map_err(map_error)?;

        Ok(Snapshot::new(snap))
    }

    /// Flush the database to ensure durability.
    pub fn flush(&self) -> Result<(), Error> {
        block_on(async { self.inner.flush().await }).map_err(map_error)?;
        Ok(())
    }

    /// Close the database.
    pub fn close(&self) -> Result<(), Error> {
        block_on(async { self.inner.close().await }).map_err(map_error)?;
        Ok(())
    }
}

/// Define the Database class on the SlateDb module.
pub fn define_database_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Database", ruby.class_object())?;

    // Class methods
    class.define_singleton_method("_open", function!(Database::open, 2))?;

    // Instance methods - simple versions
    class.define_method("_get", method!(Database::get, 1))?;
    class.define_method("_get_with_options", method!(Database::get_with_options, 2))?;
    class.define_method("get_bytes", method!(Database::get_bytes, 1))?;
    class.define_method("_put", method!(Database::put, 2))?;
    class.define_method("_put_with_options", method!(Database::put_with_options, 3))?;
    class.define_method("_delete", method!(Database::delete, 1))?;
    class.define_method(
        "_delete_with_options",
        method!(Database::delete_with_options, 2),
    )?;
    class.define_method("_scan", method!(Database::scan, 2))?;
    class.define_method(
        "_scan_with_options",
        method!(Database::scan_with_options, 3),
    )?;
    class.define_method("_write", method!(Database::write, 1))?;
    class.define_method(
        "_write_with_options",
        method!(Database::write_with_options, 2),
    )?;
    class.define_method(
        "_begin_transaction",
        method!(Database::begin_transaction, 1),
    )?;
    class.define_method("_snapshot", method!(Database::snapshot, 0))?;
    class.define_method("flush", method!(Database::flush, 0))?;
    class.define_method("close", method!(Database::close, 0))?;

    Ok(())
}
