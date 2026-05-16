use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use magnus::prelude::*;
use magnus::{function, method, Error, RHash, Ruby};
use slatedb::config::{
    DurabilityLevel, MergeOptions, PutOptions, ReadOptions, ScanOptions, Ttl, WriteOptions,
};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel, IterationOrder, KeyValue};

use crate::errors::invalid_argument_error;
use crate::iterator::Iterator;
use crate::merge_ops::{parse_merge_operator, parse_merge_operator_proc};
use crate::metrics::Metrics;
use crate::runtime::block_on_result;
use crate::snapshot::Snapshot;
use crate::transaction::Transaction;
use crate::utils::{get_optional, resolve_object_store};
use crate::write_batch::WriteBatch;

/// Ruby wrapper for SlateDB database.
///
/// This struct is exposed to Ruby as `SlateDb::Database`.
#[magnus::wrap(class = "SlateDb::Database", free_immediately, size)]
pub struct Database {
    inner: Arc<Db>,
    metrics: Arc<Mutex<HashMap<String, i64>>>,
}

impl Database {
    fn increment_metric(&self, name: &str) {
        let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
        *metrics.entry(name.to_string()).or_insert(0) += 1;
    }

    fn key_value_to_hash(kv: KeyValue) -> Result<RHash, Error> {
        let ruby = Ruby::get().expect("Ruby runtime not available");
        let hash = ruby.hash_new();
        hash.aset(
            ruby.to_symbol("key"),
            String::from_utf8_lossy(&kv.key).to_string(),
        )?;
        hash.aset(
            ruby.to_symbol("value"),
            String::from_utf8_lossy(&kv.value).to_string(),
        )?;
        hash.aset(ruby.to_symbol("seq"), kv.seq)?;
        hash.aset(ruby.to_symbol("create_ts"), kv.create_ts)?;
        hash.aset(ruby.to_symbol("expire_ts"), kv.expire_ts)?;
        Ok(hash)
    }

    fn read_options_from_kwargs(kwargs: &RHash) -> Result<ReadOptions, Error> {
        let mut opts = ReadOptions::default();

        if let Some(df) = get_optional::<String>(kwargs, "durability_filter")? {
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

        if let Some(dirty) = get_optional::<bool>(kwargs, "dirty")? {
            opts.dirty = dirty;
        }

        if let Some(cb) = get_optional::<bool>(kwargs, "cache_blocks")? {
            opts.cache_blocks = cb;
        }

        Ok(opts)
    }

    /// Open a database at the given path.
    ///
    /// # Arguments
    /// * `path` - The path identifier for the database
    /// * `url` - Optional object store URL (e.g., "s3://bucket/path")
    /// * `kwargs` - Additional options (merge_operator, merge_operator_proc)
    ///
    /// # Returns
    /// A new Database instance
    pub fn open(path: String, url: Option<String>, kwargs: RHash) -> Result<Self, Error> {
        // Try string-based merge operator first, then proc-based
        let merge_operator = parse_merge_operator(&kwargs)?.or(parse_merge_operator_proc(&kwargs)?);

        let db = block_on_result(async {
            let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
                if let Some(ref url_str) = url {
                    resolve_object_store(url_str)?
                } else {
                    Arc::new(InMemory::new())
                };

            let mut builder = Db::builder(path, object_store);
            if let Some(merge_operator) = merge_operator {
                builder = builder.with_merge_operator(merge_operator);
            }

            builder.build().await
        })?;

        Ok(Self {
            inner: Arc::new(db),
            metrics: Arc::new(Mutex::new(HashMap::new())),
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

        let result =
            block_on_result(async { self.inner.get_with_options(key.as_bytes(), &opts).await })?;
        self.increment_metric("db.get.count");

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

        let opts = Self::read_options_from_kwargs(&kwargs)?;

        let result =
            block_on_result(async { self.inner.get_with_options(key.as_bytes(), &opts).await })?;
        self.increment_metric("db.get_with_options.count");

        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a key-value pair with metadata by key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// A Hash with key, value, seq, create_ts, and expire_ts, or nil if not found
    pub fn get_key_value(&self, key: String) -> Result<Option<RHash>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let opts = ReadOptions::default();
        let result = block_on_result(async {
            self.inner
                .get_key_value_with_options(key.as_bytes(), &opts)
                .await
        })?;
        self.increment_metric("db.get_key_value.count");

        result.map(Self::key_value_to_hash).transpose()
    }

    /// Get a key-value pair with metadata by key with options.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    /// * `kwargs` - Keyword arguments (durability_filter, dirty, cache_blocks)
    ///
    /// # Returns
    /// A Hash with key, value, seq, create_ts, and expire_ts, or nil if not found
    pub fn get_key_value_with_options(
        &self,
        key: String,
        kwargs: RHash,
    ) -> Result<Option<RHash>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let opts = Self::read_options_from_kwargs(&kwargs)?;
        let result = block_on_result(async {
            self.inner
                .get_key_value_with_options(key.as_bytes(), &opts)
                .await
        })?;
        self.increment_metric("db.get_key_value_with_options.count");

        result.map(Self::key_value_to_hash).transpose()
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

        let result =
            block_on_result(async { self.inner.get_with_options(key.as_bytes(), &opts).await })?;
        self.increment_metric("db.get_bytes.count");

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

        block_on_result(async {
            self.inner
                .put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
                .await
        })?;
        self.increment_metric("db.put.count");

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

        block_on_result(async {
            self.inner
                .put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
                .await
        })?;
        self.increment_metric("db.put_with_options.count");

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

        block_on_result(async {
            self.inner
                .delete_with_options(key.as_bytes(), &write_opts)
                .await
        })?;
        self.increment_metric("db.delete.count");

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

        block_on_result(async {
            self.inner
                .delete_with_options(key.as_bytes(), &write_opts)
                .await
        })?;
        self.increment_metric("db.delete_with_options.count");

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

        let iter = block_on_result(async {
            match end_bytes {
                Some(end) => self.inner.scan_with_options(start_bytes..end, &opts).await,
                None => self.inner.scan_with_options(start_bytes.., &opts).await,
            }
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
        if let Some(order) = get_optional::<String>(&kwargs, "order")? {
            opts.order = match order.as_str() {
                "ascending" | "asc" => IterationOrder::Ascending,
                "descending" | "desc" => IterationOrder::Descending,
                other => {
                    return Err(invalid_argument_error(&format!(
                        "invalid order: {} (expected 'asc' or 'desc')",
                        other
                    )))
                }
            };
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

    /// Scan all keys with a given prefix.
    ///
    /// # Arguments
    /// * `prefix` - The key prefix to scan
    ///
    /// # Returns
    /// An Iterator over key-value pairs
    pub fn scan_prefix(&self, prefix: String) -> Result<Iterator, Error> {
        if prefix.is_empty() {
            return Err(invalid_argument_error("prefix cannot be empty"));
        }

        let opts = ScanOptions::default();
        let iter = block_on_result(async {
            self.inner
                .scan_prefix_with_options(prefix.as_bytes(), &opts)
                .await
        })?;

        Ok(Iterator::new(iter))
    }

    /// Scan all keys with a given prefix with options.
    ///
    /// # Arguments
    /// * `prefix` - The key prefix to scan
    /// * `kwargs` - Keyword arguments (durability_filter, dirty, read_ahead_bytes, cache_blocks, max_fetch_tasks)
    ///
    /// # Returns
    /// An Iterator over key-value pairs
    pub fn scan_prefix_with_options(
        &self,
        prefix: String,
        kwargs: RHash,
    ) -> Result<Iterator, Error> {
        if prefix.is_empty() {
            return Err(invalid_argument_error("prefix cannot be empty"));
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
        if let Some(order) = get_optional::<String>(&kwargs, "order")? {
            opts.order = match order.as_str() {
                "ascending" | "asc" => IterationOrder::Ascending,
                "descending" | "desc" => IterationOrder::Descending,
                other => {
                    return Err(invalid_argument_error(&format!(
                        "invalid order: {} (expected 'asc' or 'desc')",
                        other
                    )))
                }
            };
        }

        let iter = block_on_result(async {
            self.inner
                .scan_prefix_with_options(prefix.as_bytes(), &opts)
                .await
        })?;

        Ok(Iterator::new(iter))
    }

    /// Write a batch of operations atomically.
    ///
    /// # Arguments
    /// * `batch` - The WriteBatch to write
    pub fn write(&self, batch: &WriteBatch) -> Result<(), Error> {
        let batch_inner = batch.take()?;
        block_on_result(async { self.inner.write(batch_inner).await })?;
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

        block_on_result(async {
            self.inner
                .write_with_options(batch_inner, &write_opts)
                .await
        })?;

        Ok(())
    }

    /// Merge a value into the database.
    ///
    /// # Arguments
    /// * `key` - The key to merge into
    /// * `value` - The merge operand to apply
    pub fn merge(&self, key: String, value: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let merge_opts = MergeOptions { ttl: Ttl::Default };

        let write_opts = WriteOptions {
            await_durable: true,
        };

        block_on_result(async {
            self.inner
                .merge_with_options(key.as_bytes(), value.as_bytes(), &merge_opts, &write_opts)
                .await
        })?;

        Ok(())
    }

    /// Merge a value into the database with options.
    ///
    /// # Arguments
    /// * `key` - The key to merge into
    /// * `value` - The merge operand to apply
    /// * `kwargs` - Keyword arguments (ttl, await_durable)
    pub fn merge_with_options(
        &self,
        key: String,
        value: String,
        kwargs: RHash,
    ) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let ttl = get_optional::<u64>(&kwargs, "ttl")?;
        let merge_opts = MergeOptions {
            ttl: match ttl {
                Some(ms) => Ttl::ExpireAfter(ms),
                None => Ttl::Default,
            },
        };

        let await_durable = get_optional::<bool>(&kwargs, "await_durable")?.unwrap_or(true);
        let write_opts = WriteOptions { await_durable };

        block_on_result(async {
            self.inner
                .merge_with_options(key.as_bytes(), value.as_bytes(), &merge_opts, &write_opts)
                .await
        })?;

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

        let txn = block_on_result(async { self.inner.begin(isolation_level).await })?;
        Ok(Transaction::new(txn))
    }

    /// Create a snapshot for consistent reads.
    ///
    /// # Returns
    /// A new Snapshot instance
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let snap = block_on_result(async { self.inner.snapshot().await })?;
        Ok(Snapshot::new(snap))
    }

    /// Create a checkpoint of the database.
    ///
    /// # Arguments
    /// * `kwargs` - Options: lifetime (ms), name
    ///
    /// # Returns
    /// Hash with id (UUID string) and manifest_id (int)
    pub fn create_checkpoint(&self, kwargs: RHash) -> Result<RHash, Error> {
        use slatedb::config::{CheckpointOptions, CheckpointScope};

        let lifetime =
            get_optional::<u64>(&kwargs, "lifetime")?.map(std::time::Duration::from_millis);
        let name = get_optional::<String>(&kwargs, "name")?;

        let options = CheckpointOptions {
            lifetime,
            source: None,
            name,
        };

        let result = block_on_result(async {
            self.inner
                .create_checkpoint(CheckpointScope::Durable, &options)
                .await
        })?;

        let ruby = Ruby::get().expect("Ruby runtime not available");
        let hash = ruby.hash_new();
        hash.aset(ruby.to_symbol("id"), result.id.to_string())?;
        hash.aset(ruby.to_symbol("manifest_id"), result.manifest_id)?;

        Ok(hash)
    }

    /// Flush the database to ensure durability.
    pub fn flush(&self) -> Result<(), Error> {
        block_on_result(async { self.inner.flush().await })?;
        Ok(())
    }

    /// Return the database metrics registry.
    pub fn metrics(&self) -> Result<Metrics, Error> {
        Ok(Metrics::new(self.metrics.clone()))
    }

    /// Close the database.
    pub fn close(&self) -> Result<(), Error> {
        block_on_result(async { self.inner.close().await })?;
        Ok(())
    }
}

/// Define the Database class on the SlateDb module.
pub fn define_database_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Database", ruby.class_object())?;

    // Class methods
    class.define_singleton_method("_open", function!(Database::open, 3))?;

    // Instance methods - simple versions
    class.define_method("_get", method!(Database::get, 1))?;
    class.define_method("_get_with_options", method!(Database::get_with_options, 2))?;
    class.define_method("_get_key_value", method!(Database::get_key_value, 1))?;
    class.define_method(
        "_get_key_value_with_options",
        method!(Database::get_key_value_with_options, 2),
    )?;
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
    class.define_method("_scan_prefix", method!(Database::scan_prefix, 1))?;
    class.define_method(
        "_scan_prefix_with_options",
        method!(Database::scan_prefix_with_options, 2),
    )?;
    class.define_method("_write", method!(Database::write, 1))?;
    class.define_method(
        "_write_with_options",
        method!(Database::write_with_options, 2),
    )?;
    class.define_method("_merge", method!(Database::merge, 2))?;
    class.define_method(
        "_merge_with_options",
        method!(Database::merge_with_options, 3),
    )?;
    class.define_method(
        "_begin_transaction",
        method!(Database::begin_transaction, 1),
    )?;
    class.define_method("_snapshot", method!(Database::snapshot, 0))?;
    class.define_method(
        "_create_checkpoint",
        method!(Database::create_checkpoint, 1),
    )?;
    class.define_method("flush", method!(Database::flush, 0))?;
    class.define_method("_metrics", method!(Database::metrics, 0))?;
    class.define_method("close", method!(Database::close, 0))?;

    Ok(())
}
