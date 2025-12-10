use std::cell::RefCell;

use magnus::prelude::*;
use magnus::{method, Error, RHash, Ruby};
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions, Ttl, WriteOptions};
use slatedb::DBTransaction;

use crate::errors::{closed_error, invalid_argument_error, map_error};
use crate::iterator::Iterator;
use crate::runtime::block_on_result;
use crate::utils::get_optional;

/// Ruby wrapper for SlateDB Transaction.
///
/// This struct is exposed to Ruby as `SlateDb::Transaction`.
/// After commit or rollback, the transaction is closed.
#[magnus::wrap(class = "SlateDb::Transaction", free_immediately, size)]
pub struct Transaction {
    inner: RefCell<Option<DBTransaction>>,
}

impl Transaction {
    /// Create a new Transaction from a DBTransaction.
    pub fn new(txn: DBTransaction) -> Self {
        Self {
            inner: RefCell::new(Some(txn)),
        }
    }

    /// Get a value by key within the transaction.
    pub fn get(&self, key: String) -> Result<Option<String>, Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let guard = self.inner.borrow();
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        let result = block_on_result(async { txn.get(key.as_bytes()).await })?;
        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Get a value by key with options within the transaction.
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
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        let result =
            block_on_result(async { txn.get_with_options(key.as_bytes(), &opts).await })?;
        Ok(result.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// Put a key-value pair within the transaction.
    pub fn put(&self, key: String, value: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let guard = self.inner.borrow();
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        txn.put(key.as_bytes(), value.as_bytes())
            .map_err(map_error)?;

        Ok(())
    }

    /// Put a key-value pair with options within the transaction.
    pub fn put_with_options(&self, key: String, value: String, kwargs: RHash) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let ttl = get_optional::<u64>(&kwargs, "ttl")?;
        let put_opts = PutOptions {
            ttl: match ttl {
                Some(ms) => Ttl::ExpireAfter(ms),
                None => Ttl::Default,
            },
        };

        let guard = self.inner.borrow();
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        txn.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts)
            .map_err(map_error)?;

        Ok(())
    }

    /// Delete a key within the transaction.
    pub fn delete(&self, key: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let guard = self.inner.borrow();
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        txn.delete(key.as_bytes()).map_err(map_error)?;

        Ok(())
    }

    /// Scan a range of keys within the transaction.
    pub fn scan(&self, start: String, end_key: Option<String>) -> Result<Iterator, Error> {
        if start.is_empty() {
            return Err(invalid_argument_error("start key cannot be empty"));
        }

        let guard = self.inner.borrow();
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on_result(async {
            match end_bytes {
                Some(end) => txn.scan(start_bytes..end).await,
                None => txn.scan(start_bytes..).await,
            }
        })?;

        Ok(Iterator::new(iter))
    }

    /// Scan a range of keys with options within the transaction.
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
        let txn = guard
            .as_ref()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        let start_bytes = start.into_bytes();
        let end_bytes = end_key.map(|e| e.into_bytes());

        let iter = block_on_result(async {
            match end_bytes {
                Some(end) => txn.scan_with_options(start_bytes..end, &opts).await,
                None => txn.scan_with_options(start_bytes.., &opts).await,
            }
        })?;

        Ok(Iterator::new(iter))
    }

    /// Commit the transaction.
    pub fn commit(&self) -> Result<(), Error> {
        let txn = self
            .inner
            .borrow_mut()
            .take()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        block_on_result(async { txn.commit().await })?;
        Ok(())
    }

    /// Commit the transaction with options.
    pub fn commit_with_options(&self, kwargs: RHash) -> Result<(), Error> {
        let await_durable = get_optional::<bool>(&kwargs, "await_durable")?.unwrap_or(true);
        let write_opts = WriteOptions { await_durable };

        let txn = self
            .inner
            .borrow_mut()
            .take()
            .ok_or_else(|| closed_error("transaction is closed"))?;

        block_on_result(async { txn.commit_with_options(&write_opts).await })?;
        Ok(())
    }

    /// Rollback the transaction (discard all changes).
    pub fn rollback(&self) -> Result<(), Error> {
        // Simply drop the transaction - changes are not committed
        let _ = self.inner.borrow_mut().take();
        Ok(())
    }

    /// Check if the transaction is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.borrow().is_none()
    }
}

/// Define the Transaction class on the SlateDb module.
pub fn define_transaction_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Transaction", ruby.class_object())?;

    // Instance methods
    class.define_method("_get", method!(Transaction::get, 1))?;
    class.define_method(
        "_get_with_options",
        method!(Transaction::get_with_options, 2),
    )?;
    class.define_method("_put", method!(Transaction::put, 2))?;
    class.define_method(
        "_put_with_options",
        method!(Transaction::put_with_options, 3),
    )?;
    class.define_method("_delete", method!(Transaction::delete, 1))?;
    class.define_method("_scan", method!(Transaction::scan, 2))?;
    class.define_method(
        "_scan_with_options",
        method!(Transaction::scan_with_options, 3),
    )?;
    class.define_method("commit", method!(Transaction::commit, 0))?;
    class.define_method(
        "_commit_with_options",
        method!(Transaction::commit_with_options, 1),
    )?;
    class.define_method("rollback", method!(Transaction::rollback, 0))?;
    class.define_method("closed?", method!(Transaction::is_closed, 0))?;

    Ok(())
}
