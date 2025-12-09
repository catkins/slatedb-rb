use std::cell::RefCell;

use magnus::prelude::*;
use magnus::{function, method, Error, RHash, Ruby};
use slatedb::config::{PutOptions, Ttl};
use slatedb::WriteBatch as SlateWriteBatch;

use crate::errors::invalid_argument_error;
use crate::utils::get_optional;

/// Ruby wrapper for SlateDB WriteBatch.
///
/// This struct is exposed to Ruby as `SlateDb::WriteBatch`.
#[magnus::wrap(class = "SlateDb::WriteBatch", free_immediately, size)]
pub struct WriteBatch {
    inner: RefCell<SlateWriteBatch>,
}

impl WriteBatch {
    /// Create a new empty WriteBatch.
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(SlateWriteBatch::new()),
        }
    }

    /// Add a put operation to the batch.
    pub fn put(&self, key: String, value: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        self.inner
            .borrow_mut()
            .put(key.as_bytes(), value.as_bytes());

        Ok(())
    }

    /// Add a put operation with options to the batch.
    ///
    /// Options:
    /// - ttl: Time-to-live in milliseconds
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

        self.inner
            .borrow_mut()
            .put_with_options(key.as_bytes(), value.as_bytes(), &put_opts);

        Ok(())
    }

    /// Add a delete operation to the batch.
    pub fn delete(&self, key: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        self.inner.borrow_mut().delete(key.as_bytes());

        Ok(())
    }

    /// Take ownership of the inner WriteBatch (consumes it).
    /// Used internally when writing the batch to the database.
    pub fn take(&self) -> Result<SlateWriteBatch, Error> {
        Ok(self.inner.replace(SlateWriteBatch::new()))
    }
}

/// Define the WriteBatch class on the SlateDb module.
pub fn define_write_batch_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("WriteBatch", ruby.class_object())?;

    // Class methods
    class.define_singleton_method("new", function!(WriteBatch::new, 0))?;

    // Instance methods
    class.define_method("_put", method!(WriteBatch::put, 2))?;
    class.define_method(
        "_put_with_options",
        method!(WriteBatch::put_with_options, 3),
    )?;
    class.define_method("_delete", method!(WriteBatch::delete, 1))?;

    Ok(())
}
