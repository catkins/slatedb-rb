use std::sync::Arc;

use magnus::prelude::*;
use magnus::{method, Error, Ruby};
use slatedb::DbIterator;
use tokio::sync::Mutex;

use crate::errors::{internal_error, invalid_argument_error, map_error};
use crate::runtime::block_on;

/// Result type for raw byte key-value pairs.
type ByteKvResult = Result<Option<(Vec<u8>, Vec<u8>)>, Error>;

/// Internal error type for iterator operations (converted to Ruby errors after block_on).
enum IteratorError {
    Closed,
    Slate(slatedb::Error),
}

/// Ruby wrapper for SlateDB iterator.
///
/// This struct is exposed to Ruby as `SlateDb::Iterator`.
/// It includes Enumerable support via the `each` method implemented in Ruby.
#[magnus::wrap(class = "SlateDb::Iterator", free_immediately, size)]
pub struct Iterator {
    inner: Arc<Mutex<Option<DbIterator>>>,
}

impl Iterator {
    /// Create a new Iterator from a DbIterator.
    pub fn new(iter: DbIterator) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(iter))),
        }
    }

    /// Get the next key-value pair.
    ///
    /// Returns [key, value] as an array, or nil if iteration is complete.
    pub fn next_entry(&self) -> Result<Option<(String, String)>, Error> {
        let inner = self.inner.clone();

        let result = block_on(async {
            let mut guard = inner.lock().await;
            match guard.as_mut() {
                Some(iter) => iter.next().await.map_err(IteratorError::Slate),
                None => Err(IteratorError::Closed),
            }
        });

        let kv = match result {
            Ok(kv) => kv,
            Err(IteratorError::Closed) => return Err(internal_error("iterator has been closed")),
            Err(IteratorError::Slate(e)) => return Err(map_error(e)),
        };

        Ok(kv.map(|kv| {
            (
                String::from_utf8_lossy(&kv.key).to_string(),
                String::from_utf8_lossy(&kv.value).to_string(),
            )
        }))
    }

    /// Get the next key-value pair as raw bytes.
    ///
    /// Returns [key, value] as byte arrays, or nil if iteration is complete.
    pub fn next_entry_bytes(&self) -> ByteKvResult {
        let inner = self.inner.clone();

        let result = block_on(async {
            let mut guard = inner.lock().await;
            match guard.as_mut() {
                Some(iter) => iter.next().await.map_err(IteratorError::Slate),
                None => Err(IteratorError::Closed),
            }
        });

        let kv = match result {
            Ok(kv) => kv,
            Err(IteratorError::Closed) => return Err(internal_error("iterator has been closed")),
            Err(IteratorError::Slate(e)) => return Err(map_error(e)),
        };

        Ok(kv.map(|kv| (kv.key.to_vec(), kv.value.to_vec())))
    }

    /// Seek to a specific key position.
    ///
    /// After seeking, `next` will return entries starting from the given key.
    pub fn seek(&self, key: String) -> Result<(), Error> {
        if key.is_empty() {
            return Err(invalid_argument_error("key cannot be empty"));
        }

        let inner = self.inner.clone();

        let result = block_on(async {
            let mut guard = inner.lock().await;
            match guard.as_mut() {
                Some(iter) => iter.seek(key.as_bytes()).await.map_err(IteratorError::Slate),
                None => Err(IteratorError::Closed),
            }
        });

        match result {
            Ok(()) => Ok(()),
            Err(IteratorError::Closed) => Err(internal_error("iterator has been closed")),
            Err(IteratorError::Slate(e)) => Err(map_error(e)),
        }
    }

    /// Close the iterator and release resources.
    pub fn close(&self) -> Result<(), Error> {
        let inner = self.inner.clone();

        block_on(async {
            let mut guard = inner.lock().await;
            *guard = None;
        });

        Ok(())
    }
}

/// Define the Iterator class on the SlateDb module.
pub fn define_iterator_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Iterator", ruby.class_object())?;

    // Instance methods
    class.define_method("next_entry", method!(Iterator::next_entry, 0))?;
    class.define_method("next_entry_bytes", method!(Iterator::next_entry_bytes, 0))?;
    class.define_method("seek", method!(Iterator::seek, 1))?;
    class.define_method("close", method!(Iterator::close, 0))?;

    Ok(())
}
