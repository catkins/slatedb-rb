//! SlateDB Ruby bindings
//!
//! This crate provides Ruby bindings for SlateDB, a cloud-native embedded
//! key-value store built on object storage.
//!
//! # Example
//!
//! ```ruby
//! require 'slatedb'
//!
//! db = SlateDb::Database.open("/tmp/mydb")
//! db.put("hello", "world")
//! db.get("hello") # => "world"
//! db.close
//! ```

use magnus::{Error, Ruby};

mod admin;
mod database;
mod errors;
mod iterator;
mod reader;
mod runtime;
mod snapshot;
mod transaction;
mod utils;
mod write_batch;

/// Initialize the SlateDb Ruby module.
///
/// This is called automatically when the native extension is loaded.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("SlateDb")?;

    // Define exception classes first
    errors::define_exceptions(ruby, &module)?;

    // Define core classes
    database::define_database_class(ruby, &module)?;
    iterator::define_iterator_class(ruby, &module)?;
    write_batch::define_write_batch_class(ruby, &module)?;
    transaction::define_transaction_class(ruby, &module)?;
    snapshot::define_snapshot_class(ruby, &module)?;
    reader::define_reader_class(ruby, &module)?;
    admin::define_admin_class(ruby, &module)?;

    Ok(())
}
