use magnus::prelude::*;
use magnus::{Error, ExceptionClass, Ruby};
use slatedb::Error as SlateError;
use slatedb::ErrorKind;
use std::cell::RefCell;

// Store exception classes in thread-local storage for access during error mapping
thread_local! {
    static SLATE_DB_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
    static TRANSACTION_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
    static CLOSED_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
    static UNAVAILABLE_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
    static INVALID_ARGUMENT_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
    static DATA_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
    static INTERNAL_ERROR: RefCell<Option<ExceptionClass>> = const { RefCell::new(None) };
}

/// Define SlateDB exception classes under the SlateDb module.
///
/// Exception hierarchy:
/// - SlateDb::Error (base class, inherits from StandardError)
///   - SlateDb::TransactionError
///   - SlateDb::ClosedError
///   - SlateDb::UnavailableError
///   - SlateDb::InvalidArgumentError
///   - SlateDb::DataError
///   - SlateDb::InternalError
pub fn define_exceptions(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let standard_error = ruby.exception_standard_error();

    // Define base SlateDb::Error
    let slate_error = module.define_error("Error", standard_error)?;
    SLATE_DB_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(slate_error);
    });

    // Define specific error types
    let transaction_error = module.define_error("TransactionError", slate_error)?;
    TRANSACTION_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(transaction_error);
    });

    let closed_error = module.define_error("ClosedError", slate_error)?;
    CLOSED_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(closed_error);
    });

    let unavailable_error = module.define_error("UnavailableError", slate_error)?;
    UNAVAILABLE_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(unavailable_error);
    });

    let invalid_argument_error = module.define_error("InvalidArgumentError", slate_error)?;
    INVALID_ARGUMENT_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(invalid_argument_error);
    });

    let data_error = module.define_error("DataError", slate_error)?;
    DATA_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(data_error);
    });

    let internal_error = module.define_error("InternalError", slate_error)?;
    INTERNAL_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(internal_error);
    });

    Ok(())
}

/// Map a SlateDB error to the appropriate Ruby exception.
pub fn map_error(err: SlateError) -> Error {
    let msg = format!("{}", err);
    let ruby = Ruby::get().expect("Ruby runtime not available");

    match err.kind() {
        ErrorKind::Transaction => TRANSACTION_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.clone()))
        }),
        ErrorKind::Closed(_) => CLOSED_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.clone()))
        }),
        ErrorKind::Unavailable => UNAVAILABLE_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.clone()))
        }),
        ErrorKind::Invalid => INVALID_ARGUMENT_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_arg_error(), msg.clone()))
        }),
        ErrorKind::Data => DATA_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.clone()))
        }),
        ErrorKind::Internal => INTERNAL_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.clone()))
        }),
        _ => INTERNAL_ERROR.with(|cell| {
            cell.borrow()
                .map(|exc| Error::new(exc, msg.clone()))
                .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.clone()))
        }),
    }
}

/// Create an InvalidArgumentError with the given message.
pub fn invalid_argument_error(msg: &str) -> Error {
    let ruby = Ruby::get().expect("Ruby runtime not available");
    INVALID_ARGUMENT_ERROR.with(|cell| {
        cell.borrow()
            .map(|exc| Error::new(exc, msg.to_string()))
            .unwrap_or_else(|| Error::new(ruby.exception_arg_error(), msg.to_string()))
    })
}

/// Create an InternalError with the given message.
#[allow(dead_code)]
pub fn internal_error(msg: &str) -> Error {
    let ruby = Ruby::get().expect("Ruby runtime not available");
    INTERNAL_ERROR.with(|cell| {
        cell.borrow()
            .map(|exc| Error::new(exc, msg.to_string()))
            .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.to_string()))
    })
}

/// Create a ClosedError with the given message.
pub fn closed_error(msg: &str) -> Error {
    let ruby = Ruby::get().expect("Ruby runtime not available");
    CLOSED_ERROR.with(|cell| {
        cell.borrow()
            .map(|exc| Error::new(exc, msg.to_string()))
            .unwrap_or_else(|| Error::new(ruby.exception_runtime_error(), msg.to_string()))
    })
}
