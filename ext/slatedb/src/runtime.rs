use magnus::Error;
use once_cell::sync::OnceCell;
use rb_sys::rb_thread_call_without_gvl;
use slatedb::Error as SlateError;
use std::ffi::c_void;
use std::future::Future;
use tokio::runtime::Runtime;

use crate::errors::map_error;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

/// Get or initialize the shared Tokio runtime for all SlateDB operations.
///
/// We use a multi-threaded runtime to support concurrent access from multiple
/// Ruby threads. This is important for use with Sidekiq, Puma, and other
/// multi-threaded Ruby applications.
fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

/// Execute a future on the runtime, releasing the Ruby GVL while waiting.
///
/// # GVL Safety
///
/// This function releases Ruby's Global VM Lock (GVL) while the future executes,
/// allowing other Ruby threads to run concurrently. This means:
///
/// - **Do NOT call Ruby APIs** inside the future (e.g., `Ruby::get()`, creating
///   Ruby exceptions, or calling magnus functions that require Ruby)
/// - **Do NOT use `map_error`** or other error converters inside the async block
/// - Return raw Rust types/errors from the future, then convert to Ruby types
///   after `block_on` returns (when the GVL is re-acquired)
///
/// For futures that return `Result<T, slatedb::Error>`, use [`block_on_result`]
/// which handles error conversion automatically.
pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    let rt = get_runtime();
    without_gvl(|| rt.block_on(future))
}

/// Execute a future returning `Result<T, slatedb::Error>`, converting errors to Ruby.
///
/// This is a convenience wrapper around [`block_on`] that automatically converts
/// SlateDB errors to Ruby exceptions after the GVL is re-acquired.
pub fn block_on_result<F, T>(future: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, SlateError>>,
{
    block_on(future).map_err(map_error)
}

/// Execute a closure without holding the Ruby GVL.
///
/// This releases the Global VM Lock, allowing other Ruby threads to run
/// while this closure executes. Essential for I/O-bound operations.
fn without_gvl<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    struct Closure<F, T> {
        f: Option<F>,
        result: Option<T>,
    }

    extern "C" fn call_closure<F, T>(data: *mut c_void) -> *mut c_void
    where
        F: FnOnce() -> T,
    {
        let closure = unsafe { &mut *(data as *mut Closure<F, T>) };
        if let Some(f) = closure.f.take() {
            closure.result = Some(f());
        }
        std::ptr::null_mut()
    }

    let mut closure = Closure {
        f: Some(f),
        result: None,
    };

    unsafe {
        rb_thread_call_without_gvl(
            Some(call_closure::<F, T>),
            &mut closure as *mut _ as *mut c_void,
            None,
            std::ptr::null_mut(),
        );
    }

    closure.result.expect("closure did not run")
}
