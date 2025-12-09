use once_cell::sync::OnceCell;
use rb_sys::rb_thread_call_without_gvl;
use std::ffi::c_void;
use std::future::Future;
use tokio::runtime::Runtime;

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
/// This is critical for thread safety - it allows other Ruby threads to run
/// while this thread waits for I/O operations to complete. Without this,
/// multiple Ruby threads calling SlateDB operations would deadlock.
pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    let rt = get_runtime();

    // Use rb_thread_call_without_gvl to release the GVL while blocking
    // This allows other Ruby threads to execute while we wait for I/O
    without_gvl(|| rt.block_on(future))
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
