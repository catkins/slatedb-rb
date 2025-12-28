use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use log::error;
use magnus::rb_sys::{AsRawValue, FromRawValue};
use magnus::value::ReprValue;
use magnus::{Error, RHash, Ruby, Value};
use slatedb::{MergeOperator, MergeOperatorError};

use crate::errors::invalid_argument_error;
use crate::utils::get_optional;

struct StringConcatMergeOperator;

impl MergeOperator for StringConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut result = existing_value.unwrap_or_default().to_vec();
        result.extend_from_slice(&value);
        Ok(Bytes::from(result))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut result = existing_value.unwrap_or_default().to_vec();
        for operand in operands {
            result.extend_from_slice(operand);
        }
        Ok(Bytes::from(result))
    }
}

/// A merge operator that calls a Ruby block/proc.
///
/// This stores the raw Ruby VALUE and calls it via `with_gvl` when merge
/// operations are needed. The proc is called with (key, existing_value, new_value)
/// and should return the merged value as a String.
///
/// # Thread Safety
///
/// The Ruby proc can only be called from the Ruby thread that created this operator.
/// If the merge is called from a different thread (e.g., a Tokio worker thread during
/// background compaction), the merge will use a fallback string concatenation behavior.
///
/// # Safety
///
/// The Ruby proc must be kept alive (not garbage collected) for the lifetime
/// of this operator. This is typically handled by storing a reference to the
/// proc in the Ruby Database object.
pub struct RubyProcMergeOperator {
    /// The raw Ruby VALUE of the proc. We store this as a raw value because
    /// magnus::Value is not Send+Sync, but we need to be thread-safe.
    /// We re-acquire the GVL before using it, which makes this safe.
    proc_value: usize,
    /// The thread ID of the Ruby thread that created this operator.
    /// We can only safely call Ruby from this thread.
    ruby_thread_id: thread::ThreadId,
}

// SAFETY: We only access the proc_value when we hold the GVL via the Ruby thread,
// which ensures thread-safe access to Ruby objects.
unsafe impl Send for RubyProcMergeOperator {}
unsafe impl Sync for RubyProcMergeOperator {}

impl RubyProcMergeOperator {
    /// Create a new RubyProcMergeOperator from a Ruby proc/block.
    ///
    /// # Safety
    ///
    /// The caller must ensure the proc remains alive (not GC'd) for the
    /// lifetime of this operator.
    pub fn new(proc: Value) -> Self {
        Self {
            proc_value: proc.as_raw() as usize,
            ruby_thread_id: thread::current().id(),
        }
    }

    /// Check if we're on the Ruby thread that created this operator.
    fn is_ruby_thread(&self) -> bool {
        thread::current().id() == self.ruby_thread_id
    }

    /// Call the Ruby proc with the given arguments.
    /// This must only be called from the Ruby thread (after checking is_ruby_thread).
    fn call_proc_on_ruby_thread(
        &self,
        key: &str,
        existing_value: Option<&str>,
        new_value: &str,
    ) -> Result<Bytes, MergeOperatorError> {
        // We're on the Ruby thread, so we can use with_gvl
        // Import here to avoid the circular dependency at module level
        use crate::runtime::with_gvl;

        let key_owned = key.to_string();
        let existing_owned = existing_value.map(|s| s.to_string());
        let new_owned = new_value.to_string();

        with_gvl(|| {
            let ruby = Ruby::get().expect("Ruby runtime not available");

            // Reconstruct the proc Value from the raw pointer
            let proc = unsafe { Value::from_raw(self.proc_value as _) };

            // Build arguments: (key, existing_value, new_value)
            let existing_arg: Value = match &existing_owned {
                Some(s) => ruby.str_new(s).as_value(),
                None => ruby.qnil().as_value(),
            };

            // Call the proc
            let result: Result<String, magnus::Error> = proc.funcall(
                "call",
                (
                    ruby.str_new(&key_owned),
                    existing_arg,
                    ruby.str_new(&new_owned),
                ),
            );

            match result {
                Ok(merged) => Ok(Bytes::from(merged)),
                Err(e) => {
                    error!("Ruby merge operator error: {}", e);
                    Err(MergeOperatorError::EmptyBatch)
                }
            }
        })
    }

    /// Fallback merge when we're not on the Ruby thread.
    /// Uses simple concatenation as a safe default.
    fn fallback_merge(
        &self,
        existing_value: Option<&Bytes>,
        new_value: &Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        error!(
            "Ruby merge operator called from non-Ruby thread, using fallback concatenation. \
             This can happen during background compaction."
        );
        let mut result = existing_value
            .map(|v| v.to_vec())
            .unwrap_or_default();
        result.extend_from_slice(new_value);
        Ok(Bytes::from(result))
    }

    /// Call the Ruby proc with the given arguments, handling thread safety.
    fn call_proc(
        &self,
        key: &Bytes,
        existing_value: Option<&Bytes>,
        new_value: &Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let key_str = String::from_utf8_lossy(key);
        let existing_str = existing_value.map(|v| String::from_utf8_lossy(v));
        let new_str = String::from_utf8_lossy(new_value);

        if self.is_ruby_thread() {
            self.call_proc_on_ruby_thread(&key_str, existing_str.as_deref(), &new_str)
        } else {
            // We're on a worker thread, use fallback
            self.fallback_merge(existing_value, new_value)
        }
    }
}

impl MergeOperator for RubyProcMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.call_proc(key, existing_value.as_ref(), &value)
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        // Apply operands one at a time through the Ruby proc
        let mut current = existing_value;
        for operand in operands {
            current = Some(self.call_proc(key, current.as_ref(), operand)?);
        }
        Ok(current.unwrap_or_default())
    }
}

pub fn parse_merge_operator(
    kwargs: &RHash,
) -> Result<Option<Arc<dyn MergeOperator + Send + Sync>>, Error> {
    let merge_operator = get_optional::<String>(kwargs, "merge_operator")?;
    let Some(merge_operator) = merge_operator else {
        return Ok(None);
    };

    let operator: Arc<dyn MergeOperator + Send + Sync> = match merge_operator.as_str() {
        "string_concat" | "concat" => Arc::new(StringConcatMergeOperator),
        _ => {
            return Err(invalid_argument_error(&format!(
                "invalid merge_operator: {} (expected 'string_concat', 'concat', or use merge_operator_proc for a custom block)",
                merge_operator
            )))
        }
    };

    Ok(Some(operator))
}

/// Parse a Ruby proc as a merge operator.
pub fn parse_merge_operator_proc(
    kwargs: &RHash,
) -> Result<Option<Arc<dyn MergeOperator + Send + Sync>>, Error> {
    let proc_value = get_optional::<Value>(kwargs, "merge_operator_proc")?;
    let Some(proc) = proc_value else {
        return Ok(None);
    };

    // Verify it's callable
    if !proc.respond_to("call", false).unwrap_or(false) {
        return Err(invalid_argument_error(
            "merge_operator_proc must respond to 'call'",
        ));
    }

    Ok(Some(Arc::new(RubyProcMergeOperator::new(proc))))
}
