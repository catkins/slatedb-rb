use magnus::prelude::*;
use magnus::{method, Error, RHash, Ruby};
use std::cell::RefCell;

use crate::errors::closed_error;
use crate::runtime::block_on_result;

#[magnus::wrap(class = "SlateDb::CheckpointHandle", free_immediately, size)]
pub struct CheckpointHandle {
    id: String,
    inner: RefCell<Option<slatedb::CheckpointHandle>>,
}

impl CheckpointHandle {
    pub fn new(handle: slatedb::CheckpointHandle) -> Self {
        Self {
            id: handle.id().to_string(),
            inner: RefCell::new(Some(handle)),
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn wait(&self) -> Result<RHash, Error> {
        let handle = self
            .inner
            .borrow_mut()
            .take()
            .ok_or_else(|| closed_error("checkpoint handle has already been waited on"))?;
        let result = block_on_result(handle.wait())?;

        let ruby = Ruby::get().expect("Ruby runtime not available");
        let hash = ruby.hash_new();
        hash.aset(ruby.to_symbol("id"), result.id.to_string())?;
        hash.aset(ruby.to_symbol("manifest_id"), result.manifest_id)?;
        Ok(hash)
    }
}

pub fn define_checkpoint_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("CheckpointHandle", ruby.class_object())?;
    class.define_method("id", method!(CheckpointHandle::id, 0))?;
    class.define_method("wait", method!(CheckpointHandle::wait, 0))?;
    Ok(())
}
