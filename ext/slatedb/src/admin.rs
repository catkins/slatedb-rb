use std::sync::Arc;

use magnus::prelude::*;
use magnus::{function, method, Error, RHash, Ruby};
use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, GarbageCollectorOptions};

use crate::errors::invalid_argument_error;
use crate::runtime::{block_on, block_on_result};
use crate::utils::get_optional;

/// Ruby wrapper for SlateDB Admin.
///
/// This struct is exposed to Ruby as `SlateDb::Admin`.
/// Provides administrative functions for managing manifests, checkpoints, and GC.
#[magnus::wrap(class = "SlateDb::Admin", free_immediately, size)]
pub struct Admin {
    inner: slatedb::admin::Admin,
}

impl Admin {
    /// Create an admin handle for a database path/object store.
    ///
    /// # Arguments
    /// * `path` - The path identifier for the database
    /// * `url` - Optional object store URL
    pub fn new(path: String, url: Option<String>) -> Result<Self, Error> {
        let object_store: Arc<dyn object_store::ObjectStore> = if let Some(ref url) = url {
            block_on_result(async { slatedb::Db::resolve_object_store(url) })?
        } else {
            Arc::new(object_store::memory::InMemory::new())
        };

        let admin = AdminBuilder::new(path, object_store).build();
        Ok(Self { inner: admin })
    }

    /// Read the latest or a specific manifest as a JSON string.
    ///
    /// # Arguments
    /// * `id` - Optional manifest id to read. If None, reads the latest.
    ///
    /// # Returns
    /// JSON string of the manifest, or None if no manifests exist.
    pub fn read_manifest(&self, id: Option<u64>) -> Result<Option<String>, Error> {
        block_on(async { self.inner.read_manifest(id).await }).map_err(|e| {
            let ruby = Ruby::get().expect("Ruby runtime not available");
            Error::new(ruby.exception_runtime_error(), format!("{}", e))
        })
    }

    /// List manifests within an optional [start, end) range as JSON.
    ///
    /// # Arguments
    /// * `start` - Optional inclusive start id
    /// * `end_id` - Optional exclusive end id
    ///
    /// # Returns
    /// JSON string containing a list of manifest metadata.
    pub fn list_manifests(&self, start: Option<u64>, end_id: Option<u64>) -> Result<String, Error> {
        let range = match (start, end_id) {
            (Some(s), Some(e)) => s..e,
            (Some(s), None) => s..u64::MAX,
            (None, Some(e)) => 0..e,
            (None, None) => 0..u64::MAX,
        };

        block_on(async { self.inner.list_manifests(range).await }).map_err(|e| {
            let ruby = Ruby::get().expect("Ruby runtime not available");
            Error::new(ruby.exception_runtime_error(), format!("{}", e))
        })
    }

    /// Create a detached checkpoint.
    ///
    /// # Arguments
    /// * `kwargs` - Options: lifetime (ms), source (UUID string), name
    ///
    /// # Returns
    /// Hash with id (UUID string) and manifest_id (int)
    pub fn create_checkpoint(&self, kwargs: RHash) -> Result<RHash, Error> {
        let lifetime =
            get_optional::<u64>(&kwargs, "lifetime")?.map(std::time::Duration::from_millis);
        let source = get_optional::<String>(&kwargs, "source")?;
        let name = get_optional::<String>(&kwargs, "name")?;

        // Parse source UUID if provided
        let source_uuid = if let Some(ref s) = source {
            Some(
                uuid::Uuid::parse_str(s)
                    .map_err(|e| invalid_argument_error(&format!("invalid source UUID: {}", e)))?,
            )
        } else {
            None
        };

        let options = CheckpointOptions {
            lifetime,
            source: source_uuid,
            name,
        };

        let result =
            block_on_result(async { self.inner.create_detached_checkpoint(&options).await })?;

        let ruby = Ruby::get().expect("Ruby runtime not available");
        let hash = ruby.hash_new();
        hash.aset(ruby.to_symbol("id"), result.id.to_string())?;
        hash.aset(ruby.to_symbol("manifest_id"), result.manifest_id)?;

        Ok(hash)
    }

    /// List known checkpoints for the database.
    ///
    /// # Arguments
    /// * `name` - Optional checkpoint name filter
    ///
    /// # Returns
    /// Array of checkpoint hashes
    pub fn list_checkpoints(&self, name: Option<String>) -> Result<magnus::RArray, Error> {
        let checkpoints = block_on(async { self.inner.list_checkpoints(name.as_deref()).await })
            .map_err(|e| {
                let ruby = Ruby::get().expect("Ruby runtime not available");
                Error::new(ruby.exception_runtime_error(), format!("{}", e))
            })?;

        let ruby = Ruby::get().expect("Ruby runtime not available");
        let result = ruby.ary_new_capa(checkpoints.len());

        for cp in checkpoints {
            let hash = ruby.hash_new();
            hash.aset(ruby.to_symbol("id"), cp.id.to_string())?;
            hash.aset(ruby.to_symbol("manifest_id"), cp.manifest_id)?;
            hash.aset(
                ruby.to_symbol("expire_time"),
                cp.expire_time.map(|t| t.to_rfc3339()),
            )?;
            hash.aset(ruby.to_symbol("create_time"), cp.create_time.to_rfc3339())?;
            hash.aset(ruby.to_symbol("name"), cp.name)?;
            result.push(hash)?;
        }

        Ok(result)
    }

    /// Refresh a checkpoint's lifetime.
    ///
    /// # Arguments
    /// * `id` - Checkpoint UUID string
    /// * `lifetime` - Optional new lifetime in milliseconds
    pub fn refresh_checkpoint(&self, id: String, lifetime: Option<u64>) -> Result<(), Error> {
        let checkpoint_uuid = uuid::Uuid::parse_str(&id)
            .map_err(|e| invalid_argument_error(&format!("invalid checkpoint UUID: {}", e)))?;

        let lifetime_duration = lifetime.map(std::time::Duration::from_millis);

        block_on_result(async {
            self.inner
                .refresh_checkpoint(checkpoint_uuid, lifetime_duration)
                .await
        })?;

        Ok(())
    }

    /// Delete a checkpoint.
    ///
    /// # Arguments
    /// * `id` - Checkpoint UUID string
    pub fn delete_checkpoint(&self, id: String) -> Result<(), Error> {
        let checkpoint_uuid = uuid::Uuid::parse_str(&id)
            .map_err(|e| invalid_argument_error(&format!("invalid checkpoint UUID: {}", e)))?;

        block_on_result(async { self.inner.delete_checkpoint(checkpoint_uuid).await })?;
        Ok(())
    }

    /// Run garbage collection once.
    ///
    /// # Arguments
    /// * `kwargs` - GC options:
    ///   - `min_age`: Minimum age in milliseconds for all directories (applies to manifest, wal, compacted)
    ///   - `manifest_min_age`: Specific minimum age in milliseconds for manifest directory
    ///   - `wal_min_age`: Specific minimum age in milliseconds for WAL directory
    ///   - `compacted_min_age`: Specific minimum age in milliseconds for compacted directory
    ///
    /// If `min_age` is provided, it will be used for all directories unless a specific override is provided.
    /// If no options are provided, defaults are used (manifest: 1 day, wal: 1 minute, compacted: 1 minute).
    pub fn run_gc(&self, kwargs: RHash) -> Result<(), Error> {
        use slatedb::config::GarbageCollectorDirectoryOptions;

        // Extract options from kwargs
        let min_age = get_optional::<u64>(&kwargs, "min_age")?;
        let manifest_min_age = get_optional::<u64>(&kwargs, "manifest_min_age")?;
        let wal_min_age = get_optional::<u64>(&kwargs, "wal_min_age")?;
        let compacted_min_age = get_optional::<u64>(&kwargs, "compacted_min_age")?;

        // Build GC options
        let gc_opts = if min_age.is_none()
            && manifest_min_age.is_none()
            && wal_min_age.is_none()
            && compacted_min_age.is_none()
        {
            // No options provided, use defaults
            GarbageCollectorOptions::default()
        } else {
            let default_opts = GarbageCollectorOptions::default();

            // Helper to create directory options with custom min_age
            let make_dir_opts =
                |specific_age: Option<u64>,
                 fallback_age: Option<u64>,
                 default_opts: Option<GarbageCollectorDirectoryOptions>| {
                    let age_ms = specific_age.or(fallback_age);
                    if let Some(ms) = age_ms {
                        Some(GarbageCollectorDirectoryOptions {
                            interval: default_opts.as_ref().and_then(|o| o.interval),
                            min_age: std::time::Duration::from_millis(ms),
                        })
                    } else {
                        default_opts
                    }
                };

            GarbageCollectorOptions {
                manifest_options: make_dir_opts(
                    manifest_min_age,
                    min_age,
                    default_opts.manifest_options,
                ),
                wal_options: make_dir_opts(wal_min_age, min_age, default_opts.wal_options),
                compacted_options: make_dir_opts(
                    compacted_min_age,
                    min_age,
                    default_opts.compacted_options,
                ),
            }
        };

        block_on(async { self.inner.run_gc_once(gc_opts).await }).map_err(|e| {
            let ruby = Ruby::get().expect("Ruby runtime not available");
            Error::new(ruby.exception_runtime_error(), format!("{}", e))
        })?;

        Ok(())
    }
}

/// Define the Admin class on the SlateDb module.
pub fn define_admin_class(ruby: &Ruby, module: &magnus::RModule) -> Result<(), Error> {
    let class = module.define_class("Admin", ruby.class_object())?;

    // Class methods
    class.define_singleton_method("_new", function!(Admin::new, 2))?;

    // Instance methods
    class.define_method("_read_manifest", method!(Admin::read_manifest, 1))?;
    class.define_method("_list_manifests", method!(Admin::list_manifests, 2))?;
    class.define_method("_create_checkpoint", method!(Admin::create_checkpoint, 1))?;
    class.define_method("_list_checkpoints", method!(Admin::list_checkpoints, 1))?;
    class.define_method("_refresh_checkpoint", method!(Admin::refresh_checkpoint, 2))?;
    class.define_method("_delete_checkpoint", method!(Admin::delete_checkpoint, 1))?;
    class.define_method("_run_gc", method!(Admin::run_gc, 1))?;

    Ok(())
}
