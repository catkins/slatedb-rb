use std::ops::Bound;
use std::sync::Arc;

use magnus::value::ReprValue;
use magnus::{Error, RHash, Ruby, TryConvert};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::prefix::PrefixStore;
use slatedb::object_store::{
    parse_url_opts, Error as ObjectStoreError, ObjectStore, ObjectStoreScheme,
};
use slatedb::Error as SlateError;
use url::Url;

/// Helper to extract an optional value from an RHash
pub fn get_optional<T: TryConvert>(hash: &RHash, key: &str) -> Result<Option<T>, Error> {
    let ruby = Ruby::get().expect("Ruby runtime not available");
    let sym = ruby.to_symbol(key);
    match hash.get(sym) {
        Some(val) => {
            if val.is_nil() {
                Ok(None)
            } else {
                Ok(Some(T::try_convert(val)?))
            }
        }
        None => Ok(None),
    }
}

/// A key sub-range for a prefix scan, expressed as owned byte bounds. This
/// implements `slatedb::bytes_range::ByteRangeBounds`, so it can be passed
/// straight to `scan_prefix`/`scan_prefix_with_options`.
pub type PrefixSubrange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Build a suffix sub-range (relative to a scan prefix) from optional
/// `subrange_from` / `subrange_to` keyword arguments.
///
/// The bounds are key *suffixes* appended to the prefix: `subrange_from` is an
/// inclusive lower bound and `subrange_to` is an exclusive upper bound. A
/// missing bound is unbounded, so an empty hash yields a full-prefix scan
/// (equivalent to `..`).
pub fn prefix_subrange_from_kwargs(hash: &RHash) -> Result<PrefixSubrange, Error> {
    let start = match get_optional::<String>(hash, "subrange_from")? {
        Some(s) => Bound::Included(s.into_bytes()),
        None => Bound::Unbounded,
    };
    let end = match get_optional::<String>(hash, "subrange_to")? {
        Some(s) => Bound::Excluded(s.into_bytes()),
        None => Bound::Unbounded,
    };
    Ok((start, end))
}

/// Convert an object_store error to a SlateDB error
fn to_slate_error(e: ObjectStoreError) -> SlateError {
    SlateError::unavailable(e.to_string())
}

/// Resolve an object store URL to an ObjectStore instance.
///
/// This function handles S3 URLs specially to ensure environment variables
/// like AWS_ACCESS_KEY_ID are properly recognized (the default object_store
/// registry only recognizes lowercase variants like aws_access_key_id).
pub fn resolve_object_store(url: &str) -> Result<Arc<dyn ObjectStore>, SlateError> {
    let parsed_url: Url = url
        .try_into()
        .map_err(|e: url::ParseError| SlateError::invalid(format!("invalid URL: {}", e)))?;

    let (scheme, _path) =
        ObjectStoreScheme::parse(&parsed_url).map_err(|e| to_slate_error(e.into()))?;

    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            // Use from_env() to properly handle uppercase AWS_* environment variables
            // (the default object_store registry only recognizes lowercase variants)
            let store = AmazonS3Builder::from_env()
                .with_url(url)
                .build()
                .map_err(to_slate_error)?;
            Ok(Arc::new(store))
        }
        _ => {
            // SlateDB 0.14 changed `Db::resolve_object_store` to reject any URL
            // that carries a path component (returning `InvalidObjectStorePath`),
            // whereas 0.13 transparently wrapped it in a `PrefixStore`. Preserve
            // the old, more forgiving behavior here so callers can keep passing a
            // full location such as "file:///data/mydb" or "gs://bucket/prefix".
            //
            // Env keys are lowercased because `parse_url_opts` only recognizes
            // lower-case option keys.
            let env_vars = std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v));
            let (store, path) = parse_url_opts(&parsed_url, env_vars).map_err(to_slate_error)?;
            let store: Arc<dyn ObjectStore> = Arc::from(store);
            if path.as_ref().is_empty() {
                Ok(store)
            } else {
                Ok(Arc::new(PrefixStore::new(store, path)))
            }
        }
    }
}
