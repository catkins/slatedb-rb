use std::ops::Bound;
use std::sync::Arc;

use magnus::value::ReprValue;
use magnus::{Error, RHash, Ruby, TryConvert};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreScheme};
use slatedb::Error as SlateError;
use url::Url;

use crate::errors::invalid_argument_error;

/// A prefix scan sub-range expressed as inclusive/exclusive/unbounded byte
/// suffix bounds. Implements `slatedb::ByteRangeBounds`.
pub type PrefixSubrange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

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

/// Build a prefix scan sub-range from optional suffix bounds supplied in
/// `kwargs`.
///
/// The bounds are key *suffixes* interpreted relative to the scan prefix, as
/// documented by SlateDB's `scan_prefix`: a bound `s` selects the full key
/// `prefix ++ s`. The Ruby layer decomposes a `Range` into these kwargs:
/// * `start` — inclusive lower bound (a `Range#begin`); unbounded when absent.
/// * `end` — upper bound (a `Range#end`); unbounded when absent.
/// * `end_inclusive` — whether `end` is inclusive (`..`) vs exclusive (`...`).
///
/// The returned tuple implements `ByteRangeBounds`; `..` (both unbounded)
/// reproduces the pre-0.14.0 behaviour of scanning the prefix's entire
/// keyspace.
///
/// Empty suffix bounds and an empty/degenerate range are rejected up front,
/// because SlateDB's `BytesRange` panics on an empty range.
pub fn prefix_subrange_from_kwargs(kwargs: &RHash) -> Result<PrefixSubrange, Error> {
    let start = get_optional::<String>(kwargs, "start")?.map(String::into_bytes);
    let end = get_optional::<String>(kwargs, "end")?.map(String::into_bytes);
    let end_inclusive = get_optional::<bool>(kwargs, "end_inclusive")?.unwrap_or(false);

    if let Some(s) = &start {
        if s.is_empty() {
            return Err(invalid_argument_error("start suffix cannot be empty"));
        }
    }
    if let Some(e) = &end {
        if e.is_empty() {
            return Err(invalid_argument_error("end suffix cannot be empty"));
        }
    }
    if let (Some(s), Some(e)) = (&start, &end) {
        // An inclusive end allows the single-point range start == end; an
        // exclusive end does not. Either way, start > end (or >=) is empty and
        // would panic inside SlateDB.
        let empty = if end_inclusive { s > e } else { s >= e };
        if empty {
            return Err(invalid_argument_error(
                "scan_prefix suffix range is empty (start must be <= end for an \
                 inclusive range, or < end for an exclusive one)",
            ));
        }
    }

    let start_bound = match start {
        Some(s) => Bound::Included(s),
        None => Bound::Unbounded,
    };
    let end_bound = match end {
        Some(e) if end_inclusive => Bound::Included(e),
        Some(e) => Bound::Excluded(e),
        None => Bound::Unbounded,
    };
    Ok((start_bound, end_bound))
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
            // Mirror SlateDB's pre-0.14 resolver behaviour. As of object_store
            // 0.14 both `object_store::parse_url` and SlateDB's own
            // `Db::resolve_object_store` reject URLs that carry a path
            // component ("provide path to builder instead"). Resolve the store
            // ourselves and re-apply the path via a PrefixStore so a URL like
            // `file:///data/store` keeps rooting the store at that location,
            // with the caller's `path` argument nested underneath.
            use slatedb::object_store::prefix::PrefixStore;

            // Lowercase env keys because parse_url_opts only recognises
            // lower-case option keys.
            let env_vars = std::env::vars().map(|(key, value)| (key.to_ascii_lowercase(), value));
            let (object_store, path) =
                slatedb::object_store::parse_url_opts(&parsed_url, env_vars)
                    .map_err(to_slate_error)?;

            if path.as_ref().is_empty() {
                Ok(Arc::from(object_store))
            } else {
                let object_store: Arc<dyn ObjectStore> = Arc::from(object_store);
                Ok(Arc::new(PrefixStore::new(object_store, path)))
            }
        }
    }
}
