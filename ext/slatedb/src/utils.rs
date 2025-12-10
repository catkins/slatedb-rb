use std::sync::Arc;

use magnus::value::ReprValue;
use magnus::{Error, RHash, Ruby, TryConvert};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStoreScheme;
use slatedb::{Db, Error as SlateError};
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

/// Convert an object_store error to a SlateDB error
fn to_slate_error(e: object_store::Error) -> SlateError {
    SlateError::unavailable(e.to_string())
}

/// Resolve an object store URL to an ObjectStore instance.
///
/// This function handles S3 URLs specially to ensure environment variables
/// like AWS_ACCESS_KEY_ID are properly recognized (the default object_store
/// registry only recognizes lowercase variants like aws_access_key_id).
pub fn resolve_object_store(url: &str) -> Result<Arc<dyn object_store::ObjectStore>, SlateError> {
    let parsed_url: Url = url
        .try_into()
        .map_err(|e: url::ParseError| SlateError::invalid(format!("invalid URL: {}", e)))?;

    let (scheme, _path) =
        ObjectStoreScheme::parse(&parsed_url).map_err(|e| to_slate_error(e.into()))?;

    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            // Use from_env() to properly handle uppercase AWS_* environment variables
            let store = AmazonS3Builder::from_env()
                .with_url(url)
                .build()
                .map_err(to_slate_error)?;
            Ok(Arc::new(store))
        }
        _ => {
            // Fall back to slatedb's default resolver for other schemes
            Db::resolve_object_store(url)
        }
    }
}
