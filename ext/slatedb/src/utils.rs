use magnus::value::ReprValue;
use magnus::{Error, RHash, Ruby, TryConvert};

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
