use std::sync::Arc;

use bytes::Bytes;
use magnus::{Error, RHash};
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
                "invalid merge_operator: {} (expected 'string_concat' or 'concat')",
                merge_operator
            )))
        }
    };

    Ok(Some(operator))
}
