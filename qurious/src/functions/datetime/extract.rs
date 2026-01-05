use std::str::FromStr;

use arrow::array::Array;
use arrow::compute::kernels::cast_utils::IntervalUnit;
use arrow::{
    array::new_empty_array,
    array::{ArrayRef, AsArray},
    compute::{cast, date_part, DatePart},
    datatypes::DataType,
};

use crate::{arrow_err, internal_err};
use crate::{
    error::{Error, Result},
    functions::UserDefinedFunction,
};

#[derive(Debug)]
pub struct DatetimeExtract;

impl UserDefinedFunction for DatetimeExtract {
    fn name(&self) -> &str {
        "EXTRACT"
    }

    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        if args.len() != 2 {
            return Err(Error::InvalidArgumentError("EXTRACT requires 2 arguments".to_string()));
        }

        // EXTRACT is often used in projections that can be evaluated on empty batches (e.g. after filters).
        // In that case, literal args can be represented as empty arrays, so we must not assume len > 0.
        if args[1].len() == 0 {
            return Ok(new_empty_array(&DataType::Int64));
        }

        // get interval_unit value
        let interval_unit = if let Some(val) = args.get(0) {
            let arr = val.as_string::<i32>();
            if arr.len() == 0 {
                // no rows => empty output already handled above; keep defensive
                return Ok(new_empty_array(&DataType::Int64));
            }
            if !arr.is_valid(0) {
                return Err(Error::InvalidArgumentError(
                    "First argument of `EXTRACT` must be non-null scalar Utf8".to_string(),
                ));
            }
            arr.value(0)
        } else {
            return Err(Error::InvalidArgumentError(
                "First argument of `DATE_PART` must be non-null scalar Utf8".to_string(),
            ));
        };

        // Normalize case so we accept YEAR / year / Year, etc.
        let interval_unit = interval_unit.to_ascii_lowercase();
        match IntervalUnit::from_str(&interval_unit)? {
            IntervalUnit::Year => date_part_f64(args[1].as_ref(), DatePart::Year),
            IntervalUnit::Month => date_part_f64(args[1].as_ref(), DatePart::Month),
            IntervalUnit::Week => date_part_f64(args[1].as_ref(), DatePart::Week),
            IntervalUnit::Day => date_part_f64(args[1].as_ref(), DatePart::Day),
            IntervalUnit::Hour => date_part_f64(args[1].as_ref(), DatePart::Hour),
            IntervalUnit::Minute => date_part_f64(args[1].as_ref(), DatePart::Minute),
            IntervalUnit::Second => date_part_f64(args[1].as_ref(), DatePart::Second),
            IntervalUnit::Millisecond => date_part_f64(args[1].as_ref(), DatePart::Millisecond),
            IntervalUnit::Microsecond => date_part_f64(args[1].as_ref(), DatePart::Microsecond),
            IntervalUnit::Nanosecond => date_part_f64(args[1].as_ref(), DatePart::Nanosecond),
            // century and decade are not supported by `DatePart`, although they are supported in postgres
            _ => internal_err!("Date part '{}' not supported", interval_unit),
        }
    }
}

fn date_part_f64(array: &dyn Array, part: DatePart) -> Result<ArrayRef> {
    cast(date_part(array, part)?.as_ref(), &DataType::Int64).map_err(|e| arrow_err!(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{new_empty_array, StringArray};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    #[test]
    fn test_extract_empty_input_does_not_panic() {
        let udf = DatetimeExtract;
        let unit = Arc::new(StringArray::from(vec!["YEAR"])) as ArrayRef;
        let empty_date = new_empty_array(&DataType::Date32);

        let out = udf.eval(vec![unit, empty_date]).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &DataType::Int64);
    }
}
