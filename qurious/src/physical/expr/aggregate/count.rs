use std::collections::HashSet;
use std::{fmt::Display, sync::Arc};

use super::{Accumulator, AggregateExpr};
use crate::error::{Error, Result};
use crate::{arrow_err, datatypes::scalar::ScalarValue, physical::expr::PhysicalExpr};
use arrow::array::{Array, ArrayRef};
use arrow::compute::{filter, is_not_null};
use arrow::row::{RowConverter, SortField};

#[derive(Debug)]
pub struct CountAggregateExpr {
    pub expr: Arc<dyn PhysicalExpr>,
    pub distinct: bool,
}

impl CountAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, distinct: bool) -> Self {
        Self { expr, distinct }
    }
}

impl Display for CountAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "COUNT({}{})",
            if self.distinct { "DISTINCT " } else { "" },
            self.expr
        )
    }
}

impl AggregateExpr for CountAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        if self.distinct {
            Ok(Box::new(DistinctCountAccumulator::default()))
        } else {
            Ok(Box::new(CountAccumulator::default()))
        }
    }
}

#[derive(Debug, Default)]
pub struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn accumluate(&mut self, values: &ArrayRef) -> Result<()> {
        self.count += (values.len() - values.null_count()) as i64;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
}

/// `COUNT(DISTINCT x)`.
///
/// Values are hashed through arrow's row format, which gives a comparable byte encoding for any
/// data type. `ScalarValue` cannot be used as a key here because it holds floats and so implements
/// neither `Hash` nor `Eq`.
#[derive(Debug, Default)]
pub struct DistinctCountAccumulator {
    seen: HashSet<Vec<u8>>,
    converter: Option<RowConverter>,
}

impl Accumulator for DistinctCountAccumulator {
    fn accumluate(&mut self, values: &ArrayRef) -> Result<()> {
        // COUNT ignores NULLs, and dropping them here keeps them out of the distinct set too.
        let values = if values.null_count() > 0 {
            let mask = is_not_null(values.as_ref()).map_err(|e| arrow_err!(e))?;
            filter(values.as_ref(), &mask).map_err(|e| arrow_err!(e))?
        } else {
            values.clone()
        };

        if values.is_empty() {
            return Ok(());
        }

        let converter = match &self.converter {
            Some(converter) => converter,
            None => {
                let field = SortField::new(values.data_type().clone());
                let converter = RowConverter::new(vec![field]).map_err(|e| arrow_err!(e))?;
                self.converter.insert(converter)
            }
        };

        let rows = converter.convert_columns(&[values]).map_err(|e| arrow_err!(e))?;
        for row in rows.iter() {
            self.seen.insert(row.as_ref().to_vec());
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.seen.len() as i64)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;

    fn count_distinct(batches: Vec<ArrayRef>) -> i64 {
        let mut acc = DistinctCountAccumulator::default();
        for batch in batches {
            acc.accumluate(&batch).unwrap();
        }
        match acc.evaluate().unwrap() {
            ScalarValue::Int64(Some(v)) => v,
            other => panic!("expected an Int64 count, got {other:?}"),
        }
    }

    #[test]
    fn counts_distinct_values_across_batches() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![2i64, 3]));

        assert_eq!(count_distinct(vec![a.clone()]), 2);
        assert_eq!(count_distinct(vec![a, b]), 3);
    }

    #[test]
    fn ignores_nulls_like_plain_count() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None, Some(2)]));
        assert_eq!(count_distinct(vec![values]), 2);

        // a batch of only nulls contributes nothing
        let all_null: ArrayRef = Arc::new(Int64Array::from(vec![None, None] as Vec<Option<i64>>));
        assert_eq!(count_distinct(vec![all_null]), 0);
    }

    #[test]
    fn works_for_non_primitive_types() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("x"), None]));
        assert_eq!(count_distinct(vec![values]), 2);
    }

    #[test]
    fn empty_input_counts_zero() {
        assert_eq!(count_distinct(vec![]), 0);

        let empty: ArrayRef = arrow::array::new_empty_array(&DataType::Int64);
        assert_eq!(count_distinct(vec![empty]), 0);
    }
}
