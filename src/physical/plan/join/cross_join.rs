use crate::{
    error::{Error, Result},
    physical::plan::PhysicalPlan,
};
use arrow::{
    array::{
        new_null_array, Array, ArrayRef, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, RecordBatch, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{Schema, SchemaRef},
};
use std::sync::Arc;

/// Cross join implementation
/// A cross join is a cartesian product of the left and right input plans
/// It is implemented by creating a new RecordBatch for each combination of left and right
/// The schema of the cross join is the concatenation of the left and right schema
/// ```text
///           ┌───────────────────┐         ┌──────────────────┐          
///           │                   │         │                  │          
///           │ ┌───────────────┐ │         │ ┌──────────────┐ │          
///           │ │     Row 1     │ │         │ │     Row 1    │ │          
///           │ └───────────────┘ │         │ └──────────────┘ │          
/// Batch One │                   │         │                  │ Batch two
///           │ ┌───────────────┐ │         │ ┌──────────────┐ │          
///    Left   │ │     Row 2     │ │         │ │    Row 2     │ │    Right
///           │ └───────────────┘ │         │ └──────────────┘ │          
///           │                   │         │                  │          
///           └────────┬──────────┘         └─────────┬────────┘          
///                    │     After Cross Join         │                   
///                    │                              │                   
///                    └──────────────┬───────────────┘                   
///                                   │                                   
///                                   │                                   
///                                   │                                   
///           ┌───────────────────────▼────────────────────────┐          
///           │                                                │          
///           │ ┌───────────────┐          ┌───────────────┐   │          
///           │ │     Row 1     │          │     Row 1     │   │          
///           │ └───────────────┘          └───────────────┘   │          
///           │                                                │          
///           │ ┌───────────────┐          ┌───────────────┐   │          
///           │ │     Row 1     │          │     Row 2     │   │          
///           │ └───────────────┘          └───────────────┘   │          
///           │                                                │          
///           │ ┌───────────────┐          ┌───────────────┐   │          
///           │ │     Row 2     │          │     Row 1     │   │          
///           │ └───────────────┘          └───────────────┘   │          
///           │                                                │          
///           │ ┌───────────────┐          ┌───────────────┐   │          
///           │ │     Row 2     │          │     Row 2     │   │          
///           │ └───────────────┘          └───────────────┘   │          
///           │                                                │          
///           └────────────────────────────────────────────────┘                 
/// ```
pub struct CrossJoin {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub schema: SchemaRef,
}

impl CrossJoin {
    pub fn new(left: Arc<dyn PhysicalPlan>, right: Arc<dyn PhysicalPlan>) -> Self {
        let schema = Schema::new(
            left.schema()
                .fields()
                .iter()
                .chain(right.schema().fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
        );
        Self {
            left,
            right,
            schema: Arc::new(schema),
        }
    }

    fn build_batch(&self, left_batch: &RecordBatch, right_batch: &Vec<RecordBatch>) -> Vec<Result<RecordBatch>> {
        let mut batches = vec![];

        for rb in right_batch {
            // chain right batch data row by row
            for row_index in 0..left_batch.num_rows() {
                let array = left_batch
                    .columns()
                    .iter()
                    .map(|array| {
                        // we need repeat array data n times with row_index value
                        repeat_array(&array, row_index, rb.num_rows())
                    })
                    .collect::<Result<Vec<_>>>()
                    .and_then(|array| {
                        RecordBatch::try_new(
                            self.schema(),
                            array.into_iter().chain(rb.columns().iter().cloned()).collect(),
                        )
                        .map_err(|e| Error::ArrowError(e))
                    });

                batches.push(array);
            }
        }

        batches
    }
}

impl PhysicalPlan for CrossJoin {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let left_batch = self.left.execute()?;
        let right_batch = self.right.execute()?;

        left_batch
            .into_iter()
            .flat_map(|left_batch| self.build_batch(&left_batch, &right_batch))
            .collect()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}

macro_rules! build_primary_array {
    ($ARRY_TYPE:ident, $ary:expr, $index:expr, $size:expr) => {{
        use std::any::type_name;

        let pary = $ary
            .as_any()
            .downcast_ref::<arrow::array::$ARRY_TYPE>()
            .ok_or(Error::InternalError(format!(
                "could not cast value to {}",
                type_name::<$ARRY_TYPE>()
            )))?;
        if pary.is_null($index) {
            return Ok(new_null_array(pary.data_type(), $size));
        }
        let val = pary.value($index);
        Ok(Arc::new($ARRY_TYPE::from(vec![val; $size])))
    }};
}

fn repeat_array(ary: &ArrayRef, index: usize, size: usize) -> Result<ArrayRef> {
    match ary.data_type() {
        arrow::datatypes::DataType::Null => Ok(new_null_array(ary.data_type(), size)),
        arrow::datatypes::DataType::Boolean => build_primary_array!(BooleanArray, ary, index, size),
        arrow::datatypes::DataType::Int8 => build_primary_array!(Int8Array, ary, index, size),
        arrow::datatypes::DataType::Int16 => build_primary_array!(Int16Array, ary, index, size),
        arrow::datatypes::DataType::Int32 => build_primary_array!(Int32Array, ary, index, size),
        arrow::datatypes::DataType::Int64 => build_primary_array!(Int64Array, ary, index, size),
        arrow::datatypes::DataType::UInt8 => build_primary_array!(UInt8Array, ary, index, size),
        arrow::datatypes::DataType::UInt16 => build_primary_array!(UInt16Array, ary, index, size),
        arrow::datatypes::DataType::UInt32 => build_primary_array!(UInt32Array, ary, index, size),
        arrow::datatypes::DataType::UInt64 => build_primary_array!(UInt64Array, ary, index, size),
        arrow::datatypes::DataType::Float16 => build_primary_array!(Float16Array, ary, index, size),
        arrow::datatypes::DataType::Float32 => build_primary_array!(Float32Array, ary, index, size),
        arrow::datatypes::DataType::Float64 => build_primary_array!(Float64Array, ary, index, size),
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::physical::plan::tests::build_table_scan_i32;

    use super::*;
    use arrow::util;

    #[test]
    fn test_cross_join() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 6]),
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![("a2", vec![10, 11]), ("b2", vec![12, 13]), ("c2", vec![14, 15])]);

        let join = CrossJoin::new(left, right);
        let result = join.execute().unwrap();

        assert_eq!(result.len(), 3);

        let str = util::pretty::pretty_format_batches(&result).unwrap().to_string();
        let actual = str.split('\n').collect::<Vec<&str>>();

        assert_eq!(
            actual,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 12 | 14 |",
                "| 1  | 4  | 7  | 11 | 13 | 15 |",
                "| 2  | 5  | 8  | 10 | 12 | 14 |",
                "| 2  | 5  | 8  | 11 | 13 | 15 |",
                "| 3  | 6  | 9  | 10 | 12 | 14 |",
                "| 3  | 6  | 9  | 11 | 13 | 15 |",
                "+----+----+----+----+----+----+",
            ]
        );
    }
}
