use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};

use crate::{
    error::{Error, Result},
    physical::plan::PhysicalPlan,
    utils::array::repeat_array,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{assert_batch_eq, build_table_scan_i32};
    use std::vec;

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

        assert_batch_eq(
            &result,
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
            ],
        );
    }
}
