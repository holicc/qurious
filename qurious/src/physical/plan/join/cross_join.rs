use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};

use crate::{
    common::table_schema::FIELD_QUALIFIERS_META_KEY,
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
        let left_schema = left.schema();
        let right_schema = right.schema();

        let fields = left_schema
            .fields()
            .iter()
            .chain(right_schema.fields().iter())
            .cloned()
            .collect::<Vec<_>>();

        // Preserve per-field qualifiers (stored in Schema metadata) across schema concatenation.
        // This is required so physical column lookup can disambiguate same-named columns coming
        // from different relations (e.g. `nation n1` and `nation n2`).
        let mut metadata = left_schema.metadata().clone();

        let sep = '\u{1f}';
        let fallback = |len: usize| vec![""; len];

        let left_q = left_schema
            .metadata()
            .get(FIELD_QUALIFIERS_META_KEY)
            .cloned()
            .unwrap_or_else(|| sep.to_string().repeat(left_schema.fields().len().saturating_sub(1)));
        let mut left_parts = left_q.split(sep).collect::<Vec<_>>();
        if left_parts.len() != left_schema.fields().len() {
            left_parts = fallback(left_schema.fields().len());
        }

        let right_q = right_schema
            .metadata()
            .get(FIELD_QUALIFIERS_META_KEY)
            .cloned()
            .unwrap_or_else(|| sep.to_string().repeat(right_schema.fields().len().saturating_sub(1)));
        let mut right_parts = right_q.split(sep).collect::<Vec<_>>();
        if right_parts.len() != right_schema.fields().len() {
            right_parts = fallback(right_schema.fields().len());
        }

        let combined = left_parts
            .into_iter()
            .chain(right_parts.into_iter())
            .collect::<Vec<_>>()
            .join(&sep.to_string());
        metadata.insert(FIELD_QUALIFIERS_META_KEY.to_string(), combined);

        let schema = Schema::new_with_metadata(fields, metadata);
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
                        .map_err(|e| {
                            Error::ArrowError(
                                e,
                                Some(format!(
                                    "physical::plan::cross_join.rs: CrossJoin::build_batch: RecordBatch::try_new error"
                                )),
                            )
                        })
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
