use super::PhysicalPlan;
use crate::error::{Error, Result};
use arrow::{
    array::{RecordBatch, RecordBatchOptions},
    compute::concat_batches,
    datatypes::{Schema, SchemaRef},
};
use std::sync::Arc;

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
}

impl PhysicalPlan for CrossJoin {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Execute the cross join
    /// A cross join is a cartesian product of the left and right input plans
    /// It is implemented by creating a new RecordBatch for each combination of left and right
    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.left
            .execute()?
            .into_iter()
            .zip(self.right.execute()?.into_iter())
            .map(|(l, r)| {
                RecordBatch::try_new_with_options(
                    self.schema(),
                    l.columns()
                        .into_iter()
                        .chain(r.columns().into_iter())
                        .cloned()
                        .collect::<Vec<_>>(),
                    &RecordBatchOptions::new().with_row_count(Some(l.num_rows())),
                )
                .map_err(|e| Error::ArrowError(e))
            })
            .collect()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}

#[cfg(test)]
mod tests {
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

        let right = build_table_scan_i32(vec![
            ("a2", vec![10, 11, 0]),
            ("b2", vec![12, 13, 0]),
            ("c2", vec![14, 15, 0]),
        ]);

        let join = CrossJoin::new(left, right);
        let result = join.execute().unwrap();

        println!("{}", util::pretty::pretty_format_batches(&result).unwrap());
    }
}
