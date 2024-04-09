use super::PhysicalPlan;
use crate::error::{Error, Result};
use arrow::{
    array::{Int32Array, RecordBatch, RecordBatchOptions},
    compute::concat_batches,
    datatypes::{DataType, Schema, SchemaRef},
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
            .map(|(mut l, mut r)| {
                if l.num_rows() < r.num_rows() {
                    l = append_null_to_record_batch(&l, r.num_rows() - l.num_rows())?;
                }
                if l.num_rows() > r.num_rows() {
                    r = append_null_to_record_batch(&r, l.num_rows() - r.num_rows())?;
                }

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

fn append_null_to_record_batch(r: &RecordBatch, size: usize) -> Result<RecordBatch> {
    let schema = r.schema();
    let null_batch = RecordBatch::try_new(
        schema.clone(),
        schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int32 => {
                    Arc::new(Int32Array::new_null(size)) as Arc<dyn arrow::array::Array>
                }
                _ => todo!(),
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    concat_batches(&schema, [r, &null_batch]).map_err(|e| Error::ArrowError(e))
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::physical::plan::tests::{build_table_scan_i32};

    use super::*;
    use arrow::{
        datatypes::{Field, Fields},
        util,
    };

    #[test]
    fn test_cross_join() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 6]),
            ("c1", vec![7, 8, 9]),
        ]);

        let right =
            build_table_scan_i32(vec![("a2", vec![10, 11, 2, 2]), ("b2", vec![12, 13, 2, 2])]);

        let join = CrossJoin::new(left, right);
        let result = join.execute().unwrap();
        let _schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("a1", DataType::Int32, true),
            Field::new("b1", DataType::Int32, true),
            Field::new("c1", DataType::Int32, true),
            Field::new("a2", DataType::Int32, true),
            Field::new("b2", DataType::Int32, true),
        ])));
        assert_eq!(result.len(), 1);

        let str = util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let actual = str.split('\n').collect::<Vec<&str>>();

        assert_eq!(
            actual,
            vec![
                "+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 |",
                "+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 12 |",
                "| 2  | 5  | 8  | 11 | 13 |",
                "| 3  | 6  | 9  | 2  | 2  |",
                "|    |    |    | 2  | 2  |",
                "+----+----+----+----+----+",
            ]
        );
    }
}
