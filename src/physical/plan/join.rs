use super::PhysicalPlan;
use crate::{
    common::JoinType,
    error::{Error, Result},
    physical::expr::PhysicalExpr,
};
use arrow::{
    array::{new_null_array, Int32Array, RecordBatch, RecordBatchOptions},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use std::sync::Arc;

pub struct Join {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub join_type: JoinType,
    pub schema: SchemaRef,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
}

impl Join {
    pub fn try_new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        join_type: JoinType,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        todo!()
    }
}

impl PhysicalPlan for Join {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let join_schema = join_schema(&left_schema, &right_schema, &self.join_type);

        todo!()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}

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
            .map(|f| new_null_array(f.data_type(), size))
            .collect::<Vec<_>>(),
    )
    .unwrap();

    concat_batches(&schema, [r, &null_batch]).map_err(|e| Error::ArrowError(e))
}

fn join_schema(left: &Schema, right: &Schema, join_type: &JoinType) -> Schema {
    let (left_nullable, right_nullable) = match join_type {
        JoinType::Left => (false, true),
        JoinType::Right => (true, false),
        JoinType::Inner => (false, false),
        JoinType::Full => (true, true),
    };

    let with_nullable = |nullable| -> Box<dyn FnMut(&Arc<Field>) -> Field> {
        if nullable {
            Box::new(|f| f.as_ref().clone().with_nullable(true))
        } else {
            Box::new(|f| f.as_ref().clone())
        }
    };

    let fields = left
        .fields()
        .iter()
        .map(with_nullable(left_nullable))
        .chain(right.fields().iter().map(with_nullable(right_nullable)))
        .collect::<Vec<_>>();

    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::physical::plan::tests::build_table_scan_i32;

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

        let right = build_table_scan_i32(vec![("a2", vec![10, 11, 2, 2]), ("b2", vec![12, 13, 2, 2])]);

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

        let str = util::pretty::pretty_format_batches(&result).unwrap().to_string();

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

    #[test]
    fn test_join_schema() {
        let left = Schema::new(vec![
            Field::new("a1", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c1", DataType::Int32, false),
        ]);
        let left_nulls = Schema::new(vec![
            Field::new("a1", DataType::Int32, true),
            Field::new("b1", DataType::Int32, true),
            Field::new("c1", DataType::Int32, true),
        ]);

        let right = Schema::new(vec![
            Field::new("a2", DataType::Int32, false),
            Field::new("b2", DataType::Int32, false),
        ]);
        let right_nulls = Schema::new(vec![
            Field::new("a2", DataType::Int32, true),
            Field::new("b2", DataType::Int32, true),
        ]);

        let cases = vec![
            // right input of a `LEFT` join can be null, regardless of input nullness
            (JoinType::Left, &left, &right, &left, &right_nulls),
            (JoinType::Left, &left, &right_nulls, &left, &right_nulls),
            // left input of a `RIGHT` join can be null, regardless of input nullness
            (JoinType::Right, &left, &right, &left_nulls, &right),
            (JoinType::Right, &left, &right_nulls, &left_nulls, &right_nulls),
            // `INNER` join does not change nullability
            (JoinType::Inner, &left, &right, &left, &right),
            (JoinType::Inner, &left, &right_nulls, &left, &right_nulls),
            (JoinType::Inner, &left_nulls, &right, &left_nulls, &right),
            (JoinType::Inner, &left_nulls, &right_nulls, &left_nulls, &right_nulls),
            // `FULL` join makes all fields nullable
            (JoinType::Full, &left, &right, &left_nulls, &right_nulls),
            (JoinType::Full, &left, &right_nulls, &left_nulls, &right_nulls),
            (JoinType::Full, &left_nulls, &right, &left_nulls, &right_nulls),
            (JoinType::Full, &left_nulls, &right_nulls, &left_nulls, &right_nulls),
        ];

        for (join_type, left, right, expected_left, expected_right) in cases {
            let result = join_schema(left, right, &join_type);

            let expected_fields = expected_left
                .fields()
                .iter()
                .cloned()
                .chain(expected_right.fields().iter().cloned())
                .collect::<Fields>();
            let expected_schema = Schema::new(expected_fields);

            assert_eq!(result, expected_schema);
        }
    }
}
