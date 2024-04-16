mod cross_join;
// mod nested_loop_join;

use std::sync::Arc;

pub use cross_join::CrossJoin;

use arrow::{
    array::{new_null_array, RecordBatch},
    compute::concat_batches,
    datatypes::{Field, Schema},
};

use crate::common::JoinType;
use crate::error::{Error, Result};

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
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    use crate::{build_schema, common::JoinType, physical::plan::join::join_schema};

    #[test]
    fn test_join_schema() {
        let left = build_schema!(
            ("b1", DataType::Int32),
            ("a1", DataType::Int32),
            ("c1", DataType::Int32)
        );

        let left_nulls = build_schema!(
            ("a1", DataType::Int32, true),
            ("b1", DataType::Int32, true),
            ("c1", DataType::Int32, true)
        );

        let right = build_schema!(("a2", DataType::Int32), ("b2", DataType::Int32));

        let right_nulls = build_schema!(("a2", DataType::Int32, true), ("b2", DataType::Int32, true));

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
