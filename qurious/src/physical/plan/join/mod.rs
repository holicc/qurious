mod cross_join;

pub use cross_join::CrossJoin;

use std::sync::Arc;

use arrow::{
    array::{
        downcast_array, new_empty_array, new_null_array, AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch,
        RecordBatchOptions, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    compute::{self, concat_batches},
    datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef},
};

use crate::{
    common::join_type::JoinType,
    error::{Error, Result},
    physical::expr::PhysicalExpr,
};

use super::PhysicalPlan;

pub type ColumnIndex = (usize, JoinSide);

#[derive(Debug)]
pub enum JoinSide {
    Left,
    Right,
}

#[derive(Debug)]
pub struct JoinFilter {
    pub expr: Arc<dyn PhysicalExpr>,
    pub schema: SchemaRef,
    /// Indices of origin table
    /// eg:
    ///  left table: a1[0] b1[1] c1[2]
    ///  right table: a2[0] b2[1]
    ///  join on: b1 = b2
    ///  then columns indices: [ (1,LEFT),(1,RIGHT) ]
    pub column_indices: Vec<ColumnIndex>,
}

pub struct Join {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub join_type: JoinType,
    pub filter: Option<JoinFilter>,
    schema: SchemaRef,
    // Schema Indices of left and right, placement of columns
    column_indices: Vec<ColumnIndex>,
}

impl Join {
    pub fn try_new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        join_type: JoinType,
        filter: Option<JoinFilter>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let (schema, column_indices) = join_schema(&left_schema, &right_schema, &join_type);

        Ok(Self {
            left,
            right,
            join_type,
            filter,
            schema,
            column_indices,
        })
    }
}

impl PhysicalPlan for Join {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let right_schema = self.right.schema();

        let left_batches = self.left.execute()?;
        let right_batche =
            concat_batches(&right_schema, self.right.execute()?.as_slice()).map_err(|e| Error::ArrowError(e))?;

        // build indices for left table and right table
        // create intermediate record batches for indices and filter
        // apply mask to left and right record batches and take columns
        // join columns to a record batch
        left_batches
            .into_iter()
            .map(|left_batch| {
                let (li, ri) = build_join_indices(&left_batch, &right_batche, &self.join_type, self.filter.as_ref())?;
                build_batch_from_indices(
                    self.schema.clone(),
                    &self.column_indices,
                    &left_batch,
                    &right_batche,
                    &li,
                    &ri,
                )
            })
            .collect()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}

/// Build join indices for left and right record batches
/// Step 1: Combine every left row with each right row, so that we will get  
///     left indices: [left_row_index, left_row_index, ...., left_row_index]
///     right indices: [0, 1, 2, 3, 4,....,right_row_count]
/// Step 2: Convert the result above from rows to columns, resulting in two arrays with the same length
///     left array: [ left_row_index, left_row_index, left_row_index, left_row_index+1 .. , left_row_index+n ]
///     right indices: [ 0, 1, .. right_row_count, 0, 1, .. right_row_count .. ]    
/// For example, if the left has 3 rows and the right has 2 rows, then the result will be
///    left indices:  [0, 0, 1, 1, 2, 2]
///    right indices: [0, 1, 0, 1, 0, 1]
fn build_join_indices(
    left: &RecordBatch,
    right: &RecordBatch,
    join_type: &JoinType,
    filter_join: Option<&JoinFilter>,
) -> Result<(UInt64Array, UInt32Array)> {
    let indices = (0..left.num_rows())
        .map(|row_index| {
            let li = UInt64Array::from_value(row_index as u64, right.num_rows());
            let ri = UInt32Array::from_iter_values(0..(right.num_rows() as u32));

            (li, ri)
        })
        .collect::<Vec<_>>();

    // apply filter to the indices
    let masks = if let Some(filter) = filter_join {
        indices
            .iter()
            .map(|(li, ri)| join_filter_indices(left, right, li, ri, filter))
            .collect::<Result<Vec<_>>>()?
    } else {
        (0..left.num_rows())
            .map(|_| BooleanArray::from(vec![true; right.num_rows()]))
            .collect()
    };

    // bitmap for full join
    let mut left_bitmap = build_bitmap(join_type, left.num_rows());
    let mut right_bitmap = build_bitmap(join_type, right.num_rows());

    let mut l = UInt64Builder::new();
    let mut r = UInt32Builder::new();
    for ((left_indices, right_indices), mask) in indices.into_iter().zip(masks.into_iter()) {
        let is_valid = &mask
            .iter()
            .map(|a| a.ok_or(Error::InternalError(format!("unable expand mask array {:?}", mask))))
            .collect::<Result<Vec<_>>>()?;

        match join_type {
            JoinType::Left => {
                l.append_values(left_indices.values(), &vec![true; left_indices.len()]);
                r.append_values(right_indices.values(), &is_valid);
            }
            JoinType::Right => {
                l.append_values(left_indices.values(), &is_valid);
                r.append_values(right_indices.values(), &vec![true; right_indices.len()]);
            }
            JoinType::Inner => {
                let left_indices = compute::filter(&left_indices, &mask)?;
                let left_indices: UInt64Array = downcast_array(&left_indices);

                let right_indices = compute::filter(&right_indices, &mask)?;
                let right_indices: UInt32Array = downcast_array(&right_indices);

                l.append_values(left_indices.values(), &vec![true; left_indices.len()]);
                r.append_values(right_indices.values(), &vec![true; right_indices.len()]);
            }
            JoinType::Full => {
                let left_indices = compute::filter(&left_indices, &mask)?;
                let left_indices: UInt64Array = downcast_array(&left_indices);

                let right_indices = compute::filter(&right_indices, &mask)?;
                let right_indices: UInt32Array = downcast_array(&right_indices);

                left_indices
                    .iter()
                    .flatten()
                    .for_each(|v| left_bitmap.set_bit(v as usize, false));
                right_indices
                    .iter()
                    .flatten()
                    .for_each(|v| right_bitmap.set_bit(v as usize, false));

                l.append_values(left_indices.values(), &vec![true; left_indices.len()]);
                r.append_values(right_indices.values(), &vec![true; right_indices.len()]);
            }
        }
    }

    // combain left and right null values
    if join_type == &JoinType::Full {
        let right_remain_indices = (0..right.num_rows())
            .filter_map(|idx| right_bitmap.get_bit(idx).then_some(idx as u32))
            .collect::<UInt32Array>();
        for remain_row in right_remain_indices.iter().flatten() {
            l.append_null();
            r.append_value(remain_row);
        }
        let left_remain_indices = (0..left.num_rows())
            .filter_map(|idx| left_bitmap.get_bit(idx).then_some(idx as u64))
            .collect::<UInt64Array>();
        for remain_row in left_remain_indices.iter().flatten() {
            l.append_value(remain_row);
            r.append_null();
        }
    }

    Ok((l.finish(), r.finish()))
}

fn join_filter_indices(
    lb: &RecordBatch,
    rb: &RecordBatch,
    li: &UInt64Array,
    ri: &UInt32Array,
    filter: &JoinFilter,
) -> Result<BooleanArray> {
    if li.is_empty() && ri.is_empty() {
        return Ok(new_empty_array(&DataType::Boolean).as_boolean().clone());
    }

    let intermediate_batch = build_batch_from_indices(
        filter.schema.clone(),
        &filter.column_indices.as_slice(),
        lb,
        rb,
        &li,
        &ri,
    )?;

    Ok(downcast_array(filter.expr.evaluate(&intermediate_batch)?.as_ref()))
}

fn build_batch_from_indices(
    schema: SchemaRef,
    columns_index: &[ColumnIndex],
    lb: &RecordBatch,
    rb: &RecordBatch,
    li: &UInt64Array,
    ri: &UInt32Array,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(li.len()));
        return Ok(RecordBatch::try_new_with_options(schema.clone(), vec![], &options)?);
    }

    let mut columns = Vec::with_capacity(schema.fields().len());

    for (index, join_side) in columns_index {
        match join_side {
            JoinSide::Left => {
                let array = lb.column(*index);
                if array.is_empty() {
                    columns.push(new_null_array(array.data_type(), li.len()));
                } else {
                    columns.push(compute::take(array, &li, None)?);
                }
            }
            JoinSide::Right => {
                let array = rb.column(*index);
                if array.is_empty() {
                    columns.push(new_null_array(array.data_type(), ri.len()));
                } else {
                    columns.push(compute::take(array, &ri, None)?);
                }
            }
        }
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn build_bitmap(join_type: &JoinType, num_rows: usize) -> BooleanBufferBuilder {
    match join_type {
        JoinType::Full => {
            let mut buffer = BooleanBufferBuilder::new(num_rows);
            buffer.append_n(num_rows, true);
            buffer
        }
        _ => BooleanBufferBuilder::new(0),
    }
}

pub fn join_schema(left: &Schema, right: &Schema, join_type: &JoinType) -> (SchemaRef, Vec<ColumnIndex>) {
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

    let left_fields = left
        .fields()
        .iter()
        .map(with_nullable(left_nullable))
        .enumerate()
        .map(|(index, f)| (f, (index, JoinSide::Left)));

    let right_fields = right
        .fields()
        .iter()
        .map(with_nullable(right_nullable))
        .enumerate()
        .map(|(index, f)| (f, (index, JoinSide::Right)));

    let (fields, column_indices): (SchemaBuilder, Vec<ColumnIndex>) = left_fields.chain(right_fields).unzip();

    (Arc::new(fields.finish()), column_indices)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        build_schema,
        common::join_type::JoinType,
        datatypes::operator::Operator,
        physical::{
            expr::{BinaryExpr, Column, Literal},
            plan::{
                join::{join_schema, Join},
                tests::build_table_scan_i32,
                PhysicalPlan,
            },
        },
        test_utils::assert_batch_eq,
    };
    use arrow::datatypes::{DataType, Fields, Schema};

    use super::{JoinFilter, JoinSide};

    /// +----+----+----+----+----+----+
    /// | a1 | b1 | c1 | a2 | b2 | c2 |
    /// +----+----+----+----+----+----+
    /// | 1  | 4  | 7  | 10 | 12 | 14 |
    /// | 1  | 4  | 7  | 11 | 13 | 15 |
    /// | 2  | 5  | 8  | 10 | 12 | 14 |
    /// | 2  | 5  | 8  | 11 | 13 | 15 |
    /// | 3  | 6  | 9  | 10 | 12 | 14 |
    /// | 3  | 6  | 9  | 11 | 13 | 15 |
    /// +----+----+----+----+----+----+
    fn build_table(join_type: JoinType, join_filter: Option<JoinFilter>) -> Join {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 6]),
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![("a2", vec![10, 11]), ("b2", vec![12, 13]), ("c2", vec![14, 15])]);
        Join::try_new(left, right, join_type, join_filter).unwrap()
    }

    // a1 != ? and b2 != ?
    fn build_filter(a1: i32, b2: i32) -> Option<JoinFilter> {
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a1", 0)),
                Operator::NotEq,
                Arc::new(Literal::new(a1.into())),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("b2", 1)),
                Operator::NotEq,
                Arc::new(Literal::new(b2.into())),
            )),
        ));

        let filter_schema = Arc::new(build_schema!(("a1", DataType::Int32), ("b2", DataType::Int32)));
        let column_indices = vec![(0, JoinSide::Left), (1, JoinSide::Right)];

        Some(JoinFilter {
            expr,
            schema: filter_schema,
            column_indices,
        })
    }

    #[test]
    fn test_inner_join() {
        let filter_join = build_filter(2, 10);
        let join = build_table(JoinType::Inner, filter_join);
        let r = join.execute().unwrap();

        assert_batch_eq(
            &r,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 12 | 14 |",
                "| 1  | 4  | 7  | 11 | 13 | 15 |",
                "| 3  | 6  | 9  | 10 | 12 | 14 |",
                "| 3  | 6  | 9  | 11 | 13 | 15 |",
                "+----+----+----+----+----+----+",
            ],
        )
    }

    #[test]
    fn test_left_join() {
        let filter_join = build_filter(2, 10);
        let join = build_table(JoinType::Left, filter_join);
        let r = join.execute().unwrap();

        assert_batch_eq(
            &r,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 12 | 14 |",
                "| 1  | 4  | 7  | 11 | 13 | 15 |",
                "| 2  | 5  | 8  |    |    |    |",
                "| 2  | 5  | 8  |    |    |    |",
                "| 3  | 6  | 9  | 10 | 12 | 14 |",
                "| 3  | 6  | 9  | 11 | 13 | 15 |",
                "+----+----+----+----+----+----+",
            ],
        )
    }

    #[test]
    fn test_right_join() {
        let filter_join = build_filter(2, 10);
        let join = build_table(JoinType::Right, filter_join);
        let r = join.execute().unwrap();

        assert_batch_eq(
            &r,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 12 | 14 |",
                "| 1  | 4  | 7  | 11 | 13 | 15 |",
                "|    |    |    | 10 | 12 | 14 |",
                "|    |    |    | 11 | 13 | 15 |",
                "| 3  | 6  | 9  | 10 | 12 | 14 |",
                "| 3  | 6  | 9  | 11 | 13 | 15 |",
                "+----+----+----+----+----+----+",
            ],
        )
    }

    #[test]
    fn test_full_join() {
        let filter_join = build_filter(2, 12);
        let join = build_table(JoinType::Full, filter_join);
        let r = join.execute().unwrap();

        assert_batch_eq(
            &r,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 11 | 13 | 15 |",
                "| 3  | 6  | 9  | 11 | 13 | 15 |",
                "|    |    |    | 10 | 12 | 14 |",
                "| 2  | 5  | 8  |    |    |    |",
                "+----+----+----+----+----+----+",
            ],
        )
    }

    #[test]
    fn test_join_schema() {
        let left = build_schema!(
            ("a1", DataType::Int32),
            ("b1", DataType::Int32),
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
            let (result, _) = join_schema(left, right, &join_type);
            let expected_fields = expected_left
                .fields()
                .iter()
                .cloned()
                .chain(expected_right.fields().iter().cloned())
                .collect::<Fields>();
            let expected_schema = Arc::new(Schema::new(expected_fields));

            assert_eq!(result, expected_schema);
        }
    }
}
