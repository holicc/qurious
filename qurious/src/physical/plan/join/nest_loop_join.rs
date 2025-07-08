use std::sync::Arc;

use arrow::{
    array::{
        downcast_array, new_null_array, ArrayData, AsArray, BooleanBufferBuilder, RecordBatch, RecordBatchOptions,
        UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    compute::{self, concat_batches},
    datatypes::{DataType, SchemaRef},
};

use crate::{
    common::join_type::JoinType,
    error::Result,
    physical::{
        expr::PhysicalExpr,
        plan::{build_join_schema, ColumnIndex, PhysicalPlan},
    },
};

use super::need_produce_result_in_final;

#[derive(Debug, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

#[derive(Debug)]
pub struct JoinFilter {
    pub expr: Arc<dyn PhysicalExpr>,
    pub schema: SchemaRef,
    /// Indices of columns in the original tables
    /// Example:
    ///   Left table:  a1[0], b1[1], c1[2]
    ///   Right table: a2[0], b2[1]
    ///   Join condition: b1 = b2
    ///   Resulting column indices: [(1, LEFT), (1, RIGHT)]
    pub column_indices: Vec<ColumnIndex>,
}

pub struct NestedLoopJoinExec {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub join_type: JoinType,
    pub filter: Option<JoinFilter>,
    schema: SchemaRef,
    // Schema Indices of left and right, placement of columns
    column_indices: Vec<ColumnIndex>,
}

impl NestedLoopJoinExec {
    pub fn try_new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        join_type: JoinType,
        filter: Option<JoinFilter>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let (schema, column_indices) = build_join_schema(&left_schema, &right_schema, &join_type);

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

impl PhysicalPlan for NestedLoopJoinExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();

        let left_batch = concat_batches(&left_schema, self.left.execute()?.as_slice())?;
        let right_batch = concat_batches(&right_schema, self.right.execute()?.as_slice())?;

        if right_batch.num_rows() == 0 && self.join_type != JoinType::Left && self.join_type != JoinType::Full {
            return Ok(vec![]);
        }

        // build indices for left table and right table
        // create intermediate record batches for indices and filter
        // apply mask to left and right record batches and take columns
        // join columns to a record batch
        let (li, ri) = build_join_indices(&left_batch, &right_batch, self.filter.as_ref())?;
        let matched_batch = build_batch_from_indices(
            self.schema.clone(),
            &self.column_indices,
            &left_batch,
            &right_batch,
            &li,
            &ri,
        )?;

        if !need_produce_result_in_final(&self.join_type) {
            return Ok(vec![matched_batch]);
        }

        let mut visited_left_indices = build_bitmap(&self.join_type, left_batch.num_rows());
        let mut visited_right_indices = build_bitmap(&self.join_type, right_batch.num_rows());

        li.values().iter().for_each(|i| {
            if visited_left_indices.len() > 0 {
                visited_left_indices.set_bit(*i as usize, true)
            }
        });
        ri.values().iter().for_each(|i| {
            if visited_right_indices.len() > 0 {
                visited_right_indices.set_bit(*i as usize, true)
            }
        });

        let mut l = UInt64Builder::new();
        let mut r = UInt32Builder::new();

        if self.join_type == JoinType::Left || self.join_type == JoinType::Full {
            let left_indices = (0..visited_left_indices.len())
                .filter_map(|i| (!visited_left_indices.get_bit(i)).then_some(i as u64))
                .collect::<UInt64Array>();

            let mut right_indices = UInt32Builder::with_capacity(left_indices.len());
            right_indices.append_nulls(left_indices.len());
            let right_indices = right_indices.finish();

            l.extend(left_indices.iter());
            r.extend(right_indices.iter());
        }

        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
            let right_indices = (0..visited_right_indices.len())
                .filter_map(|i| (!visited_right_indices.get_bit(i)).then_some(i as u32))
                .collect::<UInt32Array>();

            let mut left_indices = UInt64Builder::with_capacity(right_indices.len());
            left_indices.append_nulls(right_indices.len());
            let left_indices = left_indices.finish();

            l.extend(left_indices.iter());
            r.extend(right_indices.iter());
        }

        let unmatched_batch = build_batch_from_indices(
            self.schema.clone(),
            &self.column_indices,
            &left_batch,
            &right_batch,
            &l.finish(),
            &r.finish(),
        )?;

        Ok(vec![matched_batch, unmatched_batch])
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}

fn build_join_indices(
    left: &RecordBatch,
    right: &RecordBatch,
    filter_join: Option<&JoinFilter>,
) -> Result<(UInt64Array, UInt32Array)> {
    if right.num_rows() == 0 {
        return Ok((
            UInt64Array::from_iter_values(0..(left.num_rows() as u64)),
            UInt32Array::from(ArrayData::new_null(&DataType::UInt32, left.num_rows())),
        ));
    }

    let indices = (0..right.num_rows())
        .map(|row_index| {
            let li = UInt64Array::from_iter_values(0..(left.num_rows() as u64));
            let ri = UInt32Array::from(vec![row_index as u32; left.num_rows()]);
            if let Some(filter) = filter_join {
                join_filter_indices(left, right, li, ri, filter)
            } else {
                Ok((li, ri))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut l = UInt64Builder::new();
    let mut r = UInt32Builder::new();
    for (left, right) in indices {
        l.extend(left.iter());
        r.extend(right.iter());
    }

    Ok((l.finish(), r.finish()))
}

fn join_filter_indices(
    lb: &RecordBatch,
    rb: &RecordBatch,
    li: UInt64Array,
    ri: UInt32Array,
    filter: &JoinFilter,
) -> Result<(UInt64Array, UInt32Array)> {
    if li.is_empty() && ri.is_empty() {
        return Ok((li, ri));
    }

    let intermediate_batch = build_batch_from_indices(
        filter.schema.clone(),
        &filter.column_indices.as_slice(),
        lb,
        rb,
        &li,
        &ri,
    )?;

    let filter_results = filter.expr.evaluate(&intermediate_batch)?;
    let mast = filter_results.as_boolean();
    let li = compute::filter(&li, &mast)?;
    let ri = compute::filter(&ri, &mast)?;

    Ok((downcast_array(&li), downcast_array(&ri)))
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
    if need_produce_result_in_final(join_type) {
        let mut buffer = BooleanBufferBuilder::new(num_rows);
        buffer.append_n(num_rows, false);
        return buffer;
    }

    BooleanBufferBuilder::new(0)
}
