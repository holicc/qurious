mod cross_join;
mod hash_join;
mod nest_loop_join;

use arrow::datatypes::{Field, Schema, SchemaBuilder, SchemaRef};
use arrow::{array::*, compute};
use std::sync::Arc;

pub use cross_join::CrossJoin;
pub use hash_join::*;
pub use nest_loop_join::*;

use crate::error::Result;
use crate::utils::batch::build_batch_from_indices;
use crate::{common::join_type::JoinType, physical::expr::PhysicalExpr};

pub type ColumnIndex = (usize, JoinSide);
/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>;

pub(crate) fn need_produce_result_in_final(join_type: &JoinType) -> bool {
    return join_type == &JoinType::Left || join_type == &JoinType::Full || join_type == &JoinType::Right;
}

pub(crate) fn build_join_schema(left: &Schema, right: &Schema, join_type: &JoinType) -> (SchemaRef, Vec<ColumnIndex>) {
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

pub(crate) fn apply_join_filter_to_indices(
    build_side_batch: &RecordBatch,
    build_side_indices: UInt64Array,
    probe_side_batch: &RecordBatch,
    probe_side_indices: UInt32Array,
    filter: &JoinFilter,
    build_side: JoinSide,
) -> Result<(UInt64Array, UInt32Array)> {
    if build_side_indices.is_empty() && probe_side_indices.is_empty() {
        return Ok((build_side_indices, probe_side_indices));
    }

    let intermediate_batch = build_batch_from_indices(
        &filter.schema,
        &filter.column_indices,
        build_side_batch,
        probe_side_batch,
        &build_side_indices,
        &probe_side_indices,
        &build_side,
    )?;

    let filter_values = filter.expr.evaluate(&intermediate_batch)?;
    let mask = filter_values.as_boolean();

    let left_filtered = compute::filter(&build_side_indices, mask)?;
    let right_filtered = compute::filter(&probe_side_indices, mask)?;

    Ok((downcast_array(&left_filtered), downcast_array(&right_filtered)))
}

pub(crate) fn adjust_indices_by_join_type(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    right_rows: usize,
    join_type: &JoinType,
) -> Result<(UInt64Array, UInt32Array)> {
    match join_type {
        JoinType::Inner => Ok((left_indices, right_indices)),
        JoinType::Left => {
            // unmatched left row will be produced in the `process_unmatched_build_batch`
            Ok((left_indices, right_indices))
        }
        JoinType::Right | JoinType::Full => adjust_right_indices(left_indices, right_indices, right_rows),
    }
}

pub(crate) fn adjust_right_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    right_rows: usize,
) -> Result<(UInt64Array, UInt32Array)> {
    let mut build_indices = UInt64Builder::new();
    let mut probe_indices = UInt32Builder::new();

    let mut last_joined_right_idx = 0;

    for (left_idx, right_idx) in left_indices.values().iter().zip(right_indices.values().iter()) {
        for value in last_joined_right_idx..*right_idx {
            probe_indices.append_value(value);
            build_indices.append_null();
        }

        probe_indices.append_value(*right_idx);
        build_indices.append_value(*left_idx);

        last_joined_right_idx = *right_idx + 1;
    }

    for value in last_joined_right_idx..(right_rows) as u32 {
        probe_indices.append_value(value);
        build_indices.append_null();
    }

    Ok((
        downcast_array(&build_indices.finish()),
        downcast_array(&probe_indices.finish()),
    ))
}
