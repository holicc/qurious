use std::sync::Arc;

use crate::{error::Result, physical::plan::JoinSide};
use arrow::{
    array::{new_null_array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, UInt64Array},
    compute::{self},
    datatypes::SchemaRef,
    error::ArrowError,
};

use crate::physical::plan::ColumnIndex;

pub fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    RecordBatch::try_from_iter_with_nullable(vec![("row", array, false)]).unwrap()
}

pub fn build_batch_from_indices(
    schema: &SchemaRef,
    column_indices: &[ColumnIndex],
    build_batch: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: &UInt64Array,
    probe_indices: &UInt32Array,
    build_side: &JoinSide,
) -> Result<RecordBatch, ArrowError> {
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new()
                .with_match_field_names(true)
                .with_row_count(build_indices.len().into()),
        );
    }

    let mut columns = Vec::with_capacity(schema.fields().len());

    for (idx, side) in column_indices {
        let array = if side == build_side {
            let col = build_batch.column(*idx);

            if col.is_empty() || col.null_count() == col.len() {
                new_null_array(col.data_type(), build_indices.len())
            } else {
                compute::take(col, build_indices, None)?
            }
        } else {
            let col = probe_batch.column(*idx);
            if col.is_empty() || col.null_count() == col.len() {
                new_null_array(col.data_type(), probe_indices.len())
            } else {
                compute::take(&col, probe_indices, None)?
            }
        };

        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns)
}
