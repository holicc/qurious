use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};

pub fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    RecordBatch::try_from_iter_with_nullable(vec![("row", array, false)]).unwrap()
}
