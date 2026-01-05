use crate::error::Error;
use crate::physical::plan::{adjust_indices_by_join_type, apply_join_filter_to_indices};
use crate::utils::array::create_hashes;
use crate::utils::batch::build_batch_from_indices;
use crate::{
    error::Result,
    internal_err,
    physical::{
        expr::PhysicalExpr,
        plan::{
            build_join_schema,
            join::{JoinFilter, JoinSide, JoinType},
            ColumnIndex, JoinOn, PhysicalPlan,
        },
    },
};
use arrow::array::{downcast_array, ArrayRef, BooleanBufferBuilder, UInt32Builder, UInt64Builder};
use arrow::compute::kernels::cmp::eq;
use arrow::compute::{and, FilterBuilder};
use arrow::{
    array::{RecordBatch, UInt32Array, UInt64Array},
    compute::{concat_batches, take},
    datatypes::SchemaRef,
};
use std::hash::DefaultHasher;
use std::{collections::HashMap, sync::Arc};

struct JoinLeftData {
    /// The hash table with indices into `batch`
    hashmap: JoinHashMap,
    /// The build side on expressions values
    values: Vec<ArrayRef>,
    /// The input rows for the build side
    batch: RecordBatch,
    /// The bitmap for visited left-side indices
    visited_indices_bitmap: BooleanBufferBuilder,
}

#[derive(Debug)]
struct JoinHashMap {
    map: HashMap<u64, u64>,
    next: Vec<u64>,
}

impl JoinHashMap {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            next: vec![0; capacity],
        }
    }

    fn update<'a>(&mut self, hash_values: impl Iterator<Item = (usize, &'a u64)>, delete_offset: usize) {
        for (row, hash) in hash_values {
            if let Some(index) = self.map.get_mut(hash) {
                let pre_index = *index;
                *index = (row + 1) as u64;

                self.next[row - delete_offset] = pre_index;
            } else {
                self.map.insert(*hash, (row + 1) as u64);
            }
        }
    }

    fn is_distinct(&self) -> bool {
        self.map.len() == self.next.len()
    }

    fn get_matches_indices(&self, hash_values: &[u64]) -> (Vec<u32>, Vec<u64>) {
        let mut input_indices = Vec::with_capacity(hash_values.len());
        let mut match_indices = Vec::with_capacity(hash_values.len());

        let iter = hash_values.iter().enumerate();

        if self.is_distinct() {
            for (row, hash) in iter {
                if let Some(matched_chain_index) = self.map.get(hash) {
                    input_indices.push(row as u32);
                    match_indices.push(matched_chain_index - 1 as u64);
                }
            }

            return (input_indices, match_indices);
        }

        for (row, hash) in iter {
            if let Some(matched_chain_index) = self.map.get(hash) {
                let mut matched_row_index = matched_chain_index - 1;

                loop {
                    input_indices.push(row as u32);
                    match_indices.push(matched_row_index);

                    let next = self.next[matched_row_index as usize];

                    if next == 0 {
                        break;
                    }

                    matched_row_index = next - 1;
                }
            }
        }

        (input_indices, match_indices)
    }
}

pub struct HashJoinExec {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub join_type: JoinType,
    pub on: JoinOn,
    pub filter: Option<JoinFilter>,

    schema: SchemaRef,
    column_indices: Vec<ColumnIndex>,
}

impl HashJoinExec {
    pub fn try_new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        join_type: JoinType,
        on: JoinOn,
        filter: Option<JoinFilter>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return internal_err!("On constraints in HashJoinExec should be non-empty");
        }

        let (schema, column_indices) = build_join_schema(&left_schema, &right_schema, &join_type);

        Ok(Self {
            left,
            right,
            join_type,
            on,
            filter,
            schema,
            column_indices,
        })
    }

    fn build_hash_table(
        &self,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        on: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<JoinLeftData> {
        let batch = concat_batches(schema, batches)?;
        let keys_values = on
            .iter()
            .map(|expr| expr.evaluate(&batch))
            .collect::<Result<Vec<_>>>()?;

        let mut hashmap = JoinHashMap::with_capacity(batch.num_rows());
        let mut hash_buffer = vec![DefaultHasher::default(); batch.num_rows()];
        let hash_values = create_hashes(&keys_values, &mut hash_buffer)?;

        hashmap.update(hash_values.iter().enumerate().rev(), 0);

        let mut visited_indices_bitmap = BooleanBufferBuilder::new(batch.num_rows());
        visited_indices_bitmap.append_n(batch.num_rows(), false);

        Ok(JoinLeftData {
            visited_indices_bitmap,
            hashmap,
            batch,
            values: keys_values,
        })
    }

    fn probe_hash_table(
        &self,
        left_data: &JoinLeftData,
        hash_values: &[u64],
        probe_side_values: &[ArrayRef],
    ) -> Result<(UInt64Array, UInt32Array)> {
        let (probe_indices, build_indices) = left_data.hashmap.get_matches_indices(hash_values);
        let (left_indices, right_indices): (UInt64Array, UInt32Array) = (build_indices.into(), probe_indices.into());

        let mut iter = left_data.values.iter().zip(probe_side_values.iter());
        let (first_left, first_right) = iter.next().ok_or(Error::InternalError(
            "At least one array should be provided for both left and right".to_string(),
        ))?;

        let (first_left_arr, first_right_arr) = (
            take(&first_left, &left_indices, None)?,
            take(&first_right, &right_indices, None)?,
        );

        let equal = iter
            .map(|(left, right)| {
                let left_arr = take(&left, &left_indices, None)?;
                let right_arr = take(&right, &right_indices, None)?;

                eq(&left_arr, &right_arr)
            })
            .try_fold(eq(&first_left_arr, &first_right_arr)?, |acc, new_equal| {
                and(&acc, &new_equal?)
            })?;

        let filter_builder = FilterBuilder::new(&equal).optimize().build();

        let left_filtered = filter_builder.filter(&left_indices)?;
        let right_filtered = filter_builder.filter(&right_indices)?;

        Ok((
            downcast_array(left_filtered.as_ref()),
            downcast_array(right_filtered.as_ref()),
        ))
    }

    fn process_probe_batch(
        &self,
        right_on: &[Arc<dyn PhysicalExpr>],
        right_batch: &RecordBatch,
        left_data: &mut JoinLeftData,
        hash_buffer: &mut Vec<DefaultHasher>,
    ) -> Result<RecordBatch> {
        let key_values = right_on
            .iter()
            .map(|expr| expr.evaluate(&right_batch))
            .collect::<Result<Vec<_>>>()?;

        let hash_values = create_hashes(&key_values, hash_buffer)?;
        let (left_indices, right_indices) = self.probe_hash_table(&left_data, &hash_values, &key_values)?;

        // apply filter
        let (left_indices, right_indices) = if let Some(filter) = &self.filter {
            apply_join_filter_to_indices(
                &left_data.batch,
                left_indices,
                &right_batch,
                right_indices,
                filter,
                JoinSide::Left,
            )?
        } else {
            (left_indices, right_indices)
        };

        let (left_indices, right_indices) =
            adjust_indices_by_join_type(left_indices, right_indices, right_batch.num_rows(), &self.join_type)?;

        // update visited indices bitmap
        // For LeftSemi/LeftAnti, we mark matched rows as visited so we can emit a distinct set of
        // left rows (EXISTS/NOT EXISTS semantics) during the final build-side processing.
        left_indices.iter().flatten().for_each(|idx| {
            left_data.visited_indices_bitmap.set_bit(idx as usize, true);
        });

        // Semi/Anti joins must not return one row per match: they return a subset of the left side
        // with DISTINCT left rows. We therefore emit nothing during probing and finalize using the
        // visited bitmap.
        if matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // build probe matched batch
        build_batch_from_indices(
            &self.schema,
            &self.column_indices,
            &left_data.batch,
            &right_batch,
            &left_indices,
            &right_indices,
            &JoinSide::Left,
        )
        .map_err(|e| Error::InternalError(format!("Failed to build probe matched batch: {}", e)))
    }

    fn process_unmatched_build_batch(&self, left_data: &JoinLeftData) -> Result<Option<RecordBatch>> {
        // LeftAnti should process unmatched rows (return unmatched left rows)
        // LeftSemi should skip unmatched rows (only matched rows are returned)
        if matches!(self.join_type, JoinType::Right | JoinType::Inner | JoinType::LeftSemi) {
            return Ok(None);
        }

        let mut left_indices = UInt64Builder::new();
        let mut right_indices = UInt32Builder::new();

        for idx in 0..left_data.batch.num_rows() {
            if !left_data.visited_indices_bitmap.get_bit(idx) {
                left_indices.append_value(idx as u64);
                right_indices.append_null();
            }
        }

        let (left_indices, right_indices): (UInt64Array, UInt32Array) = (
            downcast_array(&left_indices.finish()),
            downcast_array(&right_indices.finish()),
        );

        let empty_right_batch = RecordBatch::new_empty(self.right.schema());

        build_batch_from_indices(
            &self.schema,
            &self.column_indices,
            &left_data.batch,
            &empty_right_batch,
            &left_indices,
            &right_indices,
            &JoinSide::Left,
        )
        .map(Option::Some)
        .map_err(|e| Error::InternalError(format!("Failed to build unmatched build batch: {}", e)))
    }

    fn process_left_semi_build_batch(&self, left_data: &JoinLeftData) -> Result<RecordBatch> {
        let mut left_indices = UInt64Builder::new();
        let mut right_indices = UInt32Builder::new();

        for idx in 0..left_data.batch.num_rows() {
            if left_data.visited_indices_bitmap.get_bit(idx) {
                left_indices.append_value(idx as u64);
                right_indices.append_null();
            }
        }

        let (left_indices, right_indices): (UInt64Array, UInt32Array) = (
            downcast_array(&left_indices.finish()),
            downcast_array(&right_indices.finish()),
        );

        let empty_right_batch = RecordBatch::new_empty(self.right.schema());

        build_batch_from_indices(
            &self.schema,
            &self.column_indices,
            &left_data.batch,
            &empty_right_batch,
            &left_indices,
            &right_indices,
            &JoinSide::Left,
        )
        .map_err(|e| Error::InternalError(format!("Failed to build left semi batch: {}", e)))
    }
}

impl PhysicalPlan for HashJoinExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let left_batches = self.left.execute()?;
        let (left_on, right_on): (Vec<Arc<dyn PhysicalExpr>>, Vec<Arc<dyn PhysicalExpr>>) =
            self.on.iter().map(|on| (Arc::clone(&on.0), Arc::clone(&on.1))).unzip();
        let left_schema = self.left.schema();
        let mut left_data = self.build_hash_table(&left_schema, &left_batches, left_on)?;

        let mut result_batches = Vec::new();
        let mut hash_buffer = vec![];
        for right_batch in self.right.execute()? {
            hash_buffer.clear();
            hash_buffer.resize(right_batch.num_rows(), DefaultHasher::default());

            let probe_batch = self.process_probe_batch(&right_on, &right_batch, &mut left_data, &mut hash_buffer)?;
            // Filter out empty batches (e.g., LeftAnti returns empty batches for matched rows)
            if probe_batch.num_rows() > 0 {
                result_batches.push(probe_batch);
            }
        }

        if self.join_type == JoinType::LeftSemi {
            result_batches.push(self.process_left_semi_build_batch(&left_data)?);
            return Ok(result_batches);
        }

        if let Some(unmatched_build_batch) = self.process_unmatched_build_batch(&left_data)? {
            result_batches.push(unmatched_build_batch);
        }

        Ok(result_batches)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        physical::expr::Column,
        test_utils::{assert_batch_eq, build_table_scan_i32},
    };

    use super::*;

    #[test]
    fn test_hash_join_inner() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 6]),
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![
            ("a2", vec![10, 20, 30]),
            ("b2", vec![4, 5, 6]),
            ("c2", vec![70, 80, 90]),
        ]);

        // Execute inner join
        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Inner,
            vec![(Arc::new(Column::new("b1", 1)), Arc::new(Column::new("b2", 1)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 4  | 70 |",
                "| 2  | 5  | 8  | 20 | 5  | 80 |",
                "| 3  | 6  | 9  | 30 | 6  | 90 |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_inner_one_no_shared_column_names() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 5]),
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![
            ("a2", vec![10, 20, 30]),
            ("b2", vec![4, 5, 6]),
            ("c2", vec![70, 80, 90]),
        ]);

        // Execute inner join
        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Inner,
            vec![(Arc::new(Column::new("b1", 1)), Arc::new(Column::new("b2", 1)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 4  | 70 |",
                "| 2  | 5  | 8  | 20 | 5  | 80 |",
                "| 3  | 5  | 9  | 20 | 5  | 80 |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_inner_one_randomly_ordered() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![0, 3, 2, 1]),
            ("b1", vec![4, 5, 5, 4]),
            ("c1", vec![6, 9, 8, 7]),
        ]);

        let right = build_table_scan_i32(vec![
            ("a2", vec![20, 30, 10]),
            ("b2", vec![5, 6, 4]),
            ("c2", vec![80, 90, 70]),
        ]);

        // Execute inner join
        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Inner,
            vec![(Arc::new(Column::new("b1", 1)), Arc::new(Column::new("b2", 1)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 3  | 5  | 9  | 20 | 5  | 80 |",
                "| 2  | 5  | 8  | 20 | 5  | 80 |",
                "| 0  | 4  | 6  | 10 | 4  | 70 |",
                "| 1  | 4  | 7  | 10 | 4  | 70 |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_inner_two() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 2]),
            ("b2", vec![1, 2, 2]),
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b2", vec![1, 2, 2]),
            ("c2", vec![70, 80, 90]),
        ]);

        // Execute inner join
        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Inner,
            vec![
                (Arc::new(Column::new("a1", 0)), Arc::new(Column::new("a1", 0))),
                (Arc::new(Column::new("b2", 1)), Arc::new(Column::new("b2", 1))),
            ],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b2 | c1 | a1 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 1  | 7  | 1  | 1  | 70 |",
                "| 2  | 2  | 8  | 2  | 2  | 80 |",
                "| 2  | 2  | 9  | 2  | 2  | 80 |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_left_one() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![
            ("a2", vec![10, 20, 30]),
            ("b1", vec![4, 5, 6]),
            ("c2", vec![70, 80, 90]),
        ]);

        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Left,
            vec![(Arc::new(Column::new("b1", 1)), Arc::new(Column::new("b2", 1)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b1 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 4  | 70 |",
                "| 2  | 5  | 8  | 20 | 5  | 80 |",
                "| 3  | 7  | 9  |    |    |    |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_left_empty_right() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![("a2", vec![]), ("b2", vec![]), ("c2", vec![])]);

        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Left,
            vec![(Arc::new(Column::new("b1", 1)), Arc::new(Column::new("b2", 1)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  |    |    |    |",
                "| 2  | 5  | 8  |    |    |    |",
                "| 3  | 7  | 9  |    |    |    |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_full() {
        let left = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", vec![7, 8, 9]),
        ]);

        let right = build_table_scan_i32(vec![
            ("a2", vec![10, 20, 30]),
            ("b2", vec![4, 5, 6]),
            ("c2", vec![70, 80, 90]),
        ]);

        // Execute inner join
        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Full,
            vec![(Arc::new(Column::new("b1", 1)), Arc::new(Column::new("b2", 1)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+----+----+----+",
                "| a1 | b1 | c1 | a2 | b2 | c2 |",
                "+----+----+----+----+----+----+",
                "| 1  | 4  | 7  | 10 | 4  | 70 |",
                "| 2  | 5  | 8  | 20 | 5  | 80 |",
                "|    |    |    | 30 | 6  | 90 |",
                "| 3  | 7  | 9  |    |    |    |",
                "+----+----+----+----+----+----+",
            ],
        );
    }

    #[test]
    fn test_join_full_one() {
        let left = build_table_scan_i32(vec![("v1", vec![1, 2, 3]), ("v2", vec![1, 2, 3])]);

        let right = build_table_scan_i32(vec![("v3", vec![1, 3, 4]), ("v4", vec![100, 300, 400])]);

        // Execute inner join
        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::Full,
            vec![(Arc::new(Column::new("v1", 0)), Arc::new(Column::new("v3", 0)))],
            None,
        )
        .unwrap();
        let result = join.execute().unwrap();

        assert_batch_eq(
            &result,
            vec![
                "+----+----+----+-----+",
                "| v1 | v2 | v3 | v4  |",
                "+----+----+----+-----+",
                "| 1  | 1  | 1  | 100 |",
                "| 3  | 3  | 3  | 300 |",
                "|    |    | 4  | 400 |",
                "| 2  | 2  |    |     |",
                "+----+----+----+-----+",
            ],
        );
    }

    #[test]
    fn test_join_hash_map_update_distinct_values() {
        let mut hashmap = JoinHashMap::with_capacity(3);
        let hash_values = vec![100, 200, 300];

        hashmap.update(hash_values.iter().enumerate(), 0);

        // Verify that map stores correct values
        assert_eq!(hashmap.map.get(&100), Some(&1));
        assert_eq!(hashmap.map.get(&200), Some(&2));
        assert_eq!(hashmap.map.get(&300), Some(&3));

        // Verify that next array remains all zeros (no duplicates)
        assert_eq!(hashmap.next, vec![0, 0, 0]);

        // Verify it is distinct
        assert!(hashmap.is_distinct());
    }

    #[test]
    fn test_join_hash_map_update_duplicate_values() {
        let mut hashmap = JoinHashMap::with_capacity(4);
        let hash_values = vec![100, 200, 100, 300]; // 100 is duplicated

        hashmap.update(hash_values.iter().enumerate(), 0);

        // Verify that map stores the index of the last duplicate value
        assert_eq!(hashmap.map.get(&100), Some(&3)); // Last 100 has index 3
        assert_eq!(hashmap.map.get(&200), Some(&2));
        assert_eq!(hashmap.map.get(&300), Some(&4));

        // Verify that next array builds a linked list
        // next[0] = 0 (first 100, no next)
        // next[1] = 0 (200, no duplicate)
        // next[2] = 1 (second 100, points to first 100)
        // next[3] = 0 (300, no duplicate)
        assert_eq!(hashmap.next, vec![0, 0, 1, 0]);

        // Verify it is not distinct
        assert!(!hashmap.is_distinct());
    }

    #[test]
    fn test_join_hash_map_update_multiple_duplicates() {
        let mut hashmap = JoinHashMap::with_capacity(5);
        let hash_values = vec![100, 200, 100, 300, 100]; // 100 appears three times

        hashmap.update(hash_values.iter().enumerate(), 0);

        // Verify that map stores the index of the last duplicate value
        assert_eq!(hashmap.map.get(&100), Some(&5)); // Last 100 has index 5
        assert_eq!(hashmap.map.get(&200), Some(&2));
        assert_eq!(hashmap.map.get(&300), Some(&4));

        // Verify that next array builds a linked list
        // next[0] = 0 (first 100, no next)
        // next[1] = 0 (200, no duplicate)
        // next[2] = 1 (second 100, points to first 100)
        // next[3] = 0 (300, no duplicate)
        // next[4] = 3 (third 100, points to second 100)
        assert_eq!(hashmap.next, vec![0, 0, 1, 0, 3]);
    }

    #[test]
    fn test_join_hash_map_update_with_delete_offset() {
        let mut hashmap = JoinHashMap::with_capacity(3);
        let hash_values = vec![100, 200, 300];

        // Use delete_offset = 2
        hashmap.update(hash_values.iter().enumerate(), 2);

        // Verify map stored values
        assert_eq!(hashmap.map.get(&100), Some(&1));
        assert_eq!(hashmap.map.get(&200), Some(&2));
        assert_eq!(hashmap.map.get(&300), Some(&3));

        // Verify next array indices are adjusted
        // Since delete_offset = 2, next array indices need to subtract 2
        assert_eq!(hashmap.next, vec![0, 0, 0]);
    }

    #[test]
    fn test_get_matches_indices_distinct_values() {
        let mut hashmap = JoinHashMap::with_capacity(3);
        let hash_values = vec![100, 200, 300];
        hashmap.update(hash_values.iter().enumerate(), 0);

        // Test matching all values
        let probe_hashes = vec![100, 200, 300];
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        assert_eq!(input_indices, vec![0, 1, 2]); // Corresponding input row indices
        assert_eq!(match_indices, vec![0, 1, 2]); // Corresponding match row indices

        // Test partial matching
        let probe_hashes = vec![100, 400, 300]; // 400 doesn't exist
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        assert_eq!(input_indices, vec![0, 2]); // Only match 100 and 300
        assert_eq!(match_indices, vec![0, 2]); // Corresponding match row indices
    }

    #[test]
    fn test_get_matches_indices_duplicate_values() {
        let mut hashmap = JoinHashMap::with_capacity(4);
        let hash_values = vec![100, 200, 100, 300]; // 100 is duplicated
        hashmap.update(hash_values.iter().enumerate(), 0);

        // Test matching duplicate values
        let probe_hashes = vec![100, 200, 100];
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        // For first 100, should match all rows with 100
        // For 200, should match row with 200
        // For second 100, should match all rows with 100 again
        assert_eq!(match_indices, vec![2, 0, 1, 2, 0]); // All matching input row indices
        assert_eq!(input_indices, vec![0, 0, 1, 2, 2]); // Corresponding match row indices
    }

    #[test]
    fn test_get_matches_indices_multiple_duplicates() {
        let mut hashmap = JoinHashMap::with_capacity(5);
        let hash_values = vec![100, 200, 100, 300, 100]; // 100 appears three times
        hashmap.update(hash_values.iter().enumerate(), 0);

        // Test matching multiple duplicate values
        let probe_hashes = vec![100, 100];
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        // Each 100 should match all three rows with 100
        assert_eq!(match_indices, vec![4, 2, 0, 4, 2, 0]); // All matching input row indices
        assert_eq!(input_indices, vec![0, 0, 0, 1, 1, 1]); // Corresponding match row indices
    }

    #[test]
    fn test_get_matches_indices_no_matches() {
        let mut hashmap = JoinHashMap::with_capacity(2);
        let hash_values = vec![100, 200];
        hashmap.update(hash_values.iter().enumerate(), 0);

        // Test case with no matches
        let probe_hashes = vec![300, 400, 500];
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        assert_eq!(input_indices, vec![] as Vec<u32>); // No matches
        assert_eq!(match_indices, vec![] as Vec<u64>); // No matches
    }

    #[test]
    fn test_get_matches_indices_empty_probe() {
        let mut hashmap = JoinHashMap::with_capacity(2);
        let hash_values = vec![100, 200];
        hashmap.update(hash_values.iter().enumerate(), 0);

        // Test empty probe array
        let probe_hashes: Vec<u64> = vec![];
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        assert_eq!(input_indices, vec![] as Vec<u32>);
        assert_eq!(match_indices, vec![] as Vec<u64>);
    }

    #[test]
    fn test_get_matches_indices_empty_hashmap() {
        let hashmap = JoinHashMap::with_capacity(0);

        // Test empty hashmap
        let probe_hashes = vec![100, 200];
        let (input_indices, match_indices) = hashmap.get_matches_indices(&probe_hashes);

        assert_eq!(input_indices, vec![] as Vec<u32>);
        assert_eq!(match_indices, vec![] as Vec<u64>);
    }

    #[test]
    fn test_is_distinct() {
        let mut hashmap = JoinHashMap::with_capacity(3);

        let hash_values = vec![100, 200, 300];
        hashmap.update(hash_values.iter().enumerate(), 0);
        assert!(hashmap.is_distinct());

        let mut hashmap2 = JoinHashMap::with_capacity(3);
        let hash_values = vec![100, 200, 100];
        hashmap2.update(hash_values.iter().enumerate(), 0);
        assert!(!hashmap2.is_distinct());
    }

    #[test]
    fn test_hash_join_left_semi_distinct_left_rows() {
        let left = build_table_scan_i32(vec![("a1", vec![1, 2, 3]), ("k1", vec![10, 20, 30])]);
        // Two matching rows on the right for k1=10 should still return the left row only once.
        let right = build_table_scan_i32(vec![("k2", vec![10, 10, 999]), ("b2", vec![1, 2, 3])]);

        let join = HashJoinExec::try_new(
            left,
            right,
            JoinType::LeftSemi,
            vec![(Arc::new(Column::new("k1", 1)), Arc::new(Column::new("k2", 0)))],
            None,
        )
        .unwrap();

        let result = join.execute().unwrap();
        assert_batch_eq(
            &result,
            vec![
                "+----+----+",
                "| a1 | k1 |",
                "+----+----+",
                "| 1  | 10 |",
                "+----+----+",
            ],
        );
    }
}
