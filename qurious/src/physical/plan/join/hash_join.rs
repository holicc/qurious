use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{RecordBatch, UInt32Array, UInt64Array},
    compute::{concat_batches, take},
    datatypes::{Schema, SchemaRef},
};

use crate::{
    error::Result,
    physical::{
        expr::PhysicalExpr,
        plan::{
            join::{JoinFilter, JoinSide, JoinType},
            ColumnIndex, PhysicalPlan,
        },
    },
};

pub struct HashJoinExec {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub join_type: JoinType,
    pub on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    pub filter: Option<JoinFilter>,
    schema: SchemaRef,
    // Schema Indices of left and right, placement of columns
    column_indices: Vec<ColumnIndex>,
}

impl HashJoinExec {
    pub fn try_new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        join_type: JoinType,
        on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        filter: Option<JoinFilter>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        // Build the output schema by combining left and right schemas
        let mut fields = Vec::new();
        let mut column_indices = Vec::new();

        // Add columns from left schema
        for (i, field) in left_schema.fields().iter().enumerate() {
            fields.push(field.clone());
            column_indices.push((i, JoinSide::Left));
        }

        // Add columns from right schema
        for (i, field) in right_schema.fields().iter().enumerate() {
            fields.push(field.clone());
            column_indices.push((i, JoinSide::Right));
        }

        Ok(Self {
            left,
            right,
            join_type,
            on,
            filter,
            schema: Arc::new(Schema::new(fields)),
            column_indices,
        })
    }

    fn build_hash_table(&self, right_batch: &RecordBatch) -> Result<HashMap<Vec<u8>, Vec<u32>>> {
        let mut hash_table = HashMap::new();

        // Get the join columns from the right side
        let join_columns = self
            .on
            .iter()
            .map(|(_, right_expr)| right_expr.evaluate(right_batch))
            .collect::<Result<Vec<_>>>()?;

        // Build hash table
        for row_idx in 0..right_batch.num_rows() {
            let mut key = Vec::new();
            for col in &join_columns {
                let value = col.slice(row_idx, 1);
                // Use array's debug format for hashing
                key.extend_from_slice(format!("{:?}", value).as_bytes());
            }

            hash_table.entry(key).or_insert_with(Vec::new).push(row_idx as u32);
        }

        Ok(hash_table)
    }

    fn probe_hash_table(
        &self,
        left_batch: &RecordBatch,
        hash_table: &HashMap<Vec<u8>, Vec<u32>>,
    ) -> Result<(UInt64Array, UInt32Array)> {
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        // Get the join columns from the left side
        let join_columns = self
            .on
            .iter()
            .map(|(left_expr, _)| left_expr.evaluate(left_batch))
            .collect::<Result<Vec<_>>>()?;

        // Probe hash table
        for row_idx in 0..left_batch.num_rows() {
            let mut key = Vec::new();
            for col in &join_columns {
                let value = col.slice(row_idx, 1);
                // Use array's debug format for hashing
                key.extend_from_slice(format!("{:?}", value).as_bytes());
            }

            if let Some(right_matches) = hash_table.get(&key) {
                for &right_idx in right_matches {
                    left_indices.push(row_idx as u64);
                    right_indices.push(right_idx);
                }
            } else if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                left_indices.push(row_idx as u64);
                right_indices.push(u32::MAX); // NULL index for right side
            }
        }

        Ok((UInt64Array::from(left_indices), UInt32Array::from(right_indices)))
    }

    fn build_output_batch(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        left_indices: &UInt64Array,
        right_indices: &UInt32Array,
    ) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(self.schema.fields().len());

        for (idx, side) in &self.column_indices {
            let array = match side {
                JoinSide::Left => {
                    let col = left_batch.column(*idx);
                    take(col, left_indices, None)?
                }
                JoinSide::Right => {
                    if right_indices.is_empty() {
                        let col = right_batch.column(*idx);
                        let data_type = col.data_type();
                        let null_array = arrow::array::new_null_array(data_type, 0);
                        null_array
                    } else {
                        let col = right_batch.column(*idx);
                        take(col, right_indices, None)?
                    }
                }
            };
            columns.push(array);
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
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
        // Execute child plans
        let left_batches = self.left.execute()?;
        let right_batches = self.right.execute()?;

        // Concatenate batches
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let left_batch = concat_batches(&left_schema, &left_batches)?;
        let right_batch = concat_batches(&right_schema, &right_batches)?;

        // Build hash table from right side
        let hash_table = self.build_hash_table(&right_batch)?;

        // Probe hash table with left side
        let (left_indices, right_indices) = self.probe_hash_table(&left_batch, &hash_table)?;

        // Build output batch
        let matched_batch = self.build_output_batch(&left_batch, &right_batch, &left_indices, &right_indices)?;

        // Handle different join types
        match self.join_type {
            JoinType::Inner => Ok(vec![matched_batch]),
            JoinType::Left | JoinType::Full => {
                // Add unmatched rows from left side
                let mut unmatched_left_indices = Vec::new();
                let mut unmatched_right_indices = Vec::new();

                for i in 0..left_batch.num_rows() {
                    if !left_indices.values().contains(&(i as u64)) {
                        unmatched_left_indices.push(i as u64);
                        unmatched_right_indices.push(u32::MAX);
                    }
                }

                if !unmatched_left_indices.is_empty() {
                    let unmatched_batch = self.build_output_batch(
                        &left_batch,
                        &right_batch,
                        &UInt64Array::from(unmatched_left_indices),
                        &UInt32Array::from(unmatched_right_indices),
                    )?;
                    Ok(vec![matched_batch, unmatched_batch])
                } else {
                    Ok(vec![matched_batch])
                }
            }
            JoinType::Right => {
                // Add unmatched rows from right side
                let mut unmatched_left_indices = Vec::new();
                let mut unmatched_right_indices = Vec::new();

                for i in 0..right_batch.num_rows() {
                    if !right_indices.values().contains(&(i as u32)) {
                        unmatched_left_indices.push(u64::MAX);
                        unmatched_right_indices.push(i as u32);
                    }
                }

                if !unmatched_right_indices.is_empty() {
                    let unmatched_batch = self.build_output_batch(
                        &left_batch,
                        &right_batch,
                        &UInt64Array::from(unmatched_left_indices),
                        &UInt32Array::from(unmatched_right_indices),
                    )?;
                    Ok(vec![matched_batch, unmatched_batch])
                } else {
                    Ok(vec![matched_batch])
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::plan::empty::EmptyRelation;
    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field},
    };

    #[test]
    fn test_hash_join_schema() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ]));

        let left = Arc::new(EmptyRelation::new(left_schema.clone(), false));
        let right = Arc::new(EmptyRelation::new(right_schema.clone(), false));

        let join = HashJoinExec::try_new(left, right, JoinType::Inner, vec![], None).unwrap();
        let schema = join.schema();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "a");
        assert_eq!(schema.field(1).name(), "b");
        assert_eq!(schema.field(2).name(), "c");
        assert_eq!(schema.field(3).name(), "d");
    }

    #[test]
    fn test_hash_join_children() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));

        let left = Arc::new(EmptyRelation::new(left_schema.clone(), false));
        let right = Arc::new(EmptyRelation::new(right_schema.clone(), false));

        let join = HashJoinExec::try_new(left.clone(), right.clone(), JoinType::Inner, vec![], None).unwrap();
        let children = join.children().unwrap();

        assert_eq!(children.len(), 2);
        assert_eq!(children[0].schema().fields().len(), 1);
        assert_eq!(children[1].schema().fields().len(), 1);
    }

    #[test]
    fn test_hash_join_inner() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ]));

        // Create test data
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 4])),
                Arc::new(Int32Array::from(vec![200, 300, 400])),
            ],
        )
        .unwrap();

        // Create mock physical plans that return our test data
        struct MockPlan {
            schema: SchemaRef,
            batch: RecordBatch,
        }

        impl PhysicalPlan for MockPlan {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }

            fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
                None
            }

            fn execute(&self) -> Result<Vec<RecordBatch>> {
                Ok(vec![self.batch.clone()])
            }
        }

        let left = Arc::new(MockPlan {
            schema: left_schema,
            batch: left_batch,
        });

        let right = Arc::new(MockPlan {
            schema: right_schema,
            batch: right_batch,
        });

        // Execute inner join
        let join = HashJoinExec::try_new(left, right, JoinType::Inner, vec![], None).unwrap();
        let result = join.execute().unwrap();

        // Verify results
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);

        // Check first row (a=2, b=20, c=2, d=200)
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            2
        );
        assert_eq!(
            batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            20
        );
        assert_eq!(
            batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            2
        );
        assert_eq!(
            batch.column(3).as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            200
        );

        // Check second row (a=3, b=30, c=3, d=300)
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            3
        );
        assert_eq!(
            batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            30
        );
        assert_eq!(
            batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            3
        );
        assert_eq!(
            batch.column(3).as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            300
        );
    }

    #[test]
    fn test_hash_join_left() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ]));

        // Create test data
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 4])),
                Arc::new(Int32Array::from(vec![200, 300, 400])),
            ],
        )
        .unwrap();

        // Create mock physical plans that return our test data
        struct MockPlan {
            schema: SchemaRef,
            batch: RecordBatch,
        }

        impl PhysicalPlan for MockPlan {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }

            fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
                None
            }

            fn execute(&self) -> Result<Vec<RecordBatch>> {
                Ok(vec![self.batch.clone()])
            }
        }

        let left = Arc::new(MockPlan {
            schema: left_schema,
            batch: left_batch,
        });

        let right = Arc::new(MockPlan {
            schema: right_schema,
            batch: right_batch,
        });

        // Execute left join
        let join = HashJoinExec::try_new(left, right, JoinType::Left, vec![], None).unwrap();
        let result = join.execute().unwrap();

        // Verify results
        assert_eq!(result.len(), 2);

        // Check matched rows
        let matched_batch = &result[0];
        assert_eq!(matched_batch.num_rows(), 2);

        // Check first row (a=2, b=20, c=2, d=200)
        assert_eq!(
            matched_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            matched_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            20
        );
        assert_eq!(
            matched_batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            matched_batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            200
        );

        // Check second row (a=3, b=30, c=3, d=300)
        assert_eq!(
            matched_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            3
        );
        assert_eq!(
            matched_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            30
        );
        assert_eq!(
            matched_batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            3
        );
        assert_eq!(
            matched_batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            300
        );

        // Check unmatched row
        let unmatched_batch = &result[1];
        assert_eq!(unmatched_batch.num_rows(), 1);

        // Check unmatched row (a=1, b=10, c=null, d=null)
        assert_eq!(
            unmatched_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            unmatched_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
        assert!(unmatched_batch.column(2).is_null(0));
        assert!(unmatched_batch.column(3).is_null(0));
    }
}
