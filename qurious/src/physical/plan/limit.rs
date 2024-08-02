use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::physical::plan::PhysicalPlan;

use std::sync::Arc;
use std::usize;

pub struct Limit {
    pub input: Arc<dyn PhysicalPlan>,
    pub fetch: Option<usize>,
    pub skip: usize,
}

impl Limit {
    pub fn new(input: Arc<dyn PhysicalPlan>, fetch: Option<usize>, skip: usize) -> Self {
        Self { input, fetch, skip }
    }
}

impl PhysicalPlan for Limit {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let max_fetch = self.fetch.unwrap_or(usize::MAX);
        let batchs = self.input.execute()?;

        let mut results = vec![];
        let mut fetched = 0;
        let mut skip = self.skip;

        for batch in batchs {
            let rows = batch.num_rows();

            if rows <= skip {
                skip -= rows;
                continue;
            }

            let new_batch = batch.slice(skip, rows - skip);

            let new_rows = new_batch.num_rows();
            let remaining = max_fetch - fetched;

            if new_rows <= remaining {
                results.push(new_batch);
                fetched += new_rows;
            } else {
                results.push(new_batch.slice(0, remaining));
                break;
            }
        }

        Ok(results)
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        self.input.children()
    }
}

#[cfg(test)]
mod test {
    use crate::{build_table_scan, physical::plan::PhysicalPlan, test_utils::assert_batch_eq};

    use super::Limit;

    #[test]
    fn test_limit() {
        let input = build_table_scan!(
            ("a", Int32Type, DataType::Int32, vec![1, 2, 3, 4]),
            ("b", Float64Type, DataType::Float64, vec![1.0, 2.0, 3.0, 4.0]),
            ("c", UInt64Type, DataType::UInt64, vec![1, 2, 3, 4]),
        );

        let limit = Limit::new(input, Some(3), 1);

        let reuslts = limit.execute().unwrap();

        assert_batch_eq(
            &reuslts,
            vec![
                "+---+-----+---+",
                "| a | b   | c |",
                "+---+-----+---+",
                "| 2 | 2.0 | 2 |",
                "| 3 | 3.0 | 3 |",
                "| 4 | 4.0 | 4 |",
                "+---+-----+---+",
            ],
        )
    }
}
