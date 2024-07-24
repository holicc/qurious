use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::physical::plan::PhysicalPlan;

use std::sync::Arc;

pub struct Limit {
    pub input: Arc<dyn PhysicalPlan>,
    pub fetch: usize,
    pub offset: usize,
}

impl Limit {
    pub fn new(input: Arc<dyn PhysicalPlan>, fetch: usize, offset: Option<usize>) -> Self {
        Self {
            input,
            fetch,
            offset: offset.unwrap_or_default(),
        }
    }
}

impl PhysicalPlan for Limit {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        Ok(self
            .input
            .execute()?
            .into_iter()
            .map(|batch| batch.slice(self.offset, self.fetch))
            .collect())
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

        let limit = Limit::new(input, 3, Some(1));

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
