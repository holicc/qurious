use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{self, lexsort_to_indices, SortColumn, SortOptions};
use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use crate::physical::plan::PhysicalPlan;

pub struct PhyscialSortExpr {
    expr: Arc<dyn PhysicalExpr>,
    options: SortOptions,
}

impl PhyscialSortExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, options: SortOptions) -> Self {
        Self { expr, options }
    }
}

pub struct Sort {
    exprs: Vec<PhyscialSortExpr>,
    input: Arc<dyn PhysicalPlan>,
}

impl Sort {
    pub fn new(exprs: Vec<PhyscialSortExpr>, input: Arc<dyn PhysicalPlan>) -> Self {
        Self { exprs, input }
    }
}

impl PhysicalPlan for Sort {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let merged_batch = compute::concat_batches(&self.input.schema(), &self.input.execute()?)?;
        let sort_columns = self
            .exprs
            .iter()
            .map(|expr| {
                expr.expr.evaluate(&merged_batch).map(|array| SortColumn {
                    values: array,
                    options: Some(expr.options),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let indices = lexsort_to_indices(&sort_columns, None)?;
        let columns = merged_batch
            .columns()
            .iter()
            .map(|c| compute::take(c.as_ref(), &indices, None))
            .collect::<Result<_, _>>()?;

        Ok(vec![RecordBatch::try_new(self.input.schema().clone(), columns)?])
    }

    fn children(&self) -> Option<Vec<std::sync::Arc<dyn PhysicalPlan>>> {
        self.input.children()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_table_scan;
    use crate::physical::expr::Column;
    use crate::test_utils::assert_batch_eq;

    #[test]
    fn test_sort() {
        let input = build_table_scan!(
            ("a", Int32Type, DataType::Int32, vec![1, 2, 3, 4]),
            ("b", Float64Type, DataType::Float64, vec![1.0, 2.0, 3.0, 4.0]),
            ("c", UInt64Type, DataType::UInt64, vec![1, 2, 3, 4]),
        );

        let sort = Sort::new(
            vec![PhyscialSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            )],
            input,
        );

        let result = sort.execute().unwrap();

        assert_eq!(result.len(), 1);
        assert_batch_eq(
            &result,
            vec![
                "+---+-----+---+",
                "| a | b   | c |",
                "+---+-----+---+",
                "| 4 | 4.0 | 4 |",
                "| 3 | 3.0 | 3 |",
                "| 2 | 2.0 | 2 |",
                "| 1 | 1.0 | 1 |",
                "+---+-----+---+",
            ],
        );
    }
}
