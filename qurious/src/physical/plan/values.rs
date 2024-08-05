use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::concat;
use arrow::datatypes::{Schema, SchemaRef};

use crate::error::{Error, Result};
use crate::physical::expr::PhysicalExpr;
use crate::physical::plan::PhysicalPlan;

pub struct Values {
    schema: SchemaRef,
    exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

impl Values {
    pub fn new(schema: SchemaRef, exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Self {
        Self { schema, exprs }
    }
}

impl PhysicalPlan for Values {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        if self.exprs.is_empty() {
            return Err(Error::InternalError(
                "Values plan must have at least one row".to_string(),
            ));
        }

        let input = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        let columns = self
            .exprs
            .iter()
            .map(|array| {
                let array_batch = array
                    .iter()
                    .map(|expr| expr.evaluate(&input))
                    .collect::<Result<Vec<_>>>()?;
                let array_ref = array_batch.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                concat(&array_ref).map_err(|e| Error::ArrowError(e))
            })
            .collect::<Result<_>>()?;

        RecordBatch::try_new(self.schema(), columns)
            .map(|v| vec![v])
            .map_err(|e| Error::ArrowError(e))
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use crate::{build_schema, datatypes::scalar::ScalarValue, physical, test_utils::assert_batch_eq};

    use super::*;

    #[test]
    fn test_empty_case() {
        let values = Values::new(Arc::new(Schema::empty()), vec![]);

        assert!(values.execute().is_err());
    }

    #[test]
    fn test_with_batches() {
        let schema = build_schema!(
            ("a", arrow::datatypes::DataType::Int64, true),
            ("b", arrow::datatypes::DataType::Utf8, true),
            ("c", arrow::datatypes::DataType::Boolean, true)
        );

        let exprs: Vec<Vec<Arc<dyn PhysicalExpr>>> = vec![
            vec![
                Arc::new(physical::expr::Literal::new(ScalarValue::Int64(Some(1)))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Int64(Some(2)))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Int64(Some(3)))),
            ],
            vec![
                Arc::new(physical::expr::Literal::new(ScalarValue::Utf8(Some("a".to_string())))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Utf8(None))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Utf8(Some("c".to_string())))),
            ],
            vec![
                Arc::new(physical::expr::Literal::new(ScalarValue::Boolean(Some(true)))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Boolean(Some(false)))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Boolean(Some(true)))),
            ],
        ];
        let values = Values::new(Arc::new(schema), exprs);

        let batch = values.execute().unwrap();

        assert_batch_eq(
            &batch,
            vec![
                "+---+---+-------+",
                "| a | b | c     |",
                "+---+---+-------+",
                "| 1 | a | true  |",
                "| 2 |   | false |",
                "| 3 | c | true  |",
                "+---+---+-------+",
            ],
        )
    }
}
