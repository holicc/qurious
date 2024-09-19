use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::concat;
use arrow::datatypes::{Schema, SchemaRef};

use crate::arrow_err;
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

        let empty_batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        let n_row = self.exprs.len();
        let n_col = self.schema.fields().len();

        let columns = (0..n_col)
            .map(|j| {
                let data_type = self.schema.field(j).data_type();
                (0..n_row)
                    .map(|i| self.exprs[i][j].evaluate(&empty_batch))
                    .collect::<Result<Vec<ArrayRef>>>()
                    .and_then(|rows| {
                        let rows = rows
                            .into_iter()
                            .map(|x| {
                                if x.data_type() != data_type {
                                    arrow::compute::cast(&x, data_type).map_err(|e| arrow_err!(e))
                                } else {
                                    Ok(x)
                                }
                            })
                            .collect::<Result<Vec<_>>>()?;

                        concat(&rows.iter().map(|x| x.as_ref()).collect::<Vec<_>>()).map_err(|e| arrow_err!(e))
                    })
            })
            .collect::<Result<_>>()?;

        RecordBatch::try_new(self.schema(), columns)
            .map(|v| vec![v])
            .map_err(|e| arrow_err!(e))
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
                Arc::new(physical::expr::Literal::new(ScalarValue::Utf8(Some("a".to_string())))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Boolean(Some(true)))),
            ],
            vec![
                Arc::new(physical::expr::Literal::new(ScalarValue::Int64(Some(2)))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Utf8(None))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Boolean(Some(false)))),
            ],
            vec![
                Arc::new(physical::expr::Literal::new(ScalarValue::Int64(Some(3)))),
                Arc::new(physical::expr::Literal::new(ScalarValue::Utf8(Some("c".to_string())))),
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
