use crate::{
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
};
use std::{collections::HashMap, fmt::Display, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::{DataType, Int16Type, Int8Type, SchemaRef},
    record_batch::RecordBatch,
};

use crate::physical::{
    expr::{AggregateExpr, PhysicalExpr},
    plan::PhysicalPlan,
};

pub struct HashAggregate {
    schema: SchemaRef,
    input: Arc<dyn PhysicalPlan>,
    group_exprs: Vec<Arc<dyn PhysicalExpr>>,
    aggregate_exprs: Vec<Arc<dyn AggregateExpr>>,
}

impl HashAggregate {
    pub fn new(
        schema: SchemaRef,
        input: Arc<dyn PhysicalPlan>,
        group_exprs: Vec<Arc<dyn PhysicalExpr>>,
        aggregate_exprs: Vec<Arc<dyn AggregateExpr>>,
    ) -> Self {
        Self {
            schema,
            input,
            group_exprs,
            aggregate_exprs,
        }
    }

    fn get_array_value(&self, ary: &ArrayRef, index: usize) -> ScalarValue {
        match ary.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => ScalarValue::Boolean(Some(ary.as_boolean().value(index))),
            DataType::Int8 => ScalarValue::Int8(Some(ary.as_primitive::<Int8Type>().value(index))),
            DataType::Int16 => ScalarValue::Int16(Some(ary.as_primitive::<Int16Type>().value(index))),
            _ => unimplemented!(),
        }
    }
}

impl PhysicalPlan for HashAggregate {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let mut map = HashMap::new();

        // for each batch from the input executor
        let batches = self.input.execute()?;
        for batch in batches {
            // evaluate the groupt expression
            let gourp_keys = self
                .group_exprs
                .iter()
                .map(|e| e.evaluate(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;
            // evalute the expressions that are inputs to the aggregate functions
            let aggr_input_values = self
                .aggregate_exprs
                .iter()
                .map(|f| f.expression().evaluate(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;
            // for each row in the batch
            for row_index in 0..batch.num_rows() {
                // create the key for the hash map
                // FIXME: need a better way to create a key
                let row_key = gourp_keys
                    .iter()
                    .map(|key| match key.data_type() {
                        DataType::Binary => key.as_boolean().value(row_index).to_string(),
                        DataType::Utf8 => key.as_string::<i64>().value(row_index).to_string(),
                        _ => unimplemented!(),
                    })
                    .fold(String::new(), |a, b| a + &b);

                // gather the inputs to call actual accumulator
                let accumulators = map.entry(row_key).or_insert_with(|| {
                    self.aggregate_exprs
                        .iter()
                        .map(|f| f.create_accumulator())
                        .collect::<Vec<_>>()
                });

                // perform accumulation
                for (i, acc) in accumulators.iter_mut().enumerate() {
                    acc.accumluate(&self.get_array_value(&aggr_input_values[i], row_index))?;
                }
            }
        }

        // create result batch containing final aggregate values
        let num_size = map.len();
        let columns = map
            .values_mut()
            .flatten()
            .map(|f| f.evaluate().map(|v| v.to_array(num_size)))
            .collect::<Result<Vec<_>>>()?;

        Ok(vec![
            RecordBatch::try_new(self.schema(), columns).map_err(|e| Error::ArrowError(e))?
        ])
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.input.clone()])
    }
}

impl Display for HashAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HashAggregateExec: groupExpr={:?}, aggrExpr={:?}",
            self.group_exprs, self.aggregate_exprs
        )
    }
}
