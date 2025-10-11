use crate::arrow_err;
use crate::common::table_schema::TableSchemaRef;
use crate::error::{Error, Result};
use crate::physical::expr::Accumulator;
use crate::physical::{
    expr::{AggregateExpr, PhysicalExpr},
    plan::PhysicalPlan,
};
use crate::utils::array::create_hashes;
use arrow::compute::{concat_batches, TakeOptions};
use arrow::row::{RowConverter, SortField};
use arrow::{
    array::{ArrayRef, UInt64Array},
    compute,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use std::{collections::HashMap, fmt::Display, hash::DefaultHasher, sync::Arc};

struct GroupAccumulator<'a> {
    /// accumulators each group has a vector of accumulators
    /// Key: group index Value: (group_values, accumulators, indices)
    accumulators: HashMap<usize, (Vec<ArrayRef>, Vec<Box<dyn Accumulator>>)>,
    /// Key: row num Value: hash
    hashes_buffer: Vec<DefaultHasher>,
    /// Key: hash Value: group index
    map: HashMap<u64, usize>,
    ///
    accumlator_factory: &'a dyn Fn() -> Result<Vec<Box<dyn Accumulator>>>,
}

impl<'a> GroupAccumulator<'a> {
    fn try_new<F>(f: &'a F) -> Result<Self>
    where
        F: Fn() -> Result<Vec<Box<dyn Accumulator>>>,
    {
        Ok(Self {
            accumulators: HashMap::new(),
            hashes_buffer: Vec::new(),
            map: HashMap::new(),
            accumlator_factory: f,
        })
    }

    fn update(&mut self, rows: usize, group_by_values: &[ArrayRef], input_values: &[ArrayRef]) -> Result<()> {
        self.hashes_buffer.clear();
        self.hashes_buffer.resize(rows, DefaultHasher::new());

        let hashes = create_hashes(&group_by_values, &mut self.hashes_buffer)?;
        let mut accs_indices = HashMap::new();
        for (row, target_hash) in hashes.into_iter().enumerate() {
            match self.map.get_mut(&target_hash) {
                Some(group_index) => {
                    accs_indices.entry(*group_index).or_insert(vec![]).push(row as u64);
                }
                None => {
                    self.map.insert(target_hash, row);

                    accs_indices.insert(row, vec![row as u64]);

                    let accs = (self.accumlator_factory)()?;
                    let indices = UInt64Array::from_iter(vec![row as u64]);
                    let group_values = group_by_values
                        .iter()
                        .map(|values| compute::take(&values, &indices, None).map_err(|e| arrow_err!(e)))
                        .collect::<Result<Vec<_>>>()?;

                    self.accumulators.insert(row, (group_values, accs));
                }
            }
        }

        for (group_indix, (_, accs)) in &mut self.accumulators {
            let acc_indices = accs_indices.get(&group_indix);

            if let Some(acc_indices) = acc_indices {
                for (values, acc) in input_values.iter().zip(accs.iter_mut()) {
                    let indices = UInt64Array::from_iter(acc_indices.clone());
                    compute::take(values, &indices, Some(TakeOptions { check_bounds: true }))
                        .map_err(|e| arrow_err!(e))
                        .and_then(|v| acc.accumluate(&v))?;
                }
            }
        }

        Ok(())
    }

    fn output(self, schema: &SchemaRef) -> Result<Vec<ArrayRef>> {
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        let mut rows = row_converter.empty_rows(64, 1024);
        for (mut group_values, accs) in self.accumulators.into_values() {
            for mut acc in accs {
                group_values.push(acc.evaluate().and_then(|v| v.to_array(1))?);
            }

            row_converter.append(&mut rows, &group_values)?;
        }

        row_converter.convert_rows(&rows).map_err(|e| arrow_err!(e))
    }
}

pub struct HashAggregate {
    schema: TableSchemaRef,
    input: Arc<dyn PhysicalPlan>,
    group_exprs: Vec<Arc<dyn PhysicalExpr>>,
    aggregate_exprs: Vec<Arc<dyn AggregateExpr>>,
}

impl HashAggregate {
    pub fn new(
        schema: TableSchemaRef,
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
}

impl PhysicalPlan for HashAggregate {
    fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let batches = self.input.execute()?;
        let accumlator_factory = || -> Result<Vec<Box<dyn Accumulator>>> {
            self.aggregate_exprs
                .iter()
                .map(|e| e.create_accumulator())
                .collect::<Result<Vec<_>>>()
        };
        if batches.is_empty() {
            return Ok(vec![]);
        }
        let schema = batches[0].schema();
        let batch = concat_batches(&schema, &batches)?;
        let mut group_accumulator = GroupAccumulator::try_new(&accumlator_factory)?;
        let group_by_values = self
            .group_exprs
            .iter()
            .map(|e| e.evaluate(&batch))
            .collect::<Result<Vec<ArrayRef>>>()?;
        // evaluate the aggregate expression
        let input_values = self
            .aggregate_exprs
            .iter()
            .map(|e| e.expression().evaluate(&batch))
            .collect::<Result<Vec<ArrayRef>>>()?;

        group_accumulator.update(batch.num_rows(), &group_by_values, &input_values)?;

        let schema = self.schema.arrow_schema();
        RecordBatch::try_new(schema.clone(), group_accumulator.output(&schema)?)
            .map(|v| vec![v])
            .map_err(|e| arrow_err!(e))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::{
        build_table_schema,
        physical::{self, expr::MaxAggregateExpr, plan::PhysicalPlan},
        test_utils::build_table_scan_i32,
    };

    use super::HashAggregate;

    #[test]
    fn test_group_by() {
        let schema = build_table_schema!(
            (("", "c1"), DataType::Int32),
            (("", "b1"), DataType::Int32),
            (("", "MAX(a1)"), DataType::Int32)
        );

        let input = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 13, 6]),
            ("b1", vec![4, 5, 6, 6]),
            ("c1", vec![7, 8, 9, 9]),
        ]);

        // group by b1,c1
        let group_exprs = vec![
            Arc::new(physical::expr::Column::new("c1", 2)) as Arc<_>,
            Arc::new(physical::expr::Column::new("b1", 1)) as Arc<_>,
        ];
        // max(a1)
        let aggregate_exprs = vec![Arc::new(MaxAggregateExpr {
            expr: Arc::new(physical::expr::Column::new("a1", 0)),
            return_type: DataType::Int32,
        }) as Arc<_>];

        let agg = HashAggregate::new(schema, input, group_exprs, aggregate_exprs);

        let results = agg.execute().unwrap();

        assert_eq!(results.len(), 1);
    }
}
