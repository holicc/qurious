use crate::{
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
};
use std::{
    collections::HashMap,
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher, RandomState},
    sync::Arc,
};

use arrow::{
    array::{make_array, make_builder, ArrayRef, AsArray, Int32Builder, Int64Array, PrimitiveArray, UInt64Array},
    compute,
    datatypes::{ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int8Type, SchemaRef},
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

    /// we should group the same value using hash
    /// for example:
    /// select max(1),c1 from table group by c1
    /// +----+----+----+
    /// | a1 | b1 | c1 |    hash   group_indices   accumulator func
    /// +----+----+----+
    /// | 1  | 4  | 7  |    0x01        0               1  :  7
    /// | 2  | 5  | 8  |    0x10        1               2  :  8
    /// | 3  | 6  | 9  |    0x11        2               3  :  9
    /// | 0  | 0  | 9  |    0x11        2               3  :  9
    /// +----+----+----+
    /// after evaluate group_by_values: [7,8,9,9]
    /// hasing each value of group_by_values, will get: [7,8,9]
    /// according to hash array, produce a mask for aggrates input values
    /// take column value and feed to accumulator
    /// after that we create new columns by origin group indices [0,1,2]
    /// merge or distinct results ?
    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let mut results = vec![];
        // for each batch from the input executor
        for batch in self.input.execute()? {
            // evaluate the groupt expression
            let gourp_by_values = self
                .group_exprs
                .iter()
                .map(|e| e.evaluate(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;
            let group_indices = group_indices(&gourp_by_values)?;
            // evalute the expressions that are inputs to the aggregate functions
            let agg_input_values = self
                .aggregate_exprs
                .iter()
                .map(|f| f.expression().evaluate(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;
            // for each row in the batch perform accumulation
            let mut columns = vec![];
            for (i, agg_expr) in self.aggregate_exprs.iter().enumerate() {
                let mut array = vec![];
                let mut acc = agg_expr.create_accumulator();

                for indices in &group_indices {
                    compute::take(&agg_input_values[i], indices, None)
                        .map_err(|err| Error::ArrowError(err))
                        .and_then(|input_values| acc.accumluate(&input_values))?;

                    let single_array = acc.evaluate().map(|v| v.to_array(1))?;

                    if let Some(latest) = array.pop() {
                        array.push(compute::concat(&[&single_array, &latest])?);
                    } else {
                        array.push(single_array);
                    }
                }
                columns.extend(array);
            }

            results.push(RecordBatch::try_new(self.schema(), columns)?);
        }

        // create result batch containing final aggregate values
        Ok(results)
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

fn group_indices(group_values: &Vec<ArrayRef>) -> Result<Vec<UInt64Array>> {
    use arrow::datatypes::*;

    let mut hasher_map = HashMap::new();
    for values in group_values {
        match values.data_type() {
            DataType::UInt8 => hash_primitive_array::<UInt8Type>(values, &mut hasher_map),
            DataType::UInt16 => hash_primitive_array::<UInt16Type>(values, &mut hasher_map),
            DataType::UInt32 => hash_primitive_array::<UInt32Type>(values, &mut hasher_map),
            DataType::UInt64 => hash_primitive_array::<UInt64Type>(values, &mut hasher_map),
            DataType::Int8 => hash_primitive_array::<Int8Type>(values, &mut hasher_map),
            DataType::Int16 => hash_primitive_array::<Int16Type>(values, &mut hasher_map),
            DataType::Int32 => hash_primitive_array::<Int32Type>(values, &mut hasher_map),
            DataType::Int64 => hash_primitive_array::<Int64Type>(values, &mut hasher_map),
            _ => {
                return Err(Error::InternalError(format!(
                    "Unsupported data type {:?}",
                    values.data_type()
                )))
            }
        };
    }
    let mut indices_map = HashMap::new();
    for (index, hasher) in hasher_map {
        let hash = hasher.finish();
        let indices = indices_map.entry(hash).or_insert(vec![]);
        indices.push(index as u64);
    }
    Ok(indices_map
        .into_iter()
        .map(|(_, v)| UInt64Array::from_iter(v))
        .collect())
}

fn hash_primitive_array<T: ArrowPrimitiveType>(values: &ArrayRef, hasher_map: &mut HashMap<usize, DefaultHasher>)
where
    T::Native: Hash,
{
    for (index, v) in values.as_primitive::<T>().iter().enumerate() {
        let mut hasher = hasher_map.entry(index).or_insert(DefaultHasher::new());
        match v {
            Some(key) => {
                key.hash(&mut hasher);
            }
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::Int32Array, datatypes::DataType};

    use crate::{
        build_schema,
        physical::{
            self,
            expr::MaxAggregateExpr,
            plan::{tests::build_table_scan_i32, PhysicalPlan},
        },
        test_utils::assert_batch_eq,
    };

    use super::{group_indices, HashAggregate};

    #[test]
    fn test_group_by() {
        let schema = build_schema!(("MAX(a1)", DataType::Int32), ("c1", DataType::Int32),);

        let input = build_table_scan_i32(vec![
            ("a1", vec![1, 2, 3]),
            ("b1", vec![4, 5, 6]),
            ("c1", vec![7, 8, 9]),
        ]);

        // group by b1
        let group_exprs = vec![Arc::new(physical::expr::Column::new("b1", 1)) as Arc<_>];
        // max(a1)
        let aggregate_exprs = vec![
            Arc::new(MaxAggregateExpr {
                expr: Arc::new(physical::expr::Column::new("a1", 0)),
            }) as Arc<_>,
            Arc::new(MaxAggregateExpr {
                expr: Arc::new(physical::expr::Column::new("a1", 0)),
            }) as Arc<_>,
        ];

        let agg = HashAggregate::new(Arc::new(schema), input, group_exprs, aggregate_exprs);

        let results = agg.execute().unwrap();

        assert_batch_eq(
            &results,
            vec![
                "+------------+",
                "|   MAX(a1)  |",
                "+------------+",
                "| 2          |",
                "| 1          |",
                "| 3          |",
                "+------------+",
            ],
        )
    }

    #[test]
    fn test_group_indices() {
        let group_field = Arc::new(Int32Array::from_iter(vec![7, 8, 9, 9]));
        let results = group_indices(&vec![group_field]).unwrap();

        assert_eq!(results.len(), 3);
    }
}
