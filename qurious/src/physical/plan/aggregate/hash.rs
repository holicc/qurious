use crate::{
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
};
use std::{
    collections::HashMap,
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, AsArray, UInt64Array},
    compute,
    datatypes::{ArrowPrimitiveType, DataType, FieldRef, Int16Type, Int8Type, SchemaRef},
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

    fn is_group_field(&self, field: &FieldRef) -> bool {
        let metadata = field.metadata();
        metadata.contains_key("IS_GROUP_FIELD")
    }

    fn take_group_values_by_field(&self, _group_values: &Vec<ArrayRef>, _field: &FieldRef) -> ArrayRef {
        todo!()
    }

    fn get_agg_expr_by_field(&self, _field: &FieldRef) -> Option<&Arc<dyn AggregateExpr>> {
        todo!()
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
            let group_by_values = self
                .group_exprs
                .iter()
                .map(|e| e.evaluate(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;

            let mut columns = vec![];
            let (group_array, group_indices) = group_indices(&group_by_values)?;

            columns.extend(group_array);

            for agg_expr in &self.aggregate_exprs {
                let mut agg_array = vec![];
                for group in &group_indices {
                    let mut acc = agg_expr.create_accumulator();
                    // every aggregate expression only evaluate one value as output
                    let input_array = agg_expr
                        .expression()
                        .evaluate(&batch)
                        .and_then(|values| compute::take(&values, &group, None).map_err(|e| Error::ArrowError(e)))?;
                    acc.accumluate(&input_array)?;
                    agg_array.push(acc.evaluate().map(|v| v.to_array(1))?);
                }
                let t = agg_array.iter().map(|f| f.as_ref()).collect::<Vec<_>>();

                columns.push(compute::concat(&t)?);
            }

            results.push(RecordBatch::try_new(self.schema(), columns)?);
        }

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

fn group_indices(values: &Vec<ArrayRef>) -> Result<(Vec<ArrayRef>, Vec<UInt64Array>)> {
    use arrow::datatypes::*;

    let mut hasher_map = HashMap::new();
    for group in values.iter() {
        match group.data_type() {
            DataType::UInt8 => hash_primitive_array::<UInt8Type>(group, &mut hasher_map),
            DataType::Int32 => hash_primitive_array::<Int32Type>(group, &mut hasher_map),
            _ => {
                return Err(Error::InternalError(format!(
                    "Unsupported data type {:?}",
                    group.data_type()
                )))
            }
        }
    }

    let mut map: HashMap<u64, Vec<u64>> = HashMap::new();
    for (index, hasher) in hasher_map {
        let hash = hasher.finish();
        if let Some(val) = map.get_mut(&hash) {
            val.push(index as u64);
        } else {
            map.insert(hash, vec![index as u64]);
        }
    }

    let mut group_indices = vec![];
    let mut agg_indices = vec![];
    for group in map.into_values() {
        group_indices.push(group[0]);
        agg_indices.push(UInt64Array::from_iter(group));
    }

    let mut group_values = vec![];
    let group_indices = UInt64Array::from_iter(group_indices);
    for v in values {
        group_values.push(compute::take(v, &group_indices, None)?);
    }

    Ok((group_values, agg_indices))
}

fn hash_primitive_array<T: ArrowPrimitiveType>(group_values: &ArrayRef, hasher_map: &mut HashMap<usize, DefaultHasher>)
where
    T::Native: Hash,
{
    for (i, v) in group_values.as_primitive::<T>().iter().enumerate() {
        if let Some(val) = v {
            let mut hasher = hasher_map.entry(i).or_insert(DefaultHasher::new());
            val.hash(&mut hasher);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int32Array},
        datatypes::DataType,
    };

    use crate::{
        build_schema,
        physical::{self, expr::MaxAggregateExpr, plan::PhysicalPlan},
        test_utils::build_table_scan_i32,
    };

    use super::{group_indices, HashAggregate};

    #[test]
    fn test_group_by() {
        let schema = build_schema!(
            ("c1", DataType::Int32),
            ("b1", DataType::Int32),
            ("MAX(a1)", DataType::Int32)
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
        }) as Arc<_>];

        let agg = HashAggregate::new(Arc::new(schema), input, group_exprs, aggregate_exprs);

        let results = agg.execute().unwrap();

        assert_eq!(results.len(), 1);
        // assert_batch_eq(
        //     &results,
        //     vec![
        //         "+------------+",
        //         "|   MAX(a1)  |",
        //         "+------------+",
        //         "| 2          |",
        //         "| 1          |",
        //         "| 3          |",
        //         "+------------+",
        //     ],
        // )
    }

    #[test]
    fn test_group_indices() {
        let group_field0: Arc<dyn Array> = Arc::new(Int32Array::from_iter(vec![7, 9, 8, 9]));
        let group_field1: Arc<dyn Array> = Arc::new(Int32Array::from_iter(vec![1, 3, 2, 3]));

        let (_, results) = group_indices(&vec![group_field0, group_field1]).unwrap();

        assert_eq!(results.len(), 3);
    }
}
