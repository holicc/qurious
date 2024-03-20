use super::PhysicalPlan;
use crate::error::{Error, Result};
use arrow::{array::RecordBatch, compute::concat_batches, datatypes::SchemaRef};
use std::sync::Arc;

pub struct CrossJoin {
    pub left: Arc<dyn PhysicalPlan>,
    pub right: Arc<dyn PhysicalPlan>,
    pub schema: SchemaRef,
}

impl CrossJoin {
    pub fn new(
        left: Arc<dyn PhysicalPlan>,
        right: Arc<dyn PhysicalPlan>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            left,
            right,
            schema,
        }
    }
}

impl PhysicalPlan for CrossJoin {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Execute the cross join
    /// A cross join is a cartesian product of the left and right input plans
    /// It is implemented by creating a new RecordBatch for each combination of left and right
    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let lr = self.left.execute()?;
        let rr = self.right.execute()?;

        let results = lr.iter().chain(rr.iter());

        concat_batches(&self.schema, results)
            .map(|v| vec![v])
            .map_err(|e| Error::ArrowError(e))
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.left.clone(), self.right.clone()])
    }
}

#[cfg(test)]
mod tests {
    use crate::{datasource, physical::plan::Scan};

    use super::*;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use std::{sync::Arc, vec};

    #[test]
    fn test_cross_join() {
        let sa = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let da = RecordBatch::try_new(sa.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .unwrap();
        let daa = datasource::memory::MemoryDataSource::new(sa.clone(), vec![da]);

        let ba = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let bba = RecordBatch::try_new(sa.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .unwrap();
        let baa = datasource::memory::MemoryDataSource::new(sa.clone(), vec![bba]);

        let left = Arc::new(Scan::new(sa.clone(), Arc::new(daa), None));
        let right = Arc::new(Scan::new(ba.clone(), Arc::new(baa), None));

        let join = CrossJoin::new(
            left,
            right,
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ])),
        );

        let result = join.execute().unwrap();

        println!("{:?}",result)
    }
}
