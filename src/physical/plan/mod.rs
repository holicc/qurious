mod aggregate;
mod filter;
mod join;
mod projection;
mod scan;

pub use aggregate::HashAggregate;
pub use filter::Filter;
pub use projection::Projection;
pub use scan::Scan;

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalPlan {
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>>;
}

#[cfg(test)]
mod tests {
    use super::{PhysicalPlan, Scan};
    use crate::{datasource::memory::MemoryDataSource};
    use arrow::{
        array::{Array, Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use std::sync::Arc;

    pub fn build_table_scan_i32(fields: Vec<(&str, Vec<i32>)>) -> Arc<dyn PhysicalPlan> {
        let schema = Schema::new(
            fields
                .iter()
                .map(|(name, _)| Field::new(name.to_string(), DataType::Int32, true))
                .collect::<Vec<_>>(),
        );

        let columns = fields
            .iter()
            .map(|(_, v)| Arc::new(Int32Array::from(v.clone())) as Arc<dyn Array>)
            .collect::<Vec<_>>();

        let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();

        let datasource = MemoryDataSource::new(Arc::new(schema.clone()), vec![record_batch]);

        Arc::new(Scan::new(Arc::new(schema), Arc::new(datasource), None))
    }

    pub fn build_record_i32(schema: SchemaRef, ary: Vec<Vec<i32>>) -> Vec<RecordBatch> {
        ary.into_iter()
            .map(|v| {
                let columns = v
                    .into_iter()
                    .map(|v| Arc::new(Int32Array::from(vec![v])) as Arc<dyn Array>)
                    .collect::<Vec<_>>();
                RecordBatch::try_new(schema.clone(), columns).unwrap()
            })
            .collect()
    }
}
