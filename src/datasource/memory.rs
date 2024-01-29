use crate::error::Result;
use crate::{
    error::Error,
    types::{batch::RecordBatch, schema::Schema},
};

use super::DataSource;

#[derive(Debug)]
pub struct MemoryDataSource {
    schema: Schema,
    data: Vec<RecordBatch>,
}

impl Default for MemoryDataSource {
    fn default() -> Self {
        Self {
            schema: Schema { fields: vec![] },
            data: vec![],
        }
    }
}

impl DataSource for MemoryDataSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(&self, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>> {
        if let Some(projection) = projection {
            let mut r = vec![];
            for p in projection {
                let index = self
                    .schema
                    .fields
                    .iter()
                    .position(|f| f.name == p)
                    .ok_or(Error::ColumnNotFound(p))?;
                r.push(self.data[index].clone());
            }
            Ok(r)
        } else {
            Ok(self.data.clone())
        }
    }
}
