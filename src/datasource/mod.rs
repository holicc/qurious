use crate::types::{batch::RecordBatch, schema::Schema};

pub trait DataSource {
    fn schema(&self) -> &Schema;

    fn scan(&self, projection: Option<Vec<String>>) -> Vec<RecordBatch>;
}
