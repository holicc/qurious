use crate::physical::plan::PhysicalPlan;

pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

impl DropTable {
    pub fn new(name: String, if_exists: bool) -> Self {
        Self { name, if_exists }
    }
}

impl PhysicalPlan for DropTable {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        todo!()
    }

    fn execute(&self) -> crate::error::Result<Vec<arrow::array::RecordBatch>> {
        todo!()
    }

    fn children(&self) -> Option<Vec<std::sync::Arc<dyn PhysicalPlan>>> {
        todo!()
    }
}
