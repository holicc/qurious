use std::sync::Arc;

use crate::dataframe::DataFrame;
use crate::datasource;
use crate::error::Result;
use crate::logical::plan::{LogicalPlan, TableScan};
use crate::types::schema::Schema;

pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn memory(schame: Schema) -> Result<DataFrame> {
        let source = datasource::memory::MemoryDataSource::new(schame, vec![]);
        let plan = TableScan::new("test", Arc::new(source), None);
        Ok(DataFrame::new(LogicalPlan::TableScan(plan)))
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        logical::expr::{column, eq, literal},
        types::{datatype::DataType, field::Field},
        utils,
    };

    use super::*;

    #[test]
    pub fn test_simple_df() -> Result<()> {
        let schema = Schema {
            fields: vec![
                Field::new("id", DataType::UInt32),
                Field::new("first_name", DataType::Utf8),
                Field::new("last_name", DataType::Utf8),
                Field::new("state", DataType::Utf8),
                Field::new("salary", DataType::UInt32),
            ],
        };

        let df = ExecutionContext::memory(schema)?;

        let plan = df
            .filter(eq(column("state"), literal("CO")))?
            .project(vec![
                column("id"),
                column("first_name"),
                column("last_name"),
                column("state"),
                column("salary"),
            ])?;

        println!("Plan: {}", utils::format(&plan.plan(), 0));

        Ok(())
    }
}
