use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::dataframe::DataFrame;
use crate::datasource;
use crate::error::Result;
use crate::logical::plan::{LogicalPlan, TableScan};

pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn memory(schame: SchemaRef) -> Result<DataFrame> {
        let source = datasource::memory::MemoryDataSource::new(schame, vec![]);
        let plan = TableScan::new("test", Arc::new(source), None);
        Ok(DataFrame::new(LogicalPlan::TableScan(plan)))
    }
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field, Fields, Schema};

    use crate::{
        logical::expr::{column, eq, literal},
        utils,
    };

    use super::*;

    #[test]
    pub fn test_simple_df() -> Result<()> {
        let schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("first_name", DataType::Utf8, true),
            Field::new("last_name", DataType::Utf8, true),
            Field::new("state", DataType::Utf8, true),
            Field::new("salary", DataType::Float64, true),
        ])));

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
