use std::sync::Arc;

use crate::dataframe::DataFrame;
use crate::error::Result;
use crate::logical_plan::LogicalPlan;
use crate::{datasource, logical_plan};

pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn memory() -> Result<DataFrame> {
        let source = datasource::memory::MemoryDataSource::default();
        let plan = logical_plan::TableScan::new("test", Arc::new(source), None);
        Ok(DataFrame::new(LogicalPlan::TableScan(plan)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::expr::{self, column, eq, literal, Column};

    use super::*;

    #[test]
    pub fn test_simple_df() -> Result<()> {
        let df = ExecutionContext::memory()?;

        let plan = df
            .filter(eq(column("state"), literal("CO")))?
            .project(vec![
                column("id"),
                column("first_name"),
                column("last_name"),
                column("state"),
                column("salary"),
            ])?;

        println!("Plan: {:?}", plan);

        Ok(())
    }
}
