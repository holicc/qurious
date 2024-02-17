use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::datasource::DataSource;

use super::{
    expr::LogicalExpr,
    plan::{EmptyRelation, LogicalPlan, TableScan},
};

pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        LogicalPlanBuilder { plan }
    }

    pub fn project(self, fields: Vec<LogicalExpr>) -> Self {
        todo!("project")
    }

    pub fn empty() -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::EmptyRelation(EmptyRelation::new(Arc::new(Schema::empty()))),
        }
    }

    pub fn build(self) -> LogicalPlan {
        self.plan
    }

    pub fn scan(table_name: &str, table_source: Arc<dyn DataSource>) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::TableScan(TableScan::new(table_name, table_source, None)),
        }
    }
}
