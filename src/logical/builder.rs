use arrow::datatypes::Schema;
use std::sync::Arc;

use super::{
    expr::{alias::Alias, LogicalExpr},
    plan::{EmptyRelation, LogicalPlan, Projection, TableScan},
};
use crate::datasource::DataSource;
use crate::error::Result;

pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        LogicalPlanBuilder { plan }
    }

    pub fn project(
        input: LogicalPlan,
        exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>,
    ) -> Result<LogicalPlan> {
        let project_exprs = exprs
            .into_iter()
            .map(|exp| exp.into())
            .map(|exp: LogicalExpr| match exp {
                LogicalExpr::Alias(Alias { expr, name }) => {
                    LogicalExpr::Alias(Alias::new(name, *expr))
                }
                LogicalExpr::Column(_) | LogicalExpr::Literal(_) => exp,
                _ => unimplemented!(),
            })
            .collect();

        Projection::try_new(input, project_exprs).map(LogicalPlan::Projection)
    }

    pub fn empty() -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::EmptyRelation(EmptyRelation::new(Arc::new(Schema::empty()))),
        }
    }

    pub fn build(self) -> LogicalPlan {
        self.plan
    }

    pub fn scan(
        table_name: &str,
        table_source: Arc<dyn DataSource>,
        filter: Option<LogicalExpr>,
    ) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::TableScan(TableScan::new(table_name, table_source, None, filter)),
        }
    }
}
