use arrow::datatypes::Schema;
use std::sync::Arc;

use super::{
    expr::{alias::Alias, LogicalExpr},
    plan::{EmptyRelation, LogicalPlan, Projection, TableScan},
};
use crate::{common::OwnedTableRelation, datasource::DataSource};
use crate::{common::TableRelation, error::Result};

pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        LogicalPlanBuilder { plan }
    }

    pub fn project(
        input: LogicalPlan,
        exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>,
    ) -> Result<LogicalPlan> {
        Projection::try_new(input, exprs.into_iter().map(|exp| exp.into()).collect())
            .map(LogicalPlan::Projection)
    }

    pub fn empty() -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::EmptyRelation(EmptyRelation::new(Arc::new(Schema::empty()))),
        }
    }

    pub fn scan(
        relation: impl Into<OwnedTableRelation>,
        table_source: Arc<dyn DataSource>,
        filter: Option<LogicalExpr>,
    ) -> Result<Self> {
        TableScan::try_new(relation.into(), table_source, None, filter)
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::TableScan(s)))
    }
}
