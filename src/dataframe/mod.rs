use arrow::datatypes::SchemaRef;

use crate::{
    error::Result,
    logical::expr::{self, LogicalExpr},
    logical::{
        plan::LogicalPlan,
        plan::{Aggregate, Filter, Projection},
    },
};

#[derive(Debug)]
pub struct DataFrame {
    plan: LogicalPlan,
}

impl DataFrame {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
}

impl DataFrame {
    pub fn project(self, columns: Vec<LogicalExpr>) -> Result<Self> {
        Projection::try_new(self.plan, columns).map(|plan| Self {
            plan: LogicalPlan::Projection(plan),
        })
    }

    pub fn filter(self, predicate: LogicalExpr) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Filter(Filter::new(self.plan.clone(), predicate)),
        })
    }

    pub fn aggregate(
        self,
        group_by: Vec<LogicalExpr>,
        aggr_expr: Vec<expr::AggregateExpr>,
    ) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Aggregate(Aggregate::new(self.plan.clone(), group_by, aggr_expr)),
        })
    }
}
