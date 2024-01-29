use crate::{
    error::Result,
    expr::{self, LogicalExpr},
    logical_plan::{self, LogicalPlan},
    types::schema::Schema,
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

    pub fn schema(&self) -> &Schema {
        self.plan.schema()
    }
}

impl DataFrame {
    pub fn project(self, columns: Vec<LogicalExpr>) -> Result<Self> {
        logical_plan::Projection::try_new(self.plan, columns).map(|plan| Self {
            plan: LogicalPlan::Projection(plan),
        })
    }

    pub fn filter(self, predicate: LogicalExpr) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Filter(logical_plan::Filter::new(self.plan.clone(), predicate)),
        })
    }

    pub fn aggregate(
        self,
        group_by: Vec<LogicalExpr>,
        aggr_expr: Vec<expr::AggregateExpr>,
    ) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Aggregate(logical_plan::Aggregate::new(
                self.plan.clone(),
                group_by,
                aggr_expr,
            )),
        })
    }
}
