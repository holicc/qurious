use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    error::Result,
    logical::{
        expr::LogicalExpr,
        plan::{Aggregate, Filter, LogicalPlan, Projection},
    },
    planner::QueryPlanner,
};

#[derive(Debug)]
pub struct DataFrame {
    planner: Arc<dyn QueryPlanner>,
    plan: LogicalPlan,
}

impl DataFrame {
    pub fn new(plan: LogicalPlan, planner: Arc<dyn QueryPlanner>) -> Self {
        Self { plan, planner }
    }

    pub fn plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub fn collect(self) -> Result<Vec<RecordBatch>> {
        self.planner.create_physical_plan(&self.plan)?.execute()
    }
}

impl DataFrame {
    pub fn project(self, columns: Vec<LogicalExpr>) -> Result<Self> {
        Projection::try_new(self.plan, columns).map(|plan| Self {
            planner: self.planner,
            plan: LogicalPlan::Projection(plan),
        })
    }

    pub fn filter(self, predicate: LogicalExpr) -> Result<Self> {
        Ok(Self {
            planner: self.planner,
            plan: LogicalPlan::Filter(Filter::try_new(self.plan.clone(), predicate)?),
        })
    }

    pub fn aggregate(self, group_by: Vec<LogicalExpr>, aggr_expr: Vec<LogicalExpr>) -> Result<Self> {
        Aggregate::try_new(self.plan.clone(), group_by, aggr_expr).map(|a| Self {
            planner: self.planner,
            plan: LogicalPlan::Aggregate(a),
        })
    }
}
