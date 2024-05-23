use arrow::datatypes::Schema;
use std::sync::Arc;

use super::{
    expr::{AggregateExpr, LogicalExpr, SortExpr},
    plan::{Aggregate, CrossJoin, EmptyRelation, Join, LogicalPlan, Projection, Sort, TableScan},
};
use crate::{common::JoinType, error::Result};
use crate::{common::OwnedTableRelation, datasource::DataSource};

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

    pub fn project(input: LogicalPlan, exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>) -> Result<LogicalPlan> {
        Projection::try_new(input, exprs.into_iter().map(|exp| exp.into()).collect()).map(LogicalPlan::Projection)
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

    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let left_fields = self.plan.schema().fields.clone();
        let right_fields = right.schema().fields.clone();

        // left then right
        let schema = Schema::new(
            left_fields
                .iter()
                .chain(right_fields.iter())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::CrossJoin(CrossJoin::new(Arc::new(self.plan), Arc::new(right), Arc::new(schema))),
        })
    }

    pub fn join_on(self, right: LogicalPlan, join_type: JoinType, on: LogicalExpr) -> Result<Self> {
        let left_fields = self.plan.schema().fields.clone();
        let right_fields = right.schema().fields.clone();

        // left then right
        let schema = Schema::new(
            left_fields
                .iter()
                .chain(right_fields.iter())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Join(Join {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                join_type,
                filter: on,
                schema: Arc::new(schema),
            }),
        })
    }

    pub fn aggregate(self, group_expr: Vec<LogicalExpr>, aggr_expr: Vec<AggregateExpr>) -> Result<Self> {
        Aggregate::try_new(self.plan, group_expr, aggr_expr)
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::Aggregate(s)))
    }

    pub fn sort(self, order_by: Vec<SortExpr>) -> Result<Self> {
        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Sort(Sort {
                exprs: order_by,
                input: Box::new(self.plan),
            }),
        })
    }

    pub(crate) fn limit(&self) -> Self {
        todo!()
    }
}
