use arrow::datatypes::Schema;
use std::sync::Arc;

use super::{
    expr::{AggregateExpr, LogicalExpr, SortExpr},
    plan::{Aggregate, CrossJoin, EmptyRelation, Join, Limit, LogicalPlan, Projection, Sort, TableScan},
};
use crate::{
    common::join_type::JoinType, planner::normalize_col_with_schemas_and_ambiguity_check,
    provider::table::TableProvider,
};
use crate::{common::table_relation::TableRelation, error::Result};

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

    pub fn add_project(self, exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>) -> Result<Self> {
        Projection::try_new(self.plan, exprs.into_iter().map(|exp| exp.into()).collect())
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::Projection(s)))
    }

    pub fn empty(produce_one_row: bool) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::EmptyRelation(EmptyRelation {
                schema: Arc::new(Schema::empty()),
                produce_one_row,
            }),
        }
    }

    pub fn scan(
        relation: impl Into<TableRelation>,
        table_source: Arc<dyn TableProvider>,
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
        order_by
            .into_iter()
            .map(|sort| {
                normalize_col_with_schemas_and_ambiguity_check(*sort.expr, &[&self.plan.relation()]).map(|expr| {
                    SortExpr {
                        expr: expr.into(),
                        asc: sort.asc,
                    }
                })
            })
            .collect::<Result<_>>()
            .map(|sort_exprs| LogicalPlanBuilder {
                plan: LogicalPlan::Sort(Sort {
                    exprs: sort_exprs,
                    input: Box::new(self.plan),
                }),
            })
    }

    pub fn limit(self, fetch: Option<usize>, skip: usize) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::Limit(Limit {
                input: Box::new(self.plan),
                fetch,
                skip,
            }),
        }
    }
}
